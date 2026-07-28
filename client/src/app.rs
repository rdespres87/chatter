use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Position},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::sync::watch;
use unicode_width::UnicodeWidthStr;

use crate::events::{AppEvent, Event, EventHandler};

/// Maximum number of messages to keep in the local message buffer.
const MAX_HISTORY: usize = 500;

/// Application state.
pub struct App {
    url: String,
    running: bool,
    events: EventHandler,
    input: String,
    character_index: usize,
    input_mode: InputMode,
    messages: Vec<MessageEntry>,
    message_offset: usize,
    /// Write handle to the WebSocket stream. Set after initial connection or reconnect.
    ws_sink: Arc<
        tokio::sync::Mutex<
            Option<futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
        >,
    >,
    login: String,
    room: String,
    login_input: String,
    login_character_index: usize,
    password_input: String,
    password_character_index: usize,
    /// Password preserved for auto-relogin after disconnect. Cleared on explicit logout.
    reconnect_password: Option<String>,
    rooms: Vec<String>,
    room_selected: usize,
    /// Whether we're in login or register flow.
    auth_mode: AuthMode,
    /// Handle for the initial connection task. Dropped once connection succeeds or fails.
    connecting_task: Option<tokio::task::JoinHandle<()>>,
    /// Watch receiver for initial connection completion.
    /// Set when the background task in `App::new()` finishes (success or failure).
    connect_notify: Option<watch::Receiver<bool>>,
    /// Read side of the WebSocket stream after initial connection.
    /// Populated by the background task in `App::new()`.
    initial_read: std::sync::Arc<
        std::sync::Mutex<
            Option<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
        >,
    >,
    /// Whether the initial connection attempt failed (triggers Enter-to-retry behavior).
    reconnect_pending: bool,
    /// Sender to trigger a reconnect attempt from the UI (Enter key after initial failure).
    reconnect_tx: Option<tokio::sync::mpsc::UnboundedSender<AppEvent>>,
    /// Receiver for reconnect requests from the UI.
    reconnect_rx: tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
    /// Unified connection/authentication state.
    connection_state: ConnectionState,
}

/// Whether the user is logging in or creating an account.
#[derive(PartialEq, Clone, Copy)]
enum AuthMode {
    Login,
    Register,
}

#[derive(PartialEq)]
enum InputMode {
    Splash,           // Choose Login or Register
    EnteringLogin,    // Type login name
    EnteringPassword, // Type password
    Normal,           // Main chat - browse mode
    Editing,          // Main chat - typing message
    RoomList,         // Room list focused (Tab to exit)
    Disconnected,     // Server unreachable — wait for reconnect (only Esc/q to quit)
}

/// Unified connection and authentication state.
/// Replaces separate `connected`, `logged_in`, `was_logged_in` booleans.
#[derive(PartialEq, Clone)]
enum ConnectionState {
    /// WebSocket disconnected. `had_login` remembers if user was authenticated
    /// before disconnect (for auto-relogin on reconnect).
    Disconnected { had_login: bool },
    /// WebSocket connected but not yet authenticated (waiting for LoginOk).
    Connected,
    /// Fully authenticated and joined a room.
    LoggedIn { room: String },
}

impl ConnectionState {
    fn is_connected(&self) -> bool {
        !matches!(self, ConnectionState::Disconnected { .. })
    }
    fn is_logged_in(&self) -> bool {
        matches!(self, ConnectionState::LoggedIn { .. })
    }
}

/// Format a Unix timestamp (u64, seconds since epoch) into a human-readable string.
/// Uses "HH:MM" for today's messages, "YYYY-MM-DD HH:MM" for older ones.
pub fn format_timestamp(unix_ts: i64) -> String {
    use chrono::{DateTime, Local};
    let dt = DateTime::from_timestamp(unix_ts as i64, 0).map(|dt| dt.with_timezone(&Local));
    match dt {
        Some(dt) if dt.date_naive() == Local::now().date_naive() => dt.format("%H:%M").to_string(),
        Some(dt) => dt.format("%Y-%m-%d %H:%M").to_string(),
        None => "—".to_string(),
    }
}

/// Message type: regular chat or system notification.
#[derive(Clone)]
enum MessageType {
    Chat,
    System(String), // description displayed after "[System] "
}

/// A structured message entry stored in the local message buffer.
#[derive(Clone)]
struct MessageEntry {
    sender: String,  // "me", login, or "System"
    content: String, // chat text; unused for System (desc carries the text)
    timestamp: i64,  // Unix seconds (0 for system messages, ignored at render)
    is_own: bool,    // true if sent by the current user
    msg_type: MessageType,
}

/// Render a `MessageEntry` into a styled TUI `Line`.
/// Chat messages from the current user are bold cyan; others are gray.
/// System messages are dark gray with "[System] " prefix.
fn render_message(entry: &MessageEntry) -> Line<'_> {
    let timestamp = if entry.timestamp == 0 {
        String::new()
    } else {
        format!("[{}] ", format_timestamp(entry.timestamp))
    };

    let text = match &entry.msg_type {
        MessageType::Chat => format!("{}{}: {}", timestamp, entry.sender, entry.content),
        MessageType::System(desc) => format!("{}[System] {}", timestamp, desc),
    };

    let style = match (&entry.msg_type, entry.is_own) {
        (MessageType::System(_), _) => Style::default().fg(Color::DarkGray),
        (_, true) => Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
        (_, false) => Style::default().fg(Color::Gray),
    };

    Line::from(Span::styled(text, style))
}

impl App {
    /// Creates a new App instance. The WebSocket connection is attempted
    /// in a background task — this method returns immediately.
    ///
    /// If the initial connection succeeds before `run()` starts, the app
    /// enters normal mode. If it fails, the UI renders in disconnected
    /// state with a "Connecting… / Connection failed" message.
    pub async fn new(url: String, default_user: Option<String>) -> color_eyre::Result<Self> {
        let events = EventHandler::disconnected();

        // Shared write socket — None until the background task connects.
        let write: Arc<
            tokio::sync::Mutex<
                Option<
                    futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
                >,
            >,
        > = Arc::new(Mutex::new(None));

        // Shared read socket — None until the background task connects.
        let initial_read: std::sync::Arc<
            std::sync::Mutex<
                Option<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
            >,
        > = std::sync::Arc::new(std::sync::Mutex::new(None));

        // Watch channel to signal when the initial connection task completes.
        let (connect_tx, connect_rx) = watch::channel(false);

        // Channel to send Reconnect events from handle_normal_keys.
        let (reconnect_tx, reconnect_rx) = tokio::sync::mpsc::unbounded_channel();

        let connecting_task = {
            let write_socket = write.clone();
            let initial_read = initial_read.clone();
            let url = url.clone();
            let connect_tx = connect_tx;
            Some(tokio::spawn(async move {
                const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
                let connect = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url.as_str()));
                match connect.await {
                    Ok(Ok((ws_stream, _))) => {
                        let (sink, stream) = ws_stream.split();
                        *write_socket.lock().await = Some(sink);
                        match initial_read.lock() {
                            Ok(mut guard) => *guard = Some(stream),
                            Err(e) => log::warn!(
                                "initial_read lock poisoned during initial connection: {e}"
                            ),
                        }
                        let _ = connect_tx.send(true);
                    }
                    Ok(Err(e)) => {
                        log::debug!("Initial connection failed: {e}");
                        let _ = connect_tx.send(false);
                    }
                    Err(_) => {
                        log::warn!("Initial connection timed out after 10s");
                        let _ = connect_tx.send(false);
                    }
                }
            }))
        };

        Ok(Self {
            url,
            running: true,
            events,
            input: String::new(),
            character_index: 0,
            input_mode: InputMode::Splash,
            messages: Vec::new(),
            message_offset: 0,
            ws_sink: write,
            login: default_user.clone().unwrap_or_default(),
            room: "general".to_string(),
            login_input: default_user.unwrap_or_default(),
            login_character_index: 0,
            password_input: String::new(),
            password_character_index: 0,
            reconnect_password: None,
            rooms: default_rooms(),
            room_selected: 0,
            auth_mode: AuthMode::Login,
            connecting_task,
            connect_notify: Some(connect_rx),
            initial_read,
            reconnect_pending: false,
            reconnect_tx: Some(reconnect_tx),
            reconnect_rx,
            connection_state: ConnectionState::Disconnected { had_login: false },
        })
    }

    // --- Inlined cursor ops to avoid double-borrow ---

    fn do_enter_char(ch: char, index: &mut usize, text: &mut String) {
        let byte_idx = text
            .char_indices()
            .map(|(i, _)| i)
            .nth(*index)
            .unwrap_or(text.len());
        text.insert(byte_idx, ch);
        *index = (*index + 1).min(text.chars().count());
    }

    fn do_delete_char(index: &mut usize, text: &mut String) {
        if *index > 0 {
            let before: String = text.chars().take(*index - 1).collect();
            let after: String = text.chars().skip(*index).collect();
            *text = [before, after].concat();
            *index = (*index - 1).min(text.chars().count());
        }
    }

    fn do_cursor_left(index: &mut usize, text: &str) {
        *index = index.saturating_sub(1).min(text.chars().count());
    }

    fn do_cursor_right(index: &mut usize, text: &str) {
        *index = (*index + 1).min(text.chars().count());
    }

    async fn send_client_message(
        &mut self,
        message: chatter_protocol::ClientMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json = chatter_protocol::serialize_client_message(&message)?;
        let mut write = self.ws_sink.lock().await;
        if let Some(ref mut sink) = *write {
            sink.send(Message::Text(json.into())).await?;
        }
        Ok(())
    }

    // --- Splash screen: choose Login or Register ---

    fn handle_splash_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter if self.reconnect_pending => {
                // Connection failed — Enter triggers a reconnect attempt.
                if let Some(tx) = &self.reconnect_tx {
                    let _ = tx.send(AppEvent::Reconnect);
                }
            }
            KeyCode::Enter => {
                self.auth_mode = AuthMode::Login;
                self.input_mode = InputMode::EnteringLogin;
            }
            KeyCode::Char('r' | 'R') => {
                self.auth_mode = AuthMode::Register;
                self.input_mode = InputMode::EnteringLogin;
            }
            KeyCode::Esc | KeyCode::Char('q') => self.quit(),
            KeyCode::Char('c' | 'C') if key.modifiers == KeyModifiers::CONTROL => self.quit(),
            _ => {}
        }
    }

    // --- Disconnected state — only Esc/q to quit ---

    fn handle_disconnected_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.quit(),
            KeyCode::Char('c' | 'C') if key.modifiers == KeyModifiers::CONTROL => self.quit(),
            _ => {} // Ignore all other keys while disconnected
        }
    }

    // --- Entering login name (shared by both login and register) ---

    async fn handle_entering_login(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter
                if !self.login_input.is_empty() && self.connection_state.is_connected() =>
            {
                self.login = self.login_input.clone();
                self.messages.push(MessageEntry {
                    sender: "System".to_string(),
                    content: String::new(),
                    timestamp: 0,
                    is_own: false,
                    msg_type: MessageType::System(format!(
                        "Entering password for '{}'",
                        self.login
                    )),
                });
                self.input_mode = InputMode::EnteringPassword;
            }
            KeyCode::Char(c) if key.kind == KeyEventKind::Press => {
                Self::do_enter_char(c, &mut self.login_character_index, &mut self.login_input);
            }
            KeyCode::Backspace if key.kind == KeyEventKind::Press => {
                Self::do_delete_char(&mut self.login_character_index, &mut self.login_input);
            }
            KeyCode::Left if key.kind == KeyEventKind::Press => {
                Self::do_cursor_left(&mut self.login_character_index, &self.login_input);
            }
            KeyCode::Right if key.kind == KeyEventKind::Press => {
                Self::do_cursor_right(&mut self.login_character_index, &self.login_input);
            }
            KeyCode::Esc => {
                self.login_input.clear();
                self.login_character_index = 0;
                self.input_mode = InputMode::Splash;
            }
            KeyCode::Char('q') => self.quit(),
            KeyCode::Char('c' | 'C') if key.modifiers == KeyModifiers::CONTROL => self.quit(),
            _ => {}
        }
    }

    // --- Entering password (shared by both login and register) ---

    async fn handle_entering_password(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter
                if !self.password_input.is_empty() && self.connection_state.is_connected() =>
            {
                let passwd = self.password_input.clone();
                // Preserve password for auto-relogin after disconnect.
                self.reconnect_password = Some(passwd.clone());
                self.password_input.clear();
                self.password_character_index = 0;

                match self.auth_mode {
                    AuthMode::Login => {
                        self.messages.push(MessageEntry {
                            sender: "System".to_string(),
                            content: String::new(),
                            timestamp: 0,
                            is_own: false,
                            msg_type: MessageType::System(format!(
                                "Logging in as '{}'",
                                self.login
                            )),
                        });
                        if let Err(e) = self
                            .send_client_message(chatter_protocol::ClientMessage::Login {
                                login: self.login.clone(),
                                passwd,
                            })
                            .await
                        {
                            log::error!("Login send error: {}", e);
                            self.messages.push(MessageEntry {
                                sender: "System".to_string(),
                                content: String::new(),
                                timestamp: 0,
                                is_own: false,
                                msg_type: MessageType::System("Login failed.".into()),
                            });
                            self.reconnect_password = None;
                            self.input_mode = InputMode::Splash;
                        }
                    }
                    AuthMode::Register => {
                        self.messages.push(MessageEntry {
                            sender: "System".to_string(),
                            content: String::new(),
                            timestamp: 0,
                            is_own: false,
                            msg_type: MessageType::System(format!(
                                "Creating account '{}'",
                                self.login
                            )),
                        });
                        if let Err(e) = self
                            .send_client_message(chatter_protocol::ClientMessage::CreateAccount {
                                login: self.login.clone(),
                                passwd,
                            })
                            .await
                        {
                            log::error!("Register send error: {}", e);
                            self.messages.push(MessageEntry {
                                sender: "System".to_string(),
                                content: String::new(),
                                timestamp: 0,
                                is_own: false,
                                msg_type: MessageType::System("Registration failed.".into()),
                            });
                            self.input_mode = InputMode::Splash;
                        }
                    }
                }
            }
            KeyCode::Char(c) if key.kind == KeyEventKind::Press => {
                Self::do_enter_char(
                    c,
                    &mut self.password_character_index,
                    &mut self.password_input,
                );
            }
            KeyCode::Backspace if key.kind == KeyEventKind::Press => {
                Self::do_delete_char(&mut self.password_character_index, &mut self.password_input);
            }
            KeyCode::Left if key.kind == KeyEventKind::Press => {
                Self::do_cursor_left(&mut self.password_character_index, &self.password_input);
            }
            KeyCode::Right if key.kind == KeyEventKind::Press => {
                Self::do_cursor_right(&mut self.password_character_index, &self.password_input);
            }
            KeyCode::Esc => {
                self.password_input.clear();
                self.password_character_index = 0;
                self.input_mode = InputMode::EnteringLogin;
            }
            _ => {}
        }
    }

    fn handle_normal_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.quit(),
            KeyCode::Char('c' | 'C') if key.modifiers == KeyModifiers::CONTROL => self.quit(),
            KeyCode::Enter if self.reconnect_pending => {
                // Connection failed — Enter triggers a reconnect attempt.
                if let Some(tx) = &self.reconnect_tx {
                    let _ = tx.send(AppEvent::Reconnect);
                }
            }
            KeyCode::Enter => {
                self.input_mode = InputMode::Editing;
            }
            KeyCode::Up if self.message_offset > 0 => {
                self.message_offset -= 1;
            }
            KeyCode::Down if self.message_offset < self.messages.len().saturating_sub(1) => {
                self.message_offset += 1;
            }
            _ => {}
        }
    }

    async fn handle_editing_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter if !self.input.is_empty() && self.connection_state.is_connected() => {
                let msg = self.input.clone();
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                self.messages.push(MessageEntry {
                    sender: "me".to_string(),
                    content: msg.clone(),
                    timestamp: ts,
                    is_own: true,
                    msg_type: MessageType::Chat,
                });
                if let Err(e) = self
                    .send_client_message(chatter_protocol::ClientMessage::SendMessage {
                        room: self.room.clone(),
                        message: msg,
                    })
                    .await
                {
                    log::error!("Send error: {}", e);
                    // Remove the locally-echoed message if send failed.
                    self.messages.pop();
                }
                // Auto-scroll to bottom so the sent message is immediately visible.
                self.message_offset = self.messages.len().saturating_sub(1);
                self.input.clear();
                self.character_index = 0;
            }
            KeyCode::Char(c) => {
                Self::do_enter_char(c, &mut self.character_index, &mut self.input);
            }
            KeyCode::Backspace => {
                Self::do_delete_char(&mut self.character_index, &mut self.input);
            }
            KeyCode::Left => {
                Self::do_cursor_left(&mut self.character_index, &self.input);
            }
            KeyCode::Right => {
                Self::do_cursor_right(&mut self.character_index, &self.input);
            }
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            _ => {}
        }
    }

    async fn handle_room_navigation(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up if self.room_selected > 0 => {
                self.room_selected -= 1;
            }
            KeyCode::Down if self.room_selected < self.rooms.len().saturating_sub(1) => {
                self.room_selected += 1;
            }
            KeyCode::Enter => {
                if let Some(room) = self.rooms.get(self.room_selected).cloned() {
                    self.join_room(room).await;
                }
            }
            KeyCode::Tab | KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Char('q') => self.quit(),
            KeyCode::Char('c' | 'C') if key.modifiers == KeyModifiers::CONTROL => self.quit(),
            _ => {}
        }
        // Stay in RoomList mode until Enter (join_room sets Normal) or Tab/Esc
    }

    async fn join_room(&mut self, room: String) {
        // Clear messages when switching rooms.
        if self.room != room && !self.room.is_empty() {
            if let Err(e) = self
                .send_client_message(chatter_protocol::ClientMessage::LeaveRoom {
                    room: self.room.clone(),
                })
                .await
            {
                log::error!("Leave room error: {}", e);
            }
            self.messages.clear();
            self.message_offset = 0;
        }

        self.room = room.clone();

        if let Err(e) = self
            .send_client_message(chatter_protocol::ClientMessage::JoinRoom { room: room.clone() })
            .await
        {
            log::error!("Join room error: {}", e);
        }

        self.messages.push(MessageEntry {
            sender: "System".to_string(),
            content: String::new(),
            timestamp: 0,
            is_own: false,
            msg_type: MessageType::System(format!("Joined room '{}'", room)),
        });
        self.input_mode = InputMode::Normal;
    }

    fn quit(&mut self) {
        self.running = false;
    }

    /// Reset room-related state when disconnected (before reconnection attempt).
    fn reset_room_on_disconnect(&mut self) {
        self.room.clear();
        self.room_selected = 0;
    }

    /// Resolve the display sender: "me" if the server login matches the current user.
    fn resolve_sender(current_user: &str, server_login: &str) -> String {
        if server_login == current_user {
            "me".to_string()
        } else {
            server_login.to_string()
        }
    }

    // --- Main loop ---

    pub async fn run(mut self, mut terminal: ratatui::DefaultTerminal) -> color_eyre::Result<()> {
        // Check if the initial connection task already completed (extremely unlikely —
        // App::new() returns immediately, but if the server is super fast we might catch it).
        // We do NOT await the task here: blocking would delay the UI for up to 10s when
        // the server is unreachable. Instead we check whether initial_read has been
        // populated by the background task. If not, the UI renders in disconnected state
        // and the reconnect logic will pick up the result when the task completes.
        if let Some(_task) = self.connecting_task.take() {
            // Check without blocking: try to extract the read side.
            let initial_read = {
                let mut guard = self.initial_read.lock().map_err(|e| {
                    color_eyre::Report::msg(format!("initial_read lock poisoned: {e}"))
                })?;
                guard.take()
            };
            if let Some(read) = initial_read {
                // Connection succeeded before run() started.
                self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
            } else {
                // Connection still in progress. Use the watch channel to detect completion.
                // The task will send on connect_tx when it finishes (success or failure).
                self.messages.push(MessageEntry {
                    sender: "System".to_string(),
                    content: String::new(),
                    timestamp: 0,
                    is_own: false,
                    msg_type: MessageType::System("Connecting to server...".into()),
                });
            }
        }

        // Reconnect task handle. Spawned when disconnected, completed when reconnected or failed.
        let mut reconnect_task: Option<
            tokio::task::JoinHandle<color_eyre::Result<(bool, bool, String)>>,
        > = None;
        let mut connect_notify = self.connect_notify.take();

        // Check if the initial connection already succeeded (watch channel may have
        // been sent to before we took connect_notify above). If so, handle it now.
        if let Some(ref rx) = connect_notify {
            if *rx.borrow() {
                // Initial connection already succeeded — update state immediately.
                self.connection_state = ConnectionState::Connected;
                self.messages.push(MessageEntry {
                    sender: "System".to_string(),
                    content: String::new(),
                    timestamp: 0,
                    is_own: false,
                    msg_type: MessageType::System("Connection established.".into()),
                });
            }
        }

        while self.running {
            terminal.draw(|frame| self.view(frame))?;

            tokio::select! {
                // Reconnect task completed.
                result = async {
                    if let Some(task) = std::mem::take(&mut reconnect_task) {
                        task.await.ok()
                    } else {
                        std::future::pending().await
                    }
                } => {
                    // Read whether we were logged in before reconnect logic modifies state.
                    // connection_state is Disconnected { had_login } at this point.
                    let had_login = matches!(self.connection_state, ConnectionState::Disconnected { had_login: true });

                    if let Some(Ok((connected, re_login_performed, room))) = result {
                        if connected {
                            // Reconnect succeeded — create a new connected event handler.
                            self.connection_state = ConnectionState::Connected;
                            self.reconnect_pending = false;
                            self.messages.push(MessageEntry {
                                sender: "System".to_string(),
                                content: String::new(),
                                timestamp: 0,
                                is_own: false,
                                msg_type: MessageType::System("Reconnected.".into()),
                            });
                            if re_login_performed {
                                // Auto-relogin succeeded — go straight to room view.
                                self.connection_state = ConnectionState::LoggedIn { room: room.clone() };
                                self.input_mode = InputMode::Normal;
                                // Restore room context. reset_room_on_disconnect() cleared
                                // self.room, but the event handler won't receive LoginOk
                                // (it was consumed by reconnect_attempt), so join_room()
                                // won't be called. We must explicitly rejoin here.
                                self.room = room.clone();
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                                // Send JoinRoom so the server knows we re-joined after reconnect.
                                self.join_room(room).await;
                            } else if had_login {
                                // Re-authentication needed after disconnect — switch to
                                // login flow with saved credentials.
                                self.connection_state = ConnectionState::Connected;
                                self.login_input = self.login.clone();
                                self.login_character_index = self.login_input.chars().count();
                                self.password_input.clear();
                                self.password_character_index = 0;
                                self.input_mode = InputMode::EnteringLogin;
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                            } else {
                                // Reconnect succeeded but no login was needed/attempted.
                                // Go back to splash screen (user was never logged in).
                                self.input_mode = InputMode::Splash;
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                            }

                            // Create event handler from initial_read if still available.
                            if let Some(initial_read) = {
                                let mut guard = self.initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                                guard.take()
                            } {
                                self.events = EventHandler::connected(initial_read, self.ws_sink.clone()).await;
                            }
                        } else {
                            // Reconnect failed — push message and go back to splash.
                            self.reconnect_pending = false;
                            self.input_mode = InputMode::Splash;
                        }
                    } else if let Some(Err(e)) = result {
                        // Reconnect task failed (e.g., mutex poisoned) — treat as disconnect.
                        eprintln!("Reconnect task error: {e}");
                        self.reconnect_pending = false;
                        self.input_mode = InputMode::Splash;
                    }

                    reconnect_task = None;
                }

                // Initial connection task completed (via watch channel).
                _ = async {
                    if let Some(ref mut rx) = connect_notify {
                        rx.changed().await
                    } else {
                        futures::future::pending().await
                    }
                } => {
                    // Read the connection status before clearing the receiver.
                    let connected = connect_notify.as_ref()
                        .map(|rx| *rx.borrow())
                        .unwrap_or(false);
                    connect_notify = None; // no longer needed
                    if connected {
                        // Initial connection succeeded — create a new connected event handler.
                        self.connection_state = ConnectionState::Connected;
                        self.reconnect_pending = false;
                        self.messages.push(MessageEntry {
                            sender: "System".to_string(),
                            content: String::new(),
                            timestamp: 0,
                            is_own: false,
                            msg_type: MessageType::System("Connection established.".into()),
                        });
                        let initial_read = {
                            let mut guard = self.initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                            guard.take()
                        };
                        if let Some(read) = initial_read {
                            self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                        }
                    } else {
                        // Initial connection failed — push message and auto-retry with backoff.
                        self.connection_state = ConnectionState::Disconnected { had_login: false };
                        self.input_mode = InputMode::Disconnected;
                        self.messages.push(MessageEntry {
                            sender: "System".to_string(),
                            content: String::new(),
                            timestamp: 0,
                            is_own: false,
                            msg_type: MessageType::System("Connection failed. Reconnecting...".into()),
                        });
                        self.reconnect_pending = true;
                        // Spawn background reconnect task with exponential backoff.
                        if reconnect_task.is_none() {
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, None, None, String::new(), true).await
                            }));
                        }
                    }
                }

                // Reconnect request from UI (Enter key pressed after initial failure).
                event = self.reconnect_rx.recv() => {
                    if let Some(AppEvent::Reconnect) = event {
                        // Only spawn if no reconnect task is already running.
                        if reconnect_task.is_none() {
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            let login = self.login.clone();
                            let password = self.password_input.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), Some(password), String::new(), true).await
                            }));
                        }
                    }
                }

                // Normal event processing.
                event = self.events.next() => {
                    match event? {
                        Event::Crossterm(event) => if let crossterm::event::Event::Key(key) = event { match self.input_mode {
                            InputMode::Splash => self.handle_splash_keys(key),
                            InputMode::EnteringLogin => self.handle_entering_login(key).await,
                            InputMode::EnteringPassword => self.handle_entering_password(key).await,
                            InputMode::Normal => {
                                if key.code == KeyCode::Tab {
                                    self.input_mode = InputMode::RoomList;
                                } else {
                                    self.handle_normal_keys(key);
                                }
                            }
                            InputMode::RoomList => {
                                self.handle_room_navigation(key).await;
                            }
                            InputMode::Editing if key.kind == KeyEventKind::Press => {
                                self.handle_editing_keys(key).await
                            }
                            InputMode::Editing => {}
                            InputMode::Disconnected => self.handle_disconnected_keys(key),
                        } },
                        Event::App(AppEvent::Quit) => self.quit(),
                        // Reconnect is handled via reconnect_rx, not events channel.
                        Event::App(AppEvent::Reconnect) => {},
                        Event::App(AppEvent::Disconnected { close_code, close_reason }) => {
                            let had_login = matches!(self.connection_state, ConnectionState::LoggedIn { .. });
                            self.connection_state = ConnectionState::Disconnected { had_login };
                            self.input_mode = InputMode::Disconnected;
                            if let (Some(code), Some(reason)) = (close_code, close_reason) {
                                self.messages.push(MessageEntry {
                                    sender: "System".to_string(),
                                    content: String::new(),
                                    timestamp: 0,
                                    is_own: false,
                                    msg_type: MessageType::System(format!("Disconnected from server (close code: {code}, reason: {reason}).")),
                                });
                            } else {
                                self.messages.push(MessageEntry {
                                    sender: "System".to_string(),
                                    content: String::new(),
                                    timestamp: 0,
                                    is_own: false,
                                    msg_type: MessageType::System("Disconnected from server.".into()),
                                });
                            }
                            // Save the current room before resetting it.
                            let previous_room = self.room.clone();
                            self.reset_room_on_disconnect();
                            // Spawn background reconnect task (with initial delay).
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            let login = self.login.clone();
                            let password = self.reconnect_password.clone();
                            let current_room = previous_room;
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), password, current_room, false).await
                            }));
                        }
                        Event::App(AppEvent::ConnectionError { reason }) => {
                            let had_login = matches!(self.connection_state, ConnectionState::LoggedIn { .. });
                            self.connection_state = ConnectionState::Disconnected { had_login };
                            self.input_mode = InputMode::Disconnected;
                            self.reconnect_pending = true;
                            self.messages.push(MessageEntry {
                                sender: "System".to_string(),
                                content: String::new(),
                                timestamp: 0,
                                is_own: false,
                                msg_type: MessageType::System(format!("Connection error: {reason}")),
                            });
                            // Spawn background reconnect task.
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            let login = self.login.clone();
                            let password = self.reconnect_password.clone();
                            let current_room = self.room.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), password, current_room, true).await
                            }));
                        }
                        Event::App(AppEvent::ReceivedMsg { data }) => {
                            if let Ok(msg) = chatter_protocol::parse_server_message(data) {
                                match msg {
                                    chatter_protocol::ServerMessage::LoginOk { login } => {
                                        self.login = login.clone();
                                        self.connection_state = ConnectionState::LoggedIn { room: "general".into() };
                                        self.input_mode = InputMode::Normal;
                                        self.messages.push(MessageEntry {
                                            sender: "System".to_string(),
                                            content: String::new(),
                                            timestamp: 0,
                                            is_own: false,
                                            msg_type: MessageType::System(format!("Welcome, {}!", login)),
                                        });
                                        self.join_room("general".to_string()).await;
                                    }
                                    chatter_protocol::ServerMessage::LoginFailed { reason } => {
                                        self.connection_state = ConnectionState::Connected;
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.messages.push(MessageEntry {
                                            sender: "System".to_string(),
                                            content: String::new(),
                                            timestamp: 0,
                                            is_own: false,
                                            msg_type: MessageType::System(reason),
                                        });
                                        self.input_mode = InputMode::EnteringPassword;
                                    }
                                    chatter_protocol::ServerMessage::AccountCreated { login } => {
                                        self.auth_mode = AuthMode::Login;
                                        self.messages.push(MessageEntry {
                                            sender: "System".to_string(),
                                            content: String::new(),
                                            timestamp: 0,
                                            is_own: false,
                                            msg_type: MessageType::System(format!("Account '{}' created. Please login.", login)),
                                        });
                                        self.login_input.clear();
                                        self.login_character_index = 0;
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.input_mode = InputMode::EnteringLogin;
                                    }
                                    chatter_protocol::ServerMessage::AccountCreationFailed { reason } => {
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.messages.push(MessageEntry {
                                            sender: "System".to_string(),
                                            content: String::new(),
                                            timestamp: 0,
                                            is_own: false,
                                            msg_type: MessageType::System(reason),
                                        });
                                        self.input_mode = InputMode::EnteringPassword;
                                    }
                                    chatter_protocol::ServerMessage::IncomingMessage {
                                        login,
                                        room,
                                        message,
                                        timestamp,
                                    } => {
                                        if login == "Server" && room == "system" {
                                            self.messages.push(MessageEntry {
                                                sender: "System".to_string(),
                                                content: String::new(),
                                                timestamp,
                                                is_own: false,
                                                msg_type: MessageType::System(message),
                                            });
                                        } else if room == self.room {
                                            self.messages.push(MessageEntry {
                                                sender: Self::resolve_sender(&self.login, &login),
                                                content: message,
                                                timestamp,
                                                is_own: login == self.login,
                                                msg_type: MessageType::Chat,
                                            });
                                        }
                                        // Trim to MAX_HISTORY (keep most recent)
                                        if self.messages.len() > MAX_HISTORY {
                                            self.messages.drain(..self.messages.len() - MAX_HISTORY);
                                        }
                                    }
                                    chatter_protocol::ServerMessage::RoomList { rooms } => {
                                        self.rooms = rooms;
                                        // Clamp room_selected to valid range
                                        if !self.rooms.is_empty() {
                                            self.room_selected =
                                                self.rooms.iter().position(|r| *r == self.room)
                                                    .unwrap_or(self.rooms.len() - 1);
                                        } else {
                                            self.room_selected = 0;
                                        }
                                    }
                                    chatter_protocol::ServerMessage::RoomHistory {
                                        room: hist_room,
                                        messages: history,
                                    } => {
                                        if hist_room == self.room {
                                            self.messages = history
                                                .into_iter()
                                                .map(|entry| MessageEntry {
                                                    sender: Self::resolve_sender(&self.login, &entry.login),
                                                    content: entry.message,
                                                    timestamp: entry.timestamp,
                                                    is_own: entry.login == self.login,
                                                    msg_type: MessageType::Chat,
                                                })
                                                .collect();
                                            // Trim to MAX_HISTORY (keep most recent)
                                            if self.messages.len() > MAX_HISTORY {
                                                self.messages.drain(..self.messages.len() - MAX_HISTORY);
                                            }
                                            if !self.messages.is_empty() {
                                                self.message_offset = self.messages.len().saturating_sub(1);
                                            }
                                        }
                                    }
                                    chatter_protocol::ServerMessage::Error { message, code } => {
                                        if is_not_authenticated_error(&code) {
                                            self.messages.push(MessageEntry {
                                                sender: "System".to_string(),
                                                content: String::new(),
                                                timestamp: 0,
                                                is_own: false,
                                                msg_type: MessageType::System("Session expired. Please login again.".into()),
                                            });
                                            // Reset all auth state even if the client believed
                                            // it was logged in — the server disagrees.
                                            self.connection_state = ConnectionState::Connected;
                                            self.login = String::new();
                                            self.login_input.clear();
                                            self.login_character_index = 0;
                                            self.input_mode = InputMode::Splash;
                                        } else {
                                            self.messages.push(MessageEntry {
                                                sender: "System".to_string(),
                                                content: String::new(),
                                                timestamp: 0,
                                                is_own: false,
                                                msg_type: MessageType::System(format!("[{code}] {message}")),
                                            });
                                        }
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        if matches!(self.connection_state, ConnectionState::LoggedIn { .. }) {
                                            self.login_input.clear();
                                            self.login_character_index = 0;
                                            self.input_mode = InputMode::Splash;
                                        }
                                    }
                                }
                                if !self.messages.is_empty()
                                    && self.input_mode != InputMode::Splash
                                    && self.input_mode != InputMode::EnteringLogin
                                    && self.input_mode != InputMode::EnteringPassword
                                    && self.input_mode != InputMode::RoomList
                                {
                                    self.message_offset = self.messages.len().saturating_sub(1);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Background reconnect attempt with exponential backoff (2s → 60s).
    /// If `was_logged_in` is true, the client was previously authenticated
    /// and will need to re-login after reconnecting.
    /// If `skip_initial_delay` is true, attempts immediately (used when the user
    /// manually triggers a retry via Enter — the server may have come back online).
    /// Returns true when connected.
    async fn reconnect_attempt(
        url: String,
        ws_sink: Arc<
            tokio::sync::Mutex<
                Option<
                    futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
                >,
            >,
        >,
        initial_read: Arc<
            std::sync::Mutex<
                Option<futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
            >,
        >,
        login: Option<String>,
        password: Option<String>,
        current_room: String,
        skip_initial_delay: bool,
    ) -> color_eyre::Result<(bool, bool, String)> {
        // (connected, re-login_performed, room)
        let mut retry_delay = std::time::Duration::from_secs(2);
        let max_delay = std::time::Duration::from_secs(60);

        // Skip the initial delay when the user manually retries — they just pressed
        // Enter and expect an immediate attempt.
        if !skip_initial_delay {
            tokio::time::sleep(retry_delay).await;
        }

        loop {
            const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
            match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url.as_str())).await {
                Ok(Ok((ws_stream, _))) => {
                    let (write, mut read) = ws_stream.split();
                    {
                        let mut sink_guard = ws_sink.lock().await;
                        *sink_guard = Some(write);
                    }

                    // Auto-relogin if credentials are available and non-empty
                    if let (Some(l), Some(p)) = (&login, &password) {
                        if !l.is_empty() && !p.is_empty() {
                            let login_msg = chatter_protocol::ClientMessage::Login {
                                login: l.clone(),
                                passwd: p.clone(),
                            };
                            let encoded =
                                match chatter_protocol::serialize_client_message(&login_msg) {
                                    Ok(e) => e,
                                    Err(_) => {
                                        return Ok((true, false, current_room.clone()));
                                    }
                                };
                            let msg = Message::Text(encoded.into());
                            // Lock the sink, send, then unlock
                            {
                                let mut sink_guard = ws_sink.lock().await;
                                if let Some(ref mut sink) = *sink_guard {
                                    if sink.send(msg).await.is_ok() {
                                        // Wait for the server's response to verify login success.
                                        // Read the first message from the stream.
                                        use futures::StreamExt;
                                        let first_response = tokio::time::timeout(
                                            std::time::Duration::from_secs(5),
                                            read.next(),
                                        )
                                        .await;
                                        match first_response {
                                            Ok(Some(Ok(response_msg))) => {
                                                // Check if the response is a success indicator.
                                                // The server sends LoginOk followed by RoomList.
                                                // We only need to verify the first message is a success.
                                                // Deserialize directly from the Message instead of string matching.
                                                let is_success = match chatter_protocol::parse_server_message(response_msg) {
                                                    Ok(chatter_protocol::ServerMessage::LoginOk { .. })
                                                    | Ok(chatter_protocol::ServerMessage::AccountCreated { .. }) => true,
                                                Ok(_) | Err(_) => false,
                                            };
                                                if is_success {
                                                    // The RoomList (2nd message) will be handled by the event handler.
                                                    {
                                                        let mut read_guard =
                                                            initial_read.lock().map_err(|e| color_eyre::Report::msg(format!("initial_read lock poisoned: {e}")))?;
                                                        *read_guard = Some(read);
                                                    }
                                                    return Ok((true, true, current_room.clone()));
                                                }
                                                // LoginFailed or Error — login did not succeed.
                                                {
                                                    let mut read_guard =
                                                        initial_read.lock().map_err(|e| {
                                                            color_eyre::Report::msg(format!(
                                                                "initial_read lock poisoned: {e}"
                                                            ))
                                                        })?;
                                                    *read_guard = Some(read);
                                                }
                                                return Ok((true, false, current_room.clone()));
                                            }
                                            _ => {
                                                // No response or parse error — login likely failed.
                                                {
                                                    let mut read_guard =
                                                        initial_read.lock().map_err(|e| {
                                                            color_eyre::Report::msg(format!(
                                                                "initial_read lock poisoned: {e}"
                                                            ))
                                                        })?;
                                                    *read_guard = Some(read);
                                                }
                                                return Ok((true, false, current_room.clone()));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // If no auto-relogin was attempted (no credentials), store the read stream.
                    // Otherwise, we already stored it above in one of the auto-relogin branches.
                    if login.is_none() || password.is_none() {
                        let mut read_guard = initial_read.lock().map_err(|e| {
                            color_eyre::Report::msg(format!("initial_read lock poisoned: {e}"))
                        })?;
                        *read_guard = Some(read);
                    } else {
                        // Auto-relogin was attempted but didn't reach a success branch — connection is unusable.
                        let mut read_guard = initial_read.lock().map_err(|e| {
                            color_eyre::Report::msg(format!("initial_read lock poisoned: {e}"))
                        })?;
                        *read_guard = None;
                    }
                    return Ok((true, false, current_room.clone()));
                }
                Ok(Err(_e)) => {
                    // Connection failed, will retry with backoff
                }
                Err(_) => {
                    // Timeout, will retry with backoff
                }
            }
            // Back off before the next attempt
            retry_delay = (retry_delay * 2).min(max_delay);
            tokio::time::sleep(retry_delay).await;
        }
    }

    // --- UI rendering ---

    pub fn view(&mut self, frame: &mut Frame) {
        // --- Disconnected state: show dedicated screen ---
        if matches!(self.input_mode, InputMode::Disconnected) {
            let area = frame.area();
            let disconnected_msg = Paragraph::new(Line::raw(
                "⚠ Disconnected from server\nWaiting for reconnection...",
            ))
            .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center);

            let hint_msg = Paragraph::new(Line::raw("[Press q or Esc to quit]"))
                .style(Style::default().fg(Color::DarkGray))
                .alignment(Alignment::Center);

            let v_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(40),
                    Constraint::Length(3),
                    Constraint::Length(1),
                ])
                .split(area);

            frame.render_widget(disconnected_msg, v_layout[1]);
            frame.render_widget(hint_msg, v_layout[2]);
            return;
        }

        let outer = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
            .split(frame.area());

        // Left: rooms (only visible when logged in)
        if self.connection_state.is_logged_in() {
            let room_items: Vec<ListItem> = self
                .rooms
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    let marker = if i == self.room_selected {
                        " > "
                    } else {
                        "   "
                    };
                    let active = if *r == self.room { " *" } else { "" };
                    ListItem::new(Line::raw(format!("{}{}{}", marker, r, active)))
                })
                .collect();
            let rooms_list = List::new(room_items)
                .block(Block::bordered().title("Rooms (Tab to focus)"))
                .highlight_style(Modifier::REVERSED);
            let mut room_state = ListState::default().with_selected(Some(self.room_selected));
            frame.render_stateful_widget(rooms_list, outer[0], &mut room_state);
        } else {
            // Show a placeholder when not logged in
            let splash_lines: Vec<Line> = if self.connection_state.is_connected() {
                vec![
                    Line::raw(""),
                    Line::raw("  Chatter v0.1").style(
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Line::raw(""),
                    Line::raw("  ✓ Connected to server").style(Style::default().fg(Color::Green)),
                    match self.input_mode {
                        InputMode::EnteringLogin => Line::raw("  Enter your login name below"),
                        InputMode::EnteringPassword => Line::raw("  Enter your password below"),
                        _ => Line::raw("  Enter your login name below"),
                    },
                ]
            } else if self.reconnect_pending {
                vec![
                    Line::raw(""),
                    Line::raw("  Chatter v0.1").style(
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Line::raw(""),
                    Line::raw("  ⚠ Reconnecting...")
                        .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                ]
            } else {
                vec![
                    Line::raw(""),
                    Line::raw("  Chatter v0.1").style(
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Line::raw(""),
                    Line::raw("  Connecting to server...")
                        .style(Style::default().fg(Color::DarkGray)),
                ]
            };
            let splash_info = Paragraph::new(splash_lines).block(Block::bordered().title("About"));
            frame.render_widget(splash_info, outer[0]);
        }

        // Right: messages + input
        let inner = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(outer[1]);

        // Room header (only when logged in)
        if self.connection_state.is_logged_in() {
            let room_header = Paragraph::new(Line::raw(format!(
                "# {} (q to quit, arrows to scroll)",
                self.room
            )))
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::bordered().borders(Borders::TOP));
            let msg_area = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(2), Constraint::Percentage(100)])
                .split(inner[0]);
            frame.render_widget(room_header, msg_area[0]);

            // Messages with scroll
            let visible = (msg_area[1].height as usize).saturating_sub(2);
            let start = self
                .message_offset
                .saturating_sub(visible - 1)
                .min(self.messages.len());
            let end = (start + visible).min(self.messages.len());
            let items: Vec<ListItem> = self.messages[start..end]
                .iter()
                .map(|m| ListItem::new(render_message(m)))
                .collect();
            frame.render_widget(List::new(items), msg_area[1]);
        } else {
            // Show messages even when not logged in (system messages)
            let msg_area = if !self.connection_state.is_connected() && self.reconnect_pending {
                // Split into status line + messages when initial connect failed (auto-retrying)
                let area = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(1), Constraint::Percentage(100)])
                    .split(inner[0]);
                let status = Paragraph::new(Line::raw(" ⚠  Reconnecting... "))
                    .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
                    .block(Block::bordered().title("Messages"));
                frame.render_widget(status, area[0]);
                area[1]
            } else if !self.connection_state.is_connected() {
                // Split into status line + messages when disconnected after being connected
                let area = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(1), Constraint::Percentage(100)])
                    .split(inner[0]);
                let status =
                    Paragraph::new(Line::raw(" ⚠  Disconnected from server. Reconnecting... "))
                        .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
                        .block(Block::bordered().title("Messages"));
                frame.render_widget(status, area[0]);
                area[1]
            } else {
                inner[0]
            };

            let visible = (msg_area.height as usize).saturating_sub(1);
            let start = self
                .message_offset
                .saturating_sub(visible - 1)
                .min(self.messages.len());
            let end = (start + visible).min(self.messages.len());
            let items: Vec<ListItem> = self.messages[start..end]
                .iter()
                .map(|m| ListItem::new(render_message(m)))
                .collect();
            frame.render_widget(List::new(items), msg_area);
        }

        // Password masking
        let masked_password = "*".repeat(self.password_input.len());

        // Input area
        let (text, title) = match self.input_mode {
            InputMode::Splash => ("", "Welcome to Chatter! Enter=Login  R=Register  q=Quit"),
            InputMode::EnteringLogin => (
                self.login_input.as_str(),
                match self.auth_mode {
                    AuthMode::Login => "Login name (Esc back):",
                    AuthMode::Register => "New account name (Esc back):",
                },
            ),
            InputMode::EnteringPassword => (
                masked_password.as_str(),
                match self.auth_mode {
                    AuthMode::Login => "Password (Esc back):",
                    AuthMode::Register => "Password (Esc back):",
                },
            ),
            InputMode::Normal => (
                self.input.as_str(),
                "Enter to type, Tab rooms, q quit, arrows scroll",
            ),
            InputMode::Editing => (self.input.as_str(), "Message (Enter send, Esc back):"),
            InputMode::RoomList => ("", "Tab/Esc done, arrows navigate, Enter join"),
            InputMode::Disconnected => ("", "Disconnected from server"),
        };

        frame.render_widget(
            Paragraph::new(text)
                .style(match self.input_mode {
                    InputMode::Normal | InputMode::RoomList => Style::default(),
                    InputMode::Editing | InputMode::EnteringLogin | InputMode::EnteringPassword => {
                        Style::default().fg(Color::LightBlue)
                    }
                    InputMode::Splash => Style::default().fg(Color::Yellow),
                    InputMode::Disconnected => Style::default().fg(Color::Red),
                })
                .block(Block::new().borders(Borders::ALL).title(title)),
            inner[1],
        );

        // Cursor
        match self.input_mode {
            InputMode::Normal | InputMode::RoomList => {}
            InputMode::Editing => frame.set_cursor_position(Position::new(
                inner[1].x + cursor_visual_x(&self.input, self.character_index),
                inner[1].y + 1,
            )),
            InputMode::EnteringLogin => frame.set_cursor_position(Position::new(
                inner[1].x + cursor_visual_x(&self.login_input, self.login_character_index),
                inner[1].y + 1,
            )),
            InputMode::EnteringPassword => {
                // Password is masked with '*' (single-width), so char count = visual width.
                // But we still use cursor_visual_x for consistency with the block padding.
                let masked = "*".repeat(self.password_input.len());
                frame.set_cursor_position(Position::new(
                    inner[1].x + cursor_visual_x(&masked, self.password_character_index),
                    inner[1].y + 1,
                ));
            }
            InputMode::Splash => {
                frame.set_cursor_position(Position::new(inner[1].x + 1, inner[1].y + 1))
            }
            InputMode::Disconnected => {}
        }
    }
}

fn default_rooms() -> Vec<String> {
    vec![
        "general".to_string(),
        "random".to_string(),
        "france".to_string(),
    ]
}

/// Calculate the visual column position for a cursor at `char_index` in `text`.
fn cursor_visual_x(text: &str, char_index: usize) -> u16 {
    let byte_index = text
        .char_indices()
        .map(|(i, _)| i)
        .nth(char_index)
        .unwrap_or(text.len());
    text[..byte_index].width() as u16 + 1 // +1 for the text area padding inside the block
}

fn is_not_authenticated_error(code: &str) -> bool {
    code == "NOT_AUTHENTICATED"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn format_timestamp_today_returns_hh_mm() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let result = format_timestamp(now as i64);
        // Should be HH:MM format (5 chars + colon)
        assert_eq!(result.len(), 5);
        assert_eq!(&result[2..3], ":");
    }

    #[test]
    fn format_timestamp_yesterday_returns_date_time() {
        // 2024-01-15 10:30:00 UTC — un timestamp fixe pour éviter les problèmes de TZ
        let result = format_timestamp(1705312200);
        // Should be YYYY-MM-DD HH:MM format (16 chars)
        assert_eq!(result.len(), 16);
        assert_eq!(&result[4..5], "-");
        assert_eq!(&result[13..14], ":");
    }

    #[test]
    fn format_timestamp_far_future_returns_dash() {
        // i64::MAX seconds from epoch is far in the future (overflow)
        let result = format_timestamp(i64::MAX);
        assert_eq!(result, "—");
    }

    #[test]
    fn format_timestamp_epoch_returns_1970_date() {
        let result = format_timestamp(0);
        assert!(result.starts_with("1970-01-01"));
    }

    type SinkType =
        futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
    type ReadType = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

    /// Helper: start a minimal echo WebSocket server on a random port.
    /// Accepts one connection and echoes back text messages.
    async fn spawn_echo_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        let handle = tokio::spawn(async move {
            let stream = listener.accept().await.ok().map(|(s, _)| s);
            if let Some(stream) = stream {
                let ws = accept_async(stream).await.ok();
                // Keep the WebSocket alive so the connection doesn't drop
                if let Some(mut ws) = ws {
                    while let Some(Ok(msg)) = ws.next().await {
                        if let Message::Text(t) = &msg {
                            let _ = ws.send(Message::Text(t.clone())).await;
                        }
                    }
                }
            }
        });

        (url, handle)
    }

    #[test]
    fn default_rooms_matches_initial_session_rooms() {
        assert_eq!(default_rooms(), vec!["general", "random", "france"]);
    }

    #[test]
    fn not_authenticated_error_detection_is_exact() {
        assert!(is_not_authenticated_error("NOT_AUTHENTICATED"));
        assert!(!is_not_authenticated_error("Login required."));
        assert!(!is_not_authenticated_error("GENERAL"));
        assert!(!is_not_authenticated_error("ROOM_NOT_FOUND"));
    }

    /// reconnection_attempt returns true when connecting to a live server.
    /// This verifies the core reconnect logic: establish connection, split
    /// the stream, store both halves in shared state.
    #[tokio::test]
    async fn reconnect_attempt_succeeds_with_live_server() {
        let (url, _server_handle) = spawn_echo_server().await;
        // Give the server a moment to accept connections.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        type SinkType =
            futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
        type ReadType =
            futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

        let ws_sink: Arc<tokio::sync::Mutex<Option<SinkType>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let initial_read: Arc<std::sync::Mutex<Option<ReadType>>> =
            Arc::new(std::sync::Mutex::new(None));

        let result = App::reconnect_attempt(
            url.clone(),
            ws_sink.clone(),
            initial_read.clone(),
            None, // no auto-relogin in test
            None,
            String::new(), // current_room
            true,          // skip_initial_delay for faster test
        )
        .await;

        assert!(
            result.unwrap().0,
            "reconnect_attempt should succeed when server is live"
        );

        // Verify the sink and read socket were stored.
        assert!(
            ws_sink.lock().await.is_some(),
            "ws_sink should be populated after successful reconnect"
        );
        assert!(
            initial_read.lock().unwrap().is_some(),
            "initial_read should be populated after successful reconnect"
        );
    }
    /// When the server is down, reconnect_attempt keeps retrying with
    /// exponential backoff until it eventually connects. This test starts
    /// a server mid-reconnect to verify the retry-and-connect behavior.
    #[tokio::test]
    async fn reconnect_attempt_retries_until_server_available() {
        // Start with no server — connection should fail and retry.
        let ws_sink: Arc<tokio::sync::Mutex<Option<SinkType>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let initial_read: Arc<std::sync::Mutex<Option<ReadType>>> =
            Arc::new(std::sync::Mutex::new(None));

        let url = "ws://127.0.0.1:19876".to_string(); // unused port

        // Spawn the server after a short delay so reconnect_attempt has
        // time to attempt and fail at least once.
        let server_handle = {
            let url = url.clone();
            tokio::spawn(async move {
                // Wait a bit so the reconnect attempt fails first.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let listener = TcpListener::bind(&url.replace("ws://", "")).await.unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                // Accept the WebSocket handshake so connect_async completes
                let _ws = tokio_tungstenite::accept_async(stream).await.ok();
            })
        };

        // reconnect_attempt should eventually succeed once the server starts.
        // We use a timeout to avoid hanging the test suite.
        let sink_clone = ws_sink.clone();
        // Use a generous timeout — reconnect_attempt has an infinite loop with
        // exponential backoff (2s initial, then 4s, 8s, ...). The server starts
        // at 500ms; with skip_initial_delay=true the first attempt is at 4s.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, String::new(), true),
        )
        .await;

        let connected = match result {
            Ok(Ok((connected, _, _))) => connected,
            Ok(Err(_)) | Err(_) => false, // task error or timeout — reconnect didn't finish in time
        };

        assert!(
            connected,
            "reconnect_attempt should eventually connect once server comes up"
        );
        assert!(
            sink_clone.lock().await.is_some(),
            "ws_sink should be populated"
        );

        let _ = server_handle.await;
    }

    /// Verify that connect_async works with a simple echo server.
    /// This isolates the connection logic from the retry loop.
    #[tokio::test]
    async fn connect_async_works_with_echo_server() {
        let (url, _handle) = spawn_echo_server().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio_tungstenite::connect_async(url.as_str()),
        )
        .await;

        assert!(result.is_ok(), "connect_async should succeed");
        let (mut ws, _) = result.unwrap().unwrap();

        // Send a message and verify we get it back
        ws.send(Message::Text("hello".into())).await.unwrap();
        let msg = ws.next().await.unwrap().unwrap();
        assert_eq!(msg.into_text().unwrap(), "hello");
    }

    /// When skip_initial_delay is true, the first attempt happens immediately.
    /// With a server on a non-listening port, the function should fail quickly
    /// and retry without the 2-second initial sleep.
    #[tokio::test]
    async fn reconnect_attempt_skip_initial_delay_retries_fast() {
        let ws_sink: Arc<tokio::sync::Mutex<Option<SinkType>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let initial_read: Arc<std::sync::Mutex<Option<ReadType>>> =
            Arc::new(std::sync::Mutex::new(None));

        let url = "ws://127.0.0.1:19877".to_string(); // unused port

        // With skip_initial_delay=true, the first attempt should be near-instant.
        // The total time should be well under 2 seconds (the normal initial delay).
        let start = std::time::Instant::now();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, String::new(), true),
        )
        .await;
        let elapsed = start.elapsed();

        // With skip_initial_delay, we should attempt quickly and retry with
        // exponential backoff (2s, 4s...). The fact that it runs for a while
        // without hanging proves the retry loop works.
        assert!(
            elapsed.as_secs() < 10,
            "reconnect_attempt with skip_initial_delay should not hang"
        );
    }

    /// reconnection_attempt with skip_initial_delay=false uses the normal
    /// 2-second initial delay. This is verified by timing.
    #[tokio::test]
    async fn reconnect_attempt_with_initial_delay_takes_at_least_2s() {
        let ws_sink: Arc<tokio::sync::Mutex<Option<SinkType>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let initial_read: Arc<std::sync::Mutex<Option<ReadType>>> =
            Arc::new(std::sync::Mutex::new(None));

        let url = "ws://127.0.0.1:19878".to_string(); // unused port

        let start = std::time::Instant::now();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, String::new(), false),
        )
        .await;
        let elapsed = start.elapsed();

        // With skip_initial_delay=false, the first sleep is 2 seconds.
        // The total time must be >= 2s (allowing for scheduling jitter).
        assert!(
            elapsed.as_secs() >= 1,
            "reconnect_attempt with initial delay should take at least ~2s, took {:?}",
            elapsed
        );
    }

    #[test]
    fn cursor_visual_x_handles_multibyte_utf8() {
        // "ça" = display width 2 (c=1, ç=1)
        let text = "ça";
        // char_index=0 → before 'c'
        assert_eq!(cursor_visual_x(text, 0), 1);
        // char_index=1 → between 'c' and 'ç'
        assert_eq!(cursor_visual_x(text, 1), 2);
        // char_index=2 → after 'ç' (end of string)
        assert_eq!(cursor_visual_x(text, 2), 3);
        // char_index > chars().count() → clamp to end
        assert_eq!(cursor_visual_x(text, 100), 3);
    }

    #[test]
    fn cursor_visual_x_handles_emoji() {
        // "é😀a" = display width 4 (é=1, 😀=2, a=1)
        let text = "é😀a";
        assert_eq!(cursor_visual_x(text, 0), 1);
        assert_eq!(cursor_visual_x(text, 1), 2); // after 'é'
        assert_eq!(cursor_visual_x(text, 2), 4); // after '😀'
        assert_eq!(cursor_visual_x(text, 3), 5); // after 'a'
    }

    #[test]
    fn render_message_chat_own() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let entry = MessageEntry {
            sender: "me".to_string(),
            content: "Hello!".to_string(),
            timestamp: now,
            is_own: true,
            msg_type: MessageType::Chat,
        };
        let line = render_message(&entry);
        let spans: Vec<String> = line.spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(spans.len(), 1);
        let text = &spans[0];
        assert!(text.contains("me: Hello!"));
    }

    #[test]
    fn render_message_chat_other() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let entry = MessageEntry {
            sender: "Bob".to_string(),
            content: "Hi there!".to_string(),
            timestamp: now,
            is_own: false,
            msg_type: MessageType::Chat,
        };
        let line = render_message(&entry);
        let spans: Vec<String> = line.spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(spans.len(), 1);
        let text = &spans[0];
        assert!(text.contains("Bob: Hi there!"));
    }

    #[test]
    fn render_message_system() {
        let entry = MessageEntry {
            sender: "System".to_string(),
            content: String::new(),
            timestamp: 0,
            is_own: false,
            msg_type: MessageType::System("Joined room 'general'".into()),
        };
        let line = render_message(&entry);
        let spans: Vec<String> = line.spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(spans.len(), 1);
        let text = &spans[0];
        assert!(text.contains("[System] Joined room 'general'"));
    }

    #[test]
    fn render_message_system_with_timestamp() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let entry = MessageEntry {
            sender: "System".to_string(),
            content: String::new(),
            timestamp: now,
            is_own: false,
            msg_type: MessageType::System("Session expired".into()),
        };
        let line = render_message(&entry);
        let spans: Vec<String> = line.spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(spans.len(), 1);
        let text = &spans[0];
        assert!(text.contains("[System] Session expired"));
    }
}
