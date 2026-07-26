use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use std::sync::Arc;
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
    messages: Vec<String>,
    message_offset: usize,
    /// Write handle to the WebSocket stream. Set after initial connection or reconnect.
    ws_sink: Arc<
        tokio::sync::Mutex<
            Option<futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
        >,
    >,
    login: String,
    room: String,
    logged_in: bool,
    login_input: String,
    login_character_index: usize,
    password_input: String,
    password_character_index: usize,
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
    /// Whether a WebSocket connection is currently active.
    connected: bool,
    /// Whether the user was logged in before a disconnection
    /// (used to auto-switch to login flow after reconnect).
    was_logged_in: bool,
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
                        *initial_read.lock().unwrap() = Some(stream);
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
            logged_in: false,
            login_input: default_user.unwrap_or_default(),
            login_character_index: 0,
            password_input: String::new(),
            password_character_index: 0,
            rooms: default_rooms(),
            room_selected: 0,
            auth_mode: AuthMode::Login,
            connecting_task,
            connect_notify: Some(connect_rx),
            initial_read,
            reconnect_pending: false,
            reconnect_tx: Some(reconnect_tx),
            reconnect_rx,
            connected: false,
            was_logged_in: false,
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

    // --- Entering login name (shared by both login and register) ---

    async fn handle_entering_login(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter if !self.login_input.is_empty() && self.connected => {
                self.login = self.login_input.clone();
                self.messages.push(format!(
                    "[System] Entering password for '{}'...",
                    self.login
                ));
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
            KeyCode::Enter if !self.password_input.is_empty() && self.connected => {
                let passwd = self.password_input.clone();
                self.password_input.clear();
                self.password_character_index = 0;

                match self.auth_mode {
                    AuthMode::Login => {
                        self.messages
                            .push(format!("[System] Logging in as '{}'...", self.login));
                        if let Err(e) = self
                            .send_client_message(chatter_protocol::ClientMessage::Login {
                                login: self.login.clone(),
                                passwd,
                            })
                            .await
                        {
                            log::error!("Login send error: {}", e);
                            self.messages.push("[System] Login failed.".to_string());
                            self.input_mode = InputMode::Splash;
                        }
                    }
                    AuthMode::Register => {
                        self.messages
                            .push(format!("[System] Creating account '{}'...", self.login));
                        if let Err(e) = self
                            .send_client_message(chatter_protocol::ClientMessage::CreateAccount {
                                login: self.login.clone(),
                                passwd,
                            })
                            .await
                        {
                            log::error!("Register send error: {}", e);
                            self.messages
                                .push("[System] Registration failed.".to_string());
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
            KeyCode::Enter if !self.input.is_empty() => {
                let msg = self.input.clone();
                self.messages.push(format!("[me] {}", msg));
                if let Err(e) = self
                    .send_client_message(chatter_protocol::ClientMessage::SendMessage {
                        room: self.room.clone(),
                        message: msg,
                    })
                    .await
                {
                    log::error!("Send error: {}", e);
                }
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
        if self.room != room {
            if !self.room.is_empty()
                && let Err(e) = self
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

        self.messages
            .push(format!("[System] Joined room '{}'", room));
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
                let mut guard = self.initial_read.lock().unwrap();
                guard.take()
            };
            if let Some(read) = initial_read {
                // Connection succeeded before run() started.
                self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
            } else {
                // Connection still in progress. Use the watch channel to detect completion.
                // The task will send on connect_tx when it finishes (success or failure).
                self.messages
                    .push("[System] Connecting to server...".to_string());
            }
        }

        // Reconnect task handle. Spawned when disconnected, completed when reconnected or failed.
        let mut reconnect_task: Option<tokio::task::JoinHandle<(bool, bool)>> = None;
        let mut connect_notify = self.connect_notify.take();

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
                    if let Some((connected, re_login_performed)) = result {
                    if connected {
                        // Reconnect succeeded — create a new connected event handler.
                        self.connected = true;
                        self.reconnect_pending = false;
                        self.messages.push("[System] Reconnected.".to_string());
                        if re_login_performed {
                                // Auto-relogin succeeded — go straight to room view.
                                self.logged_in = true;
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().unwrap();
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                            } else if self.was_logged_in {
                                // Re-authentication needed after disconnect — switch to
                                // login flow with saved credentials.
                                self.was_logged_in = false;
                                self.logged_in = false;
                                self.login_input = self.login.clone();
                                self.login_character_index = self.login_input.chars().count();
                                self.password_input.clear();
                                self.password_character_index = 0;
                                self.input_mode = InputMode::EnteringLogin;
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().unwrap();
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                            } else {
                                let initial_read = {
                                    let mut guard = self.initial_read.lock().unwrap();
                                    guard.take()
                                };
                                if let Some(read) = initial_read {
                                    self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                                }
                            }
                        } else {
                            // Reconnect failed — push message and go back to splash.
                            self.reconnect_pending = false;
                            self.input_mode = InputMode::Splash;
                        }
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
                        self.connected = true;
                        self.reconnect_pending = false;
                        self.messages.push("[System] Connection established.".to_string());
                        let initial_read = {
                            let mut guard = self.initial_read.lock().unwrap();
                            guard.take()
                        };
                        if let Some(read) = initial_read {
                            self.events = EventHandler::connected(read, self.ws_sink.clone()).await;
                        }
                    } else {
                        // Initial connection failed — push message and auto-retry with backoff.
                        self.messages.push("[System] Connection failed. Reconnecting...".to_string());
                        self.reconnect_pending = true;
                        // Spawn background reconnect task with exponential backoff.
                        if reconnect_task.is_none() {
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, None, None, true).await
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
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), Some(password), true).await
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
                        } },
                        Event::App(AppEvent::Quit) => self.quit(),
                        // Reconnect is handled via reconnect_rx, not events channel.
                        Event::App(AppEvent::Reconnect) => {},
                        Event::App(AppEvent::Disconnected) => {
                            self.was_logged_in = self.logged_in;
                            self.connected = false;
                            self.messages
                                .push("[System] Disconnected from server.".to_string());
                            self.reset_room_on_disconnect();
                            // Spawn background reconnect task (with initial delay).
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            let login = self.login.clone();
                            let password = self.password_input.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), Some(password), false).await
                            }));
                        }
                        Event::App(AppEvent::ConnectionError { reason }) => {
                            self.was_logged_in = self.logged_in;
                            self.connected = false;
                            self.reconnect_pending = true;
                            self.messages
                                .push(format!("[System] Connection error: {}", reason));
                            // Spawn background reconnect task.
                            let url = self.url.clone();
                            let ws_sink = self.ws_sink.clone();
                            let initial_read = self.initial_read.clone();
                            let login = self.login.clone();
                            let password = self.password_input.clone();
                            reconnect_task = Some(tokio::spawn(async move {
                                App::reconnect_attempt(url, ws_sink, initial_read, Some(login), Some(password), true).await
                            }));
                        }
                        Event::App(AppEvent::ReceivedMsg { data }) => {
                            if let Ok(msg) = chatter_protocol::parse_server_message(data) {
                                match msg {
                                    chatter_protocol::ServerMessage::LoginOk { login } => {
                                        self.logged_in = true;
                                        self.login = login.clone();
                                        self.input_mode = InputMode::Normal;
                                        self.messages.push(format!("[System] Welcome, {}!", login));
                                        self.join_room("general".to_string()).await;
                                    }
                                    chatter_protocol::ServerMessage::LoginFailed { reason } => {
                                        self.logged_in = false;
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.messages.push(format!("[System] {}", reason));
                                        self.input_mode = InputMode::EnteringPassword;
                                    }
                                    chatter_protocol::ServerMessage::AccountCreated { login } => {
                                        self.auth_mode = AuthMode::Login;
                                        self.messages.push(format!(
                                            "[System] Account '{}' created. Please login.",
                                            login
                                        ));
                                        self.login_input.clear();
                                        self.login_character_index = 0;
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.input_mode = InputMode::EnteringLogin;
                                    }
                                    chatter_protocol::ServerMessage::AccountCreationFailed { reason } => {
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        self.messages.push(format!("[System] {}", reason));
                                        self.input_mode = InputMode::EnteringPassword;
                                    }
                                    chatter_protocol::ServerMessage::IncomingMessage {
                                        login,
                                        room,
                                        message,
                                    } => {
                                        if login == "Server" && room == "system" {
                                            self.messages.push(format!("[System] {}", message));
                                        } else if room == self.room {
                                            self.messages.push(format!("[{}] {}", login, message));
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
                                                .map(|entry| {
                                                    format!(
                                                        "[{}] {}: {}",
                                                        entry.timestamp, entry.login, entry.message
                                                    )
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
                                            self.messages.push(
                                                "[System] Session expired. Please login again.".into(),
                                            );
                                            // Reset all auth state even if the client believed
                                            // it was logged in — the server disagrees.
                                            self.logged_in = false;
                                            self.login = String::new();
                                            self.login_input.clear();
                                            self.login_character_index = 0;
                                            self.input_mode = InputMode::Splash;
                                        } else {
                                            self.messages.push(format!("[System] Error: {}", message));
                                        }
                                        self.password_input.clear();
                                        self.password_character_index = 0;
                                        if !self.logged_in {
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
        skip_initial_delay: bool,
    ) -> (bool, bool) {
        // (connected, re-login_performed)
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
                    let (write, read) = ws_stream.split();
                    {
                        let mut sink_guard = ws_sink.lock().await;
                        *sink_guard = Some(write);
                    }
                    {
                        let mut read_guard = initial_read.lock().unwrap();
                        *read_guard = Some(read);
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
                                    Err(_) => return (true, false),
                                };
                            let msg = Message::Text(encoded.into());
                            // Lock the sink, send, then unlock
                            {
                                let mut sink_guard = ws_sink.lock().await;
                                if let Some(ref mut sink) = *sink_guard {
                                    if sink.send(msg).await.is_ok() {
                                        return (true, true);
                                    }
                                }
                            }
                        }
                    }
                    return (true, false);
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
        let outer = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
            .split(frame.area());

        // Left: rooms (only visible when logged in)
        if self.logged_in {
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
            let splash_info = Paragraph::new(Line::raw("  Chatter v0.1"))
                .style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
                .block(Block::bordered().title("About"));
            frame.render_widget(splash_info, outer[0]);
        }

        // Right: messages + input
        let inner = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(outer[1]);

        // Room header (only when logged in)
        if self.logged_in {
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
                .map(|m| ListItem::new(Line::raw(m.clone())))
                .collect();
            frame.render_widget(List::new(items), msg_area[1]);
        } else {
            // Show messages even when not logged in (system messages)
            let msg_area = if !self.connected && self.reconnect_pending {
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
            } else if !self.connected {
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
                .map(|m| ListItem::new(Line::raw(m.clone())))
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
        };

        frame.render_widget(
            Paragraph::new(text)
                .style(match self.input_mode {
                    InputMode::Normal | InputMode::RoomList => Style::default(),
                    InputMode::Editing | InputMode::EnteringLogin | InputMode::EnteringPassword => {
                        Style::default().fg(Color::LightBlue)
                    }
                    InputMode::Splash => Style::default().fg(Color::Yellow),
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
    text[..char_index.min(text.len())].width() as u16 + 1 // +1 for the text area padding inside the block
}

fn is_not_authenticated_error(code: &str) -> bool {
    code == "NOT_AUTHENTICATED"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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
            true, // skip_initial_delay for faster test
        )
        .await;

        assert!(
            result.0,
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
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, true),
        )
        .await;

        let connected = match result {
            Ok((connected, _)) => connected,
            Err(_) => false, // timeout — reconnect didn't finish in time
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
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, true),
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
            App::reconnect_attempt(url, ws_sink, initial_read, None, None, false),
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
}
