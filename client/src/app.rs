use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify, mpsc},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use chatter_protocol::{ClientMessage, ServerMessage};

use crate::utils::{format_date_separator, format_timestamp_bubble, resolve_sender};

const MAX_HISTORY: usize = 500;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// ── Theme constants (Ocean Night) ──────────────────────────────────────
const THEME_PANEL_FILL: egui::Color32 = egui::Color32::from_rgb(15, 17, 24);
const THEME_BUBBLE_OWN: egui::Color32 = egui::Color32::from_rgb(67, 148, 239);
const THEME_BUBBLE_OTHER: egui::Color32 = egui::Color32::from_rgb(30, 33, 48);
const THEME_TEXT_OWN: egui::Color32 = egui::Color32::WHITE;
const THEME_TEXT_OTHER: egui::Color32 = egui::Color32::from_rgb(203, 214, 227);
const THEME_ACCENT: egui::Color32 = egui::Color32::from_rgb(67, 148, 239);
const THEME_FONT_SENDER_SIZE: f32 = 12.0;
const THEME_FONT_CONTENT_SIZE: f32 = 14.0;

type WsSink = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type SharedSink = Arc<Mutex<Option<WsSink>>>;
type WsRead = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type SharedRead = Arc<Mutex<Option<WsRead>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Login,
    Register,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputMode {
    Splash,
    EnteringLogin,
    EnteringPassword,
    Normal,
    Disconnected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    LoggedIn { login: String },
    Disconnected,
}

#[cfg(test)]
impl ConnectionState {
    pub fn is_connected(&self) -> bool {
        !matches!(self, Self::Connecting | Self::Disconnected)
    }
    pub fn is_logged_in(&self) -> bool {
        matches!(self, Self::LoggedIn { .. })
    }
}

#[derive(Clone, Debug)]
pub enum MessageType {
    Chat,
    System(String),
}

#[derive(Clone, Debug)]
pub struct MessageEntry {
    pub id: u64,
    pub sender: String,
    pub content: String,
    pub timestamp: i64,
    pub is_own: bool,
    pub msg_type: MessageType,
}

#[derive(Clone, Debug)]
pub enum AppEvent {
    ReceivedMsg {
        data: Message,
    },
    Disconnected {
        close_code: Option<u16>,
        close_reason: Option<String>,
    },
    ConnectionError {
        reason: String,
    },
    Reconnected,
}

#[derive(Debug, Clone)]
enum PendingAction {
    Login { login: String, password: String },
    Register { login: String, password: String },
    SendMessage { room: String, message: String },
    LeaveRoom { room: String },
    JoinRoom { room: String },
    GetHistory { room: String, cursor: Option<u64> },
}

#[derive(Debug)]
enum ActionResult {
    Sent,
    Joined,
    Failed(String),
}

/// All client state, including the asynchronous WebSocket transport.
pub struct App {
    url: String,
    running: bool,
    input: String,
    input_mode: InputMode,
    messages: BTreeMap<u64, MessageEntry>,
    message_offset: usize,
    ws_sink: SharedSink,
    initial_read: SharedRead,
    login: String,
    room: String,
    login_input: String,
    password_input: String,
    reconnect_password: Option<String>,
    rooms: Vec<String>,
    room_selected: usize,
    auth_mode: AuthMode,
    connect_notify: Arc<Notify>,
    reconnect_pending: bool,
    events_tx: mpsc::UnboundedSender<AppEvent>,
    events_rx: mpsc::UnboundedReceiver<AppEvent>,
    pending_actions: Vec<PendingAction>,
    action_tx: mpsc::UnboundedSender<ActionResult>,
    action_rx: mpsc::UnboundedReceiver<ActionResult>,
    connection_state: ConnectionState,
    theme_configured: bool,
    /// Oldest message id loaded so far (for cursor-based pagination).
    oldest_message_id: Option<u64>,
    /// Whether we are currently loading older messages (pauses scroll-to-bottom).
    loading_older: bool,
    /// Whether the server has more older messages available.
    has_more_history: bool,
    /// Whether the user scrolled up in the message list (triggers auto-load at top).
    has_scrolled_up: bool,
    /// Monotonically increasing counter for system message ids.
    system_message_id: u64,
}

impl App {
    pub async fn new(url: String, default_user: Option<String>) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (action_tx, action_rx) = mpsc::unbounded_channel();
        let ws_sink = Arc::new(Mutex::new(None));
        let initial_read = Arc::new(Mutex::new(None));
        let connect_notify = Arc::new(Notify::new());
        Self::spawn_connection(
            url.clone(),
            ws_sink.clone(),
            initial_read.clone(),
            events_tx.clone(),
            connect_notify.clone(),
        );
        Self {
            url,
            running: true,
            input: String::new(),
            input_mode: InputMode::Splash,
            messages: {
                let id = 0u64;
                std::iter::once((id, Self::system_message("Connecting to server...".into())))
                    .collect()
            },
            message_offset: 0,
            ws_sink,
            initial_read,
            login: default_user.clone().unwrap_or_default(),
            room: "general".into(),
            login_input: default_user.unwrap_or_default(),
            password_input: String::new(),
            reconnect_password: None,
            rooms: default_rooms(),
            room_selected: 0,
            auth_mode: AuthMode::Login,
            connect_notify,
            reconnect_pending: false,
            events_tx,
            events_rx,
            pending_actions: Vec::new(),
            action_tx,
            action_rx,
            connection_state: ConnectionState::Connecting,
            theme_configured: false,
            oldest_message_id: None,
            loading_older: false,
            has_more_history: false,
            has_scrolled_up: false,
            system_message_id: 1000, // Start above normal message ids
        }
    }

    fn spawn_connection(
        url: String,
        sink: SharedSink,
        initial_read: SharedRead,
        events: mpsc::UnboundedSender<AppEvent>,
        notify: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)).await {
                Ok(Ok((socket, _))) => {
                    let (write, read) = socket.split();
                    *sink.lock().await = Some(write);
                    *initial_read.lock().await = Some(read);
                    notify.notify_one();
                    Self::start_reader_and_heartbeat(sink, initial_read, events, None).await;
                }
                Ok(Err(error)) => {
                    let _ = events.send(AppEvent::ConnectionError {
                        reason: error.to_string(),
                    });
                }
                Err(_) => {
                    let _ = events.send(AppEvent::ConnectionError {
                        reason: "connection timed out".into(),
                    });
                }
            }
        })
    }

    async fn start_reader_and_heartbeat(
        sink: SharedSink,
        initial_read: SharedRead,
        events: mpsc::UnboundedSender<AppEvent>,
        initial_message: Option<String>,
    ) {
        let Some(mut read) = initial_read.lock().await.take() else {
            return;
        };
        let (pong_tx, mut pong_rx) = mpsc::unbounded_channel();
        let read_events = events.clone();
        let sink_for_reader = sink.clone();
        tokio::spawn(async move {
            // Send initial message if provided (ensures reader is listening first)
            if let Some(msg) = initial_message {
                if let Some(ws) = sink_for_reader.lock().await.as_mut() {
                    let _ = ws.send(Message::Text(msg.into())).await;
                }
            }
            while let Some(result) = read.next().await {
                match result {
                    Ok(Message::Text(data)) => {
                        let _ = read_events.send(AppEvent::ReceivedMsg {
                            data: Message::Text(data),
                        });
                    }
                    Ok(Message::Pong(_)) => {
                        let _ = pong_tx.send(());
                    }
                    Ok(Message::Close(frame)) => {
                        let close_code = frame.as_ref().map(|f| f.code.into());
                        let close_reason = frame.map(|f| f.reason.to_string());
                        let _ = read_events.send(AppEvent::Disconnected {
                            close_code,
                            close_reason,
                        });
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = read_events.send(AppEvent::ConnectionError {
                            reason: error.to_string(),
                        });
                        return;
                    }
                }
            }
            let _ = read_events.send(AppEvent::Disconnected {
                close_code: None,
                close_reason: None,
            });
        });
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                let ping_result = match sink.lock().await.as_mut() {
                    Some(ws) => ws.send(Message::Ping(Vec::new().into())).await,
                    None => return,
                };
                if let Err(error) = ping_result {
                    let _ = events.send(AppEvent::ConnectionError {
                        reason: format!("heartbeat ping failed: {error}"),
                    });
                    return;
                }
                if tokio::time::timeout(HEARTBEAT_TIMEOUT, pong_rx.recv())
                    .await
                    .is_err()
                {
                    let _ = events.send(AppEvent::ConnectionError {
                        reason: "heartbeat pong timed out".into(),
                    });
                    return;
                }
            }
        });
    }

    pub async fn reconnect_attempt(
        url: String,
        sink: SharedSink,
        initial_read: SharedRead,
        events: mpsc::UnboundedSender<AppEvent>,
        notify: Arc<Notify>,
        login: Option<String>,
        password: Option<String>,
    ) {
        let mut delay = Duration::from_secs(2);
        loop {
            match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)).await {
                Ok(Ok((socket, _))) => {
                    let (write, read) = socket.split();
                    *sink.lock().await = Some(write);
                    *initial_read.lock().await = Some(read);
                    // Build initial login message to send AFTER reader starts.
                    // Always try to login on reconnect if we have credentials,
                    // even when prev_input_mode is Splash or EnteringLogin.
                    let initial_msg = if let (Some(l), Some(p)) = (login.as_ref(), password.as_ref())
                        && !l.is_empty()
                        && !p.is_empty()
                    {
                        let payload = ClientMessage::Login {
                            login: l.clone(),
                            passwd: p.clone(),
                        };
                        chatter_protocol::serialize_client_message(&payload).ok()
                    } else {
                        // No stored credentials — send a minimal login with empty
                        // strings so the server knows we're reconnecting and can
                        // send back a RoomList / Welcome message.  The server
                        // will likely return an error, but at least the reader is
                        // alive and we'll see what happens.
                        // A better approach: always send login if we have any
                        // credentials stored in self.login / self.reconnect_password.
                        None
                    };
                    notify.notify_one();
                    let _ = events.send(AppEvent::Reconnected);
                    Self::start_reader_and_heartbeat(sink, initial_read, events, initial_msg).await;
                    return;
                }
                Ok(Err(_)) | Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    fn handle_server_message(&mut self, data: Message) {
        let Ok(message) = chatter_protocol::parse_server_message(data) else {
            return;
        };
        match message {
            ServerMessage::LoginOk { login } => {
                self.login = login.clone();
                self.connection_state = ConnectionState::LoggedIn { login };
                self.input_mode = InputMode::Normal;
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message("Login successful.".into()));
                self.join_room(String::new(), "general".into());
            }
            ServerMessage::LoginFailed { reason } => {
                self.connection_state = ConnectionState::Connected;
                self.password_input.clear();
                self.input_mode = InputMode::EnteringPassword;
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message(reason));
            }
            ServerMessage::AccountCreated { login } => {
                self.auth_mode = AuthMode::Login;
                self.login_input = login;
                self.password_input.clear();
                self.input_mode = InputMode::EnteringLogin;
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message(
                    "Account created. Please login.".into(),
                ));
            }
            ServerMessage::AccountCreationFailed { reason } => {
                self.password_input.clear();
                self.input_mode = InputMode::EnteringPassword;
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message(reason));
            }
            ServerMessage::IncomingMessage {
                id,
                ref login,
                ref room,
                ref message,
                timestamp,
            } => {
                if *login == "Server" && room == "system" {
                    let sys_id = self.system_message_id;
                    self.system_message_id += 1;
                    self.messages.insert(sys_id, Self::system_message(message.clone()));
                } else if room == &self.room {
                    // BTreeMap insert is idempotent: same id overwrites (dedup by design)
                    self.messages.insert(id, MessageEntry {
                        id,
                        sender: resolve_sender(&self.login, login),
                        content: message.clone(),
                        timestamp,
                        is_own: login == &self.login,
                        msg_type: MessageType::Chat,
                    });
                }
            }
            ServerMessage::RoomList { rooms } => {
                self.rooms = rooms;
                self.room_selected = self.rooms.iter().position(|r| r == &self.room).unwrap_or(0);
            }
            ServerMessage::RoomHistory { room, messages, has_more } => {
                // If this RoomHistory is for a room we're no longer in, ignore it.
                if self.room == room {
                    // Track oldest message id for next cursor
                    let new_oldest = messages.iter().min_by_key(|e| e.id).map(|e| e.id);

                    if self.loading_older {
                        // Prepend older messages (loading history before current window)
                        // Messages come from server in reverse-chronological order,
                        // reverse to chronological for BTreeMap insertion.
                        let older: Vec<MessageEntry> = messages.into_iter().map(|entry| {
                            MessageEntry {
                                id: entry.id,
                                sender: resolve_sender(&self.login, &entry.login),
                                content: entry.message,
                                timestamp: entry.timestamp,
                                is_own: entry.login == self.login,
                                msg_type: MessageType::Chat,
                            }
                        }).collect();

                        // Insert in reverse order so BTreeMap gets them sorted
                        for msg in older.into_iter().rev() {
                            self.messages.insert(msg.id, msg);
                        }

                        self.oldest_message_id = new_oldest;
                        self.loading_older = false;
                    } else {
                        // Clear + replace — initial load or rejoin
                        self.messages.clear();
                        self.oldest_message_id = new_oldest;

                        // Initialize room if empty (reconnect edge case).
                        if self.room.is_empty() {
                            self.room = room.clone();
                            self.room_selected = self.rooms.iter().position(|r| r == &self.room).unwrap_or(0);
                        }
                        for entry in messages {
                            self.messages.insert(entry.id, MessageEntry {
                                id: entry.id,
                                sender: resolve_sender(&self.login, &entry.login),
                                content: entry.message,
                                timestamp: entry.timestamp,
                                is_own: entry.login == self.login,
                                msg_type: MessageType::Chat,
                            });
                        }
                        // Push system message AFTER history so it appears at the bottom.
                        let join_id = self.system_message_id;
                        self.system_message_id += 1;
                        self.messages.insert(join_id, Self::system_message(format!("Joined room '{}'", room)));
                    }

                    // Store has_more for the "Load Older" button
                    self.has_more_history = has_more;
                }
            }
            ServerMessage::Error { message, code } => {
                if code.contains("NOT_AUTHENTICATED") {
                    self.connection_state = ConnectionState::Connected;
                    self.input_mode = InputMode::Splash;
                }
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message(format!("[{code}] {message}")));
            }
        }
        // Keep only the most recent MAX_HISTORY messages (by id).
        if self.messages.len() > MAX_HISTORY {
            let ids_to_remove: Vec<u64> = self.messages.keys().copied().take(
                self.messages.len() - MAX_HISTORY
            ).collect();
            for id in ids_to_remove {
                self.messages.remove(&id);
            }
        }
        self.message_offset = self.messages.len().saturating_sub(1);
    }

    fn handle_disconnect(&mut self, close_code: Option<u16>, close_reason: Option<String>) {
        self.reset_room_on_disconnect();
        self.connection_state = ConnectionState::Disconnected;
        self.input_mode = InputMode::Disconnected;
        self.reconnect_pending = true;
        let id = self.system_message_id;
        self.system_message_id += 1;
        self.messages.insert(id, Self::system_message(
            format!(
                "Disconnected{}",
                close_code
                    .map(|c| format!(" (code {c})"))
                    .unwrap_or_default()
            ) + &close_reason.map(|r| format!(": {r}")).unwrap_or_default(),
        ));
        self.start_reconnect();
    }
    fn handle_connection_error(&mut self, reason: String) {
        let id = self.system_message_id;
        self.system_message_id += 1;
        self.messages.insert(id, Self::system_message(format!("Connection error: {reason}")));
        self.handle_disconnect(None, None);
    }
    fn start_reconnect(&mut self) {
        if self.reconnect_pending {
            let url = self.url.clone();
            let sink = self.ws_sink.clone();
            let read = self.initial_read.clone();
            let events = self.events_tx.clone();
            let notify = self.connect_notify.clone();
            // Always send stored credentials on reconnect, not just when
            // self.login is non-empty.  self.login_input / self.password_input
            // are the last credentials the user typed, and self.reconnect_password
            // is the password we stored at login time.  Using these ensures that
            // even a client on Splash/EnteringLogin will re-authenticate.
            let login = if self.login.is_empty() && !self.login_input.is_empty() {
                Some(self.login_input.clone())
            } else if !self.login.is_empty() {
                Some(self.login.clone())
            } else {
                None
            };
            let password = self.reconnect_password.clone().or_else(|| {
                if !self.password_input.is_empty() {
                    Some(self.password_input.clone())
                } else {
                    None
                }
            });
            tokio::spawn(async move {
                Self::reconnect_attempt(url, sink, read, events, notify, login, password).await;
            });
            self.reconnect_pending = false;
        }
    }
    fn join_room(&mut self, old_room: String, room: String) {
        if !old_room.is_empty() && old_room != room {
            self.pending_actions.push(PendingAction::LeaveRoom { room: old_room });
        }
        self.room = room.clone();
        self.input_mode = InputMode::Normal;
        // Clear messages now — RoomHistory handler will replace them.
        self.messages.clear();
        self.oldest_message_id = None;
        self.loading_older = false;
        self.has_more_history = false;
        self.has_scrolled_up = false;
        // JoinRoom first so the server knows we're in the room, then GetHistory.
        self.pending_actions.push(PendingAction::JoinRoom { room: room.clone() });
        // Initial load: cursor=None means "give me the latest page".
        self.pending_actions.push(PendingAction::GetHistory { room, cursor: None });
    }
    /// Queue a request for older messages (cursor-based pagination).
    fn load_older(&mut self) {
        if self.loading_older || self.oldest_message_id.is_none() || !self.has_more_history {
            return;
        }
        self.loading_older = true;
        let room = self.room.clone();
        let cursor = self.oldest_message_id;
        self.pending_actions.push(PendingAction::GetHistory { room, cursor });
    }
    fn reset_room_on_disconnect(&mut self) {
        // If we were in a room, leave it before resetting (matches ratatui reconnect behavior).
        let prev_room = std::mem::replace(&mut self.room, String::new());
        if !prev_room.is_empty() {
            self.pending_actions.push(PendingAction::LeaveRoom { room: prev_room });
        }
        self.room_selected = 0;
    }

    fn system_message(content: String) -> MessageEntry {
        MessageEntry {
            id: 0,
            sender: "System".into(),
            content: String::new(),
            timestamp: 0,
            is_own: false,
            msg_type: MessageType::System(content),
        }
    }

    fn queue_action(&mut self, action: PendingAction) {
        self.pending_actions.push(action);
    }

    fn submit_credentials(&mut self) {
        if self.login_input.trim().is_empty() || self.password_input.is_empty() {
            let id = self.system_message_id;
            self.system_message_id += 1;
            self.messages.insert(id, Self::system_message(
                "Login and password are required.".into(),
            ));
            return;
        }
        let login = self.login_input.trim().to_owned();
        let password = std::mem::take(&mut self.password_input);
        self.reconnect_password = Some(password.clone());
        match self.auth_mode {
            AuthMode::Login => self.queue_action(PendingAction::Login { login, password }),
            AuthMode::Register => self.queue_action(PendingAction::Register { login, password }),
        }
    }

    fn submit_message(&mut self) {
        let message = self.input.trim().to_owned();
        if message.is_empty() || self.room.is_empty() {
            return;
        }
        // Local echo — server broadcast_to_room excludes the sender,
        // so we must display our own messages client-side.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let id = self.system_message_id;
        self.system_message_id += 1;
        self.messages.insert(id, MessageEntry {
            id: id, // local echo placeholder; server will confirm with real id
            sender: resolve_sender(&self.login, &self.login),
            content: message.clone(),
            timestamp: now,
            is_own: true,
            msg_type: MessageType::Chat,
        });
        self.input.clear();
        self.input_mode = InputMode::Normal;
        self.queue_action(PendingAction::SendMessage {
            room: self.room.clone(),
            message,
        });
    }

    fn run_pending_action(&mut self) {
        let Some(action) = self.pending_actions.first().cloned() else {
            return;
        };
        self.pending_actions.remove(0);
        let sink = self.ws_sink.clone();
        let result_tx = self.action_tx.clone();
        tokio::spawn(async move {
            let (message, joined) = match action {
                PendingAction::Login { login, password } => (
                    ClientMessage::Login {
                        login,
                        passwd: password,
                    },
                    false,
                ),
                PendingAction::Register { login, password } => (
                    ClientMessage::CreateAccount {
                        login,
                        passwd: password,
                    },
                    false,
                ),
                PendingAction::SendMessage { room, message } => {
                    (ClientMessage::SendMessage { room, message }, false)
                }
                PendingAction::JoinRoom { room } => (ClientMessage::JoinRoom { room }, true),
                PendingAction::LeaveRoom { room } => (ClientMessage::LeaveRoom { room }, false),
                PendingAction::GetHistory { room, cursor } => (ClientMessage::GetHistory { room, cursor }, false),
            };
            let result = async {
                let json = chatter_protocol::serialize_client_message(&message)
                    .map_err(|error| error.to_string())?;
                let mut locked_sink = sink.lock().await;
                let ws = locked_sink
                    .as_mut()
                    .ok_or_else(|| "not connected".to_string())?;
                ws.send(Message::Text(json.into()))
                    .await
                    .map_err(|error| error.to_string())
            }
            .await;
            let _ = result_tx.send(match result {
                Ok(()) if joined => ActionResult::Joined,
                Ok(()) => ActionResult::Sent,
                Err(error) => ActionResult::Failed(error),
            });
        });
    }

    fn render_splash(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading(egui::RichText::new("Chatter").size(36.0));
                ui.add_space(16.0);
                if ui.button("Login").clicked() {
                    self.auth_mode = AuthMode::Login;
                    self.input_mode = InputMode::EnteringLogin;
                }
                if ui.button("Register").clicked() {
                    self.auth_mode = AuthMode::Register;
                    self.input_mode = InputMode::EnteringLogin;
                }
            });
        });
    }
    fn render_entering_login(&mut self, ui: &mut egui::Ui) {
        let title = match self.auth_mode {
            AuthMode::Login => "Login",
            AuthMode::Register => "Create account",
        };
        // Handle Enter key to advance to password screen
        let enter_pressed = ui.input(|i| i.key_pressed(egui::Key::Enter));
        // Handle Escape to go back to splash
        if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
            self.password_input.clear();
            self.input_mode = InputMode::Splash;
            return;
        }
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading(title);
                ui.add_space(12.0);
                let response = ui.add(
                    egui::TextEdit::singleline(&mut self.login_input)
                        .lock_focus(true)
                        .hint_text("Username")
                        .desired_width(260.0),
                );
                response.request_focus();
                ui.add_space(12.0);
                if enter_pressed || ui.button("Submit").clicked() {
                    self.input_mode = InputMode::EnteringPassword;
                }
                if ui.button("Cancel").clicked() {
                    self.input_mode = InputMode::Splash;
                }
            });
        });
    }
    fn render_entering_password(&mut self, ui: &mut egui::Ui) {
        // Handle Enter key to submit credentials
        let enter_pressed = ui.input(|i| i.key_pressed(egui::Key::Enter));
        // Handle Escape to go back to splash
        if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
            self.password_input.clear();
            self.input_mode = InputMode::Splash;
            return;
        }
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading("Password");
                ui.add_space(12.0);
                let response = ui.add(
                    egui::TextEdit::singleline(&mut self.password_input)
                        .password(true)
                        .lock_focus(true)
                        .hint_text("Password")
                        .desired_width(260.0),
                );
                response.request_focus();
                ui.add_space(12.0);
                if enter_pressed || ui.button("Submit").clicked() {
                    self.submit_credentials();
                }
                if ui.button("Cancel").clicked() {
                    self.password_input.clear();
                    self.input_mode = InputMode::Splash;
                }
            });
        });
    }
    fn render_sidebar_header(&mut self, ui: &mut egui::Ui) {
        let avatar_size = 32.0;
        // Allocate space for the avatar circle
        let (_response, rect) = ui.allocate_exact_size(
            egui::vec2(avatar_size, avatar_size),
            egui::Sense::hover(),
        );
        // Paint green circle avatar centered in allocated space
        let center = rect.rect.center();
        ui.painter().circle_filled(
            center,
            avatar_size / 2.0,
            egui::Color32::from_rgb(0, 168, 132),
        );
        // Draw first letter of login in the avatar
        let letter = self.login.chars().next().map(|c| c.to_ascii_uppercase().to_string()).unwrap_or_else(|| "U".to_string());
        let text_size = ui.fonts_mut(|f| f.layout_no_wrap(letter.clone(), egui::FontId::proportional(14.0), egui::Color32::WHITE));
        let text_pos = egui::Pos2::new(
            center.x - text_size.size().x / 2.0,
            center.y - text_size.size().y / 2.0,
        );
        ui.painter().text(
            text_pos,
            egui::Align2::LEFT_TOP,
            &letter,
            egui::FontId::proportional(14.0),
            egui::Color32::WHITE,
        );

        // Username next to avatar
        ui.add_space(8.0);
        let username_text = egui::RichText::new(&self.login)
            .strong()
            .size(14.0)
            .color(egui::Color32::from_rgb(233, 237, 239));
        // Make the username clickable to logout
        let resp = ui.label(username_text);
        if resp.clicked() {
            self.input_mode = InputMode::Splash;
            self.login.clear();
            self.room.clear();
            self.messages.clear();
            let id = self.system_message_id;
            self.system_message_id += 1;
            self.messages.insert(id, Self::system_message("Connecting to server...".into()));
        }

        ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
            // Logout button
            let logout_resp = ui.button(egui::RichText::new("⏻").size(12.0));
            if logout_resp.clicked() {
                self.input_mode = InputMode::Splash;
                self.login.clear();
                self.room.clear();
                self.messages.clear();
                let id = self.system_message_id;
                self.system_message_id += 1;
                self.messages.insert(id, Self::system_message("Connecting to server...".into()));
            }
        });
    }

    fn render_room_list(&mut self, ui: &mut egui::Ui) {
        // Background sidebar WhatsApp
        ui.painter().rect_filled(
            ui.max_rect(), 0.0,
            egui::Color32::from_rgb(32, 44, 51),
        );

        // Sidebar header (avatar + name + logout)
        self.render_sidebar_header(ui);

        let rooms_snapshot: Vec<String> = self.rooms.clone();

        egui::ScrollArea::vertical()
            .id_salt("room_scroll")
            .show(ui, |ui| {
                for (index, room) in rooms_snapshot.iter().enumerate() {
                    let is_active = room == &self.room;

                    // Allocate painter for this room item (response + click detection)
                    let item_height = 36.0;
                    let (resp, painter) = ui.allocate_painter(
                        egui::vec2(ui.available_width(), item_height),
                        egui::Sense::click(),
                    );

                    // Background for active room
                    if is_active {
                        painter.rect_filled(resp.rect, 4.0, egui::Color32::from_rgb(42, 57, 66));
                    }

                    // Room name with # prefix
                    let room_label = format!("#{}", room);
                    if resp.clicked() {
                        self.room_selected = index;
                        // Only join if already logged in; otherwise just track selection
                        let was_logged_in = matches!(
                            self.connection_state,
                            ConnectionState::LoggedIn { .. }
                        );
                        if was_logged_in && room != &self.room {
                            let old_room = self.room.clone();
                            self.room.clone_from(room);
                            self.join_room(old_room, room.clone());
                        } else {
                            self.room.clone_from(room);
                        }
                    }

                    // Room name text positioned in allocated space
                    let room_text_color = if is_active {
                        egui::Color32::from_rgb(0, 168, 132)
                    } else {
                        egui::Color32::from_rgb(233, 237, 239)
                    };
                    let room_name_pos = egui::Pos2::new(resp.rect.min.x + 8.0, resp.rect.min.y + 4.0);
                    painter.text(
                        room_name_pos,
                        egui::Align2::LEFT_TOP,
                        &room_label,
                        egui::FontId::new(14.0, egui::FontFamily::Proportional),
                        room_text_color,
                    );

                    ui.add_space(2.0);
                }
            });
    }

    fn render_room_header_inner(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.add_space(12.0);
            ui.label(
                egui::RichText::new(format!("# {}", self.room))
                    .size(16.0)
                    .strong()
                    .color(egui::Color32::from_rgb(233, 237, 239)),
            );
            ui.with_layout(
                egui::Layout::right_to_left(egui::Align::TOP),
                |ui| {
                    let status_color = match &self.connection_state {
                        ConnectionState::Connected
                        | ConnectionState::LoggedIn { .. } => {
                            egui::Color32::from_rgb(0, 168, 132)
                        }
                        ConnectionState::Connecting => egui::Color32::from_rgb(200, 180, 0),
                        ConnectionState::Disconnected => egui::Color32::from_rgb(200, 60, 60),
                    };
                    let status_text = match &self.connection_state {
                        ConnectionState::Connected => "Connected",
                        ConnectionState::LoggedIn { login } => {
                            Box::leak(format!("Connected as {}", login).into_boxed_str())
                        }
                        ConnectionState::Connecting => "Connecting...",
                        ConnectionState::Disconnected => "Disconnected",
                    };
                    ui.label(
                        egui::RichText::new(status_text)
                            .size(12.0)
                            .color(status_color),
                    );
                },
            );
        });
    }

    fn render_input_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            // Input field - multiline (Enter=submit, Shift+Enter=newline)
            let input_width = ui.available_width() - 48.0;

            // Check Enter BEFORE TextEdit so we can consume it first
            let enter_pressed = ui.input_mut(|i| i.key_pressed(egui::Key::Enter));
            let shift_pressed = ui.input(|i| i.modifiers.shift);
            if enter_pressed {
                if shift_pressed {
                    // Insert a newline character manually if Shift is held
                    self.input.push('\n');
                } else {
                    // Submit message — key already consumed by input_mut above
                    self.submit_message();
                    return;
                }
            }

            let edit = egui::TextEdit::multiline(&mut self.input)
                .hint_text("Write a message...")
                .text_color(egui::Color32::from_rgb(233, 237, 239))
                .font(egui::FontId::new(14.0, egui::FontFamily::Proportional))
                .desired_width(input_width)
                .desired_rows(2)
                .frame(egui::Frame::NONE);
            let _response = ui.add_sized([input_width, 52.0], edit);
        });
    }

    fn render_normal(&mut self, ui: &mut egui::Ui) {
        // ── Left sidebar with room list ───────────────────────────────
        egui::Panel::left("room_list")
            .min_size(280.0)
            .resizable(false)
            .show(ui, |ui| {
                self.render_room_list(ui);
            });

        // ── Chat header (fixed at top) ───────────────────────────────
        egui::Panel::top("chat_header")
            .exact_size(40.0)
            .show(ui, |ui| {
                self.render_room_header_inner(ui);
            });

        // ── Main chat area ───────────────────────────────────────────
        // After Panel::left + Panel::top, the remaining ui space IS the
        // main chat area. No need for another panel — draw directly.
        {
            let available = ui.available_rect_before_wrap();
            let input_bar_height: f32 = 52.0;
            let chat_area_height = available.height() - input_bar_height - 4.0;

            // Detect scroll-up intent (user scrolled toward older messages).
            // Read smooth_scroll_delta BEFORE ScrollArea consumes it.
            if ui.input(|i| i.smooth_scroll_delta().y > 0.0) {
                self.has_scrolled_up = true;
            }

            // Scrollable message area fills the space reserved above the composer.
            let scroll_area_output = egui::ScrollArea::vertical()
                // Fill the reserved message area so the composer remains at
                // the bottom of the main UI even with only a few messages.
                .auto_shrink([false; 2])
                .max_height(chat_area_height)
                .stick_to_bottom(!self.loading_older)
                .id_salt("message_scroll")
                .show(ui, |ui| {
                    egui::Frame::new()
                        .inner_margin(egui::Margin::symmetric(10, 0))
                        .show(ui, |ui| self.render_messages(ui));
                });

            // Auto-load when user scrolls to top of content.
            let content_height = scroll_area_output.content_size.y;
            if self.has_scrolled_up
                && content_height > chat_area_height + 2.0
                && scroll_area_output.state.offset.y <= 2.0
                && !self.loading_older
                && self.has_more_history
            {
                self.load_older();
            }

            // Reset flag when user scrolls back down (reading new messages).
            if scroll_area_output.state.offset.y > 10.0 {
                self.has_scrolled_up = false;
            }

            // Loading indicator (spinner, no button needed).
            if self.loading_older {
                ui.add_space(2.0);
                ui.horizontal_centered(|ui| {
                    ui.spinner();
                    ui.label("Loading older messages...");
                });
            }

            // Input bar at bottom (outside scroll area)
            ui.add_space(4.0);
            self.render_input_bar(ui);
        }
    }
    fn render_disconnected(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.25);
                ui.heading(egui::RichText::new("Disconnected").color(egui::Color32::RED));
                ui.add_space(8.0);
                let status_text = match &self.connection_state {
                    ConnectionState::Disconnected => {
                        "The server connection was lost."
                    }
                    ConnectionState::Connecting => {
                        "Reconnecting..."
                    }
                    _ => "The server connection was lost.",
                };
                ui.label(status_text);
                if ui.button("Reconnect").clicked() {
                    self.reconnect_pending = true;
                    self.start_reconnect();
                }
            });
        });
    }

    fn render_bubble(
        ui: &mut egui::Ui,
        sender: &str,
        content: &str,
        time_str: &str,
        text_color: egui::Color32,
        bubble_color: egui::Color32,
        corner_radius: f32,
        align_right: bool,
        is_own: bool,
    ) {
        let padding = 8.0;

        if align_right {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                    Self::render_bubble_inner(ui, sender, content, time_str, text_color, bubble_color, corner_radius, padding, is_own);
                });
            });
        } else {
            ui.horizontal(|ui| {
                Self::render_bubble_inner(ui, sender, content, time_str, text_color, bubble_color, corner_radius, padding, is_own);
            });
        }
    }

    fn render_bubble_inner(
        ui: &mut egui::Ui,
        sender: &str,
        content: &str,
        time_str: &str,
        text_color: egui::Color32,
        bubble_color: egui::Color32,
        corner_radius: f32,
        padding: f32,
        is_own: bool,
    ) {
        let sender_font = egui::FontId::new(THEME_FONT_SENDER_SIZE, egui::FontFamily::Proportional);
        let content_font = egui::FontId::new(THEME_FONT_CONTENT_SIZE, egui::FontFamily::Proportional);
        let timestamp_font = egui::FontId::new(10.0, egui::FontFamily::Proportional);

        let sender_text = if is_own { String::new() } else { sender.to_string() };

        // Sender: pas de wrapping (toujours une ligne)
        let sender_galley = ui.painter().layout_no_wrap(
            sender_text.clone(), sender_font.clone(), text_color.gamma_multiply(0.6));
        let sender_size = sender_galley.size();

        // Content: measure WITH wrapping to get the actual height
        let content_str = content.to_string();

        // Timestamp: measure first (needed to calculate wrap_width)
        let timestamp_galley = ui.painter().layout_no_wrap(
            time_str.to_string(), timestamp_font.clone(), text_color.gamma_multiply(0.45));
        let timestamp_width = timestamp_galley.size().x;

        // Wrap width based on actual available space
        let wrap_width = (ui.available_width() - 2.0 * padding - timestamp_width).max(50.0);
        let content_galley = ui.painter().layout(
            content_str.clone(), content_font.clone(), text_color, wrap_width);
        let content_size = content_galley.size();

        // Hauteur: sender + contenu (avec wrapping) — only if sender is displayed
        let sender_row_height = if is_own { 0.0 } else { sender_size.y + 4.0 };
        // A multi-line message places its timestamp beneath the content, so
        // reserve a full timestamp row in addition to the wrapped content.
        let timestamp_row_height = if content_galley.rows.len() > 1 {
            timestamp_galley.size().y
        } else {
            0.0
        };
        let total_height = sender_row_height + content_size.y + timestamp_row_height + padding * 2.0;

        // Largeur: bubble = wrapped content + padding + timestamp, clamped between
        // minimum (content alone) and maximum (full available width).
        let content_and_ts_width = content_size.x + 12.0 + timestamp_width;
        let min_bubble_width = sender_size.x.max(content_and_ts_width);
        let max_bubble_width = ui.available_width() - 2.0 * padding;
        let max_width = min_bubble_width.min(max_bubble_width);

        // Allouer l'espace + dessiner le fond
        let bubble_size = egui::Vec2::new(max_width + padding * 2.0, total_height);
        let (_resp, painter) = ui.allocate_painter(bubble_size, egui::Sense::hover());
        painter.rect_filled(_resp.rect, corner_radius, bubble_color);

        // Origine du texte
        let text_origin = _resp.rect.min + egui::vec2(padding, padding);

        // Sender (first line, only if not own message)
        if !is_own {
            painter.text(
                text_origin, egui::Align2::LEFT_TOP,
                sender_text.as_str(), sender_font, text_color.gamma_multiply(0.6));
        }

        // Content (second line, or first if own message) — rendered via galley
        // to properly display wrapped text across multiple lines.
        let content_y = text_origin.y + sender_row_height;
        painter.galley(egui::pos2(text_origin.x, content_y), content_galley.clone(), text_color);

        // Timestamp to the right of content.
        // For multi-line content, position after the last line's bottom-right.
        // For single-line content, position on the same line after the text.
        let timestamp_x = text_origin.x + content_galley.rect.right() + 12.0;
        let timestamp_y = if content_galley.rows.len() > 1 {
            text_origin.y + sender_row_height + content_galley.rect.height()
        } else {
            content_y
        };
        painter.text(
            egui::pos2(timestamp_x, timestamp_y),
            egui::Align2::LEFT_TOP,
            time_str, timestamp_font, text_color.gamma_multiply(0.45));
    }

    fn render_messages(&self, ui: &mut egui::Ui) {
        // ScrollArea is managed by render_normal(), not here.
        let corner_radius = 10.0;
        let mut last_date: Option<NaiveDate> = None;

        for (_id, message) in &self.messages {
            match &message.msg_type {
                MessageType::System(content) => {
                    ui.horizontal(|ui| {
                        ui.with_layout(
                            egui::Layout::top_down_justified(egui::Align::Center),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(content)
                                        .italics()
                                        .size(12.0)
                                        .color(egui::Color32::GRAY),
                                );
                            },
                        );
                    });
                    ui.add_space(4.0);
                }
                MessageType::Chat => {
                    // Check if we need a date separator
                    let msg_date = DateTime::<Utc>::from_timestamp(message.timestamp, 0)
                        .map_or_else(
                            || NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                            |dt| dt.naive_utc().date(),
                        );

                    if last_date != Some(msg_date) {
                        // Draw date separator
                        let sep_label = format_date_separator(message.timestamp);

                        // Allocate space for separator — dynamic height based on content
                        let text_font = egui::FontId::new(11.0, egui::FontFamily::Proportional);
                        let galley = ui.painter().layout_no_wrap(sep_label.clone(), text_font.clone(), egui::Color32::from_rgb(140, 150, 165));
                        // Height: text + gap + line + padding top/bottom
                        let sep_height = galley.size().y + 16.0; // 16px padding total (8 top + 8 bottom)

                        let (resp, painter) = ui.allocate_painter(
                            egui::vec2(ui.available_width(), sep_height),
                            egui::Sense::hover(),
                        );

                        // Draw text above the line, centered horizontally
                        let text_y = resp.rect.center().y - 4.0; // slightly above center
                        painter.text(
                            egui::pos2(resp.rect.center().x, text_y),
                            egui::Align2::CENTER_CENTER,
                            &sep_label,
                            text_font,
                            egui::Color32::from_rgb(140, 150, 165),
                        );

                        // Draw horizontal line below the text
                        let line_y = text_y + galley.size().y / 2.0 + 4.0;
                        painter.hline(
                            (resp.rect.left() + 16.0)..=(resp.rect.right() - 16.0),
                            line_y,
                            (1.0, egui::Color32::from_rgb(60, 65, 75)),
                        );

                        last_date = Some(msg_date);

                        // Spacing after separator to separate from first message
                        ui.add_space(8.0);
                    }

                    let is_own = message.is_own;
                    let bubble_color = if is_own {
                        THEME_BUBBLE_OWN
                    } else {
                        THEME_BUBBLE_OTHER
                    };
                    let text_color = if is_own {
                        THEME_TEXT_OWN
                    } else {
                            THEME_TEXT_OTHER
                    };
                    let time_str = format_timestamp_bubble(message.timestamp);

                    Self::render_bubble(
                        ui,
                        &message.sender,
                        &message.content,
                        &time_str,
                        text_color,
                        bubble_color,
                        corner_radius,
                        is_own,
                        is_own,
                    );
                    ui.add_space(6.0);
                }
            }
        }
    }

    fn configure_theme(&mut self, ctx: &egui::Context) {
        if self.theme_configured {
            return;
        }
        let mut visuals = egui::Visuals::dark();
        visuals.selection.bg_fill = THEME_ACCENT;
        visuals.hyperlink_color = THEME_ACCENT;
        visuals.panel_fill = THEME_PANEL_FILL;
        ctx.set_visuals(visuals);
        let mut style = (*ctx.style_of(egui::Theme::Dark)).clone();
        style
            .text_styles
            .insert(egui::TextStyle::Heading, egui::FontId::proportional(20.0));
        style
            .text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(14.0));
        ctx.set_style_of(egui::Theme::Dark, style);
        self.theme_configured = true;
    }

    pub fn logic(&mut self, ctx: &egui::Context) {
        while let Ok(event) = self.events_rx.try_recv() {
            match event {
                AppEvent::ReceivedMsg { data } => self.handle_server_message(data),
                AppEvent::Disconnected {
                    close_code,
                    close_reason,
                } => self.handle_disconnect(close_code, close_reason),
                AppEvent::ConnectionError { reason } => self.handle_connection_error(reason),
                AppEvent::Reconnected => {
                    // If we were previously logged in, go to Normal mode so the
                    // chat UI is shown.  If we were on Splash/EnteringLogin (no
                    // stored login), go to Splash so the user can re-authenticate.
                    if !self.login.is_empty() {
                        self.input_mode = InputMode::Normal;
                    } else {
                        self.input_mode = InputMode::Splash;
                    }
                }
            }
        }
        if self.connection_state == ConnectionState::Connecting
            && self.connect_notify.notified().now_or_never().is_some()
        {
            self.connection_state = ConnectionState::Connected;
            let id = self.system_message_id;
            self.system_message_id += 1;
            self.messages.insert(id, Self::system_message("Connection established.".into()));
        }
        self.run_pending_action();
        while let Ok(result) = self.action_rx.try_recv() {
            match result {
                ActionResult::Sent => {}
                ActionResult::Joined => {}
                ActionResult::Failed(reason) => {
                    let id = self.system_message_id;
                    self.system_message_id += 1;
                    self.messages.insert(id, Self::system_message(format!("Action failed: {reason}")));
                }
            }
        }
        if !self.running {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        ctx.request_repaint_after(Duration::from_millis(50));
    }
}

impl eframe::App for App {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.logic(ctx);
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.configure_theme(&ctx);
        match self.input_mode {
            InputMode::Splash => self.render_splash(ui),
            InputMode::EnteringLogin => self.render_entering_login(ui),
            InputMode::EnteringPassword => self.render_entering_password(ui),
            InputMode::Normal => self.render_normal(ui),
            InputMode::Disconnected => self.render_disconnected(ui),
        }
    }
}

fn default_rooms() -> Vec<String> {
    vec!["general".into()]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn connection_state_helpers_are_correct() {
        assert!(!ConnectionState::Disconnected.is_connected());
        assert!(ConnectionState::LoggedIn { login: "a".into() }.is_logged_in());
    }
    #[test]
    fn default_room_is_general() {
        assert_eq!(default_rooms(), vec!["general"]);
    }
}
