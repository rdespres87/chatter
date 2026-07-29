use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify, mpsc},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use chatter_protocol::{ClientMessage, ServerMessage};

use crate::utils::{format_timestamp, resolve_sender};

const MAX_HISTORY: usize = 500;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Debug)]
enum PendingAction {
    Login { login: String, password: String },
    Register { login: String, password: String },
    SendMessage { room: String, message: String },
    JoinRoom { room: String },
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
    messages: Vec<MessageEntry>,
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
    /// Track the last message preview per room for sidebar display
    room_last_message: HashMap<String, String>,
    /// Track the last timestamp per room for sidebar display
    room_last_timestamp: HashMap<String, i64>,
    auth_mode: AuthMode,
    connect_notify: Arc<Notify>,
    reconnect_pending: bool,
    events_tx: mpsc::UnboundedSender<AppEvent>,
    events_rx: mpsc::UnboundedReceiver<AppEvent>,
    pending_action: Option<PendingAction>,
    action_tx: mpsc::UnboundedSender<ActionResult>,
    action_rx: mpsc::UnboundedReceiver<ActionResult>,
    connection_state: ConnectionState,
    theme_configured: bool,
    sidebar_search: String,
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
            messages: vec![Self::system_message("Connecting to server...".into())],
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
            room_last_message: HashMap::new(),
            room_last_timestamp: HashMap::new(),
            auth_mode: AuthMode::Login,
            connect_notify,
            reconnect_pending: false,
            events_tx,
            events_rx,
            pending_action: None,
            action_tx,
            action_rx,
            connection_state: ConnectionState::Connecting,
            theme_configured: false,
            sidebar_search: String::new(),
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
                self.messages
                    .push(Self::system_message("Login successful.".into()));
                self.join_room("general".into());
            }
            ServerMessage::LoginFailed { reason } => {
                self.connection_state = ConnectionState::Connected;
                self.password_input.clear();
                self.input_mode = InputMode::EnteringPassword;
                self.messages.push(Self::system_message(reason));
            }
            ServerMessage::AccountCreated { login } => {
                self.auth_mode = AuthMode::Login;
                self.login_input = login;
                self.password_input.clear();
                self.input_mode = InputMode::EnteringLogin;
                self.messages.push(Self::system_message(
                    "Account created. Please login.".into(),
                ));
            }
            ServerMessage::AccountCreationFailed { reason } => {
                self.password_input.clear();
                self.input_mode = InputMode::EnteringPassword;
                self.messages.push(Self::system_message(reason));
            }
            ServerMessage::IncomingMessage {
                ref login,
                ref room,
                ref message,
                timestamp,
            } => {
                // Track last message per room for sidebar preview
                self.room_last_message.insert(room.clone(), message.clone());
                self.room_last_timestamp.insert(room.clone(), timestamp);

                if *login == "Server" && room == "system" {
                    self.messages.push(Self::system_message(message.clone()));
                } else if room == &self.room {
                    self.messages.push(MessageEntry {
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
            ServerMessage::RoomHistory { room, messages } if room == self.room => {
                self.messages = messages
                    .into_iter()
                    .map(|entry| MessageEntry {
                        sender: resolve_sender(&self.login, &entry.login),
                        content: entry.message,
                        timestamp: entry.timestamp,
                        is_own: entry.login == self.login,
                        msg_type: MessageType::Chat,
                    })
                    .collect();
                // Update last message tracking from history
                if let Some(last) = self.messages.last() {
                    self.room_last_message.insert(self.room.clone(), last.content.clone());
                    self.room_last_timestamp.insert(self.room.clone(), last.timestamp);
                }
            }
            ServerMessage::RoomHistory { room, messages } => {
                // Update last message tracking for non-current rooms
                if let Some(last) = messages.last() {
                    self.room_last_message.insert(room.clone(), last.message.clone());
                    self.room_last_timestamp.insert(room.clone(), last.timestamp);
                }
            }
            ServerMessage::Error { message, code } => {
                if code.contains("NOT_AUTHENTICATED") {
                    self.connection_state = ConnectionState::Connected;
                    self.input_mode = InputMode::Splash;
                }
                self.messages
                    .push(Self::system_message(format!("[{code}] {message}")));
            }
        }
        if self.messages.len() > MAX_HISTORY {
            self.messages.drain(..self.messages.len() - MAX_HISTORY);
        }
        self.message_offset = self.messages.len().saturating_sub(1);
    }

    fn handle_disconnect(&mut self, close_code: Option<u16>, close_reason: Option<String>) {
        self.reset_room_on_disconnect();
        self.connection_state = ConnectionState::Disconnected;
        self.input_mode = InputMode::Disconnected;
        self.reconnect_pending = true;
        self.messages.push(Self::system_message(
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
        self.messages
            .push(Self::system_message(format!("Connection error: {reason}")));
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
    fn join_room(&mut self, room: String) {
        self.room = room.clone();
        self.input_mode = InputMode::Normal;
        self.messages
            .push(Self::system_message(format!("Joined room '{room}'")));
        self.queue_action(PendingAction::JoinRoom { room });
    }
    fn load_history(&self) {
        let room = self.room.clone();
        let sink = self.ws_sink.clone();
        tokio::spawn(async move {
            if let Ok(json) =
                chatter_protocol::serialize_client_message(&ClientMessage::GetHistory { room })
                && let Some(ws) = sink.lock().await.as_mut()
            {
                let _ = ws.send(Message::Text(json.into())).await;
            }
        });
    }
    fn reset_room_on_disconnect(&mut self) {
        self.room.clear();
        self.room_selected = 0;
    }

    fn truncate_preview(s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else {
            format!("{}...", &s[..max_len])
        }
    }
    fn system_message(content: String) -> MessageEntry {
        MessageEntry {
            sender: "System".into(),
            content: String::new(),
            timestamp: 0,
            is_own: false,
            msg_type: MessageType::System(content),
        }
    }

    fn queue_action(&mut self, action: PendingAction) {
        if self.pending_action.is_none() {
            self.pending_action = Some(action);
        }
    }

    fn submit_credentials(&mut self) {
        if self.login_input.trim().is_empty() || self.password_input.is_empty() {
            self.messages.push(Self::system_message(
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
        self.input.clear();
        self.input_mode = InputMode::Normal;
        self.queue_action(PendingAction::SendMessage {
            room: self.room.clone(),
            message,
        });
    }

    fn run_pending_action(&mut self) {
        let Some(action) = self.pending_action.take() else {
            return;
        };
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
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading(title);
                ui.add_space(12.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.login_input)
                        .lock_focus(true)
                        .hint_text("Username")
                        .desired_width(260.0),
                );
                ui.horizontal(|ui| {
                    if ui.button("Submit").clicked() {
                        self.input_mode = InputMode::EnteringPassword;
                    }
                    if ui.button("Cancel").clicked() {
                        self.input_mode = InputMode::Splash;
                    }
                });
            });
        });
    }
    fn render_entering_password(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading("Password");
                ui.add_space(12.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.password_input)
                        .password(true)
                        .lock_focus(true)
                        .hint_text("Password")
                        .desired_width(260.0),
                );
                ui.horizontal(|ui| {
                    if ui.button("Submit").clicked() {
                        self.submit_credentials();
                    }
                    if ui.button("Cancel").clicked() {
                        self.password_input.clear();
                        self.input_mode = InputMode::Splash;
                    }
                });
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
            self.messages.push(Self::system_message("Connecting to server...".into()));
        }

        ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
            // Logout button
            let logout_resp = ui.button(egui::RichText::new("⏻").size(12.0));
            if logout_resp.clicked() {
                self.input_mode = InputMode::Splash;
                self.login.clear();
                self.room.clear();
                self.messages.clear();
                self.messages.push(Self::system_message("Connecting to server...".into()));
            }
        });
    }

    fn render_search_bar(&mut self, ui: &mut egui::Ui) {
        let search_bg = egui::Color32::from_rgb(42, 47, 54);
        let search_height = 28.0;
        let (_resp, rect) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), search_height),
            egui::Sense::click(),
        );
        ui.painter().rect_filled(rect.rect, 6.0, search_bg);

        // Search icon
        let icon_x = rect.rect.min.x + 6.0;
        let icon_y = rect.rect.center().y;
        ui.painter().text(
            egui::Pos2::new(icon_x, icon_y - 6.0),
            egui::Align2::LEFT_TOP,
            "🔍",
            egui::FontId::proportional(12.0),
            egui::Color32::from_rgb(134, 150, 160),
        );

        // Search text input - use horizontal layout with icon
        let input_width = if !self.sidebar_search.is_empty() {
            rect.rect.width() - 40.0
        } else {
            rect.rect.width() - 26.0
        };
        ui.horizontal(|ui| {
            // Spacing to align with icon position
            ui.add_space(18.0);
            let edit = egui::TextEdit::singleline(&mut self.sidebar_search)
                .hint_text("Search rooms...")
                .text_color(egui::Color32::from_rgb(233, 237, 239))
                .desired_width(input_width)
                .frame(egui::Frame::NONE);
            ui.add(edit);
        });

        // Clear button if search is not empty
        if !self.sidebar_search.is_empty() {
            let clear_btn = ui.button("✕");
            if clear_btn.clicked() {
                self.sidebar_search.clear();
            }
        }
    }

    fn render_room_list(&mut self, ui: &mut egui::Ui) {
        // Background sidebar WhatsApp
        ui.painter().rect_filled(
            ui.max_rect(), 0.0,
            egui::Color32::from_rgb(32, 44, 51),
        );

        // Sidebar header (avatar + name + logout)
        self.render_sidebar_header(ui);

        ui.add_space(8.0);

        // Search bar
        self.render_search_bar(ui);

        ui.add_space(4.0);

        let search_lower = self.sidebar_search.to_lowercase();
        let rooms_snapshot: Vec<String> = self.rooms.clone();
        let mut filtered_count: usize = 0;

        egui::ScrollArea::vertical()
            .id_salt("room_scroll")
            .show(ui, |ui| {
                for (index, room) in rooms_snapshot.iter().enumerate() {
                    // Search filter
                    if !search_lower.is_empty()
                        && !room.to_lowercase().contains(&search_lower)
                    {
                        continue;
                    }
                    filtered_count += 1;

                    let is_active = room == &self.room;

                    // Get last message preview for this room
                    let last_msg = self.room_last_message.get(room)
                        .map(|s| Self::truncate_preview(s, 40))
                        .unwrap_or_else(|| "No messages".to_string());
                    let last_ts = self.room_last_timestamp.get(room)
                        .map(|t| format_timestamp(*t))
                        .unwrap_or_else(|| "".to_string());

                    let preview_text = if last_msg.len() >= 40 {
                        format!("{} {}", last_msg, last_ts)
                    } else {
                        format!("{}  {}", last_msg, last_ts)
                    };

                    // Allocate painter for this room item (response + click detection)
                    let item_height = 48.0; // approximate height for room name + preview
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
                            self.room.clone_from(room);
                            self.join_room(room.clone());
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

                    // Preview text below room name
                    let preview_color = egui::Color32::from_rgb(134, 150, 160);
                    let preview_pos = egui::Pos2::new(resp.rect.min.x + 8.0, resp.rect.min.y + 22.0);
                    painter.text(
                        preview_pos,
                        egui::Align2::LEFT_TOP,
                        &preview_text,
                        egui::FontId::new(11.0, egui::FontFamily::Proportional),
                        preview_color,
                    );

                    ui.add_space(2.0);
                }

                // If no search results
                if !search_lower.is_empty() && filtered_count == 0 {
                    ui.label(egui::RichText::new("No rooms found")
                        .size(12.0).color(egui::Color32::from_rgb(134, 150, 160)));
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
        let can_send = matches!(self.connection_state, ConnectionState::LoggedIn { .. });
        ui.horizontal(|ui| {
            // Input field
            let input_width = ui.available_width() - 48.0;
            let edit = egui::TextEdit::singleline(&mut self.input)
                .hint_text("Write a message...")
                .text_color(egui::Color32::from_rgb(233, 237, 239))
                .desired_width(input_width)
                .frame(egui::Frame::NONE);
            let response = ui.add(edit);

            // Enter submits message when textedit loses focus and Enter is pressed
            if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                self.submit_message();
            }

            // Send button - green circle
            let send_color = egui::Color32::from_rgb(0, 168, 132);
            let send_btn = egui::Button::new(egui::RichText::new("➤").size(14.0))
                .small()
                .frame(false);
            let send_response = ui.add(send_btn);
            if can_send {
                let btn_rect = send_response.rect;
                ui.painter().circle_filled(
                    btn_rect.center(),
                    btn_rect.width() / 2.0,
                    send_color,
                );
            }
            if send_response.clicked() && can_send {
                self.submit_message();
            }
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
        egui::CentralPanel::default().show(ui, |ui| {
            // Reserve space for the input bar at the bottom (approx 46px)
            let input_bar_height: f32 = 46.0;
            let min_scrolled_height = ui.available_height() - input_bar_height - 4.0;

            // Scrollable message area with constrained minimum height
            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .stick_to_bottom(true)
                .min_scrolled_height(min_scrolled_height)
                .show(ui, |ui| {
                    self.render_messages(ui);
                });

            // Input bar at bottom (outside scroll area)
            ui.add_space(4.0);
            self.render_input_bar(ui);
        });
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
    ) {
        let padding = 8.0;

        if align_right {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                    Self::render_bubble_inner(ui, sender, content, time_str, text_color, bubble_color, corner_radius, padding);
                });
            });
        } else {
            ui.horizontal(|ui| {
                Self::render_bubble_inner(ui, sender, content, time_str, text_color, bubble_color, corner_radius, padding);
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
    ) {
        // 1. Measure with painter.layout_no_wrap()
        let sender_font = egui::FontId::new(10.0, egui::FontFamily::Proportional);
        let content_font = egui::FontId::new(10.0, egui::FontFamily::Proportional);
        let sender_text = format!("[{}] {}", sender, time_str);

        let sender_galley = ui
            .painter()
            .layout_no_wrap(sender_text.clone(), sender_font.clone(), text_color.gamma_multiply(0.6));
        let content_galley = ui
            .painter()
            .layout_no_wrap(content.to_string(), content_font.clone(), text_color);

        let sender_size = sender_galley.size();
        let content_size = content_galley.size();
        let total_height = sender_size.y + content_size.y + 4.0; // gap between lines
        let max_width = sender_size.x.max(content_size.x) + 4.0;

        // 2. Calculate bubble size
        let bubble_size = egui::Vec2::new(max_width + padding * 2.0, total_height + padding * 2.0);

        // 3. Allocate space + draw background
        let (resp, painter) = ui.allocate_painter(bubble_size, egui::Sense::hover());
        painter.rect_filled(resp.rect, corner_radius, bubble_color);

        // 4. Draw text inside the bubble
        let text_origin = resp.rect.min + egui::vec2(padding, padding);

        // Sender + time on first line
        painter.text(
            text_origin,
            egui::Align2::LEFT_TOP,
            sender_text.as_str(),
            sender_font,
            text_color.gamma_multiply(0.6),
        );

        // Content on second line
        let content_y = text_origin.y + sender_size.y + 4.0;
        let content_origin = egui::pos2(text_origin.x, content_y);
        painter.text(
            content_origin,
            egui::Align2::LEFT_TOP,
            content,
            content_font,
            text_color,
        );
    }

    fn render_messages(&self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                let corner_radius = 10.0;

                for message in &self.messages {
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
                            let is_own = message.is_own;
                            let bubble_color = if is_own {
                                egui::Color32::from_rgb(46, 204, 113)
                            } else {
                                egui::Color32::from_rgb(50, 55, 62)
                            };
                            let text_color = if is_own {
                                egui::Color32::WHITE
                            } else {
                                    egui::Color32::LIGHT_GRAY
                            };
                            let time_str = format_timestamp(message.timestamp);

                            Self::render_bubble(
                                ui,
                                &message.sender,
                                &message.content,
                                &time_str,
                                text_color,
                                bubble_color,
                                corner_radius,
                                is_own,
                            );
                            ui.add_space(6.0);
                        }
                    }
                }
            });
    }

    fn handle_keys(&mut self, ctx: &egui::Context) {
        let (enter, escape, tab) = ctx.input_mut(|input| {
            (
                input.consume_key(egui::Modifiers::NONE, egui::Key::Enter),
                input.consume_key(egui::Modifiers::NONE, egui::Key::Escape),
                input.consume_key(egui::Modifiers::NONE, egui::Key::Tab),
            )
        });
        if escape {
            match self.input_mode {
                InputMode::EnteringLogin | InputMode::EnteringPassword => {
                    self.password_input.clear();
                    self.input_mode = InputMode::Splash;
                }
                InputMode::Normal => self.input_mode = InputMode::Normal,
                _ => {}
            }
            return;
        }
        if tab {
            self.input_mode = match self.input_mode {
                InputMode::EnteringLogin => InputMode::EnteringPassword,
                InputMode::EnteringPassword => InputMode::EnteringLogin,
                // In Normal/Editing, Tab cycles through rooms in sidebar
                InputMode::Normal => {
                    if !self.rooms.is_empty() {
                        self.room_selected = (self.room_selected + 1) % self.rooms.len();
                        self.room.clone_from(&self.rooms[self.room_selected]);
                    }
                    self.input_mode
                }
                mode => mode,
            };
            return;
        }
        if !enter {
            return;
        }
        match self.input_mode {
            InputMode::EnteringLogin => self.input_mode = InputMode::EnteringPassword,
            InputMode::EnteringPassword => self.submit_credentials(),
            // In Normal/Editing, Enter submits the message directly
            InputMode::Normal => self.submit_message(),
            _ => {}
        }
    }

    fn configure_theme(&mut self, ctx: &egui::Context) {
        if self.theme_configured {
            return;
        }
        let mut visuals = egui::Visuals::dark();
        visuals.selection.bg_fill = egui::Color32::from_rgb(46, 204, 113);
        visuals.hyperlink_color = egui::Color32::from_rgb(46, 204, 113);
        visuals.panel_fill = egui::Color32::from_rgb(24, 26, 32);
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
            self.messages
                .push(Self::system_message("Connection established.".into()));
        }
        self.run_pending_action();
        while let Ok(result) = self.action_rx.try_recv() {
            match result {
                ActionResult::Sent => {}
                ActionResult::Joined => self.load_history(),
                ActionResult::Failed(reason) => self
                    .messages
                    .push(Self::system_message(format!("Action failed: {reason}"))),
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
        self.handle_keys(&ctx);
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
