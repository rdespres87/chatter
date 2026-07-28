use std::{sync::Arc, time::Duration};

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
    RoomList,
    Editing,
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
                    Self::start_reader_and_heartbeat(sink, initial_read, events).await;
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
    ) {
        let Some(mut read) = initial_read.lock().await.take() else {
            return;
        };
        let (pong_tx, mut pong_rx) = mpsc::unbounded_channel();
        let read_events = events.clone();
        tokio::spawn(async move {
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
                    if let (Some(login), Some(passwd)) = (login.as_ref(), password.as_ref())
                        && !login.is_empty()
                        && !passwd.is_empty()
                    {
                        let payload = ClientMessage::Login {
                            login: login.clone(),
                            passwd: passwd.clone(),
                        };
                        if let Ok(json) = chatter_protocol::serialize_client_message(&payload)
                            && let Some(ws) = sink.lock().await.as_mut()
                        {
                            let _ = ws.send(Message::Text(json.into())).await;
                        }
                    }
                    notify.notify_one();
                    Self::start_reader_and_heartbeat(sink, initial_read, events).await;
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
                login,
                room,
                message,
                timestamp: _,
            } if login == "Server" && room == "system" => {
                self.messages.push(Self::system_message(message))
            }
            ServerMessage::IncomingMessage {
                login,
                room,
                message,
                timestamp,
            } if room == self.room => self.messages.push(MessageEntry {
                sender: resolve_sender(&self.login, &login),
                content: message,
                timestamp,
                is_own: login == self.login,
                msg_type: MessageType::Chat,
            }),
            ServerMessage::IncomingMessage { .. } => {}
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
            }
            ServerMessage::RoomHistory { .. } => {}
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
            let login = (!self.login.is_empty()).then(|| self.login.clone());
            let password = self.reconnect_password.clone();
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
    fn handle_focus(&self, ctx: &egui::Context) {
        let id = match self.input_mode {
            InputMode::EnteringLogin => Some(egui::Id::new("login")),
            InputMode::EnteringPassword => Some(egui::Id::new("password")),
            InputMode::Editing => Some(egui::Id::new("message")),
            _ => None,
        };
        if let Some(id) = id {
            ctx.memory_mut(|memory| memory.request_focus(id));
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
                        .id_source("login")
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
                        .id_source("password")
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
    fn render_normal(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading(if self.room.is_empty() {
                    "Chatter"
                } else {
                    &self.room
                });
                if ui.button("Room List").clicked() {
                    self.input_mode = InputMode::RoomList;
                }
                if ui.button("Write message").clicked() {
                    self.input_mode = InputMode::Editing;
                }
            });
            ui.separator();
            self.render_messages(ui);
        });
    }
    fn render_room_list(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Rooms");
                if ui.button("Back").clicked() {
                    self.input_mode = InputMode::Normal;
                }
            });
            ui.separator();
            egui::ScrollArea::vertical().show(ui, |ui| {
                for (index, room) in self.rooms.clone().into_iter().enumerate() {
                    ui.horizontal(|ui| {
                        ui.selectable_value(&mut self.room_selected, index, &room);
                        if ui.button("Join").clicked() {
                            self.join_room(room);
                        }
                    });
                }
            });
        });
    }
    fn render_editing(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading(if self.room.is_empty() {
                    "Chatter"
                } else {
                    &self.room
                });
                if ui.button("Room List").clicked() {
                    self.input_mode = InputMode::RoomList;
                }
            });
            ui.separator();
            self.render_messages(ui);
            ui.separator();
            ui.add_sized(
                [ui.available_width(), 72.0],
                egui::TextEdit::multiline(&mut self.input)
                    .id_source("message")
                    .hint_text("Write a message…"),
            );
            ui.horizontal(|ui| {
                if ui.button("Send").clicked() {
                    self.submit_message();
                }
                if ui.button("Cancel").clicked() {
                    self.input_mode = InputMode::Normal;
                }
            });
        });
    }
    fn render_disconnected(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(ui.available_height() * 0.3);
                ui.heading("Disconnected");
                ui.label("The server connection was lost.");
                if ui.button("Reconnect").clicked() {
                    self.reconnect_pending = true;
                    self.start_reconnect();
                }
            });
        });
    }

    fn render_messages(&self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for message in &self.messages {
                    match &message.msg_type {
                        MessageType::System(content) => {
                            ui.label(
                                egui::RichText::new(content)
                                    .italics()
                                    .color(egui::Color32::GRAY),
                            );
                        }
                        MessageType::Chat => {
                            let color = if message.is_own {
                                egui::Color32::from_rgb(46, 204, 113)
                            } else {
                                egui::Color32::WHITE
                            };
                            ui.horizontal_wrapped(|ui| {
                                ui.label(
                                    egui::RichText::new(&message.sender).strong().color(color),
                                );
                                ui.label(egui::RichText::new(&message.content).color(color));
                                ui.label(
                                    egui::RichText::new(format_timestamp(message.timestamp))
                                        .small()
                                        .color(egui::Color32::GRAY),
                                );
                            });
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
                InputMode::Editing | InputMode::RoomList => self.input_mode = InputMode::Normal,
                _ => {}
            }
            return;
        }
        if tab {
            self.input_mode = match self.input_mode {
                InputMode::EnteringLogin => InputMode::EnteringPassword,
                InputMode::EnteringPassword => InputMode::EnteringLogin,
                InputMode::Normal => InputMode::Editing,
                InputMode::Editing => InputMode::Normal,
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
            InputMode::Normal => self.input_mode = InputMode::Editing,
            InputMode::Editing => self.submit_message(),
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
            .insert(egui::TextStyle::Heading, egui::FontId::proportional(24.0));
        style
            .text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(16.0));
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
        self.handle_focus(&ctx);
        self.handle_keys(&ctx);
        match self.input_mode {
            InputMode::Splash => self.render_splash(ui),
            InputMode::EnteringLogin => self.render_entering_login(ui),
            InputMode::EnteringPassword => self.render_entering_password(ui),
            InputMode::Normal => self.render_normal(ui),
            InputMode::RoomList => self.render_room_list(ui),
            InputMode::Editing => self.render_editing(ui),
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
