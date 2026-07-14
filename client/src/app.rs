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

use crate::events::{AppEvent, Event, EventHandler};

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
    write: futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
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
    pub async fn new(url: String, default_user: Option<String>) -> color_eyre::Result<Self> {
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let connect = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url.as_str()));
        let (ws_stream, _) = match connect.await {
            Ok(Ok(connection)) => connection,
            Ok(Err(e)) => color_eyre::eyre::bail!("Failed to connect to '{url}': {e}"),
            Err(_) => color_eyre::eyre::bail!(
                "Timed out connecting to '{url}' after {}s",
                CONNECT_TIMEOUT.as_secs()
            ),
        };
        let (write, read) = ws_stream.split();

        Ok(Self {
            url,
            running: true,
            events: EventHandler::new(read),
            input: String::new(),
            character_index: 0,
            input_mode: InputMode::Splash,
            messages: Vec::new(),
            message_offset: 0,
            write,
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
        self.write.send(Message::Text(json.into())).await?;
        Ok(())
    }

    // --- Splash screen: choose Login or Register ---

    fn handle_splash_keys(&mut self, key: KeyEvent) {
        match key.code {
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
            KeyCode::Enter => {
                if !self.login_input.is_empty() {
                    self.login = self.login_input.clone();
                    self.messages.push(format!(
                        "[System] Entering password for '{}'...",
                        self.login
                    ));
                    self.input_mode = InputMode::EnteringPassword;
                }
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
            KeyCode::Enter => {
                if !self.password_input.is_empty() {
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
                                .send_client_message(
                                    chatter_protocol::ClientMessage::CreateAccount {
                                        login: self.login.clone(),
                                        passwd,
                                    },
                                )
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
            KeyCode::Enter => {
                self.input_mode = InputMode::Editing;
            }
            KeyCode::Up => {
                if self.message_offset > 0 {
                    self.message_offset -= 1;
                }
            }
            KeyCode::Down => {
                if self.message_offset < self.messages.len().saturating_sub(1) {
                    self.message_offset += 1;
                }
            }
            _ => {}
        }
    }

    async fn handle_editing_keys(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                if !self.input.is_empty() {
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
            KeyCode::Up => {
                if self.room_selected > 0 {
                    self.room_selected -= 1;
                }
            }
            KeyCode::Down => {
                if self.room_selected < self.rooms.len().saturating_sub(1) {
                    self.room_selected += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(room) = self.rooms.get(self.room_selected).cloned() {
                    self.join_room(room).await;
                    return;
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
            if !self.room.is_empty() {
                if let Err(e) = self
                    .send_client_message(chatter_protocol::ClientMessage::LeaveRoom {
                        room: self.room.clone(),
                    })
                    .await
                {
                    log::error!("Leave room error: {}", e);
                }
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

    fn reset_session_after_reconnect(&mut self) {
        self.room = "general".to_string();
        self.rooms = default_rooms();
        self.room_selected = 0;
        self.messages.clear();
        self.message_offset = 0;
        self.messages
            .push("[System] Reconnected. Please login again.".to_string());
        self.logged_in = false;
        self.login = String::new();
        self.login_input.clear();
        self.login_character_index = 0;
        self.password_input.clear();
        self.password_character_index = 0;
        self.input.clear();
        self.character_index = 0;
        self.input_mode = InputMode::Splash;
    }

    /// Attempt to reconnect to the server. Returns true if reconnected.
    async fn reconnect(&mut self) -> bool {
        self.messages
            .push("[System] Connection lost. Reconnecting...".to_string());

        self.reconnect_after(std::time::Duration::from_secs(2))
            .await
    }

    async fn reconnect_after(&mut self, retry_delay: std::time::Duration) -> bool {
        tokio::time::sleep(retry_delay).await;

        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let connect = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(self.url.as_str()));
        match connect.await {
            Ok(Ok((ws_stream, _))) => {
                let (write, read) = ws_stream.split();
                self.write = write;
                self.events = crate::events::EventHandler::new(read);

                self.reset_session_after_reconnect();
                true
            }
            Ok(Err(e)) => {
                self.messages.push(format!(
                    "[System] Reconnection failed: {}. Press q or Ctrl+C to quit.",
                    e
                ));
                false
            }
            Err(_) => {
                self.messages.push(
                    "[System] Reconnection timed out. Press q or Ctrl+C to quit.".to_string(),
                );
                false
            }
        }
    }

    // --- Main loop ---

    pub async fn run(mut self, mut terminal: ratatui::DefaultTerminal) -> color_eyre::Result<()> {
        while self.running {
            terminal.draw(|frame| self.view(frame))?;

            match self.events.next().await? {
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Key(key) => match self.input_mode {
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
                    },
                    _ => {}
                },
                Event::App(AppEvent::Quit) => self.quit(),
                Event::App(AppEvent::Disconnected) => {
                    self.messages
                        .push("[System] Disconnected from server.".to_string());
                    if !self.reconnect().await {
                        break;
                    }
                }
                Event::App(AppEvent::ConnectionError { reason }) => {
                    self.messages
                        .push(format!("[System] Connection error: {}", reason));
                    if !self.reconnect().await {
                        break;
                    }
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
                            }
                            chatter_protocol::ServerMessage::RoomList { rooms } => {
                                self.rooms = rooms;
                                self.room_selected =
                                    self.rooms.iter().position(|r| *r == self.room).unwrap_or(0);
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
                                    if !self.messages.is_empty() {
                                        self.message_offset = self.messages.len().saturating_sub(1);
                                    }
                                }
                            }
                            chatter_protocol::ServerMessage::Error { message } => {
                                // The current protocol represents "not authenticated" as
                                // Error("Login required.") rather than a dedicated enum variant.
                                if is_not_authenticated_error(&message) {
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
        Ok(())
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
            let start = self.message_offset.saturating_sub(visible - 1);
            let end = (start + visible).min(self.messages.len());
            let items: Vec<ListItem> = self.messages[start..end]
                .iter()
                .map(|m| ListItem::new(Line::raw(m.clone())))
                .collect();
            frame.render_widget(List::new(items), msg_area[1]);
        } else {
            // Show messages even when not logged in (system messages)
            let visible = (inner[0].height as usize).saturating_sub(2);
            let start = self.message_offset.saturating_sub(visible - 1);
            let end = (start + visible).min(self.messages.len());
            let items: Vec<ListItem> = self.messages[start..end]
                .iter()
                .map(|m| ListItem::new(Line::raw(m.clone())))
                .collect();
            frame.render_widget(
                List::new(items).block(Block::bordered().title("Messages")),
                inner[0],
            );
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
                inner[1].x + self.character_index as u16 + 1,
                inner[1].y + 1,
            )),
            InputMode::EnteringLogin => frame.set_cursor_position(Position::new(
                inner[1].x + self.login_character_index as u16 + 1,
                inner[1].y + 1,
            )),
            InputMode::EnteringPassword => frame.set_cursor_position(Position::new(
                inner[1].x + self.password_character_index as u16 + 1,
                inner[1].y + 1,
            )),
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

fn is_not_authenticated_error(message: &str) -> bool {
    message == "Login required."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rooms_matches_initial_session_rooms() {
        assert_eq!(default_rooms(), vec!["general", "random", "france"]);
    }

    #[test]
    fn not_authenticated_error_detection_is_exact() {
        assert!(is_not_authenticated_error("Login required."));
        assert!(!is_not_authenticated_error("Login required"));
        assert!(!is_not_authenticated_error(
            "Join the room before sending messages."
        ));
    }
}
