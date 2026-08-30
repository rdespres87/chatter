//! Shared protocol types for the WebSocket chat application.
//!
//! This crate defines the message types (`ClientMessage`, `ServerMessage`),
//! validation functions, and serialization helpers used by both the client
//! and server. It is a dependency of both `server` and `client`.

use std::{borrow::Cow, fmt};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use tungstenite::protocol::Message;

/// Maximum WebSocket text message size.
/// 2 MB — accommodates a full 100-entry history page (~420 KB worst case).
pub const MAX_PAYLOAD_LEN: usize = 2 * 1024 * 1024;
/// Default number of history entries per page.
/// Total payload: ~100 × ~4 KB ≈ 420 KB, well within MAX_PAYLOAD_LEN.
pub const HISTORY_PAGE_SIZE: usize = 100;
/// Minimum login name length.
pub const MIN_LOGIN_LEN: usize = 2;
/// Legacy protocol/display login limit. Existing SQLite rows may contain
/// names created before the stricter account-creation policy.
pub const MAX_LOGIN_LEN: usize = 64;
/// Maximum login name length for new accounts.
pub const MAX_NEW_LOGIN_LEN: usize = 32;
/// Minimum password length.
pub const MIN_PASSWORD_LEN: usize = 4;
/// bcrypt only uses the first 72 bytes of input.
pub const MAX_PASSWORD_LEN: usize = 72;
/// Legacy protocol/display room limit. Existing SQLite rows may contain
/// rooms created before the stricter room-creation policy.
pub const MAX_ROOM_LEN: usize = 64;
/// Maximum room name length for new rooms.
pub const MAX_NEW_ROOM_LEN: usize = 32;
/// Maximum chat message length in bytes.
pub const MAX_CHAT_MESSAGE_LEN: usize = 4096;
/// Maximum length for error/reason strings.
pub const MAX_REASON_LEN: usize = 1024;
/// Maximum number of history entries returned in a single query.
pub const MAX_HISTORY_ENTRIES: usize = 1000;

const RESERVED_LOGIN_PREFIXES: &[&str] = &["server", "system", "admin", "root", "anonymous"];

/// Client-to-server message types.
///
/// These are the messages the client can send to the server over WebSocket.
/// Each variant carries the fields required for that operation.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ClientMessage {
    /// Create a new account.
    CreateAccount {
        /// Login name for the new account (2-32 bytes, ASCII alphanumeric + `_`).
        login: String,
        /// Password for the account (4-72 bytes).
        passwd: String,
    },
    /// Login with credentials.
    Login {
        /// Login name to authenticate (up to 64 bytes, legacy).
        login: String,
        /// Password for authentication.
        passwd: String,
    },
    /// Join a chat room. The server derives the user identity from the session.
    JoinRoom {
        /// Name of the room to join (1-32 bytes, ASCII alphanumeric + `_` + `-`).
        room: String,
    },
    /// Leave a chat room. The server derives the user identity from the session.
    LeaveRoom {
        /// Name of the room to leave.
        room: String,
    },
    /// Send a message to a room. The server derives the user identity from the session.
    SendMessage {
        /// Name of the target room.
        room: String,
        /// Message content (1-4096 bytes, no control characters).
        message: String,
    },
    /// Request history for a room. `cursor` is the id of the last message
    /// already seen — only messages with a smaller id are returned, enabling
    /// cursor-based pagination.  Omit `cursor` (or pass `None`) to fetch the
    /// most recent chunk.
    GetHistory {
        /// Name of the room to query history for.
        room: String,
        /// ID of the last message already seen (for cursor-based pagination).
        /// `None` to fetch the most recent page.
        cursor: Option<u64>,
    },
    /// Request to disconnect from the server.
    Logout,
}

impl fmt::Debug for ClientMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateAccount { login, .. } => f
                .debug_struct("CreateAccount")
                .field("login", login)
                .field("passwd", &"***")
                .finish(),
            Self::Login { login, .. } => f
                .debug_struct("Login")
                .field("login", login)
                .field("passwd", &"***")
                .finish(),
            Self::JoinRoom { room } => f.debug_struct("JoinRoom").field("room", room).finish(),
            Self::LeaveRoom { room } => f.debug_struct("LeaveRoom").field("room", room).finish(),
            Self::SendMessage { room, message } => f
                .debug_struct("SendMessage")
                .field("room", room)
                .field("message", message)
                .finish(),
            Self::GetHistory { room, cursor } => f
                .debug_struct("GetHistory")
                .field("room", room)
                .field("cursor", cursor)
                .finish(),
            Self::Logout => f.debug_struct("Logout").finish(),
        }
    }
}

/// A single historical chat message entry.
///
/// Returned by `RoomHistory` server messages and stored in the SQLite database.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    /// Unique auto-incrementing message ID.
    pub id: u64,
    /// Login name of the sender.
    pub login: String,
    /// Unix timestamp (seconds) when the message was created.
    pub timestamp: i64,
    /// The chat message content.
    pub message: String,
}

/// Server-to-client message types.
///
/// These are the messages the server sends to clients over WebSocket.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum ServerMessage {
    /// Login succeeded for the given account.
    LoginOk {
        /// Login name of the authenticated user.
        login: String,
    },
    /// Login failed with a user-displayable reason.
    LoginFailed {
        /// Human-readable reason for the login failure.
        reason: String,
    },
    /// Account creation succeeded for the given login.
    AccountCreated {
        /// Login name of the newly created account.
        login: String,
    },
    /// Account creation failed with a user-displayable reason.
    AccountCreationFailed {
        /// Human-readable reason for the account creation failure.
        reason: String,
    },
    /// Incoming message from a user in a room.
    IncomingMessage {
        /// Unique auto-incrementing message ID.
        id: u64,
        /// Login name of the sender.
        login: String,
        /// Name of the room this message belongs to.
        room: String,
        /// The chat message content.
        message: String,
        /// Unix timestamp (seconds) when the message was created.
        timestamp: i64,
    },
    /// Server sends available rooms list.
    RoomList {
        /// List of available room names.
        rooms: Vec<String>,
    },
    /// Server sends historical messages for a room.
    RoomHistory {
        /// Name of the room this history belongs to.
        room: String,
        /// Historical messages for the requested page.
        messages: Vec<HistoryEntry>,
        /// True when more messages are available beyond this chunk.
        has_more: bool,
    },
    /// Logout succeeded. The peer is now unauthenticated.
    LogoutOk,
    /// Generic server-side error.
    Error {
        /// Human-readable error message.
        message: String,
        /// Machine-readable error code (e.g. "ROOM_NOT_FOUND").
        code: String,
    },
}

fn validate_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_len(value: &str, field: &str, max_len: usize) -> Result<()> {
    if value.len() > max_len {
        bail!("{field} exceeds {max_len} bytes");
    }
    Ok(())
}

fn validate_required_text(value: &str, field: &str, max_len: usize) -> Result<()> {
    validate_non_empty(value, field)?;
    validate_len(value, field, max_len)?;
    validate_no_control_chars(value, field)
}

fn validate_no_control_chars(value: &str, field: &str) -> Result<()> {
    if value
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\r')
    {
        bail!("{field} cannot contain control characters");
    }
    Ok(())
}

fn normalize_required_field(value: &mut String) {
    let trimmed = value.trim();
    if trimmed.len() != value.len() {
        *value = trimmed.to_string();
    }
}

fn normalized_text(value: &str) -> Cow<'_, str> {
    let trimmed = value.trim();
    if trimmed.len() == value.len() {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(trimmed.to_string())
    }
}

/// Validate a newly-created login name: 2-32 bytes, ASCII alphanumeric + underscore.
///
/// This validates the string exactly as provided. Callers that accept
/// surrounding whitespace should trim first or use `normalize_login`.
pub fn validate_login(login: &str) -> Result<()> {
    if login.len() < MIN_LOGIN_LEN {
        bail!("Login too short (minimum {MIN_LOGIN_LEN} bytes)");
    }
    if login.len() > MAX_NEW_LOGIN_LEN {
        bail!("Login too long (maximum {MAX_NEW_LOGIN_LEN} bytes)");
    }
    if !login.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("Login can only contain ASCII letters, numbers, and underscores");
    }
    let lower = login.to_ascii_lowercase();
    if RESERVED_LOGIN_PREFIXES
        .iter()
        .any(|reserved| lower.starts_with(reserved))
    {
        bail!("Login uses a reserved server or system name");
    }
    Ok(())
}

/// Return the canonical form of a newly-created login name.
pub fn normalize_login(login: &str) -> Result<String> {
    let normalized = login.trim();
    validate_login(normalized)?;
    Ok(normalized.to_string())
}

/// Validate a client-supplied login for authentication.
///
/// Login accepts legacy account names that may already exist in SQLite. New
/// account creation is intentionally stricter and uses `validate_login`.
fn validate_legacy_login(login: &str) -> Result<()> {
    validate_required_text(login, "login", MAX_LOGIN_LEN)
}

/// Validate a room name: 1-32 bytes, ASCII alphanumeric + underscore + hyphen.
///
/// This validates the string exactly as provided. Callers that accept
/// surrounding whitespace should trim first or use `normalize_room`.
pub fn validate_room(room: &str) -> Result<()> {
    if room.is_empty() {
        bail!("Room name cannot be empty");
    }
    if room.len() > MAX_NEW_ROOM_LEN {
        bail!("Room name too long (maximum {MAX_NEW_ROOM_LEN} bytes)");
    }
    if !room
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("Room name can only contain ASCII letters, numbers, underscores, and hyphens");
    }
    Ok(())
}

/// Return the canonical form of a room name.
pub fn normalize_room(room: &str) -> Result<String> {
    let normalized = room.trim();
    validate_room(normalized)?;
    Ok(normalized.to_string())
}

/// Validate a message: not empty, max 4096 bytes, no control characters.
pub fn validate_message(message: &str) -> Result<()> {
    if message.trim().is_empty() {
        bail!("Message cannot be empty");
    }
    if message.len() > MAX_CHAT_MESSAGE_LEN {
        bail!("Message too long (maximum {MAX_CHAT_MESSAGE_LEN} bytes)");
    }
    validate_no_control_chars(message, "Message")?;
    Ok(())
}

fn validate_create_password(passwd: &str) -> Result<()> {
    if passwd.len() < MIN_PASSWORD_LEN {
        bail!("Password too short (minimum {MIN_PASSWORD_LEN} bytes)");
    }
    validate_login_password(passwd)
}

fn validate_login_password(passwd: &str) -> Result<()> {
    if passwd.is_empty() {
        bail!("Password cannot be empty");
    }
    if passwd.len() > MAX_PASSWORD_LEN {
        bail!("Password too long (maximum {MAX_PASSWORD_LEN} bytes)");
    }
    if passwd.contains('\0') {
        bail!("Password cannot contain NUL bytes");
    }
    Ok(())
}

fn validate_history_entry(entry: &HistoryEntry) -> Result<()> {
    validate_required_text(&entry.login, "history login", MAX_LOGIN_LEN)?;
    validate_required_text(&entry.message, "history message", MAX_CHAT_MESSAGE_LEN)
}

/// Validate all fields of a ClientMessage at the protocol boundary.
/// Returns Ok(()) with trimmed fields if valid, or Err with a user-displayable message.
pub fn validate_client_message(msg: &mut ClientMessage) -> Result<()> {
    match msg {
        ClientMessage::CreateAccount { login, passwd } => {
            normalize_required_field(login);
            validate_login(login)?;
            validate_create_password(passwd)?;
        }
        ClientMessage::Login { login, passwd } => {
            validate_legacy_login(login)?;
            validate_login_password(passwd)?;
        }
        ClientMessage::JoinRoom { room } | ClientMessage::LeaveRoom { room } => {
            normalize_required_field(room);
            validate_room(room)?;
        }
        ClientMessage::GetHistory { room, .. } => {
            normalize_required_field(room);
            validate_room(room)?;
        }
        ClientMessage::SendMessage { room, message } => {
            normalize_required_field(room);
            validate_room(room)?;
            validate_message(message)?;
        }
        ClientMessage::Logout => {}
    }
    Ok(())
}

fn normalized_client_message(message: &ClientMessage) -> Result<Cow<'_, ClientMessage>> {
    match message {
        ClientMessage::CreateAccount { login, passwd } => {
            let login = normalized_text(login);
            validate_login(&login)?;
            validate_create_password(passwd)?;
            if matches!(login, Cow::Borrowed(_)) {
                Ok(Cow::Borrowed(message))
            } else {
                Ok(Cow::Owned(ClientMessage::CreateAccount {
                    login: login.into_owned(),
                    passwd: passwd.clone(),
                }))
            }
        }
        ClientMessage::Login { login, passwd } => {
            validate_legacy_login(login)?;
            validate_login_password(passwd)?;
            Ok(Cow::Borrowed(message))
        }
        ClientMessage::JoinRoom { room } => {
            let room = normalized_text(room);
            validate_room(&room)?;
            if matches!(room, Cow::Borrowed(_)) {
                Ok(Cow::Borrowed(message))
            } else {
                Ok(Cow::Owned(ClientMessage::JoinRoom {
                    room: room.into_owned(),
                }))
            }
        }
        ClientMessage::LeaveRoom { room } => {
            let room = normalized_text(room);
            validate_room(&room)?;
            if matches!(room, Cow::Borrowed(_)) {
                Ok(Cow::Borrowed(message))
            } else {
                Ok(Cow::Owned(ClientMessage::LeaveRoom {
                    room: room.into_owned(),
                }))
            }
        }
        ClientMessage::SendMessage {
            room,
            message: text,
        } => {
            let room = normalized_text(room);
            validate_room(&room)?;
            validate_message(text)?;
            if matches!(room, Cow::Borrowed(_)) {
                Ok(Cow::Borrowed(message))
            } else {
                Ok(Cow::Owned(ClientMessage::SendMessage {
                    room: room.into_owned(),
                    message: text.clone(),
                }))
            }
        }
        ClientMessage::GetHistory { room, cursor } => {
            let room = normalized_text(room);
            validate_room(&room)?;
            if matches!(room, Cow::Borrowed(_)) {
                Ok(Cow::Borrowed(message))
            } else {
                Ok(Cow::Owned(ClientMessage::GetHistory {
                    room: room.into_owned(),
                    cursor: *cursor,
                }))
            }
        }
        ClientMessage::Logout => Ok(Cow::Borrowed(message)),
    }
}

fn validate_server_message(message: &ServerMessage) -> Result<()> {
    match message {
        ServerMessage::LoginOk { login } | ServerMessage::AccountCreated { login } => {
            validate_required_text(login, "login", MAX_LOGIN_LEN)?;
        }
        ServerMessage::LoginFailed { reason } | ServerMessage::AccountCreationFailed { reason } => {
            validate_required_text(reason, "reason", MAX_REASON_LEN)?;
        }
        ServerMessage::IncomingMessage {
            id: _,
            login,
            room,
            message,
            timestamp: _,
        } => {
            validate_required_text(login, "login", MAX_LOGIN_LEN)?;
            validate_required_text(room, "room", MAX_ROOM_LEN)?;
            validate_no_control_chars(message, "Message")?;
        }
        ServerMessage::RoomList { rooms } => {
            if rooms.len() > MAX_HISTORY_ENTRIES {
                bail!("room list exceeds {MAX_HISTORY_ENTRIES} entries");
            }
            for room in rooms {
                validate_required_text(room, "room", MAX_ROOM_LEN)?;
            }
        }
        ServerMessage::RoomHistory { room, messages, .. } => {
            validate_required_text(room, "room", MAX_ROOM_LEN)?;
            if messages.len() > MAX_HISTORY_ENTRIES {
                bail!("room history exceeds {MAX_HISTORY_ENTRIES} entries");
            }
            for entry in messages {
                validate_history_entry(entry)?;
            }
        }
        ServerMessage::LogoutOk => {}
        ServerMessage::Error { message, code } => {
            validate_required_text(message, "message", MAX_REASON_LEN)?;
            validate_required_text(code, "code", 64)?;
        }
    }
    Ok(())
}

fn text_payload(data: Message) -> Result<String> {
    match data {
        Message::Text(text) => {
            if text.len() > MAX_PAYLOAD_LEN {
                bail!("websocket text message exceeds {MAX_PAYLOAD_LEN} bytes");
            }
            Ok(text.to_string())
        }
        Message::Close(_) => bail!("websocket close frame"),
        Message::Binary(_) => bail!("expected websocket text message, got binary frame"),
        Message::Ping(_) => bail!("expected websocket text message, got ping frame"),
        Message::Pong(_) => bail!("expected websocket text message, got pong frame"),
        Message::Frame(_) => bail!("expected websocket text message, got raw frame"),
    }
}

/// Serialize a client message to a JSON string.
///
/// Validates and normalizes the message fields before serialization.
/// Returns an error if any field fails validation.
pub fn serialize_client_message(message: &ClientMessage) -> Result<String> {
    let message = normalized_client_message(message)?;
    Ok(serde_json::to_string(message.as_ref())?)
}

/// Serialize a server message to a JSON string.
///
/// Validates the message fields before serialization and checks that the
/// resulting JSON does not exceed `MAX_PAYLOAD_LEN`.
pub fn serialize_server_message(message: &ServerMessage) -> Result<String> {
    validate_server_message(message)?;
    let json = serde_json::to_string(message)?;
    if json.len() > MAX_PAYLOAD_LEN {
        bail!(
            "serialized server message ({} bytes) exceeds MAX_PAYLOAD_LEN ({})",
            json.len(),
            MAX_PAYLOAD_LEN
        );
    }
    Ok(json)
}

/// Parse a WebSocket text message into a `ClientMessage`.
///
/// Reads the raw `Message`, extracts the text payload, deserializes it as
/// JSON, and validates all fields. Returns an error for any invalid input.
pub fn parse_client_message(data: Message) -> Result<ClientMessage> {
    let payload = text_payload(data)?;
    let mut message = serde_json::from_str(&payload)?;
    validate_client_message(&mut message)?;
    Ok(message)
}

/// Parse a WebSocket text message into a `ServerMessage`.
///
/// Reads the raw `Message`, extracts the text payload, deserializes it as
/// JSON, and validates all fields. Returns an error for any invalid input.
pub fn parse_server_message(data: Message) -> Result<ServerMessage> {
    let payload = text_payload(data)?;
    let message = serde_json::from_str(&payload)?;
    validate_server_message(&message)?;
    Ok(message)
}

/// Serialize a login message payload for sending to the server.
///
/// Helper that constructs a `ClientMessage::Login` and serializes it to JSON.
pub fn create_login(login: String, passwd: String) -> Result<String> {
    serialize_client_message(&ClientMessage::Login { login, passwd })
}

/// Serialize a create-account message payload for sending to the server.
///
/// Helper that constructs a `ClientMessage::CreateAccount` and serializes it to JSON.
pub fn create_account(login: String, passwd: String) -> Result<String> {
    serialize_client_message(&ClientMessage::CreateAccount { login, passwd })
}

/// Serialize an incoming message server payload for sending to clients.
///
/// Helper that constructs a `ServerMessage::IncomingMessage` with id=0
/// (used by test helpers) and serializes it to JSON.
pub fn create_incoming_message(
    login: String,
    room: String,
    message: String,
    timestamp: i64,
) -> Result<String> {
    serialize_server_message(&ServerMessage::IncomingMessage {
        id: 0, // test helper uses id=0
        login,
        room,
        message,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_message(json: &str) -> Message {
        Message::Text(json.to_string().into())
    }

    fn all_client_messages() -> Vec<ClientMessage> {
        vec![
            ClientMessage::CreateAccount {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            },
            ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            },
            ClientMessage::JoinRoom {
                room: "general".to_string(),
            },
            ClientMessage::LeaveRoom {
                room: "general".to_string(),
            },
            ClientMessage::SendMessage {
                room: "general".to_string(),
                message: "Hello from the client".to_string(),
            },
            ClientMessage::GetHistory {
                room: "general".to_string(),
                cursor: None,
            },
            ClientMessage::Logout,
        ]
    }

    fn all_server_messages() -> Vec<ServerMessage> {
        vec![
            ServerMessage::LoginOk {
                login: "alice".to_string(),
            },
            ServerMessage::LoginFailed {
                reason: "invalid credentials".to_string(),
            },
            ServerMessage::AccountCreated {
                login: "alice".to_string(),
            },
            ServerMessage::AccountCreationFailed {
                reason: "login already exists".to_string(),
            },
            ServerMessage::IncomingMessage {
                id: 1,
                login: "alice".to_string(),
                room: "general".to_string(),
                message: "Hello from the server".to_string(),
                timestamp: 1_735_732_800,
            },
            ServerMessage::RoomList {
                rooms: vec!["general".to_string(), "random".to_string()],
            },
            ServerMessage::RoomHistory {
                room: "general".to_string(),
                messages: vec![HistoryEntry {
                    id: 1,
                    login: "alice".to_string(),
                    timestamp: 1_735_732_800,
                    message: "Hello".to_string(),
                }],
                has_more: false,
            },
            ServerMessage::Error {
                message: "Something went wrong".to_string(),
                code: "GENERAL".to_string(),
            },
            ServerMessage::LogoutOk,
        ]
    }

    fn object_payload<'a>(
        value: &'a serde_json::Value,
        tag: &str,
    ) -> &'a serde_json::Map<String, serde_json::Value> {
        value
            .get(tag)
            .and_then(serde_json::Value::as_object)
            .unwrap()
    }

    fn serialized_tag(json: &str) -> String {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();

        // Unit variants serialize as strings (e.g. "Logout"), not objects.
        // For object-tagged variants, return the top-level key.
        if let Some(s) = value.as_str() {
            return s.to_string();
        }
        value.as_object().unwrap().keys().next().unwrap().clone()
    }

    #[test]
    fn validate_login_rejects_too_short() {
        assert!(validate_login("a").is_err());
    }

    #[test]
    fn validate_login_rejects_too_long() {
        let login = "a".repeat(MAX_NEW_LOGIN_LEN + 1);

        assert!(validate_login(&login).is_err());
    }

    #[test]
    fn validate_login_rejects_non_ascii_cyrillic() {
        assert!(validate_login("алиса").is_err());
    }

    #[test]
    fn validate_login_rejects_reserved_server() {
        assert!(validate_login("Server").is_err());
        assert!(validate_login("server").is_err());
        assert!(validate_login("Server_").is_err());
        assert!(validate_login("SERVER1").is_err());
        assert!(validate_login("system").is_err());
        assert!(validate_login("admin").is_err());
        assert!(validate_login("root").is_err());
        assert!(validate_login("anonymous").is_err());
    }

    #[test]
    fn validate_login_rejects_untrimmed_ascii_identifier() {
        assert!(validate_login(" alice_42 ").is_err());
    }

    #[test]
    fn normalize_login_trims_ascii_identifier() {
        assert_eq!(normalize_login(" alice_42 ").unwrap(), "alice_42");
    }

    #[test]
    fn validate_login_accepts_boundaries() {
        assert!(validate_login(&"a".repeat(MIN_LOGIN_LEN)).is_ok());
        assert!(validate_login(&"a".repeat(MAX_NEW_LOGIN_LEN)).is_ok());
    }

    #[test]
    fn validate_room_rejects_empty() {
        assert!(validate_room("").is_err());
        assert!(validate_room("   ").is_err());
    }

    #[test]
    fn validate_room_rejects_too_long() {
        let room = "a".repeat(MAX_NEW_ROOM_LEN + 1);

        assert!(validate_room(&room).is_err());
    }

    #[test]
    fn validate_room_rejects_untrimmed_ascii_name() {
        assert!(validate_room(" general-room_1 ").is_err());
    }

    #[test]
    fn normalize_room_trims_ascii_name() {
        assert_eq!(
            normalize_room(" general-room_1 ").unwrap(),
            "general-room_1"
        );
    }

    #[test]
    fn validate_room_accepts_boundary() {
        assert!(validate_room(&"a".repeat(MAX_NEW_ROOM_LEN)).is_ok());
    }

    #[test]
    fn validate_message_rejects_empty() {
        assert!(validate_message("").is_err());
        assert!(validate_message(" \n\t ").is_err());
    }

    #[test]
    fn validate_message_rejects_too_long() {
        let message = "a".repeat(MAX_CHAT_MESSAGE_LEN + 1);

        assert!(validate_message(&message).is_err());
    }

    #[test]
    fn validate_message_accepts_boundary() {
        let message = "a".repeat(MAX_CHAT_MESSAGE_LEN);

        assert!(validate_message(&message).is_ok());
    }

    #[test]
    fn validate_message_rejects_control_characters() {
        assert!(validate_message("hello\u{1b}[2J").is_err());
        assert!(validate_message("hello\u{7}").is_err());
    }

    #[test]
    fn validate_message_accepts_newlines() {
        assert!(validate_message("hello\nworld").is_ok());
        assert!(validate_message("line1\r\nline2").is_ok());
    }

    #[test]
    fn validate_client_message_trims_create_account_login_and_room() {
        let mut create_msg = ClientMessage::CreateAccount {
            login: " alice_1 ".to_string(),
            passwd: "password123".to_string(),
        };
        let mut room_msg = ClientMessage::SendMessage {
            room: " general-room ".to_string(),
            message: "hello".to_string(),
        };

        validate_client_message(&mut create_msg).unwrap();
        validate_client_message(&mut room_msg).unwrap();

        assert_eq!(
            create_msg,
            ClientMessage::CreateAccount {
                login: "alice_1".to_string(),
                passwd: "password123".to_string(),
            }
        );
        assert_eq!(
            room_msg,
            ClientMessage::SendMessage {
                room: "general-room".to_string(),
                message: "hello".to_string(),
            }
        );
    }

    #[test]
    fn validate_client_message_preserves_legacy_login() {
        let mut login_msg = ClientMessage::Login {
            login: " alice ".to_string(),
            passwd: "x".to_string(),
        };

        validate_client_message(&mut login_msg).unwrap();

        assert_eq!(
            login_msg,
            ClientMessage::Login {
                login: " alice ".to_string(),
                passwd: "x".to_string(),
            }
        );
    }

    #[test]
    fn client_login_round_trips() {
        let msg = ClientMessage::Login {
            login: "alice".to_string(),
            passwd: "password123".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn client_debug_redacts_passwords() {
        let msg = ClientMessage::Login {
            login: "alice".to_string(),
            passwd: "password123".to_string(),
        };
        let debug = format!("{msg:?}");

        assert!(debug.contains("Login"));
        assert!(debug.contains("alice"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("password123"));
    }

    #[test]
    fn client_create_account_round_trips() {
        let msg = ClientMessage::CreateAccount {
            login: "bob".to_string(),
            passwd: "secret".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
        assert!(!format!("{decoded:?}").contains("secret"));
    }

    #[test]
    fn client_join_room_has_no_login_field() {
        let msg = ClientMessage::JoinRoom {
            room: "general".to_string(),
        };

        let json = serialize_client_message(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let payload = object_payload(&value, "JoinRoom");

        assert_eq!(decoded, msg);
        assert_eq!(payload.get("room"), Some(&serde_json::json!("general")));
        assert!(payload.get("login").is_none());
    }

    #[test]
    fn client_leave_room_has_no_login_field() {
        let msg = ClientMessage::LeaveRoom {
            room: "general".to_string(),
        };

        let json = serialize_client_message(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let payload = object_payload(&value, "LeaveRoom");

        assert_eq!(decoded, msg);
        assert_eq!(payload.get("room"), Some(&serde_json::json!("general")));
        assert!(payload.get("login").is_none());
    }

    #[test]
    fn client_send_message_json_object_has_no_login_field() {
        let msg = ClientMessage::SendMessage {
            room: "general".to_string(),
            message: "Hello world!".to_string(),
        };

        let json = serialize_client_message(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let payload = object_payload(&value, "SendMessage");

        assert_eq!(decoded, msg);
        assert_eq!(payload.get("room"), Some(&serde_json::json!("general")));
        assert_eq!(
            payload.get("message"),
            Some(&serde_json::json!("Hello world!"))
        );
        assert!(payload.get("login").is_none());
    }

    #[test]
    fn client_get_history_round_trips() {
        let msg = ClientMessage::GetHistory {
            room: "general".to_string(),
            cursor: None,
        };

        let json = serialize_client_message(&msg).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn serialized_messages_reject_opposite_direction_enum() {
        for client_msg in all_client_messages() {
            let json = serialize_client_message(&client_msg).unwrap();

            assert!(serde_json::from_str::<ServerMessage>(&json).is_err());
        }

        for server_msg in all_server_messages() {
            let json = serialize_server_message(&server_msg).unwrap();

            assert!(serde_json::from_str::<ClientMessage>(&json).is_err());
        }
    }

    #[test]
    fn serialized_client_and_server_tags_are_disjoint() {
        let client_tags: std::collections::BTreeSet<_> = all_client_messages()
            .iter()
            .map(|message| serialized_tag(&serialize_client_message(message).unwrap()))
            .collect();
        let server_tags: std::collections::BTreeSet<_> = all_server_messages()
            .iter()
            .map(|message| serialized_tag(&serialize_server_message(message).unwrap()))
            .collect();

        assert!(client_tags.is_disjoint(&server_tags));
    }

    #[test]
    fn server_login_ok_round_trips() {
        let msg = ServerMessage::LoginOk {
            login: "alice".to_string(),
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_login_failed_round_trips() {
        let msg = ServerMessage::LoginFailed {
            reason: "invalid credentials".to_string(),
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_account_created_round_trips() {
        let msg = ServerMessage::AccountCreated {
            login: "bob".to_string(),
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_account_creation_failed_round_trips() {
        let msg = ServerMessage::AccountCreationFailed {
            reason: "login already exists".to_string(),
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_incoming_message_round_trips() {
        let msg = ServerMessage::IncomingMessage {
            id: 42,
            login: "bob".to_string(),
            room: "random".to_string(),
            message: "Hi there!".to_string(),
            timestamp: 1_735_732_800,
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_incoming_message_serializes_with_timestamp() {
        let _msg = ServerMessage::IncomingMessage {
            id: 0,
            login: "alice".to_string(),
            room: "general".to_string(),
            message: "hello".to_string(),
            timestamp: 1_700_000_000,
        };

        let json = create_incoming_message(
            "alice".to_string(),
            "general".to_string(),
            "hello".to_string(),
            1_700_000_000,
        )
        .unwrap();
        let parsed: ServerMessage = serde_json::from_str(&json).unwrap();
        match parsed {
            ServerMessage::IncomingMessage {
                id: _,
                login,
                room,
                message,
                timestamp,
            } => {
                assert_eq!(login, "alice");
                assert_eq!(room, "general");
                assert_eq!(message, "hello");
                assert_eq!(timestamp, 1_700_000_000);
            }
            _ => panic!("Expected IncomingMessage"),
        }
    }

    #[test]
    fn server_room_list_round_trips() {
        let msg = ServerMessage::RoomList {
            rooms: vec!["general".to_string(), "random".to_string()],
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_room_history_round_trips() {
        let msg = ServerMessage::RoomHistory {
            room: "general".to_string(),
            messages: vec![HistoryEntry {
                id: 1,
                login: "alice".to_string(),
                timestamp: 1_735_732_800,
                message: "Hello".to_string(),
            }],
            has_more: false,
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_messages_accept_legacy_display_names() {
        let legacy_room = "r".repeat(MAX_NEW_ROOM_LEN + 1);
        let legacy_login = "алиса".to_string();
        let room_list = ServerMessage::RoomList {
            rooms: vec![legacy_room.clone()],
        };
        let history = ServerMessage::RoomHistory {
            room: legacy_room.clone(),
            messages: vec![HistoryEntry {
                id: 1,
                login: legacy_login,
                timestamp: 1_735_732_800,
                message: "legacy row".to_string(),
            }],
            has_more: false,
        };

        assert!(serialize_server_message(&room_list).is_ok());
        assert!(serialize_server_message(&history).is_ok());
    }

    /// Regression test for history page payload size: a full 100-entry history
    /// page with max-length messages (~420 KB) must serialize within MAX_PAYLOAD_LEN (2 MB).
    #[test]
    fn large_history_page_serializes_within_limit() {
        let messages: Vec<HistoryEntry> = (0..HISTORY_PAGE_SIZE)
            .map(|i| HistoryEntry {
                id: i as u64,
                login: "alice".to_string(),
                message: "x".repeat(MAX_CHAT_MESSAGE_LEN), // 4096 chars
                timestamp: 1_735_732_800 + i as i64,
            })
            .collect();

        let msg = ServerMessage::RoomHistory {
            room: "general".to_string(),
            messages,
            has_more: false,
        };

        let json = serialize_server_message(&msg).unwrap();
        assert!(
            json.len() < MAX_PAYLOAD_LEN,
            "history page serialized size {} bytes exceeds limit {} bytes",
            json.len(),
            MAX_PAYLOAD_LEN
        );
    }

    /// Verify that serialize_server_message rejects payloads exceeding MAX_PAYLOAD_LEN.
    #[test]
    fn serialize_server_message_rejects_too_large() {
        // Build a RoomHistory with enough entries to exceed 2 MB.
        // Each entry ≈ 4 KB, so ~600 entries > 2 MB.
        // MAX_HISTORY_ENTRIES is 1000, so 600 passes count validation.
        let messages: Vec<HistoryEntry> = (0..600)
            .map(|i| HistoryEntry {
                id: i as u64,
                login: "alice".to_string(),
                message: "x".repeat(MAX_CHAT_MESSAGE_LEN), // 4096 chars
                timestamp: 1_735_732_800 + i as i64,
            })
            .collect();

        let msg = ServerMessage::RoomHistory {
            room: "general".to_string(),
            messages,
            has_more: false,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(
            json.len() > MAX_PAYLOAD_LEN,
            "generated {} bytes, need > {}",
            json.len(),
            MAX_PAYLOAD_LEN
        );

        let result = serialize_server_message(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn server_messages_reject_control_characters() {
        let incoming = ServerMessage::IncomingMessage {
            id: 1,
            login: "alice".to_string(),
            room: "general".to_string(),
            message: "hello\u{1b}[2J".to_string(),
            timestamp: 1_735_732_800,
        };
        let room_list = ServerMessage::RoomList {
            rooms: vec!["bad\u{1b}".to_string()],
        };

        assert!(serialize_server_message(&incoming).is_err());
        assert!(serialize_server_message(&room_list).is_err());
    }

    #[test]
    fn server_error_round_trips() {
        let msg = ServerMessage::Error {
            message: "Something went wrong".to_string(),
            code: "GENERAL".to_string(),
        };

        let json = serialize_server_message(&msg).unwrap();
        let decoded: ServerMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn parse_client_message_parses_text_frame() {
        let result = parse_client_message(text_message(
            r#"{"Login":{"login":"alice","passwd":"password123"}}"#,
        ));

        assert_eq!(
            result.unwrap(),
            ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            }
        );
    }

    #[test]
    fn parse_client_message_accepts_legacy_login_names() {
        for login in ["алиса", "a", "Server"] {
            let json = format!(r#"{{"Login":{{"login":"{login}","passwd":"x"}}}}"#);
            assert!(parse_client_message(text_message(&json)).is_ok());
        }

        let long_login = "a".repeat(MAX_NEW_LOGIN_LEN + 1);
        let json = format!(r#"{{"Login":{{"login":"{long_login}","passwd":"x"}}}}"#);
        assert!(parse_client_message(text_message(&json)).is_ok());
    }

    #[test]
    fn parse_client_message_rejects_strict_create_account_violations() {
        let short_password = parse_client_message(text_message(
            r#"{"CreateAccount":{"login":"alice","passwd":"x"}}"#,
        ));
        let legacy_login = parse_client_message(text_message(
            r#"{"CreateAccount":{"login":"алиса","passwd":"password123"}}"#,
        ));

        assert!(short_password.is_err());
        assert!(legacy_login.is_err());
    }

    #[test]
    fn parse_client_message_rejects_nul_passwords() {
        let login = parse_client_message(text_message(
            r#"{"Login":{"login":"alice","passwd":"abc\u0000"}}"#,
        ));
        let create_account = parse_client_message(text_message(
            r#"{"CreateAccount":{"login":"alice","passwd":"abc\u0000"}}"#,
        ));

        assert!(login.is_err());
        assert!(create_account.is_err());
    }

    #[test]
    fn serialize_client_message_emits_normalized_json() {
        let create_msg = ClientMessage::CreateAccount {
            login: " alice ".to_string(),
            passwd: "password123".to_string(),
        };
        let room_msg = ClientMessage::SendMessage {
            room: " general ".to_string(),
            message: "hello".to_string(),
        };

        let create_json = serialize_client_message(&create_msg).unwrap();
        let room_json = serialize_client_message(&room_msg).unwrap();

        assert_eq!(
            object_payload(
                &serde_json::from_str::<serde_json::Value>(&create_json).unwrap(),
                "CreateAccount"
            )
            .get("login"),
            Some(&serde_json::json!("alice"))
        );
        assert_eq!(
            object_payload(
                &serde_json::from_str::<serde_json::Value>(&room_json).unwrap(),
                "SendMessage"
            )
            .get("room"),
            Some(&serde_json::json!("general"))
        );
    }

    #[test]
    fn parse_client_message_rejects_server_variant() {
        let result = parse_client_message(text_message(
            r#"{"IncomingMessage":{"login":"alice","room":"general","message":"Hi!","timestamp":0}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_obsolete_login_field() {
        let result = parse_client_message(text_message(
            r#"{"SendMessage":{"login":"bob","room":"random","message":"Hello!"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_unknown_login_field() {
        let result = parse_client_message(text_message(
            r#"{"Login":{"login":"alice","passwd":"password123","room":"general"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_unknown_create_account_field() {
        let result = parse_client_message(text_message(
            r#"{"CreateAccount":{"login":"alice","passwd":"password123","room":"general"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_empty_fields() {
        assert!(
            parse_client_message(text_message(r#"{"Login":{"login":"","passwd":"x"}}"#)).is_err()
        );
        assert!(
            parse_client_message(text_message(r#"{"Login":{"login":"alice","passwd":""}}"#))
                .is_err()
        );
        assert!(parse_client_message(text_message(r#"{"JoinRoom":{"room":""}}"#)).is_err());
        assert!(
            parse_client_message(text_message(
                r#"{"SendMessage":{"room":"general","message":""}}"#
            ))
            .is_err()
        );
    }

    #[test]
    fn parse_client_message_rejects_passwords_over_bcrypt_limit() {
        let password = "x".repeat(MAX_PASSWORD_LEN + 1);
        let json = format!(r#"{{"Login":{{"login":"alice","passwd":"{password}"}}}}"#);

        assert!(parse_client_message(text_message(&json)).is_err());
    }

    #[test]
    fn parse_client_message_rejects_oversized_payload() {
        let oversized = "x".repeat(MAX_PAYLOAD_LEN + 1);
        let result = parse_client_message(Message::Text(oversized.into()));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_invalid_json() {
        let result = parse_client_message(text_message("not valid json"));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_empty_text() {
        let result = parse_client_message(text_message(""));

        assert!(result.is_err());
    }

    #[test]
    fn parse_client_message_rejects_non_text_frame() {
        assert!(parse_client_message(Message::Ping(Vec::new().into())).is_err());
        assert!(parse_client_message(Message::Binary(Vec::new().into())).is_err());
        assert!(parse_client_message(Message::Close(None)).is_err());
    }

    #[test]
    fn parse_client_close_has_distinct_error() {
        let error = parse_client_message(Message::Close(None)).unwrap_err();

        assert!(error.to_string().contains("close frame"));
    }

    #[test]
    fn parse_server_message_parses_text_frame() {
        let result = parse_server_message(text_message(
            r#"{"IncomingMessage":{"id":42,"login":"alice","room":"general","message":"Hi!","timestamp":0}}"#,
        ));

        assert_eq!(
            result.unwrap(),
            ServerMessage::IncomingMessage {
                id: 42,
                login: "alice".to_string(),
                room: "general".to_string(),
                message: "Hi!".to_string(),
                timestamp: 0,
            }
        );
    }

    #[test]
    fn parse_server_message_rejects_control_characters() {
        let result = parse_server_message(text_message(
            r#"{"IncomingMessage":{"login":"alice","room":"general","message":"Hi\u001b[2J"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_server_message_rejects_client_variant() {
        let result = parse_server_message(text_message(
            r#"{"Login":{"login":"alice","passwd":"password123"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_server_message_rejects_unknown_incoming_message_field() {
        let result = parse_server_message(text_message(
            r#"{"IncomingMessage":{"login":"alice","room":"general","message":"Hi!","passwd":"x"}}"#,
        ));

        assert!(result.is_err());
    }

    #[test]
    fn parse_server_message_rejects_invalid_json() {
        let result = parse_server_message(text_message("not valid json"));

        assert!(result.is_err());
    }

    #[test]
    fn create_login_serializes_client_message() {
        let json = create_login("alice".to_string(), "password123".to_string()).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(
            decoded,
            ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            }
        );
    }

    #[test]
    fn create_account_serializes_client_message() {
        let json = create_account("bob".to_string(), "secret".to_string()).unwrap();
        let decoded: ClientMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(
            decoded,
            ClientMessage::CreateAccount {
                login: "bob".to_string(),
                passwd: "secret".to_string(),
            }
        );
    }

    #[test]
    fn create_login_round_trips_through_parse_client_message() {
        let json = create_login("alice".to_string(), "password123".to_string()).unwrap();
        let decoded = parse_client_message(Message::Text(json.into())).unwrap();

        assert_eq!(
            decoded,
            ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            }
        );
    }

    #[test]
    fn create_account_round_trips_through_parse_client_message() {
        let json = create_account("bob".to_string(), "secret".to_string()).unwrap();
        let decoded = parse_client_message(Message::Text(json.into())).unwrap();

        assert_eq!(
            decoded,
            ClientMessage::CreateAccount {
                login: "bob".to_string(),
                passwd: "secret".to_string(),
            }
        );
    }

    #[test]
    fn client_message_clone_preserves_value() {
        let msg = ClientMessage::SendMessage {
            room: "general".to_string(),
            message: "Hello".to_string(),
        };

        assert_eq!(msg.clone(), msg);
    }

    #[test]
    fn server_message_debug_includes_variant_name() {
        let msg = ServerMessage::Error {
            message: "test error".to_string(),
            code: "GENERAL".to_string(),
        };

        assert!(format!("{msg:?}").contains("Error"));
    }
}
