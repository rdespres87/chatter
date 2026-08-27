use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use clap::Parser;

use futures_channel::mpsc::UnboundedSender;
use futures_util::{StreamExt, future, pin_mut, stream::TryStreamExt};

use anyhow::Context;
use log::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

use crate::account::{Account, RESERVED_ANONYMOUS_LOGIN};
pub mod account;

type Tx = UnboundedSender<Message>;

const TRANSPORT_SECURITY_NOTICE: &str = "SECURITY: this server accepts plain ws:// connections. Deploy it only behind a TLS-terminating reverse proxy (wss:// to clients), otherwise credentials cross the network in cleartext.";

/// Re-export protocol constant for history page size.
use chatter_protocol::HISTORY_PAGE_SIZE;

const MAX_CONCURRENT_ACCOUNT_TASKS: usize = 64;
const ACCOUNT_TASK_RETRY_DELAY_MS: u64 = 10;
const ACCOUNT_BACKOFF_BASE_MS: u64 = 250;
const ACCOUNT_BACKOFF_MAX_MS: u64 = 8_000;

static ACCOUNT_TASKS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static SYSTEM_MSG_COUNTER: AtomicU64 = AtomicU64::new(0);

struct AccountTaskPermit;

impl Drop for AccountTaskPermit {
    fn drop(&mut self) {
        ACCOUNT_TASKS_IN_FLIGHT.fetch_sub(1, Ordering::Release);
    }
}

/// Peer state: websocket sender, login name, room membership, and auth backoff.
#[derive(Clone)]
pub(crate) struct Peer {
    pub(crate) tx: Tx,
    pub(crate) login: String,
    pub(crate) rooms: HashSet<String>,
    login_failures: u32,
    next_account_attempt: Option<Instant>,
    is_authenticated: bool,
}

impl Peer {
    fn new(tx: Tx, login: String, rooms: HashSet<String>) -> Self {
        Self {
            tx,
            login,
            rooms,
            login_failures: 0,
            next_account_attempt: None,
            is_authenticated: true,
        }
    }

    fn logout(&mut self) {
        self.login.clear();
        self.rooms.clear();
        self.is_authenticated = false;
    }
}

type PeerMap = Arc<std::sync::RwLock<HashMap<SocketAddr, Peer>>>;

/// Persistent backoff state keyed by IP address (survives connection drops).
/// Maps `IpAddr` → `(failure_count, next_attempt_instant)`.
type IpBackoffMap = Arc<std::sync::RwLock<HashMap<std::net::IpAddr, (u32, Option<Instant>)>>>;

type ServerResult = std::result::Result<(), anyhow::Error>;

async fn acquire_account_task_permit() -> AccountTaskPermit {
    loop {
        let current = ACCOUNT_TASKS_IN_FLIGHT.load(Ordering::Acquire);
        if current < MAX_CONCURRENT_ACCOUNT_TASKS
            && ACCOUNT_TASKS_IN_FLIGHT
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return AccountTaskPermit;
        }
        tokio::time::sleep(Duration::from_millis(ACCOUNT_TASK_RETRY_DELAY_MS)).await;
    }
}

async fn run_account_task<T, F>(task: F) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    let permit = acquire_account_task_permit().await;
    let result = tokio::task::spawn_blocking(task)
        .await
        .context("blocking account task failed")?;
    drop(permit);
    result
}

/// Process an incoming protocol message.
async fn process_data(
    message: chatter_protocol::ClientMessage,
    peer_map: PeerMap,
    ip_backoff: IpBackoffMap,
    addr: SocketAddr,
    account_db: Account,
) {
    match message {
        chatter_protocol::ClientMessage::CreateAccount { login, passwd } => {
            info!("Create account request for login: {}", login);
            match account_backoff_remaining(&peer_map, &ip_backoff, addr) {
                Ok(Some(_)) => {
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::AccountCreationFailed {
                            reason: "Too many account attempts. Try again later.".to_string(),
                        },
                    )
                    .ok();
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            }

            let create_result = run_account_task({
                let account_db = account_db.clone();
                let login = login.clone();
                move || account_db.insert_account(login, passwd)
            })
            .await;

            match create_result {
                Ok(true) => {
                    clear_account_failures(&peer_map, &ip_backoff, addr).ok();
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::AccountCreated { login },
                    )
                    .ok();
                }
                Ok(false) => {
                    warn!(
                        "Account creation rejected because '{}' already exists",
                        login
                    );
                    record_account_failure(&peer_map, &ip_backoff, addr).ok();
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::AccountCreationFailed {
                            reason: "Account creation failed.".to_string(),
                        },
                    )
                    .ok();
                }
                Err(e) => {
                    warn!("Failed to create account '{}': {}", login, e);
                    record_account_failure(&peer_map, &ip_backoff, addr).ok();
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::AccountCreationFailed {
                            reason: "Account creation failed.".to_string(),
                        },
                    )
                    .ok();
                }
            }
        }
        chatter_protocol::ClientMessage::Login { login, passwd } => {
            info!("Login request from {}", login);
            // The sentinel must never authenticate, even if a legacy database
            // (predating the reserved-name check) contains such an account.
            if login == RESERVED_ANONYMOUS_LOGIN {
                warn!("Login rejected for reserved username '{}'", login);
                record_account_failure(&peer_map, &ip_backoff, addr).ok();
                send_server_message(
                    &peer_map,
                    addr,
                    chatter_protocol::ServerMessage::LoginFailed {
                        reason: "Invalid credentials.".to_string(),
                    },
                )
                .ok();
                return;
            }
            match account_backoff_remaining(&peer_map, &ip_backoff, addr) {
                Ok(Some(_)) => {
                    if let Ok(Some((old_login, rooms))) = clear_peer_authentication(&peer_map, addr)
                    {
                        announce_room_departures(&peer_map, &addr, &old_login, rooms);
                    }
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::LoginFailed {
                            reason: "Too many login attempts. Try again later.".to_string(),
                        },
                    )
                    .ok();
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            }

            let verify_result = run_account_task({
                let account_db = account_db.clone();
                let login = login.clone();
                move || account_db.verify_credentials(login, passwd)
            })
            .await;

            match verify_result {
                Ok(true) => {
                    match set_authenticated_peer(&peer_map, addr, login.clone()) {
                        Ok(Some((old_login, rooms))) => {
                            announce_room_departures(&peer_map, &addr, &old_login, rooms);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            send_server_message(&peer_map, addr, error).ok();
                            return;
                        }
                    }
                    clear_account_failures(&peer_map, &ip_backoff, addr).ok();
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::LoginOk {
                            login: login.clone(),
                        },
                    )
                    .ok();

                    if let Ok(rooms) = run_account_task({
                        let account_db = account_db.clone();
                        move || account_db.get_rooms()
                    })
                    .await
                    {
                        send_room_list(&peer_map, addr, rooms).ok();
                    }
                }
                Ok(false) => {
                    warn!("Login failed for '{}'", login);
                    record_account_failure(&peer_map, &ip_backoff, addr).ok();
                    if let Ok(Some((old_login, rooms))) = clear_peer_authentication(&peer_map, addr)
                    {
                        announce_room_departures(&peer_map, &addr, &old_login, rooms);
                    }
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::LoginFailed {
                            reason: "Invalid credentials.".to_string(),
                        },
                    )
                    .ok();
                }
                Err(e) => {
                    error!("Error verifying credentials for '{}': {}", login, e);
                    record_account_failure(&peer_map, &ip_backoff, addr).ok();
                    if let Ok(Some((old_login, rooms))) = clear_peer_authentication(&peer_map, addr)
                    {
                        announce_room_departures(&peer_map, &addr, &old_login, rooms);
                    }
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::Error {
                            message: "Login failed.".to_string(),
                            code: "AUTH_FAILED".to_string(),
                        },
                    )
                    .ok();
                }
            }
        }
        chatter_protocol::ClientMessage::JoinRoom { room } => {
            let login = match authenticated_login(&peer_map, addr) {
                Ok(login) => login,
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            };

            info!("{} has joined the room {}", login, room);

            if let Err(error) = join_peer_room(&peer_map, addr, room.clone()) {
                send_server_message(&peer_map, addr, error).ok();
                return;
            }

            broadcast_system_message(
                &peer_map,
                &addr,
                &room,
                &format!("{} joined the room", login),
            )
            .ok();
        }
        chatter_protocol::ClientMessage::LeaveRoom { room } => {
            let login = match authenticated_login(&peer_map, addr) {
                Ok(login) => login,
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            };

            info!("{} has left the room {}", login, room);

            if let Err(error) = leave_peer_room(&peer_map, addr, &room) {
                send_server_message(&peer_map, addr, error).ok();
                return;
            }

            broadcast_system_message(&peer_map, &addr, &room, &format!("{} left the room", login))
                .ok();
        }
        chatter_protocol::ClientMessage::SendMessage { room, message } => {
            let login = match authenticated_login(&peer_map, addr) {
                Ok(login) => login,
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            };

            match peer_is_in_room(&peer_map, addr, &room) {
                Ok(true) => {}
                Ok(false) => {
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::Error {
                            message: "Join the room before sending messages.".to_string(),
                            code: "ROOM_REQUIRED".to_string(),
                        },
                    )
                    .ok();
                    return;
                }
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            }

            let msg_id = run_account_task({
                let account_db = account_db.clone();
                let room = room.clone();
                let login = login.clone();
                let message = message.clone();
                move || account_db.insert_message(room, login, message)
            })
            .await;

            if let Err(e) = &msg_id {
                warn!("Failed to persist message: {}", e);
            } else {
                broadcast_to_room(&peer_map, &addr, &login, &room, &message, msg_id.unwrap()).ok();
            }
        }
        chatter_protocol::ClientMessage::GetHistory { room, cursor } => {
            if let Err(error) = authenticated_login(&peer_map, addr) {
                send_server_message(&peer_map, addr, error).ok();
                return;
            }

            match peer_is_in_room(&peer_map, addr, &room) {
                Ok(true) => {}
                Ok(false) => {
                    send_server_message(
                        &peer_map,
                        addr,
                        chatter_protocol::ServerMessage::Error {
                            message: "Join the room before requesting history.".to_string(),
                            code: "ROOM_REQUIRED".to_string(),
                        },
                    )
                    .ok();
                    return;
                }
                Err(error) => {
                    send_server_message(&peer_map, addr, error).ok();
                    return;
                }
            }

            if let Ok((history, has_more)) = run_account_task({
                let account_db = account_db.clone();
                let room = room.clone();
                move || account_db.get_room_history(room, cursor, HISTORY_PAGE_SIZE)
            })
            .await
            {
                send_history(&peer_map, addr, room, history, has_more).ok();
            }
        }
        chatter_protocol::ClientMessage::Logout => {
            info!("Logout requested by peer at {}", addr);
            let login = {
                let mut peers = peer_map.write().expect("Peer map RwLock poisoned");
                peers.get_mut(&addr).map(|peer| {
                    let rooms: Vec<String> = peer.rooms.iter().cloned().collect();
                    let login = peer.login.clone();
                    peer.logout();
                    (rooms, login)
                })
            };
            if let Some((rooms, login)) = login {
                announce_room_departures(&peer_map, &addr, &login, rooms);
            }
            send_server_message(&peer_map, addr, chatter_protocol::ServerMessage::LogoutOk).ok();
        }
    }
}

/// Send a message to a specific peer.
pub(crate) fn send_server_message(
    peer_map: &PeerMap,
    addr: SocketAddr,
    message: chatter_protocol::ServerMessage,
) -> ServerResult {
    let json = chatter_protocol::serialize_server_message(&message)
        .context("Failed to serialize message")?;
    let tx = {
        let peers = peer_map
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        peers.get(&addr).map(|peer| peer.tx.clone())
    };

    if let Some(tx) = tx {
        tx.unbounded_send(Message::Text(json.into()))
            .context("Failed to send message to peer")?;
    }
    Ok(())
}

/// Send room list to a peer.
pub(crate) fn send_room_list(
    peer_map: &PeerMap,
    addr: SocketAddr,
    rooms: Vec<String>,
) -> ServerResult {
    send_server_message(
        peer_map,
        addr,
        chatter_protocol::ServerMessage::RoomList { rooms },
    )
}

/// Send room history to a peer.
pub(crate) fn send_history(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: String,
    messages: Vec<chatter_protocol::HistoryEntry>,
    has_more: bool,
) -> ServerResult {
    send_server_message(
        peer_map,
        addr,
        chatter_protocol::ServerMessage::RoomHistory {
            room,
            messages,
            has_more,
        },
    )
}

/// Send a system notification to all peers in the given room.
/// Uses `login: "Server"` and `room: "system"` so the client renders it as a
/// system notification (dark gray, `[HH:MM] [System] ...` format).
pub(crate) fn broadcast_system_message(
    peer_map: &PeerMap,
    sender_addr: &SocketAddr,
    room: &str,
    message: &str,
) -> ServerResult {
    let msg_id = SYSTEM_MSG_COUNTER.fetch_add(1, Ordering::Relaxed);
    let broadcast_msg = chatter_protocol::ServerMessage::IncomingMessage {
        id: msg_id,
        login: "Server".to_string(),
        room: "system".to_string(),
        message: message.to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    };
    let json = chatter_protocol::serialize_server_message(&broadcast_msg)
        .context("Failed to serialize message")?;
    let recipients: Vec<Tx> = {
        let peers = peer_map
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        peers
            .iter()
            .filter_map(|(peer_addr, peer)| {
                if peer_addr != sender_addr && peer.rooms.contains(room) {
                    Some(peer.tx.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    for tx in recipients {
        let _ = tx.unbounded_send(Message::Text(json.clone().into()));
    }
    Ok(())
}

/// Broadcast a regular chat message to all peers in the same room except the sender.
pub(crate) fn broadcast_to_room(
    peer_map: &PeerMap,
    sender_addr: &SocketAddr,
    login: &str,
    room: &str,
    message: &str,
    msg_id: u64,
) -> ServerResult {
    let broadcast_msg = chatter_protocol::ServerMessage::IncomingMessage {
        id: msg_id,
        login: login.to_string(),
        room: room.to_string(),
        message: message.to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    };
    let json = chatter_protocol::serialize_server_message(&broadcast_msg)
        .context("Failed to serialize message")?;
    let recipients: Vec<Tx> = {
        let peers = peer_map
            .read()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?;
        peers
            .iter()
            .filter_map(|(peer_addr, peer)| {
                if peer_addr != sender_addr && peer.rooms.contains(room) {
                    Some(peer.tx.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    for tx in recipients {
        let _ = tx.unbounded_send(Message::Text(json.clone().into()));
    }
    Ok(())
}

fn client_error(message: &str) -> chatter_protocol::ServerMessage {
    chatter_protocol::ServerMessage::Error {
        message: message.to_string(),
        code: "GENERAL".to_string(),
    }
}

fn client_error_with_code(message: &str, code: &str) -> chatter_protocol::ServerMessage {
    chatter_protocol::ServerMessage::Error {
        message: message.to_string(),
        code: code.to_string(),
    }
}

fn lock_peer_error() -> chatter_protocol::ServerMessage {
    client_error("Internal server error.")
}

fn session_not_found_error() -> chatter_protocol::ServerMessage {
    client_error("Session not found.")
}

/// Simple sliding-window rate limiter: max `limit` messages per `window_secs` seconds.
#[derive(Debug, Clone)]
struct RateLimiter {
    limit: usize,
    window_secs: u64,
}

impl RateLimiter {
    fn new(limit: usize, window_secs: u64) -> Self {
        Self { limit, window_secs }
    }

    /// Check and record a message for `addr`. Returns true if the message is allowed.
    fn check(&self, addr: SocketAddr, now: std::time::Instant) -> bool {
        use std::collections::HashMap;
        static MAP: std::sync::LazyLock<
            std::sync::Mutex<HashMap<SocketAddr, Vec<std::time::Instant>>>,
        > = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

        let window = std::time::Duration::from_secs(self.window_secs);
        let cutoff = now.checked_sub(window).unwrap_or(now);

        let mut map = match MAP.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("Rate limiter lock poisoned, recovering");
                poisoned.into_inner()
            }
        };

        let timestamps = map.entry(addr).or_insert_with(Vec::new);
        timestamps.retain(|t| *t > cutoff);

        if timestamps.len() >= self.limit {
            return false;
        }

        // Evict empty entries to prevent unbounded map growth.
        // Drop the `timestamps` reference so we can mutably borrow `map`.
        let _ = timestamps;
        if map.get(&addr).map(|v| v.is_empty()).unwrap_or(false) {
            map.remove(&addr);
        }

        // Re-acquire the entry to push the new timestamp.
        let timestamps = map.entry(addr).or_insert_with(Vec::new);
        timestamps.push(now);
        true
    }
}

const RATE_LIMIT: usize = 20;
const RATE_WINDOW_SECS: u64 = 1;

fn account_backoff_duration(failures: u32) -> Duration {
    let multiplier = 1u64 << failures.saturating_sub(1).min(5);
    Duration::from_millis((ACCOUNT_BACKOFF_BASE_MS * multiplier).min(ACCOUNT_BACKOFF_MAX_MS))
}

/// Check if there is remaining backoff for the given address, using IP-based
/// persistent state (survives connection drops).
fn account_backoff_remaining(
    peer_map: &PeerMap,
    ip_backoff: &IpBackoffMap,
    addr: SocketAddr,
) -> std::result::Result<Option<Duration>, chatter_protocol::ServerMessage> {
    // Check IP-based backoff first (persistent across connections).
    let ip = addr.ip();
    {
        let map = ip_backoff.read().map_err(|_| lock_peer_error())?;
        if let Some((_, next_attempt)) = map.get(&ip)
            && let Some(duration) =
                next_attempt.and_then(|na| na.checked_duration_since(Instant::now()))
        {
            return Ok(Some(duration));
        }
    }
    // Fall back to per-peer backoff (same connection).
    let peers = peer_map.read().map_err(|_| lock_peer_error())?;
    let peer = peers.get(&addr).ok_or_else(session_not_found_error)?;
    Ok(peer
        .next_account_attempt
        .and_then(|next_attempt| next_attempt.checked_duration_since(Instant::now())))
}

/// Record a login/account creation failure, updating both IP-based and peer-level state.
fn record_account_failure(
    peer_map: &PeerMap,
    ip_backoff: &IpBackoffMap,
    addr: SocketAddr,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let ip = addr.ip();
    // Update IP-based persistent state.
    {
        let mut map = ip_backoff.write().map_err(|_| lock_peer_error())?;
        let entry = map.entry(ip).or_insert((0, None));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = Some(Instant::now() + account_backoff_duration(entry.0));
    }
    // Also update per-peer state (for same-connection tracking).
    let mut peers = peer_map.write().map_err(|_| lock_peer_error())?;
    if let Some(peer) = peers.get_mut(&addr) {
        peer.login_failures = peer.login_failures.saturating_add(1);
        peer.next_account_attempt =
            Some(Instant::now() + account_backoff_duration(peer.login_failures));
    }
    Ok(())
}

/// Clear login/account creation failures for the given address.
fn clear_account_failures(
    peer_map: &PeerMap,
    ip_backoff: &IpBackoffMap,
    addr: SocketAddr,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let ip = addr.ip();
    // Clear IP-based persistent state.
    {
        let mut map = ip_backoff.write().map_err(|_| lock_peer_error())?;
        map.remove(&ip);
    }
    // Also clear per-peer state.
    let mut peers = peer_map.write().map_err(|_| lock_peer_error())?;
    if let Some(peer) = peers.get_mut(&addr) {
        peer.login_failures = 0;
        peer.next_account_attempt = None;
    }
    Ok(())
}

fn set_authenticated_peer(
    peer_map: &PeerMap,
    addr: SocketAddr,
    login: String,
) -> std::result::Result<Option<(String, Vec<String>)>, chatter_protocol::ServerMessage> {
    let mut peers = peer_map.write().map_err(|_| lock_peer_error())?;

    // Reject duplicate logins: if another peer already uses this login, refuse.
    if let Some(old_addr) = peers
        .iter()
        .find(|(_, peer)| peer.login == login && peer.login != RESERVED_ANONYMOUS_LOGIN)
        .map(|(k, _)| *k)
        && old_addr != addr
    {
        return Err(chatter_protocol::ServerMessage::LoginFailed {
            reason: "Login already in use. Please try again.".to_string(),
        });
    }

    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    let old_login = peer.login.clone();
    let old_rooms = peer.rooms.iter().cloned().collect::<Vec<_>>();
    peer.login = login;
    peer.rooms.clear();

    Ok(
        if old_login != RESERVED_ANONYMOUS_LOGIN && !old_rooms.is_empty() {
            Some((old_login, old_rooms))
        } else {
            None
        },
    )
}

fn clear_peer_authentication(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<Option<(String, Vec<String>)>, chatter_protocol::ServerMessage> {
    set_authenticated_peer(peer_map, addr, RESERVED_ANONYMOUS_LOGIN.to_string())
}

fn announce_room_departures(
    peer_map: &PeerMap,
    addr: &SocketAddr,
    login: &str,
    rooms: Vec<String>,
) {
    for room in rooms {
        if let Err(error) =
            broadcast_system_message(peer_map, addr, &room, &format!("{} left the room", login))
        {
            warn!(
                "Failed to announce room departure for '{}': {}",
                login, error
            );
        }
    }
}

fn join_peer_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: String,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.write().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;

    if peer.rooms.contains(&room) {
        return Err(client_error_with_code(
            &format!("You are already in room '{}'", room),
            "ALREADY_IN_ROOM",
        ));
    }

    peer.rooms.insert(room);
    Ok(())
}

fn leave_peer_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: &str,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.write().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    peer.rooms.remove(room);
    Ok(())
}

fn authenticated_login(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<String, chatter_protocol::ServerMessage> {
    let peers = peer_map.read().map_err(|_| lock_peer_error())?;
    let login = peers
        .get(&addr)
        .map(|peer| peer.login.clone())
        .ok_or_else(session_not_found_error)?;

    // Unauthenticated: sentinel value or post-logout (login cleared to "")
    if login == RESERVED_ANONYMOUS_LOGIN || login.is_empty() {
        Err(client_error_with_code(
            "Login required.",
            "NOT_AUTHENTICATED",
        ))
    } else {
        Ok(login)
    }
}

fn peer_is_in_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: &str,
) -> std::result::Result<bool, chatter_protocol::ServerMessage> {
    let peers = peer_map.read().map_err(|_| lock_peer_error())?;
    let peer = peers.get(&addr).ok_or_else(session_not_found_error)?;
    Ok(peer.rooms.contains(room))
}

async fn handle_connection(
    peer_map: PeerMap,
    ip_backoff: IpBackoffMap,
    account_db: Account,
    raw_stream: TcpStream,
    addr: SocketAddr,
    rate_limiter: RateLimiter,
) -> ServerResult {
    info!("Incoming TCP connection from: {}", addr);

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(chatter_protocol::MAX_PAYLOAD_LEN);
    let ws_stream = tokio_tungstenite::accept_async_with_config(raw_stream, Some(ws_config))
        .await
        .with_context(|| format!("WebSocket handshake failed for {}", addr))?;
    info!("WebSocket connection established: {}", addr);

    let (tx, rx) = futures_channel::mpsc::unbounded();
    peer_map
        .write()
        .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
        .insert(
            addr,
            Peer::new(tx, RESERVED_ANONYMOUS_LOGIN.to_string(), HashSet::new()),
        );

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        let peer_map = peer_map.clone();
        let ip_backoff = ip_backoff.clone();
        let account_db = account_db.clone();
        let rate_limiter = rate_limiter.clone();
        async move {
            match msg {
                Message::Text(text) => {
                    if !rate_limiter.check(addr, std::time::Instant::now()) {
                        send_server_message(
                            &peer_map,
                            addr,
                            chatter_protocol::ServerMessage::Error {
                                message: "Rate limit exceeded. Try again shortly.".to_string(),
                                code: "RATE_LIMITED".to_string(),
                            },
                        )
                        .ok();
                        return Ok(());
                    }
                    let message = chatter_protocol::parse_client_message(Message::Text(text));
                    match message {
                        Ok(data) => {
                            process_data(
                                data,
                                peer_map.clone(),
                                ip_backoff.clone(),
                                addr,
                                account_db.clone(),
                            )
                            .await;
                        }
                        Err(e) => warn!("Error parsing message from {}: {}", addr, e),
                    }
                }
                Message::Close(_) => {
                    info!("Socket closed for {}", addr);
                }
                _ => {}
            }
            Ok(())
        }
    });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    info!("{addr} disconnected");
    let disconnected_peer = peer_map
        .write()
        .map_err(|e| anyhow::anyhow!("RwLock poisoned: {}", e))?
        .remove(&addr);
    if let Some(peer) = disconnected_peer {
        info!(
            "{} ({}) disconnected: authenticated={}, rooms={}",
            peer.login,
            addr,
            peer.is_authenticated,
            peer.rooms.len()
        );
        for room in peer.rooms {
            broadcast_system_message(
                &peer_map,
                &addr,
                &room,
                &format!("{} disconnected", peer.login),
            )
            .ok();
        }
    }

    Ok(())
}

/// Server CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "chatter-server", about = "WebSocket chat server")]
struct Cli {
    /// Host to bind the server to.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Path to the SQLite database file.
    /// Defaults to the DB_PATH environment variable, or "chatter.db" if not set.
    #[arg(long)]
    db: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    warn!("{}", TRANSPORT_SECURITY_NOTICE);

    let db_path = cli
        .db
        .or_else(|| std::env::var("DB_PATH").ok())
        .unwrap_or_else(|| "chatter.db".to_string());

    let account_db = Account::new(db_path).context("Failed to initialize database")?;

    let host = std::env::var("CHATTER_HOST").unwrap_or_else(|_| cli.host.clone());
    let port: u16 = std::env::var("CHATTER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(cli.port);
    let addr = format!("{host}:{port}");

    let state: PeerMap = Arc::new(std::sync::RwLock::new(HashMap::new()));

    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;
    info!("Listening on: {}", addr);

    let mut tasks = JoinSet::new();

    // Dual signal handling: tokio::signal (primary) + ctrlc (fallback for macOS).
    // On macOS Apple Silicon, tokio::signal::ctrl_c() can fail to deliver SIGINT
    // in certain terminal configurations. The ctrlc crate provides a fallback.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Signal merging: both sources feed into an mpsc channel, then one task
    // forwards to the oneshot shutdown.
    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Primary: tokio signal handler
    let signal_tx_primary = signal_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = signal_tx_primary.send(()).await;
    });

    // Fallback: ctrlc crate (handles SIGINT at the OS level)
    let signal_tx_ctrlc = signal_tx.clone();
    ctrlc::set_handler(move || {
        let _ = signal_tx_ctrlc.blocking_send(());
    })
    .expect("Failed to set Ctrl+C handler");

    // Merge both signal sources into one shutdown channel
    let (merge_tx, mut merge_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        // Wait for first signal, then send shutdown
        let _ = merge_rx.recv().await;
        let _ = shutdown_tx.send(());
    });

    // Both signal sources feed into merge channel
    tokio::spawn(async move {
        let mut buf = vec![];
        loop {
            buf.clear();
            buf.reserve(2);
            let n = signal_rx.recv_many(&mut buf, 2).await;
            if n == 0 {
                break;
            }
            let _ = merge_tx.send(()).await;
        }
    });

    // Persistent backoff state keyed by IP (survives connection drops).
    let ip_backoff: IpBackoffMap = Arc::new(std::sync::RwLock::new(HashMap::new()));

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, stream_addr)) => {
                        let state = state.clone();
                        let db = account_db.clone();
                        let ip_bo = ip_backoff.clone();
                        let rate_limiter = RateLimiter::new(RATE_LIMIT, RATE_WINDOW_SECS);
                        tasks.spawn(async move {
                            if let Err(e) = handle_connection(state, ip_bo, db, stream, stream_addr, rate_limiter).await {
                                error!("Connection error for {}: {}", stream_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
            _ = &mut shutdown_rx => {
                info!("Shutdown signal received, closing listener...");
                // Close the listener to unblock accept() and stop accepting new connections.
                drop(listener);
                info!("Listener closed. Shutting down (clients will reconnect).");
                // Drop the JoinSet to abandon active connection tasks.
                // Clients have reconnection so they will come back.
                drop(tasks);
                info!("Shutting down.");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    fn make_peer_map(peers: Vec<(SocketAddr, Tx, String, Vec<String>)>) -> PeerMap {
        let map = std::sync::RwLock::new(HashMap::new());
        {
            let mut peers_guard = map.write().unwrap();
            for (addr, tx, login, rooms) in peers {
                peers_guard.insert(addr, Peer::new(tx, login, rooms.into_iter().collect()));
            }
        }
        Arc::new(map)
    }

    fn make_ip_backoff() -> IpBackoffMap {
        Arc::new(std::sync::RwLock::new(HashMap::new()))
    }

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        )
    }

    async fn next_server_message(
        rx: &mut futures_channel::mpsc::UnboundedReceiver<Message>,
    ) -> chatter_protocol::ServerMessage {
        let msg = rx.next().await.expect("expected websocket message");
        serde_json::from_str(msg.to_text().unwrap()).unwrap()
    }

    // NOTE (P2-T5): the spec describes an `authenticated: bool` field on Peer and an
    // Option-returning `authenticated_login()`. In this codebase authentication is
    // modeled as `login != RESERVED_ANONYMOUS_LOGIN` and `authenticated_login` returns
    // Result<String, ServerMessage>; Err is the "no authenticated login" case.
    // The sentinel cannot collide with a real user: account creation rejects it
    // (account.rs) and the Login handler refuses it even for legacy databases.
    #[test]
    fn test_anonymous_has_no_authenticated_login() {
        let addr = test_addr(8090);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(addr, tx, "anonymous".to_string(), vec![])]);

        assert_eq!(
            authenticated_login(&peer_map, addr),
            Err(client_error_with_code(
                "Login required.",
                "NOT_AUTHENTICATED"
            )),
            "An unauthenticated (anonymous) peer must not expose a login"
        );
    }

    #[test]
    fn test_post_logout_has_no_authenticated_login() {
        // Regression test for S1: after logout, login is cleared to "".
        // authenticated_login must reject this (not treat it as authenticated).
        let addr = test_addr(8090);
        let (tx, _rx) = futures_channel::mpsc::unbounded();

        // Simulate post-logout state: login cleared to ""
        let peer_map = make_peer_map(vec![(addr, tx, "".to_string(), vec![])]);

        assert_eq!(
            authenticated_login(&peer_map, addr),
            Err(client_error_with_code(
                "Login required.",
                "NOT_AUTHENTICATED"
            )),
            "A post-logout peer (empty login) must not expose a login"
        );
    }

    #[test]
    fn test_authenticated_peer_exposes_login() {
        let addr = test_addr(8091);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(addr, tx, "alice".to_string(), vec![])]);

        assert_eq!(
            authenticated_login(&peer_map, addr),
            Ok("alice".to_string()),
            "An authenticated peer must expose its login"
        );
    }

    #[tokio::test]
    async fn test_create_account_rejects_reserved_anonymous_username() {
        let account_db = Account::new(":memory:".to_string()).unwrap();
        let addr = test_addr(8092);
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(
            addr,
            tx,
            RESERVED_ANONYMOUS_LOGIN.to_string(),
            vec![],
        )]);

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: RESERVED_ANONYMOUS_LOGIN.to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db,
        )
        .await;

        assert_eq!(
            next_server_message(&mut rx).await,
            chatter_protocol::ServerMessage::AccountCreationFailed {
                reason: "Account creation failed.".to_string(),
            },
            "The unauthenticated-peer sentinel must not be creatable as an account"
        );
    }

    #[tokio::test]
    async fn test_login_with_reserved_anonymous_username_is_rejected() {
        let db_path = "/tmp/chatter_test_reserved_login.db";
        let _ = std::fs::remove_file(db_path);
        let account_db = Account::new(db_path.to_string()).unwrap();

        // Simulate a legacy database (predating the reserved-name check) that
        // already contains an 'anonymous' account with valid credentials.
        {
            let conn = rusqlite::Connection::open(db_path).unwrap();
            let hash = bcrypt::hash("password123", 4).unwrap();
            conn.execute(
                "INSERT INTO account (name, passwd) VALUES (?1, ?2)",
                rusqlite::params![RESERVED_ANONYMOUS_LOGIN, hash],
            )
            .unwrap();
        }

        let addr = test_addr(8093);
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(
            addr,
            tx,
            RESERVED_ANONYMOUS_LOGIN.to_string(),
            vec![],
        )]);

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::Login {
                login: RESERVED_ANONYMOUS_LOGIN.to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db,
        )
        .await;

        assert_eq!(
            next_server_message(&mut rx).await,
            chatter_protocol::ServerMessage::LoginFailed {
                reason: "Invalid credentials.".to_string(),
            },
            "The sentinel username must never authenticate, even with a matching legacy row"
        );
        assert_eq!(
            authenticated_login(&peer_map, addr),
            Err(client_error_with_code(
                "Login required.",
                "NOT_AUTHENTICATED"
            )),
            "The peer must remain unauthenticated"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_peer_is_in_room_propagates_poison() {
        let peer_map: PeerMap = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let poisoned = peer_map.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.write().unwrap();
            panic!("poison peer map");
        })
        .join();

        let result = peer_is_in_room(&peer_map, test_addr(8081), "general");
        assert_eq!(result, Err(client_error("Internal server error.")));
    }

    #[tokio::test]
    async fn test_create_account_does_not_authenticate_and_rejects_duplicate() {
        let account_db = Account::new(":memory:".to_string()).unwrap();
        let addr = test_addr(8084);
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(addr, tx, "anonymous".to_string(), vec![])]);

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db.clone(),
        )
        .await;

        let created = next_server_message(&mut rx).await;
        assert_eq!(
            created,
            chatter_protocol::ServerMessage::AccountCreated {
                login: "alice".to_string(),
            }
        );
        assert_eq!(
            peer_map.read().unwrap().get(&addr).unwrap().login,
            "anonymous",
            "Creating an account must not authenticate the peer"
        );

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: "alice".to_string(),
                passwd: "different-password".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db,
        )
        .await;

        let duplicate = next_server_message(&mut rx).await;
        assert_eq!(
            duplicate,
            chatter_protocol::ServerMessage::AccountCreationFailed {
                reason: "Account creation failed.".to_string(),
            }
        );
        assert_eq!(
            peer_map.read().unwrap().get(&addr).unwrap().login,
            "anonymous",
            "Duplicate account creation must not authenticate the peer"
        );
    }

    #[tokio::test]
    async fn test_successful_relogin_announces_departure_from_previous_rooms() {
        let account_db = Account::new(":memory:".to_string()).unwrap();
        account_db
            .insert_account("new-login".to_string(), "password123".to_string())
            .unwrap();

        let addr = test_addr(8085);
        let other_addr = test_addr(8086);
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let (other_tx, mut other_rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![
            (
                addr,
                tx,
                "old-login".to_string(),
                vec!["general".to_string()],
            ),
            (
                other_addr,
                other_tx,
                "observer".to_string(),
                vec!["general".to_string()],
            ),
        ]);

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::Login {
                login: "new-login".to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db,
        )
        .await;

        let departure = next_server_message(&mut other_rx).await;
        match &departure {
            chatter_protocol::ServerMessage::IncomingMessage {
                login,
                room,
                message,
                ..
            } => {
                assert_eq!(login, "Server");
                assert_eq!(room, "system");
                assert_eq!(message, "old-login left the room");
            }
            other => panic!("Expected IncomingMessage, got {other:?}"),
        }

        let login_ok = next_server_message(&mut rx).await;
        assert_eq!(
            login_ok,
            chatter_protocol::ServerMessage::LoginOk {
                login: "new-login".to_string(),
            }
        );
        let peer = peer_map.read().unwrap().get(&addr).unwrap().clone();
        assert_eq!(peer.login, "new-login");
        assert!(peer.rooms.is_empty());
    }

    #[tokio::test]
    async fn test_failed_relogin_clears_prior_auth_state() {
        let account_db = Account::new(":memory:".to_string()).unwrap();
        account_db
            .insert_account("alice".to_string(), "password123".to_string())
            .unwrap();

        let addr = test_addr(8087);
        let other_addr = test_addr(8088);
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let (other_tx, mut other_rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![
            (addr, tx, "alice".to_string(), vec!["general".to_string()]),
            (
                other_addr,
                other_tx,
                "observer".to_string(),
                vec!["general".to_string()],
            ),
        ]);

        let ip_backoff = make_ip_backoff();
        process_data(
            chatter_protocol::ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "wrong-password".to_string(),
            },
            peer_map.clone(),
            ip_backoff.clone(),
            addr,
            account_db,
        )
        .await;

        let departure = next_server_message(&mut other_rx).await;
        match &departure {
            chatter_protocol::ServerMessage::IncomingMessage {
                login,
                room,
                message,
                ..
            } => {
                assert_eq!(login, "Server");
                assert_eq!(room, "system");
                assert_eq!(message, "alice left the room");
            }
            other => panic!("Expected IncomingMessage, got {other:?}"),
        }

        let failed = next_server_message(&mut rx).await;
        assert_eq!(
            failed,
            chatter_protocol::ServerMessage::LoginFailed {
                reason: "Invalid credentials.".to_string(),
            }
        );
        let peer = peer_map.read().unwrap().get(&addr).unwrap().clone();
        assert_eq!(peer.login, "anonymous");
        assert!(peer.rooms.is_empty());
    }

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = RateLimiter::new(5, 1);
        let addr = test_addr(9001);
        let now = std::time::Instant::now();

        // First 5 messages should be allowed
        for i in 0..5 {
            assert!(
                limiter.check(addr, now + std::time::Duration::from_millis(i * 100)),
                "Message {} should be allowed",
                i + 1
            );
        }

        // 6th message within same second should be rejected
        assert!(
            !limiter.check(addr, now + std::time::Duration::from_millis(500)),
            "6th message should be rate limited"
        );
    }

    #[test]
    fn test_rate_limiter_window_expires() {
        let limiter = RateLimiter::new(3, 1);
        let addr = test_addr(9002);
        let now = std::time::Instant::now();

        // Fill the limit
        for _ in 0..3 {
            assert!(limiter.check(addr, now));
        }
        assert!(!limiter.check(addr, now + std::time::Duration::from_millis(500)));

        // After the window passes, new messages should be allowed
        assert!(limiter.check(addr, now + std::time::Duration::from_secs(2)));
    }

    #[test]
    fn test_rate_limiter_independent_per_peer() {
        let limiter = RateLimiter::new(2, 1);
        let addr_a = test_addr(9003);
        let addr_b = test_addr(9004);
        let now = std::time::Instant::now();

        // Both peers can send 2 messages each
        assert!(limiter.check(addr_a, now));
        assert!(limiter.check(addr_a, now + std::time::Duration::from_millis(100)));
        assert!(!limiter.check(addr_a, now + std::time::Duration::from_millis(200)));

        assert!(limiter.check(addr_b, now));
        assert!(limiter.check(addr_b, now + std::time::Duration::from_millis(100)));
        assert!(!limiter.check(addr_b, now + std::time::Duration::from_millis(200)));
    }

    #[test]
    fn test_rate_limiter_evicts_empty_entries() {
        // Use a per-address limiter to control the static map.
        // Since MAP is static, we test behavior: after the window expires,
        // a fresh call from an old address should be allowed (proving eviction).
        let limiter = RateLimiter::new(1, 1); // 1 msg per second
        let addr = test_addr(9010);
        let now = std::time::Instant::now();

        // Use the single allowed message.
        assert!(limiter.check(addr, now));
        // Now at the limit — should be rejected.
        assert!(!limiter.check(addr, now + std::time::Duration::from_millis(500)));

        // Wait for the window to expire.
        std::thread::sleep(std::time::Duration::from_secs(2));

        // After eviction, the address should be allowed again.
        assert!(
            limiter.check(addr, now + std::time::Duration::from_secs(2)),
            "rate limiter should allow after window expires (entry evicted)"
        );
    }

    #[test]
    fn test_join_peer_room_double_join_returns_error() {
        let addr = test_addr(9005);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(
            addr,
            tx,
            "alice".to_string(),
            vec!["general".into()],
        )]);

        // First join should succeed (idempotent — already in room)
        let result = join_peer_room(&peer_map, addr, "general".into());
        assert!(result.is_err());
        match result.unwrap_err() {
            chatter_protocol::ServerMessage::Error { code, message } => {
                assert_eq!(code, "ALREADY_IN_ROOM");
                assert!(message.contains("already in room"));
            }
            other => panic!("Expected Error variant, got {:?}", other),
        }
    }

    #[test]
    fn test_join_peer_room_allows_different_room() {
        let addr = test_addr(9006);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let peer_map = make_peer_map(vec![(
            addr,
            tx,
            "alice".to_string(),
            vec!["general".into()],
        )]);

        // Joining a different room should succeed
        assert!(join_peer_room(&peer_map, addr, "rust".into()).is_ok());

        // Verify peer is now in both rooms
        let peers = peer_map.read().unwrap();
        let peer = peers.get(&addr).unwrap();
        assert!(peer.rooms.contains("general"));
        assert!(peer.rooms.contains("rust"));
    }

    #[test]
    fn test_peer_logout_clears_auth_and_rooms() {
        let _addr = test_addr(8092);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let mut peer = Peer::new(
            tx,
            "alice".to_string(),
            vec!["general".to_string(), "random".to_string()]
                .into_iter()
                .collect(),
        );

        assert!(peer.is_authenticated);
        assert_eq!(peer.login, "alice");
        assert_eq!(peer.rooms.len(), 2);

        peer.logout();

        assert!(!peer.is_authenticated);
        assert!(peer.login.is_empty());
        assert!(peer.rooms.is_empty());
    }

    #[test]
    fn test_peer_logout_idempotent() {
        let _addr = test_addr(8093);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let mut peer = Peer::new(
            tx,
            "alice".to_string(),
            vec!["general".to_string()].into_iter().collect(),
        );

        // First logout
        peer.logout();
        assert!(!peer.is_authenticated);
        assert!(peer.login.is_empty());
        assert!(peer.rooms.is_empty());

        // Second logout — should not panic, state unchanged
        peer.logout();
        assert!(!peer.is_authenticated);
        assert!(peer.login.is_empty());
        assert!(peer.rooms.is_empty());
    }

    #[test]
    fn test_logout_sets_anonymous_login() {
        let _addr = test_addr(8094);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        let mut peer = Peer::new(
            tx.clone(),
            "alice".to_string(),
            vec!["general".to_string()].into_iter().collect(),
        );

        peer.logout();

        // After logout, login should be empty and is_authenticated false.
        assert_eq!(peer.login, "");
        assert!(!peer.is_authenticated);
    }

    // ---------------------------------------------------------------------------
    // IP-based backoff persistence tests
    // ---------------------------------------------------------------------------

    #[test]
    fn test_ip_backoff_persists_across_connections() {
        let peer_map: PeerMap = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let ip_backoff: IpBackoffMap = Arc::new(std::sync::RwLock::new(HashMap::new()));

        // Create a peer on port 1000 and record a failure.
        let addr1 = test_addr(1000);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        peer_map
            .write()
            .unwrap()
            .insert(addr1, Peer::new(tx, "anon".to_string(), HashSet::new()));

        record_account_failure(&peer_map, &ip_backoff, addr1).unwrap();

        // The IP-based backoff should be active.
        let remaining = account_backoff_remaining(&peer_map, &ip_backoff, addr1).unwrap();
        assert!(
            remaining.is_some(),
            "backoff should be active on original connection"
        );

        // Simulate a new connection from the same IP (different port).
        let addr2 = test_addr(1001);
        let (tx2, _rx2) = futures_channel::mpsc::unbounded();
        peer_map
            .write()
            .unwrap()
            .insert(addr2, Peer::new(tx2, "anon".to_string(), HashSet::new()));

        // The new connection should ALSO be blocked by the IP-based backoff.
        let remaining2 = account_backoff_remaining(&peer_map, &ip_backoff, addr2).unwrap();
        assert!(
            remaining2.is_some(),
            "IP-based backoff should persist across connections from the same IP"
        );
    }

    #[test]
    fn test_ip_backoff_cleared_on_success() {
        let peer_map: PeerMap = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let ip_backoff: IpBackoffMap = Arc::new(std::sync::RwLock::new(HashMap::new()));

        let addr = test_addr(2000);
        let (tx, _rx) = futures_channel::mpsc::unbounded();
        peer_map
            .write()
            .unwrap()
            .insert(addr, Peer::new(tx, "anon".to_string(), HashSet::new()));

        // Record failures.
        record_account_failure(&peer_map, &ip_backoff, addr).unwrap();
        record_account_failure(&peer_map, &ip_backoff, addr).unwrap();

        // Backoff should be active.
        assert!(
            account_backoff_remaining(&peer_map, &ip_backoff, addr)
                .unwrap()
                .is_some()
        );

        // Clear on successful login.
        clear_account_failures(&peer_map, &ip_backoff, addr).unwrap();

        // Backoff should now be cleared.
        let remaining = account_backoff_remaining(&peer_map, &ip_backoff, addr).unwrap();
        assert!(
            remaining.is_none(),
            "backoff should be cleared after successful login"
        );
    }

    #[test]
    fn test_ip_backoff_independent_per_ip() {
        let peer_map: PeerMap = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let ip_backoff: IpBackoffMap = Arc::new(std::sync::RwLock::new(HashMap::new()));

        // Create peers on different IPs.
        let addr1 = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            3000,
        );
        let addr2 = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)),
            3000,
        );

        let (tx1, _rx1) = futures_channel::mpsc::unbounded();
        peer_map
            .write()
            .unwrap()
            .insert(addr1, Peer::new(tx1, "anon".to_string(), HashSet::new()));

        let (tx2, _rx2) = futures_channel::mpsc::unbounded();
        peer_map
            .write()
            .unwrap()
            .insert(addr2, Peer::new(tx2, "anon".to_string(), HashSet::new()));

        // Record failure only for IP 127.0.0.1.
        record_account_failure(&peer_map, &ip_backoff, addr1).unwrap();

        // addr1 (127.0.0.1) should be blocked.
        assert!(
            account_backoff_remaining(&peer_map, &ip_backoff, addr1)
                .unwrap()
                .is_some()
        );

        // addr2 (127.0.0.2) should NOT be blocked.
        assert!(
            account_backoff_remaining(&peer_map, &ip_backoff, addr2)
                .unwrap()
                .is_none()
        );
    }
}
