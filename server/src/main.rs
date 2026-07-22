use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use clap::Parser;

use futures_channel::mpsc::UnboundedSender;
use futures_util::{StreamExt, future, pin_mut, stream::TryStreamExt};

use anyhow::Context;
use log::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

use crate::account::{Account, RESERVED_ANONYMOUS_LOGIN};
pub mod account;

type Tx = UnboundedSender<Message>;

const MAX_CONCURRENT_ACCOUNT_TASKS: usize = 64;
const ACCOUNT_TASK_RETRY_DELAY_MS: u64 = 10;
const ACCOUNT_BACKOFF_BASE_MS: u64 = 250;
const ACCOUNT_BACKOFF_MAX_MS: u64 = 8_000;
const TRANSPORT_SECURITY_NOTICE: &str = "SECURITY: this server accepts plain ws:// connections. Deploy it only behind a TLS-terminating reverse proxy (wss:// to clients), otherwise credentials cross the network in cleartext.";

static ACCOUNT_TASKS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

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
}

impl Peer {
    fn new(tx: Tx, login: String, rooms: HashSet<String>) -> Self {
        Self {
            tx,
            login,
            rooms,
            login_failures: 0,
            next_account_attempt: None,
        }
    }
}

type PeerMap = Arc<std::sync::Mutex<HashMap<SocketAddr, Peer>>>;

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
    addr: SocketAddr,
    account_db: Account,
) {
    match message {
        chatter_protocol::ClientMessage::CreateAccount { login, passwd } => {
            info!("Create account request for login: {}", login);
            match account_backoff_remaining(&peer_map, addr) {
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
                    clear_account_failures(&peer_map, addr).ok();
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
                    record_account_failure(&peer_map, addr).ok();
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
                    record_account_failure(&peer_map, addr).ok();
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
                record_account_failure(&peer_map, addr).ok();
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
            match account_backoff_remaining(&peer_map, addr) {
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
                    clear_account_failures(&peer_map, addr).ok();
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
                    record_account_failure(&peer_map, addr).ok();
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
                    record_account_failure(&peer_map, addr).ok();
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

            broadcast_to_room(
                &peer_map,
                &addr,
                &login,
                &room,
                &format!("[System] {} joined the room", login),
            )
            .ok();

            if let Ok(history) = run_account_task({
                let account_db = account_db.clone();
                let room = room.clone();
                move || account_db.get_room_history(room, i32::MAX)
            })
            .await
            {
                send_history(&peer_map, addr, room.clone(), history).ok();
            }
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

            broadcast_to_room(
                &peer_map,
                &addr,
                &login,
                &room,
                &format!("[System] {} left the room", login),
            )
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

            if let Err(e) = run_account_task({
                let account_db = account_db.clone();
                let room = room.clone();
                let login = login.clone();
                let message = message.clone();
                move || account_db.insert_message(room, login, message)
            })
            .await
            {
                warn!("Failed to persist message: {}", e);
            }

            broadcast_to_room(&peer_map, &addr, &login, &room, &message).ok();
        }
        chatter_protocol::ClientMessage::GetHistory { room } => {
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

            if let Ok(history) = run_account_task({
                let account_db = account_db.clone();
                let room = room.clone();
                move || account_db.get_room_history(room, i32::MAX)
            })
            .await
            {
                send_history(&peer_map, addr, room, history).ok();
            }
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
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?;
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
) -> ServerResult {
    send_server_message(
        peer_map,
        addr,
        chatter_protocol::ServerMessage::RoomHistory { room, messages },
    )
}

/// Broadcast a message to all peers in the same room except the sender.
pub(crate) fn broadcast_to_room(
    peer_map: &PeerMap,
    sender_addr: &SocketAddr,
    login: &str,
    room: &str,
    message: &str,
) -> ServerResult {
    let broadcast_msg = chatter_protocol::ServerMessage::IncomingMessage {
        login: login.to_string(),
        room: room.to_string(),
        message: message.to_string(),
    };
    let json = chatter_protocol::serialize_server_message(&broadcast_msg)
        .context("Failed to serialize message")?;
    let recipients: Vec<Tx> = {
        let peers = peer_map
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?;
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

        let mut map = MAP.lock().unwrap();

        let timestamps = map.entry(addr).or_insert_with(Vec::new);
        timestamps.retain(|t| *t > cutoff);

        if timestamps.len() >= self.limit {
            return false;
        }

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

fn account_backoff_remaining(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<Option<Duration>, chatter_protocol::ServerMessage> {
    let peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get(&addr).ok_or_else(session_not_found_error)?;
    Ok(peer.next_account_attempt.and_then(|next_attempt| {
        next_attempt.checked_duration_since(Instant::now())
    }))
}

fn record_account_failure(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    peer.login_failures = peer.login_failures.saturating_add(1);
    peer.next_account_attempt = Some(Instant::now() + account_backoff_duration(peer.login_failures));
    Ok(())
}

fn clear_account_failures(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    peer.login_failures = 0;
    peer.next_account_attempt = None;
    Ok(())
}

fn set_authenticated_peer(
    peer_map: &PeerMap,
    addr: SocketAddr,
    login: String,
) -> std::result::Result<Option<(String, Vec<String>)>, chatter_protocol::ServerMessage> {
    let mut peers = peer_map.lock().map_err(|_| lock_peer_error())?;

    // If this login is already used by a different peer, kick the old one
    let old_addr = peers.iter()
        .find(|(_, peer)| peer.login == login && peer.login != RESERVED_ANONYMOUS_LOGIN)
        .map(|(k, _)| *k);

    if let Some(kick_addr) = old_addr {
        if kick_addr != addr {
            // Remove the old peer entry and announce departures
            if let Some(old_peer) = peers.remove(&kick_addr) {
                announce_room_departures(peer_map, &kick_addr, &old_peer.login, old_peer.rooms.iter().cloned().collect());
                // Close the old connection by dropping the tx
                drop(old_peer.tx);
            }
        }
    }

    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    let old_login = peer.login.clone();
    let old_rooms = peer.rooms.iter().cloned().collect::<Vec<_>>();
    peer.login = login;
    peer.rooms.clear();
    if old_login != RESERVED_ANONYMOUS_LOGIN && !old_rooms.is_empty() {
        Ok(Some((old_login, old_rooms)))
    } else {
        Ok(None)
    }
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
        if let Err(error) = broadcast_to_room(
            peer_map,
            addr,
            login,
            &room,
            &format!("[System] {} left the room", login),
        ) {
            warn!("Failed to announce room departure for '{}': {}", login, error);
        }
    }
}

fn join_peer_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: String,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    peer.rooms.insert(room);
    Ok(())
}

fn leave_peer_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: &str,
) -> std::result::Result<(), chatter_protocol::ServerMessage> {
    let mut peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get_mut(&addr).ok_or_else(session_not_found_error)?;
    peer.rooms.remove(room);
    Ok(())
}

fn authenticated_login(
    peer_map: &PeerMap,
    addr: SocketAddr,
) -> std::result::Result<String, chatter_protocol::ServerMessage> {
    let peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let login = peers
        .get(&addr)
        .map(|peer| peer.login.clone())
        .ok_or_else(session_not_found_error)?;

    if login == RESERVED_ANONYMOUS_LOGIN {
        Err(client_error_with_code("Login required.", "NOT_AUTHENTICATED"))
    } else {
        Ok(login)
    }
}

fn peer_is_in_room(
    peer_map: &PeerMap,
    addr: SocketAddr,
    room: &str,
) -> std::result::Result<bool, chatter_protocol::ServerMessage> {
    let peers = peer_map.lock().map_err(|_| lock_peer_error())?;
    let peer = peers.get(&addr).ok_or_else(session_not_found_error)?;
    Ok(peer.rooms.contains(room))
}

async fn handle_connection(
    peer_map: PeerMap,
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
        .lock()
        .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
        .insert(
            addr,
            Peer::new(tx, RESERVED_ANONYMOUS_LOGIN.to_string(), HashSet::new()),
        );

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        let peer_map = peer_map.clone();
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
                            process_data(data, peer_map.clone(), addr, account_db.clone()).await;
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

    info!("{} disconnected", addr);
    let disconnected_peer = peer_map
        .lock()
        .map_err(|e| anyhow::anyhow!("Mutex poisoned: {}", e))?
        .remove(&addr);
    if let Some(peer) = disconnected_peer {
        for room in peer.rooms {
            broadcast_to_room(
                &peer_map,
                &addr,
                &peer.login,
                &room,
                &format!("[System] {} disconnected", peer.login),
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    warn!("{}", TRANSPORT_SECURITY_NOTICE);

    let account_db =
        Account::new("chatter.db".to_string()).context("Failed to initialize database")?;

    let addr = format!("{}:{}", cli.host, cli.port);

    let state: PeerMap = Arc::new(std::sync::Mutex::new(HashMap::new()));

    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;
    info!("Listening on: {}", addr);

    while let Ok((stream, stream_addr)) = listener.accept().await {
        let state = state.clone();
        let db = account_db.clone();
        let rate_limiter = RateLimiter::new(RATE_LIMIT, RATE_WINDOW_SECS);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(state, db, stream, stream_addr, rate_limiter).await {
                error!("Connection error for {}: {}", stream_addr, e);
            }
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    fn make_peer_map(peers: Vec<(SocketAddr, Tx, String, Vec<String>)>) -> PeerMap {
        let map = std::sync::Mutex::new(HashMap::new());
        {
            let mut peers_guard = map.lock().unwrap();
            for (addr, tx, login, rooms) in peers {
                peers_guard.insert(
                    addr,
                    Peer::new(tx, login, rooms.into_iter().collect()),
                );
            }
        }
        Arc::new(map)
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
            Err(client_error_with_code("Login required.", "NOT_AUTHENTICATED")),
            "An unauthenticated (anonymous) peer must not expose a login"
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
        let peer_map = make_peer_map(vec![(addr, tx, RESERVED_ANONYMOUS_LOGIN.to_string(), vec![])]);

        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: RESERVED_ANONYMOUS_LOGIN.to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
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
        let peer_map = make_peer_map(vec![(addr, tx, RESERVED_ANONYMOUS_LOGIN.to_string(), vec![])]);

        process_data(
            chatter_protocol::ClientMessage::Login {
                login: RESERVED_ANONYMOUS_LOGIN.to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
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
            Err(client_error_with_code("Login required.", "NOT_AUTHENTICATED")),
            "The peer must remain unauthenticated"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn test_peer_is_in_room_propagates_poison() {
        let peer_map: PeerMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let poisoned = peer_map.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().unwrap();
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

        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: "alice".to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
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
            peer_map.lock().unwrap().get(&addr).unwrap().login,
            "anonymous",
            "Creating an account must not authenticate the peer"
        );

        process_data(
            chatter_protocol::ClientMessage::CreateAccount {
                login: "alice".to_string(),
                passwd: "different-password".to_string(),
            },
            peer_map.clone(),
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
            peer_map.lock().unwrap().get(&addr).unwrap().login,
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

        process_data(
            chatter_protocol::ClientMessage::Login {
                login: "new-login".to_string(),
                passwd: "password123".to_string(),
            },
            peer_map.clone(),
            addr,
            account_db,
        )
        .await;

        let departure = next_server_message(&mut other_rx).await;
        assert_eq!(
            departure,
            chatter_protocol::ServerMessage::IncomingMessage {
                login: "old-login".to_string(),
                room: "general".to_string(),
                message: "[System] old-login left the room".to_string(),
            }
        );

        let login_ok = next_server_message(&mut rx).await;
        assert_eq!(
            login_ok,
            chatter_protocol::ServerMessage::LoginOk {
                login: "new-login".to_string(),
            }
        );
        let peer = peer_map.lock().unwrap().get(&addr).unwrap().clone();
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
            (
                addr,
                tx,
                "alice".to_string(),
                vec!["general".to_string()],
            ),
            (
                other_addr,
                other_tx,
                "observer".to_string(),
                vec!["general".to_string()],
            ),
        ]);

        process_data(
            chatter_protocol::ClientMessage::Login {
                login: "alice".to_string(),
                passwd: "wrong-password".to_string(),
            },
            peer_map.clone(),
            addr,
            account_db,
        )
        .await;

        let departure = next_server_message(&mut other_rx).await;
        assert_eq!(
            departure,
            chatter_protocol::ServerMessage::IncomingMessage {
                login: "alice".to_string(),
                room: "general".to_string(),
                message: "[System] alice left the room".to_string(),
            }
        );

        let failed = next_server_message(&mut rx).await;
        assert_eq!(
            failed,
            chatter_protocol::ServerMessage::LoginFailed {
                reason: "Invalid credentials.".to_string(),
            }
        );
        let peer = peer_map.lock().unwrap().get(&addr).unwrap().clone();
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
}
