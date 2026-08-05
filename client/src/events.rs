use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};
use tokio::net::TcpStream;
use futures_util::{StreamExt, SinkExt};

use chatter_protocol::ClientMessage;

// ── Constants ──────────────────────────────────────────────────────────────

pub(crate) const MAX_HISTORY: usize = 500;
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// ── WebSocket types ───────────────────────────────────────────────────────

type WsSink = futures_util::stream::SplitSink<
    WebSocketStream<MaybeTlsStream<TcpStream>>,
    Message,
>;
pub(crate) type SharedSink = Arc<Mutex<Option<WsSink>>>;

type WsRead = futures_util::stream::SplitStream<
    WebSocketStream<MaybeTlsStream<TcpStream>>,
>;
pub(crate) type SharedRead = Arc<Mutex<Option<WsRead>>>;

// ── AppEvent ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum AppEvent {
    ReceivedMsg { data: Message },
    Disconnected {
        close_code: Option<u16>,
        close_reason: Option<String>,
    },
    ConnectionError { reason: String },
    Reconnected,
}

// ── Async functions (moved from App impl) ─────────────────────────────────

pub async fn spawn_connection(
    url: String,
    sink: SharedSink,
    initial_read: SharedRead,
    events: mpsc::UnboundedSender<AppEvent>,
    notify: Arc<Notify>,
) {
    tokio::spawn(async move {
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)).await {
            Ok(Ok((socket, _))) => {
                let (write, read) = socket.split();
                *sink.lock().await = Some(write);
                *initial_read.lock().await = Some(read);
                notify.notify_one();
                start_reader_and_heartbeat(sink, initial_read, events, None).await;
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
    });
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
                let initial_msg = if let (Some(l), Some(p)) =
                    (login.as_ref(), password.as_ref())
                    && !l.is_empty()
                    && !p.is_empty()
                {
                    let payload = ClientMessage::Login {
                        login: l.clone(),
                        passwd: p.clone(),
                    };
                    chatter_protocol::serialize_client_message(&payload).ok()
                } else {
                    None
                };
                notify.notify_one();
                let _ = events.send(AppEvent::Reconnected);
                start_reader_and_heartbeat(sink, initial_read, events, initial_msg).await;
                return;
            }
            Ok(Err(_)) | Err(_) => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

// ── EventHandler ──────────────────────────────────────────────────────────

pub struct EventHandler {
    events_tx: mpsc::UnboundedSender<AppEvent>,
    events_rx: mpsc::UnboundedReceiver<AppEvent>,
}

impl EventHandler {
    /// Create the event channel pair, WebSocket shared state, and connection notify.
    /// Returns (EventHandler, SharedSink, SharedRead, Arc<Notify>).
    pub fn new() -> (Self, SharedSink, SharedRead, Arc<Notify>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let ws_sink: SharedSink = Arc::new(Mutex::new(None));
        let initial_read: SharedRead = Arc::new(Mutex::new(None));
        let connect_notify = Arc::new(Notify::new());
        let handler = Self { events_tx, events_rx };
        (handler, ws_sink, initial_read, connect_notify)
    }

    /// Get a reference to the event receiver.
    pub fn receiver(&self) -> &mpsc::UnboundedReceiver<AppEvent> {
        &self.events_rx
    }

    /// Try to receive an event (delegates to the inner receiver).
    pub fn try_recv(&mut self) -> Result<AppEvent, mpsc::error::TryRecvError> {
        self.events_rx.try_recv()
    }

    /// Clone the event sender (for use in reconnect).
    pub fn events_tx(&self) -> mpsc::UnboundedSender<AppEvent> {
        self.events_tx.clone()
    }

    /// Initiate the first WebSocket connection.
    /// Takes `events_tx` as a parameter to avoid double-cloning:
    /// the caller clones once via `events_tx()`, passes it here, and stores another clone in App.
    pub fn connect(
        &self,
        url: String,
        sink: SharedSink,
        initial_read: SharedRead,
        notify: Arc<Notify>,
        events_tx: mpsc::UnboundedSender<AppEvent>,
    ) {
        spawn_connection(url, sink, initial_read, events_tx, notify);
    }

    /// Re-attempt WebSocket connection with credentials.
    pub async fn reconnect(
        &self,
        url: String,
        sink: SharedSink,
        initial_read: SharedRead,
        notify: Arc<Notify>,
        login: Option<String>,
        password: Option<String>,
    ) {
        reconnect_attempt(
            url, sink, initial_read, self.events_tx.clone(), notify, login, password,
        )
        .await;
    }
}
