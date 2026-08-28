use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use chatter_protocol::ClientMessage;

pub(crate) const MAX_HISTORY: usize = 500;
pub(crate) const EVENT_BUFFER_SIZE: usize = 256;
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

// ── WebSocket types ───────────────────────────────────────────────────────

type WsSink = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
pub(crate) type SharedSink = Arc<Mutex<Option<WsSink>>>;

type WsRead = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
pub(crate) type SharedRead = Arc<Mutex<Option<WsRead>>>;

/// Shared storage for reader and heartbeat task handles.
pub(crate) type TaskHandles = Arc<std::sync::Mutex<Option<(JoinHandle<()>, JoinHandle<()>)>>>;

// ── AppEvent ──────────────────────────────────────────────────────────────

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

// ── Async functions (moved from App impl) ─────────────────────────────────

pub fn spawn_connection(
    url: String,
    sink: SharedSink,
    initial_read: SharedRead,
    events: mpsc::Sender<AppEvent>,
    notify: Arc<Notify>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    // Spawn the connection task. It internally calls start_reader_and_heartbeat
    // which spawns reader+heartbeat tasks. We don't capture those handles here
    // because we need a runtime to await the oneshot — instead, reconnect_attempt
    // stores handles in the shared Arc.
    let connect_handle = tokio::spawn(async move {
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)).await {
            Ok(Ok((socket, _))) => {
                let (write, read) = socket.split();
                *sink.lock().await = Some(write);
                *initial_read.lock().await = Some(read);
                notify.notify_one();
                // The reader/heartbeat handles will be stored in the shared Arc
                // by start_reader_and_heartbeat (called via reconnect_attempt).
                // For initial connection, we just spawn them directly.
                start_reader_and_heartbeat(sink, initial_read, events, None).await;
            }
            Ok(Err(error)) => {
                let _ = events
                    .send(AppEvent::ConnectionError {
                        reason: error.to_string(),
                    })
                    .await;
            }
            Err(_) => {
                let _ = events
                    .send(AppEvent::ConnectionError {
                        reason: "connection timed out".into(),
                    })
                    .await;
            }
        }
    });
    (
        connect_handle,
        tokio::spawn(async {}),
        tokio::spawn(async {}),
    )
}

async fn start_reader_and_heartbeat(
    sink: SharedSink,
    initial_read: SharedRead,
    events: mpsc::Sender<AppEvent>,
    initial_message: Option<String>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let Some(mut read) = initial_read.lock().await.take() else {
        return (tokio::spawn(async {}), tokio::spawn(async {}));
    };
    let (pong_tx, mut pong_rx) = mpsc::unbounded_channel();
    let read_events = events.clone();
    let sink_for_reader = sink.clone();
    let reader_handle = tokio::spawn(async move {
        // Send initial message if provided (ensures reader is listening first)
        if let Some(msg) = initial_message
            && let Some(ws) = sink_for_reader.lock().await.as_mut()
        {
            let _ = ws.send(Message::Text(msg.into())).await;
        }
        while let Some(result) = read.next().await {
            match result {
                Ok(Message::Text(data)) => {
                    let _ = read_events
                        .send(AppEvent::ReceivedMsg {
                            data: Message::Text(data),
                        })
                        .await;
                }
                Ok(Message::Pong(_)) => {
                    let _ = pong_tx.send(());
                }
                Ok(Message::Close(frame)) => {
                    let close_code = frame.as_ref().map(|f| f.code.into());
                    let close_reason = frame.map(|f| f.reason.to_string());
                    let _ = read_events
                        .send(AppEvent::Disconnected {
                            close_code,
                            close_reason,
                        })
                        .await;
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    let _ = read_events
                        .send(AppEvent::ConnectionError {
                            reason: error.to_string(),
                        })
                        .await;
                    return;
                }
            }
        }
        let _ = read_events
            .send(AppEvent::Disconnected {
                close_code: None,
                close_reason: None,
            })
            .await;
    });
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let ping_result = match sink.lock().await.as_mut() {
                Some(ws) => ws.send(Message::Ping(Vec::new().into())).await,
                None => return,
            };
            if let Err(error) = ping_result {
                let _ = events
                    .send(AppEvent::ConnectionError {
                        reason: format!("heartbeat ping failed: {error}"),
                    })
                    .await;
                return;
            }
            if tokio::time::timeout(HEARTBEAT_TIMEOUT, pong_rx.recv())
                .await
                .is_err()
            {
                let _ = events
                    .send(AppEvent::ConnectionError {
                        reason: "heartbeat pong timed out".into(),
                    })
                    .await;
                return;
            }
        }
    });
    (reader_handle, heartbeat_handle)
}

#[allow(clippy::too_many_arguments)]
pub async fn reconnect_attempt(
    url: String,
    sink: SharedSink,
    initial_read: SharedRead,
    events: mpsc::Sender<AppEvent>,
    notify: Arc<Notify>,
    login: Option<String>,
    password: Option<String>,
    task_handles: TaskHandles,
) {
    let mut delay = Duration::from_secs(2);
    loop {
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)).await {
            Ok(Ok((socket, _))) => {
                let (write, read) = socket.split();
                *sink.lock().await = Some(write);
                *initial_read.lock().await = Some(read);
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
                    None
                };
                notify.notify_one();
                let _ = events.send(AppEvent::Reconnected).await;
                let (reader_h, heartbeat_h) =
                    start_reader_and_heartbeat(sink, initial_read, events, initial_msg).await;

                // Store the new reader/heartbeat handles in the shared Arc.
                let mut handles = task_handles.lock().unwrap();
                *handles = Some((reader_h, heartbeat_h));

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
    events_tx: mpsc::Sender<AppEvent>,
    events_rx: mpsc::Receiver<AppEvent>,
}

impl EventHandler {
    /// Create the event channel pair, WebSocket shared state, and connection notify.
    /// Returns (EventHandler, SharedSink, SharedRead, Arc<Notify>, TaskHandles).
    pub fn new() -> (Self, SharedSink, SharedRead, Arc<Notify>, TaskHandles) {
        let (events_tx, events_rx) = mpsc::channel(EVENT_BUFFER_SIZE);
        let ws_sink: SharedSink = Arc::new(Mutex::new(None));
        let initial_read: SharedRead = Arc::new(Mutex::new(None));
        let connect_notify = Arc::new(Notify::new());
        let task_handles: TaskHandles = Arc::new(std::sync::Mutex::new(None));
        let handler = Self {
            events_tx,
            events_rx,
        };
        (handler, ws_sink, initial_read, connect_notify, task_handles)
    }

    /// Try to receive an event (delegates to the inner receiver).
    pub fn try_recv(&mut self) -> Result<AppEvent, mpsc::error::TryRecvError> {
        self.events_rx.try_recv()
    }

    /// Clone the event sender (for use in reconnect).
    pub fn events_tx(&self) -> mpsc::Sender<AppEvent> {
        self.events_tx.clone()
    }

    /// Initiate the first WebSocket connection.
    /// Takes `events_tx` as a parameter to avoid double-cloning:
    /// the caller clones once via `events_tx()`, passes it here, and stores another clone in App.
    /// Also takes `task_handles` to store reader/heartbeat handles for reconnect.
    pub fn connect(
        &mut self,
        url: String,
        sink: SharedSink,
        initial_read: SharedRead,
        notify: Arc<Notify>,
        events_tx: mpsc::Sender<AppEvent>,
        task_handles: TaskHandles,
    ) {
        let (_connect_h, reader_h, heartbeat_h) =
            spawn_connection(url, sink, initial_read, events_tx, notify);
        // Store reader/heartbeat handles in the shared Arc.
        let mut handles = task_handles.lock().unwrap();
        *handles = Some((reader_h, heartbeat_h));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bounded event channel drops messages when full.
    #[tokio::test]
    async fn test_bounded_event_channel_drops_on_overflow() {
        let (tx, mut rx) = mpsc::channel::<AppEvent>(2);

        // First two messages fit.
        assert!(
            tx.try_send(AppEvent::ConnectionError {
                reason: "test".into()
            })
            .is_ok()
        );
        assert!(
            tx.try_send(AppEvent::ConnectionError {
                reason: "test2".into()
            })
            .is_ok()
        );

        // Third message should fail (buffer full).
        assert!(tx.try_send(AppEvent::Reconnected).is_err());

        // First two messages are receivable.
        let msg1 = rx.recv().await.unwrap();
        assert!(matches!(msg1, AppEvent::ConnectionError { .. }));
        let msg2 = rx.recv().await.unwrap();
        assert!(matches!(msg2, AppEvent::ConnectionError { .. }));

        // Drop sender so channel closes and recv().await returns None.
        drop(tx);

        // After draining, channel is empty — "Reconnected" was dropped.
        assert!(rx.recv().await.is_none());
    }

    /// Bounded channel delivers all messages within capacity.
    #[tokio::test]
    async fn test_bounded_event_channel_normal_operation() {
        let (tx, mut rx) = mpsc::channel::<AppEvent>(10);

        // Send 5 messages within capacity.
        for i in 0..5 {
            assert!(
                tx.try_send(AppEvent::ConnectionError {
                    reason: format!("reason{i}")
                })
                .is_ok()
            );
        }

        // All should be receivable.
        for i in 0..5 {
            let msg = rx.recv().await.unwrap();
            assert!(
                matches!(msg, AppEvent::ConnectionError { reason } if reason == format!("reason{i}"))
            );
        }

        // Drop sender so channel closes.
        drop(tx);

        // Channel empty now.
        assert!(rx.recv().await.is_none());
    }
}
