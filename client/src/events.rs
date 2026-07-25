use bytes::Bytes;
use color_eyre::eyre::OptionExt;
use crossterm::event::Event as CrosstermEvent;
use futures::FutureExt;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use std::fmt::Debug;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;

/// How often to send a ping frame.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for a pong before declaring the connection dead.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// Representation of all possible events.
#[derive(Clone, Debug)]
pub enum Event {
    /// Crossterm events.
    ///
    /// These events are emitted by the terminal.
    Crossterm(CrosstermEvent),
    /// Application events.
    ///
    /// Use this event to emit custom events that are specific to the application.
    App(AppEvent),
}

/// Application events emitted by the event tasks or queued by the app itself.
#[derive(Clone, Debug)]
pub enum AppEvent {
    /// Quit the application.
    Quit,
    /// Received a message from the web socket.
    ReceivedMsg { data: Message },
    /// The server closed the connection (close frame received or stream ended).
    Disconnected,
    /// The web socket failed with a transport error.
    ConnectionError { reason: String },
    /// User requested a reconnect attempt (Enter key pressed after initial failure).
    Reconnect,
}

/// Event handler to communicate with the app (terminal, web socket).
#[derive(Debug)]
pub struct EventHandler {
    /// Event receiver channel.
    receiver: mpsc::UnboundedReceiver<Event>,
}

impl EventHandler {
    /// Constructs a new instance of [`EventHandler`] and spawns three tokio tasks: one for
    /// terminal events, one polling the web socket, and one sending heartbeat pings.
    ///
    /// # Panics
    ///
    /// Panics if called outside of a tokio runtime context, as it calls [`tokio::spawn`].
    pub(crate) async fn connected(
        read_socket: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        write_socket: std::sync::Arc<
            tokio::sync::Mutex<
                Option<
                    futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
                >,
            >,
        >,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        // Channel for forwarding pong frames from socket task to heartbeat task.
        let (pong_tx, pong_rx) = mpsc::unbounded_channel();
        let event_task = EventTask::new(sender.clone());
        let socket_task = EventSocketTask::new(read_socket, sender.clone(), pong_tx);
        let heartbeat_task = HeartbeatTask::new(write_socket, sender.clone(), pong_rx);
        // Task: forward terminal events to the app.
        tokio::spawn(async { event_task.run().await });
        // Task: poll the web socket and forward received messages.
        tokio::spawn(async { socket_task.run().await });
        // Task: send periodic pings and detect dead connections.
        tokio::spawn(async { heartbeat_task.run().await });
        // Task: catch SIGINT (Ctrl+C) and SIGTERM (kill/systemctl stop),
        // then notify the app so it can restore the terminal before
        // the TTY is left in raw mode.
        let signal_sender = sender.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = signal_sender.send(Event::App(AppEvent::Quit));
        });
        #[cfg(unix)]
        {
            let sigterm_sender = sender.clone();
            tokio::spawn(async move {
                use tokio::signal::unix::{SignalKind, signal};
                if let Ok(mut stream) = signal(SignalKind::terminate()) {
                    stream.recv().await;
                    let _ = sigterm_sender.send(Event::App(AppEvent::Quit));
                }
            });
        }

        Self { receiver }
    }

    /// Constructs an EventHandler for the disconnected state.
    /// Only the terminal event task and signal handlers run — no socket or heartbeat tasks.
    pub fn disconnected() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let event_task = EventTask::new(sender.clone());

        // Task: forward terminal events to the app.
        tokio::spawn(async { event_task.run().await });

        // Task: catch SIGINT / SIGTERM
        let signal_sender = sender.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = signal_sender.send(Event::App(AppEvent::Quit));
        });
        #[cfg(unix)]
        {
            let sigterm_sender = sender.clone();
            tokio::spawn(async move {
                use tokio::signal::unix::{SignalKind, signal};
                if let Ok(mut stream) = signal(SignalKind::terminate()) {
                    stream.recv().await;
                    let _ = sigterm_sender.send(Event::App(AppEvent::Quit));
                }
            });
        }

        Self { receiver }
    }

    /// Receives an event from the sender.
    ///
    /// This function blocks until an event is received.
    ///
    /// # Errors
    ///
    /// This function returns an error if the channel is disconnected, i.e. every sender has
    /// been dropped. In practice this should not happen while this struct is alive, as it
    /// holds a sender itself.
    pub async fn next(&mut self) -> color_eyre::Result<Event> {
        self.receiver
            .recv()
            .await
            .ok_or_eyre("Failed to receive event")
    }
}

/// A task that reads crossterm terminal events and forwards them to the event channel.
struct EventTask {
    /// Event sender channel.
    sender: mpsc::UnboundedSender<Event>,
}

impl EventTask {
    /// Constructs a new instance of [`EventTask`].
    fn new(sender: mpsc::UnboundedSender<Event>) -> Self {
        Self { sender }
    }

    /// Runs the event task.
    ///
    /// Forwards terminal events to the event channel until the receiver side of the channel
    /// is closed or the terminal event stream ends.
    async fn run(self) {
        let mut reader = crossterm::event::EventStream::new();

        loop {
            let crossterm_event = reader.next().fuse();
            tokio::select! {
              _ = self.sender.closed() => {
                break;
              }
              maybe_event = crossterm_event => {
                match maybe_event {
                    Some(Ok(evt)) => {
                        self.send(Event::Crossterm(evt));
                    }
                    Some(Err(e)) => {
                        // A read error may be transient; log it and keep polling rather
                        // than silently killing terminal input.
                        error!("Error reading terminal event: {e}");
                    }
                    None => {
                        break;
                    }
                }
              }
            };
        }
    }

    /// Sends an event to the receiver.
    fn send(&self, event: Event) {
        // Ignores the result because shutting down the app drops the receiver, which causes the send
        // operation to fail. This is expected behavior and should not panic.
        let _ = self.sender.send(event);
    }
}

/// A task that polls the web socket and forwards received messages as app events.
struct EventSocketTask {
    /// Input web socket stream.
    read_socket: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Event sender channel.
    sender: mpsc::UnboundedSender<Event>,
    /// Pong notification channel (forwarded to heartbeat task).
    pong_tx: mpsc::UnboundedSender<AppEvent>,
}

impl EventSocketTask {
    /// Constructs a new instance of [`EventSocketTask`].
    fn new(
        read_socket: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        sender: mpsc::UnboundedSender<Event>,
        pong_tx: mpsc::UnboundedSender<AppEvent>,
    ) -> Self {
        Self {
            read_socket,
            sender,
            pong_tx,
        }
    }

    /// Polls the web socket until it closes or fails.
    ///
    /// Text messages are forwarded as [`AppEvent::ReceivedMsg`]. A close frame or the end of
    /// the stream emits [`AppEvent::Disconnected`], and a transport error emits
    /// [`AppEvent::ConnectionError`], so the app can render the state change itself instead
    /// of this task writing to stdout.
    async fn run(mut self) {
        while let Some(msg) = self.read_socket.next().await {
            match msg {
                Ok(msg @ Message::Text(_)) => {
                    self.send(AppEvent::ReceivedMsg { data: msg });
                }
                Ok(Message::Close(_)) => {
                    info!("Socket is closed");
                    self.send(AppEvent::Disconnected);
                    return;
                }
                Ok(Message::Pong(_)) => {
                    // Forward pong to heartbeat task.
                    let _ = self.pong_tx.send(AppEvent::ReceivedMsg {
                        data: Message::Pong(Bytes::new()),
                    });
                }
                // Intentionally ignored: the chat protocol is text-only, and tungstenite
                // queues Pong replies to Pings internally. If the protocol ever grows
                // Binary frames (files, voice), handle them here.
                Ok(_) => (),
                Err(e) => {
                    let is_reset = e.to_string().contains("Connection reset")
                        || e.to_string().contains("without closing handshake");
                    if is_reset {
                        debug!("Socket reset (normal disconnect): {e}");
                    } else {
                        error!("Error processing message: {e}");
                    }
                    self.send(AppEvent::ConnectionError {
                        reason: e.to_string(),
                    });
                    return;
                }
            }
        }
        // Stream ended without a close frame (e.g. the TCP connection dropped).
        self.send(AppEvent::Disconnected);
    }

    /// Sends an app event to the receiver.
    fn send(&self, event: AppEvent) {
        // Ignores the result because shutting down the app drops the receiver, which causes
        // the send operation to fail. This is expected behavior and should not panic.
        let _ = self.sender.send(Event::App(event));
    }
}

/// A task that sends periodic WebSocket pings and detects dead connections.
///
/// Sends a ping every `HEARTBEAT_INTERVAL` seconds. If no pong is received within
/// `HEARTBEAT_TIMEOUT` seconds, emits `AppEvent::Disconnected` to trigger reconnection.
///
/// The write socket is shared with the app via `Arc<Mutex>` so the app can also
/// send messages while the heartbeat runs.
struct HeartbeatTask {
    /// Output web socket (used to send ping frames). Shared with the app via Arc<Mutex<Option>>.
    write_socket: std::sync::Arc<
        tokio::sync::Mutex<
            Option<futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
        >,
    >,
    /// Event sender for signaling disconnection.
    event_sender: mpsc::UnboundedSender<Event>,
    /// Channel to receive pong notifications from the socket task.
    pong_receiver: mpsc::UnboundedReceiver<AppEvent>,
}

impl HeartbeatTask {
    /// Constructs a new heartbeat task.
    fn new(
        write_socket: std::sync::Arc<
            tokio::sync::Mutex<
                Option<
                    futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
                >,
            >,
        >,
        event_sender: mpsc::UnboundedSender<Event>,
        pong_receiver: mpsc::UnboundedReceiver<AppEvent>,
    ) -> Self {
        Self {
            write_socket,
            event_sender,
            pong_receiver,
        }
    }

    /// Runs the heartbeat loop.
    async fn run(mut self) {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Wait for the first pong before starting the heartbeat cycle.
        // This avoids sending a ping before the connection is fully established.
        if self.pong_receiver.recv().await.is_none() {
            return;
        }

        loop {
            interval.tick().await;

            // Send a ping frame.
            {
                let mut write = self.write_socket.lock().await;
                if let Some(ref mut ws) = *write {
                    if let Err(e) = ws.send(Message::Ping(Bytes::default())).await {
                        error!("Failed to send heartbeat ping: {e}");
                        self.event_sender
                            .send(Event::App(AppEvent::ConnectionError {
                                reason: format!("Ping send failed: {e}"),
                            }))
                            .ok();
                        return;
                    }
                }
            } // write lock dropped here

            // Wait for pong with timeout.
            match tokio::time::timeout(HEARTBEAT_TIMEOUT, self.pong_receiver.recv()).await {
                Ok(Some(_pong)) => {
                    // Pong received — connection is alive. Reset the interval.
                    info!("Heartbeat: pong received");
                }
                Ok(None) => {
                    // Pong channel closed — socket task exited.
                    self.event_sender
                        .send(Event::App(AppEvent::Disconnected))
                        .ok();
                    return;
                }
                Err(_) => {
                    // No pong within timeout — connection is likely dead.
                    error!("Heartbeat: pong timeout ({:?})", HEARTBEAT_TIMEOUT);
                    self.event_sender
                        .send(Event::App(AppEvent::ConnectionError {
                            reason: format!("Pong timeout after {:?}", HEARTBEAT_TIMEOUT),
                        }))
                        .ok();
                    return;
                }
            }
        }
    }
}
