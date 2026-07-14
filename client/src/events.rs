use color_eyre::eyre::OptionExt;
use crossterm::event::Event as CrosstermEvent;
use futures::FutureExt;
use futures_util::StreamExt;
use log::{error, info};
use std::fmt::Debug;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;

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
}

/// Event handler to communicate with the app (terminal, web socket).
#[derive(Debug)]
pub struct EventHandler {
    /// Event receiver channel.
    receiver: mpsc::UnboundedReceiver<Event>,
}

impl EventHandler {
    /// Constructs a new instance of [`EventHandler`] and spawns two tokio tasks: one for
    /// terminal events and one polling the web socket.
    ///
    /// # Panics
    ///
    /// Panics if called outside of a tokio runtime context, as it calls [`tokio::spawn`].
    pub fn new(
        read_socket: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let event_task = EventTask::new(sender.clone());
        let socket_task = EventSocketTask::new(read_socket, sender.clone());
        // Task: forward terminal events to the app.
        tokio::spawn(async { event_task.run().await });
        // Task: poll the web socket and forward received messages.
        tokio::spawn(async { socket_task.run().await });
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
                use tokio::signal::unix::{signal, SignalKind};
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
}

impl EventSocketTask {
    /// Constructs a new instance of [`EventSocketTask`].
    fn new(
        read_socket: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        sender: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            read_socket,
            sender,
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
                // Intentionally ignored: the chat protocol is text-only, and tungstenite
                // queues Pong replies to Pings internally. If the protocol ever grows
                // Binary frames (files, voice), handle them here.
                Ok(_) => (),
                Err(e) => {
                    error!("Error processing message: {e}");
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
