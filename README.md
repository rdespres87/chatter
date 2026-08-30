# chatter

WebSocket chat application with an egui desktop client, written in Rust.

## Table of Contents

- [chatter](#chatter)
  - [Table of Contents](#table-of-contents)
  - [Project Structure](#project-structure)
  - [Technical Choices](#technical-choices)
    - [Protocol — JSON over WebSocket](#protocol--json-over-websocket)
    - [Persistence — SQLite via rusqlite](#persistence--sqlite-via-rusqlite)
    - [Concurrency — tokio async runtime](#concurrency--tokio-async-runtime)
    - [Protocol — Message Reference](#protocol--message-reference)
    - [Protocol — Sequence Diagrams](#protocol--sequence-diagrams)
    - [Protocol — Constraints](#protocol--constraints)
  - [Crate Dependencies](#crate-dependencies)
    - [`protocol` — Shared types between client and server](#protocol--shared-types-between-client-and-server)
    - [`server` — WebSocket chat server](#server--websocket-chat-server)
    - [`client` — egui desktop chat client](#client--egui-desktop-chat-client)
  - [Architecture Overview](#architecture-overview)
  - [Features](#features)
    - [Server](#server)
    - [Client](#client)
  - [Limitations](#limitations)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Build \& Run](#build--run)
      - [Server Configuration](#server-configuration)
      - [Client Configuration](#client-configuration)
  - [Docker](#docker)
    - [Quick Start](#quick-start)
    - [Docker Compose Configuration](#docker-compose-configuration)
    - [Dockerfile (Multi-stage Build)](#dockerfile-multi-stage-build)
    - [Running Without Docker Compose](#running-without-docker-compose)
  - [Testing](#testing)
  - [License](#license)

## Project Structure

```text
chatter/
├── Cargo.toml              # Workspace manifest (Rust 2024 edition)
├── Cargo.lock
├── README.md
├── docker-compose.yml      # Docker Compose for server deployment
├── Dockerfile              # Multi-stage Docker build (server only)
├── .dockerignore
├── chatter.db              # Default SQLite database (created at runtime)
│
├── protocol/               # Shared message types crate
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs          # Message enums, serialization, history types
│
├── server/                 # WebSocket server crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # Server entry point, CLI args, WebSocket handler
│       └── account.rs      # Account management (create, login, bcrypt)
│
└── client/                 # egui desktop client crate
    ├── Cargo.toml
    └── src/
        ├── main.rs         # Client entry point, CLI args, WebSocket connection
        ├── app.rs          # egui application (splash screen, chat UI)
        ├── events.rs       # Event loop, auto-reconnect logic
        └── utils.rs        # Helper functions (username display, etc.)
```

## Technical Choices

### Protocol — JSON over WebSocket

The client and server communicate via **WebSocket** using the `tungstenite`
crate, with messages serialized as **JSON**. This choice provides:

- **Text-based framing**: JSON is human-readable for debugging and works
  naturally with serde's serialization.
- **Bidirectional communication**: WebSocket enables real-time push from server
  to client (incoming messages, room list updates) without polling.
- **Cross-platform**: WebSocket works through most firewalls and proxies,
  unlike raw TCP.

Messages are defined in the `protocol` crate as Rust enums with serde derive
macros. The two main message directions are `ClientMessage` (CreateAccount,
Login, JoinRoom, LeaveRoom, SendMessage, GetHistory, Logout) and
`ServerMessage` (LoginOk, LoginFailed, AccountCreated, AccountCreationFailed,
IncomingMessage, RoomList, RoomHistory, LogoutOk, Error).

### Persistence — SQLite via rusqlite

The server uses **SQLite** (through the `rusqlite` crate) for persistent
storage:

- **Accounts**: Username and bcrypt-hashed passwords are stored in an SQLite
  database. The `account.rs` module handles account creation and login
  verification.
- **Messages**: Chat messages are persisted room-by-room with cursor-based
  pagination. Each message stores the sender, content, and timestamp.

SQLite was chosen for its zero-configuration, single-file deployment model —
ideal for a small-to-medium chat server that doesn't need concurrent write
scaling. The database file path is configurable via the `--db` CLI flag
(default: `chatter.db`, or the `DB_PATH` environment variable).

### Concurrency — tokio async runtime

The server runs on **tokio**, Rust's asynchronous runtime:

- **Per-client tasks**: Each connected WebSocket client is handled by an
  independent async task spawned via `tokio::spawn`. This allows the server to
  manage thousands of concurrent connections without blocking.
- **Shared state**: Client rooms and message history are protected by
  `tokio::sync::Mutex` (or `RwLock`) to allow safe concurrent access across
  tasks.
- **Heartbeat/disconnect detection**: The server detects disconnection when a
  WebSocket Close frame is received. Clients are removed from all rooms and
  notified to other participants.
- **Room broadcasting**: When a message is received, the server broadcasts it
  to all other clients in the same room using async task spawning.

The client also runs on tokio for its WebSocket connection management, enabling
non-blocking I/O during reconnection attempts.

### Protocol — Message Reference

All messages are JSON-serialized WebSocket frames. The `protocol` crate defines
the two message types as Rust enums with `serde` derive macros.

**Client → Server messages (`ClientMessage`):**

| Variant | Fields | Description |
| ------- | ------ | ----------- |
| `CreateAccount` | `login: String`, `passwd: String` | Register a new account |
| `Login` | `login: String`, `passwd: String` | Authenticate |
| `JoinRoom` | `room: String` | Join a chat room |
| `LeaveRoom` | `room: String` | Leave a chat room |
| `SendMessage` | `room: String`, `message: String` | Send a chat message |
| `GetHistory` | `room: String`, `cursor: Option<u64>` | Fetch room history (cursor = oldest seen ID) |
| `Logout` | — | Disconnect gracefully |

**Server → Client messages (`ServerMessage`):**

| Variant | Fields | Description |
| ------- | ------ | ----------- |
| `LoginOk` | `login: String` | Authentication successful |
| `LoginFailed` | `reason: String` | Bad credentials |
| `AccountCreated` | `login: String` | Registration successful |
| `AccountCreationFailed` | `reason: String` | Registration rejected |
| `IncomingMessage` | `id`, `login`, `room`, `message`, `timestamp` | New message in a room |
| `RoomList` | `rooms: Vec<String>` | Available rooms |
| `RoomHistory` | `room`, `messages: Vec<HistoryEntry>`, `has_more: bool` | History page |
| `LogoutOk` | — | Logout confirmed |
| `Error` | `message: String`, `code: String` | Generic error |

### Protocol — Sequence Diagrams

The following diagrams illustrate the message flow for the main use cases.

**Account creation and login:**

```text
Client                                    Server
  |                                         |
  |  { "CreateAccount": { "login": "alice", ... } }
  |────────────────────────────────────────>│  bcrypt hash + INSERT
  |                                         │
  |  { "AccountCreated": { "login": "alice" } }
  |<────────────────────────────────────────│
  |                                         |
  |  { "Login": { "login": "alice", ... } }
  |────────────────────────────────────────>│  bcrypt verify + auth
  |                                         |
  |  { "LoginOk": { "login": "alice" } }    │
  |  { "RoomList": { "rooms": ["general", ...] } }
  |<────────────────────────────────────────│
```

**Join room, send messages, and history:**

```text
Client A                              Server          Client B
  |                                    │                 |
  |  { "Login": ... }                  │                 |
  |──────────>                         │                 |
  |  { "LoginOk": ... }                │                 |
  |<──────────                         │                 |
  |                                    │                 |
  |  { "JoinRoom": { "room": "general" } }               │
  |──────────>                         │                 |
  |                                    │  { "IncomingMessage": "alice joined" }
  |                                    |────────────────>│
  |                                    │                 |
  |  { "SendMessage": { "room": "general", "message": "Hello!" } }
  |──────────>                         │                 |
  |                                    │  { "IncomingMessage": ... }
  |                                    |──────────>      │
  |                                    |<─────────────────│ (echo back)
  |                                    │                 |
  |  (scroll up →)                     │                 |
  |  { "GetHistory": { "room": "general", "cursor": null } }
  |──────────>                         │                 |
  |  { "RoomHistory": { messages: [...], has_more: false } }
  |<──────────                         │                 |
  |                                    │                 |
  |  { "GetHistory": { "cursor": 42 } }│                 |
  |──────────>                         │                 |
  |  { "RoomHistory": { ..., has_more: true } }          │
  |<──────────                         │                 |
  |                                    │                 |
  |  { "LeaveRoom": { "room": "general" } }              │
  |──────────>                         │                 |
  |                                    │  { "IncomingMessage": "alice left" }
  |                                    |────────────────>│
```

**Graceful disconnect and auto-reconnect:**

```text
Client                                    Server
  |                                    │
  |  (connection drops / Close frame)  │
  |<───────────────────────────────────│
  |                                    │
  |  (wait 1s, retry)                  │
  |  { WebSocket connect }             │
  |──────────────────────────────────->│
  |  { WebSocket open }                │
  |<──────────────────────────────────-│
  |  (resend Login credentials)        │
  |  { "Login": ... }                  │
  |──────────>                         │
  |  { "LoginOk": ... }                │
  |<──────────                         │
  |                                    │
```

### Protocol — Constraints

| Field | Min | Max | Rules |
| ----- | --- | --- | ----- |
| Login | 2 bytes | 32 bytes (new) / 64 (legacy) | ASCII alphanumeric + `_`, no reserved prefixes |
| Password | 4 bytes | 72 bytes | No NUL, bcrypt truncates at 72 |
| Room name | 1 byte | 32 bytes (new) / 64 (legacy) | ASCII alphanumeric + `_` + `-` |
| Message | 1 byte | 4096 bytes | No control chars (except `\n`, `\r`) |
| WebSocket payload | — | 2 MB | Text frames only |

## Crate Dependencies

### `protocol` — Shared types between client and server

| Crate | Version | Why |
| ----- | ------- | --- |
| `serde` + `serde_json` | 1.0 | JSON serialization for message types. |
| `anyhow` | 1.0 | Error handling with context chaining. |
| `tungstenite` | 0.29 | WebSocket transport Message type. |

### `server` — WebSocket chat server

| Crate | Version | Why |
| ----- | ------- | --- |
| `tokio` + `tokio-tungstenite` | 1.x / 0.29 | Async runtime + WebSocket. |
| `futures-channel` | 0.3 | Unbounded channel for peer messaging. |
| `futures-util` | 0.3 | Async stream/sink combinators (sink+std). |
| `rusqlite` | 0.40 | SQLite for persistent storage (bundled). |
| `bcrypt` | 0.19 | Password hashing. |
| `clap` + `serde` | 4.x / 1.0 | CLI args with derive macros. |
| `log` + `env_logger` | 0.4 / 0.11 | Structured logging with filtering. |
| `ctrlc` | 3.4 | Graceful shutdown on SIGINT/SIGTERM. |
| `zeroize` | 1.8 | Secure memory clearing for secrets. |
| `chatter_protocol` | local | Shared message types. |
| `serde_json` | 1.0 | JSON for WebSocket payloads. |

### `client` — egui desktop chat client

| Crate | Version | Why |
| ----- | ------- | --- |
| `egui` + `eframe` | 0.35 | Immediate-mode GUI (OpenGL via glow). |
| `egui_extras` | 0.35 | Extra egui widgets and features. |
| `tokio` + `tokio-tungstenite` | 1.x / 0.29 | Async WebSocket connection. |
| `futures-util` | 0.3 | Async stream/sink combinators (StreamExt, SinkExt). |
| `clap` + `serde` | 4.x / 1.0 | CLI args for URL and port. |
| `log` + `env_logger` | 0.4 / 0.11 | Client-side debug logging. |
| `color-eyre` | 0.6 | Error reporting with backtraces. |
| `chrono` | 0.4 | Timestamp formatting for messages. |
| `chatter_protocol` | local | Shared message types. |
| `serde_json` | 1.0 | JSON for WebSocket payloads. |

## Architecture Overview

```text
+--------------------------------------------------------------+
|                     Client (egui)                            |
|  +----------+  +----------+  +------------------------------+|
|  | Splash   |  | Chat UI  |  | WebSocket (tokio)            ||
|  | Login/   |->| Room     |  | JSON messages                ||
|  | Register |  | Sidebar  |  | Auto-reconnect               ||
|  +----------+  +----------+  +------------------------------+|
|                                               | WebSocket   ||
+-----------------------------------------------|--------------+
                                                |
                                                v
+--------------------------------------------------------------+
|                   Server (tokio)                             |
|  +----------+  +----------+  +------------------------------+|
|  | WebSocket|->| Room     | ->| SQLite (rusqlite)           ||
|  | Handler  |  | Manager  |   | Accounts +                  ||
|  | (tokio)  |  | (broadcast)| Messages                      ||
|  +----------+  +----------+  +------------------------------+|
|  +----------+                                               ||
|  | Account  |  bcrypt password hashing                      ||
|  | Manager  |                                               ||
|  +----------+                                               ||
|  +----------+                                               ||
|  | Heartbeat|  Close frame disconnect detection             ||
|  +----------+                                               ||
+--------------------------------------------------------------+
```

## Features

### Server

- **Room-based messaging**: Clients join rooms and receive messages from all
  participants in real-time.
- **Account system**: Registration and login with bcrypt-hashed passwords
  stored in SQLite.
- **Room history with cursor pagination**: When joining a room, clients receive
  the latest 100 messages. Older messages can be fetched by providing a cursor
  (oldest message ID seen).
- **Room listing**: Clients receive the list of available rooms on login and
  when rooms change.
- **Heartbeat/disconnect detection**: Server detects disconnection when a
  WebSocket Close frame is received. Clients are automatically removed from
  their rooms and notified to other participants.
- **Login brute-force protection**: Exponential backoff on repeated failed
  login/account attempts (10ms base, up to 8s max).
- **Concurrent account task limiting**: Maximum 64 concurrent blocking account
  tasks (bcrypt hashing, DB queries) to prevent resource exhaustion.
- **Configurable deployment**: Host, port, and database path are configurable
  via CLI flags or the `DB_PATH` environment variable.

### Client

- **Splash screen with authentication**: Login/Register flow with form
  validation.
- **Chat room interface**: Real-time message display with scrolling history,
  room sidebar for navigation (left-click to switch rooms).
- **Auto-reconnect**: If the WebSocket connection drops, the client
  automatically attempts to reconnect with exponential backoff (starting at
  1s, doubling up to 30s max).
- **CLI mode**: Connect to a server from the command line with `--url` and
  `-p/--port` flags.
- **Password input**: Masked password field during login/registration.

## Limitations

- **Single-server deployment**: No support for horizontal scaling across
  multiple server instances. Each server manages its own SQLite database and
  in-memory room state.
- **No server-side TLS**: The server accepts plain ws:// connections. Deploy it behind a TLS-terminating reverse proxy (e.g., Caddy, Nginx) to provide wss:// to clients. The server prints a security notice on startup reminding operators of this. The client supports `wss://` URLs (via native-tls) for connecting to TLS-terminated proxies.
- **No file/image sharing**: Only text messages are supported.
- **No direct messaging (DMs)**: All messages are room-scoped; there is no
  private messaging between individual users.
- **No message editing/deletion**: Once sent, messages cannot be modified or
  removed.
- **Limited history pagination**: The initial room history fetch returns at most
  100 messages. Older messages are loaded on-demand when the user scrolls to
  the top of the chat area (cursor-based pagination). There is no explicit
  "Load Older" button — loading is triggered automatically by scroll position.
- **No rate limiting**: The server does not currently limit the rate of messages
  per client, which could be exploited for flooding.
- **No typing indicators or presence**: Users cannot see who is online or when
  someone is typing.
- **Client is desktop-only**: The egui client requires a native OS window (no
  web/WASM export yet).

## Getting Started

### Prerequisites

- Rust 1.95+ (stable toolchain, Rust 2024 edition)
- C compiler (`cc`) — required by `libsqlite3-sys`'s `bundled` feature on Linux

### Build & Run

```bash
# Build the entire workspace
cargo build --release

# Build only a specific crate
cargo check -p protocol   # Protocol types only
cargo check -p server     # Server (depends on protocol)
cargo check -p client     # Client (depends on protocol)
```

#### Server Configuration

The server accepts CLI arguments for host, port, and database path:

```bash
# Default: listen on 127.0.0.1:8080, database stored in chatter.db
cargo run -p server

# Custom host and port
cargo run -p server -- --host 127.0.0.1 --port 9000

# Custom database path
cargo run -p server -- --db /tmp/chat.db

# Using the DB_PATH environment variable
DB_PATH=/tmp/chat.db cargo run -p server

# Using CHATTER_HOST / CHATTER_PORT environment variables
CHATTER_HOST=0.0.0.0 CHATTER_PORT=9000 cargo run -p server
```

**Server CLI options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--host` | `127.0.0.1` | Host to bind the server to |
| `-p, --port` | `8080` | Port to listen on |
| `--db` | `chatter.db` | Path to the SQLite database file |

**Environment variables:**

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DB_PATH` | `chatter.db` | Path to the SQLite database file (overrides `--db`) |
| `CHATTER_HOST` | CLI `--host` value | Server bind address (overrides `--host`) |
| `CHATTER_PORT` | CLI `--port` value | Server listening port (overrides `--port`) |
| `RUST_LOG` | `info` | Log level (see log levels section) |

**Log levels:**

```bash
# Verbose logging (debug + trace)
RUST_LOG=debug cargo run -p server -- --host 127.0.0.1 --port 8080

# Only warnings and errors
RUST_LOG=warn cargo run -p server

# Specific module logging
RUST_LOG=server::account=info cargo run -p server
```

#### Client Configuration

The client accepts CLI arguments for server URL and port override:

```bash
# Interactive mode (prompts for connection details and credentials)
cargo run -p client

# CLI mode: connect to a custom server address
cargo run -p client -- --url ws://127.0.0.1:8080

# Override the port from the URL
cargo run -p client -- --url ws://127.0.0.1 --port 9000
```

**Client CLI options:**

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--url` | `ws://localhost:8080` | WebSocket server URL |
| `-p, --port` | (extracted from URL) | Server port override |

**Note:** The `--url` option takes precedence over `--port`. If only `--port`
is provided, it overrides the corresponding value in the URL. In CLI mode
(`--url` or `--port` provided), the client skips the splash screen and attempts
to connect directly.

## Docker

The project can be built and run inside a Docker container. Only the server is
containerized (the client is a native egui desktop app).

### Quick Start

```bash
# Build the Docker image
docker build -t chatter-server .

# Run with docker compose (recommended)
docker compose up -d

# View server logs in real-time
docker compose logs -f server

# Stop and remove the container
docker compose down

# Stop and remove the container + clear database
docker compose down -v
```

### Docker Compose Configuration

The `docker-compose.yml` file defines:

- **Server service**: Built from the Dockerfile, exposed on port 12345.
- **Persistent volume**: `chatter_data` mounts at `/app/data` inside the
  container for SQLite persistence.
- **Auto-restart**: `restart: unless-stopped` ensures the server recovers from
  crashes.
- **Logging**: `RUST_LOG=info` sets the default log level.
- **Configurable bind address**: `CHATTER_HOST=0.0.0.0` (all interfaces) and
  `CHATTER_PORT=12345` are set via environment variables. Change these values in
  `docker-compose.yml` or create a `.env` file to override.

To customize the server configuration, either edit `docker-compose.yml` directly
or copy `.env.example` to `.env` and modify the values there.

### Dockerfile (Multi-stage Build)

The Dockerfile uses a two-stage build to minimize the final image size:

1. **Builder stage** (`rust:1.95-bookworm`): Compiles the server in release
   mode. The client is excluded from the workspace during build via `sed`.
2. **Runtime stage** (`debian:bookworm-slim`): Contains only the compiled
   binary, CA certificates, and a non-root user (`appuser`).

The server runs as `appuser` (UID 1000) for security. The default database path
inside the container is `/app/data/chatter.db`.

### Running Without Docker Compose

```bash
# Build the image
docker build -t chatter-server .

# Run directly
docker run -d \
  --name chatter-server \
  -p 12345:12345 \
  -e RUST_LOG=info \
  -v chatter_data:/app/data \
  --restart unless-stopped \
  chatter-server

# Stop and remove
docker stop chatter-server && docker rm chatter-server
```

## Testing

```bash
# Run all tests in the workspace
cargo test --workspace

# Run tests for a specific crate
cargo test -p protocol
cargo test -p server
cargo test -p client

# Run tests with output visible
cargo test --workspace -- --nocapture
```

## License

MIT
