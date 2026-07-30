# Chatter

WebSocket chat application with an egui desktop client, written in Rust.

## Architecture

The project is organized as a Cargo workspace with three independent crates:

```
chatter/
├── protocol/   # Shared message types and serialization
├── server/     # WebSocket server (tokio-tungstenite + SQLite)
└── client/     # egui desktop chat client
```

### Protocol Crate (`protocol/`)

Defines all message types for client-server communication:

- **ClientMessage**: `Login`, `CreateAccount`, `JoinRoom`, `LeaveRoom`, `SendMessage`, `GetHistory`
- **ServerMessage**: `LoginOk`, `LoginFailed`, `AccountCreated`, `AccountCreationFailed`, `RoomList`, `RoomHistory`, `IncomingMessage`, `Error`, `Welcome`
- **HistoryEntry**: Struct for room history entries

All types implement `serde::Serialize` and `serde::Deserialize`.

### Server Crate (`server/`)

WebSocket chat server built with:

- **tokio-tungstenite** — WebSocket protocol handling
- **SQLite** (via `rusqlite`) — Account storage with bcrypt password hashing
- Room-based message broadcasting
- Login rate limiting with exponential backoff
- Room history retrieval (all messages, limited by i32::MAX)

Listens on port `8080` by default.

### Client Crate (`client/`)

Desktop chat client built with:

|- **egui** + **eframe** — GPU-accelerated 2D GUI (OpenGL via wgpu)
|- **tokio-tungstenite** — WebSocket client with auto-reconnect
|- Splash screen with Login/Register flow
|- Password input with character-by-character editing
|- Chat room interface with message history scrolling
|- Room list sidebar with left-click navigation

## Getting Started

### Prerequisites

- Rust 1.70+ (stable toolchain)

### Build and Run

```bash
# Build the entire workspace
cargo build --release
```

#### Server

The server accepts CLI arguments for host, port, and database path:

```bash
# Default: listen on 127.0.0.1:8080, database stored in chatter.db
cargo run -p server

# Custom host and port
cargo run -p server -- --host 0.0.0.0 --port 9000

# Bind to a specific interface (all interfaces on default port)
cargo run -p server -- --host 0.0.0.0
```

**Server CLI options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `127.0.0.1` | Host to bind the server to |
| `--port` | `8080` | Port to listen on |

**Log levels:**

```bash
# Verbose logging (debug + trace)
RUST_LOG=debug cargo run -p server -- --host 127.0.0.1 --port 8080

# Only warnings and errors
RUST_LOG=warn cargo run -p server

# Specific module logging
RUST_LOG=server::account=info cargo run -p server
```

#### Client

The client accepts CLI arguments for server URL, port, and username:

```bash
# Default: connect to ws://localhost:8080 with interactive login
cargo run -p client

# Connect to a custom server address
cargo run -p client -- --url ws://192.168.1.10:9000

# Specify username for automatic login
cargo run -p client -- --user alice

# Combine URL and port
cargo run -p client -- --url ws://192.168.1.10:9000 --user bob
```

**Client CLI options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | `ws://localhost:8080` | WebSocket server URL |
| `-p, --port` | (extracted from URL) | Server port (overrides URL port) |
| `--user` | (interactive prompt) | Username for automatic login |

**Note:** The `--url` option takes precedence over `--port`. If only `--port` is provided, it overrides the corresponding value in the URL.

### Running Tests

```bash
# Run all tests in the workspace
cargo test --workspace

# Run tests for a specific crate
cargo test -p protocol
cargo test -p server
cargo test -p client
```

## Project Structure

Each crate can be compiled independently:

```bash
cargo check -p protocol   # Protocol only
cargo check -p server     # Server (depends on protocol)
cargo check -p client     # Client (depends on protocol)
```

This allows building and testing each component in isolation.

## License

MIT
