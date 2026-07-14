# Chatter

WebSocket chat application with a ratatui TUI client, written in Rust.

## Architecture

The project is organized as a Cargo workspace with three independent crates:

```
chatter/
├── protocol/   # Shared message types and serialization
├── server/     # WebSocket server (tokio-tungstenite + SQLite)
└── client/     # ratatui TUI chat client
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
- Room history retrieval (last 50 messages)

Listens on port `8080` by default.

### Client Crate (`client/`)

Terminal UI chat client built with:

- **ratatui** — Terminal rendering
- **crossterm** — Terminal input/output handling
- **tokio-tungstenite** — WebSocket client with auto-reconnect
- Splash screen with Login/Register flow
- Password input with character-by-character editing
- Chat room interface with message history scrolling
- Room list navigation (Tab to switch)

## Getting Started

### Prerequisites

- Rust 1.70+ (stable toolchain)

### Build and Run

```bash
# Build the entire workspace
cargo build --release
```

#### Server

The server accepts a single positional argument for the bind address:

```bash
# Default: listen on 127.0.0.1:8080, database stored in chatter.db
cargo run -p server

# Custom bind address (e.g., all interfaces on port 9000)
cargo run -p server -- 0.0.0.0:9000

# Bind to a specific interface and port
cargo run -p server -- 192.168.1.10:8080
```

**Server configuration:**

| Setting | Value | Description |
|---------|-------|-------------|
| Bind address | `127.0.0.1:8080` (default) | First positional argument to the server binary |
| Database file | `chatter.db` | SQLite database path, stored in the current directory |
| Log level | `info` (default) | Controlled via `RUST_LOG` environment variable |

**Log levels:**

```bash
# Verbose logging (debug + trace)
RUST_LOG=debug cargo run -p server

# Only warnings and errors
RUST_LOG=warn cargo run -p server

# Specific module logging
RUST_LOG=server::account=info,cargo_run=warn cargo run -p server
```

#### Client

The client has no command-line arguments. Server connection is configured via the default URL:

```bash
# Default: connect to ws://localhost:8080
cargo run -p client

# Connect to a custom server address

# Connect to a remote server
```

**Client configuration:**

| Setting | Value | Description |
|---------|-------|-------------|
| Server URL | `ws://localhost:8080` (default) | Set via  |
| Log level | `warn` (hardcoded) | Only warnings and errors are shown in the TUI |

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
