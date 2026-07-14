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

# Run the server
cargo run -p server

# In another terminal, run the client
cargo run -p client
```

The server listens on `ws://localhost:8080` by default. The client connects to this address automatically.

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
