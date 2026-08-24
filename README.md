# chatter

WebSocket chat application with an egui desktop client, written in Rust.

## Architecture

The project is organized as a Cargo workspace with three independent crates:

```
chatter/
├── protocol/   # Shared message types and serialization (binary: none)
├── server/     # WebSocket server (tokio-tungstenite + SQLite) (binary: server)
└── client/     # egui desktop chat client (binary: chatter)
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

## Docker

The project can be built and run inside a Docker container. Only the server is containerized (the client is a native egui desktop app).

### Quick Start

```bash
# Build the Docker image
docker build -t chatter-server .

# Run in detached mode (background)
docker compose up -d

# View server logs in real-time
docker compose logs -f server

# Stop and remove the container
docker compose down
```

### Full Docker Compose Commands

| Command | Description |
|---------|-------------|
| `docker compose up -d` | Build and start the server in detached mode |
| `docker compose logs -f server` | Follow server logs in real-time |
| `docker compose ps` | Show running containers |
| `docker compose down` | Stop and remove the container + network |
| `docker compose down -v` | Stop, remove container + named volume (clears SQLite data) |
| `docker compose build --no-cache` | Rebuild image from scratch (no cache) |

### Architecture

The Dockerfile uses a **multi-stage build** to keep the final image small:

1. **Builder stage** (`rust:1.85-bookworm`): Compiles the server in release mode
2. **Runtime stage** (`debian:bookworm-slim`): Contains only the compiled binary + CA certificates

The server runs as a non-root user (`appuser`) for security.

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `RUST_LOG` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

The server listens on port `12345` by default inside the container. The host port is mapped via `docker-compose.yml`.

### Database Persistence

The SQLite database is stored in a named Docker volume (`chatter_data`) mounted at `/app/data` inside the container. Data survives container restarts.

To start fresh (delete all data):
```bash
docker compose down -v
docker compose up -d
```

### Building Without Docker Compose

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

# Stop
docker stop chatter-server && docker rm chatter-server
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
