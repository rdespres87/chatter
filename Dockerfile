# ============================================
# Stage 1: Build the Rust server
# ============================================
FROM rust:1.95-bookworm AS builder

WORKDIR /app

# Copy entire workspace (client is in context but will be excluded from build)
COPY . .

# Remove "client" from workspace members so cargo doesn't try to resolve it
RUN sed -i 's/members = \["client", "server", "protocol"\]/members = ["server", "protocol"]/' Cargo.toml

# Build only the server package in release mode
RUN cargo build --release -p server

# ============================================
# Stage 2: Minimal runtime image
# ============================================
FROM debian:bookworm-slim

# Runtime dependencies (ca-certificates is usually useful)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary
COPY --from=builder /app/target/release/server /usr/local/bin/server

# Create a non-root user
RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser

# SQLite database will live here (mounted as a volume)
ENV DB_PATH=/app/data/chatter.db

# Listen on all interfaces so the container is reachable from outside.
CMD ["server", "--host", "0.0.0.0", "--port", "12345"]
