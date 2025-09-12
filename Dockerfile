# Multi-stage Docker build for GurtPay
FROM rust:nightly as builder

# Install dependencies for building
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY frontend ./frontend

# Build the application in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1001 gurtpay

# Create app directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/gurtpay-server /app/gurtpay-server

# Create directories for certificates and database
RUN mkdir -p /app/certs /app/data && \
    chown -R gurtpay:gurtpay /app

# Switch to app user
USER gurtpay

# Expose port
EXPOSE 4878

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4878/ || exit 1

# Default command
CMD ["/app/gurtpay-server"]
