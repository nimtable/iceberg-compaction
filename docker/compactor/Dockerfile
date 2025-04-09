# Build stage
FROM rustlang/rust:nightly as builder

# Install protobuf compiler
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/src/ic

# Copy workspace files
COPY . .

# Set working directory to compactor service
WORKDIR /usr/src/ic/services/compactor

# Build project
RUN cargo build --release

# Runtime stage
FROM ubuntu:24.04 AS base

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder stage
COPY --from=builder /usr/src/ic/target/release/compactor-server /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy configuration file
COPY docker/compactor/config.yaml /app/

# Expose service port
EXPOSE 7777

# Set environment variables
ENV RUST_LOG=info

# Run service
CMD ["compactor-server"] 