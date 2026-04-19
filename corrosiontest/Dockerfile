# Two-stage build for corrosion test image.
# Pin with: ARG CORROSION_REF=1b0fcf1
# To bump the pin, change CORROSION_REF and rebuild.
ARG CORROSION_REF=1b0fcf1

# ── Builder ──────────────────────────────────────────────────────────────────
FROM rust:1.83-bookworm AS builder

ARG CORROSION_REF

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    clang \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN git clone https://github.com/superfly/corrosion.git . \
    && git checkout "${CORROSION_REF}"

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin corrosion \
    && cp target/release/corrosion /usr/local/bin/corrosion

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/corrosion /usr/local/bin/corrosion

# Bake default single-node config (API on 0.0.0.0:8080, gossip plaintext,
# admin socket at /var/run/corrosion/admin.sock, db at /tmp/corrosion.db).
RUN mkdir -p /etc/corrosion/schemas /var/run/corrosion

RUN cat > /etc/corrosion/config.toml <<'EOF'
[db]
path = "/tmp/corrosion.db"
schema_paths = ["/etc/corrosion/schemas"]

[gossip]
addr = "[::]:8787"
bootstrap = []
plaintext = true

[api]
addr = "0.0.0.0:8080"

[admin]
path = "/var/run/corrosion/admin.sock"

[log]
format = "json"
colors = false
EOF

EXPOSE 8080 8787

CMD ["corrosion", "agent", "-c", "/etc/corrosion/config.toml"]
