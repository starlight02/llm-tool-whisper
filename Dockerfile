FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends pkg-config ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates wget \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/xml-tool-bridge /usr/local/bin/xml-tool-bridge

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["wget", "-qO-", "http://127.0.0.1:8787/health"]

CMD ["/usr/local/bin/xml-tool-bridge", "/etc/xml-tool-bridge/config.toml"]
