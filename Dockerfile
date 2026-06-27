# syntax=docker/dockerfile:1.7

FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app
ARG TARGETARCH
ENV CARGO_TERM_COLOR=always
RUN apt-get update \
  && apt-get install -y --no-install-recommends pkg-config ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
  --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
  --mount=type=cache,id=cargo-target-${TARGETARCH},target=/app/target,sharing=locked \
  mkdir -p src \
  && printf 'fn main() {}\n' > src/main.rs \
  && cargo build --release --locked \
  && rm -rf src

COPY src ./src
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
  --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
  --mount=type=cache,id=cargo-target-${TARGETARCH},target=/app/target,sharing=locked \
  cargo build --release --locked \
  && cp target/release/llm-tool-whisper /usr/local/bin/llm-tool-whisper

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates wget \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/local/bin/llm-tool-whisper /usr/local/bin/llm-tool-whisper

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["wget", "-qO-", "http://127.0.0.1:8787/health"]

CMD ["/usr/local/bin/llm-tool-whisper", "/etc/llm-tool-whisper/config.toml"]
