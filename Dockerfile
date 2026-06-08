ARG RUST_VERSION=1.96

FROM rust:${RUST_VERSION}-slim-bookworm AS builder

WORKDIR /usr/src/bria
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates pkg-config clang cmake make \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin bria

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/bria --shell /usr/sbin/nologin bria \
    && mkdir -p /etc/bria /var/log/bria /tmp/bria \
    && chown -R bria:bria /var/lib/bria /var/log/bria /tmp/bria

COPY --from=builder /usr/src/bria/target/release/bria /usr/local/bin/bria

STOPSIGNAL SIGTERM
ENV BRIA_CONFIG=/etc/bria/Config.toml

LABEL org.opencontainers.image.title="Bria" \
      org.opencontainers.image.description="Rust-based multi-pipeline job orchestrator" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/melonask/bria"

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/bria", "ping"]

USER bria
WORKDIR /var/lib/bria
EXPOSE 4000

ENTRYPOINT ["/usr/local/bin/bria"]
CMD ["--config", "/etc/bria/Config.toml"]
