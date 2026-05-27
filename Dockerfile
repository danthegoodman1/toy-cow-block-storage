ARG RUST_IMAGE=rust:1-bookworm
FROM ${RUST_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        fuse3 \
        kmod \
        libfuse3-dev \
        libsqlite3-dev \
        pkg-config \
        sqlite3 \
    && rustup component add clippy rustfmt \
    && mkdir -p /cargo /workspace/target/criterion \
    && chmod 0777 /cargo /workspace /workspace/target /workspace/target/criterion \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/cargo \
    CARGO_TARGET_DIR=/workspace/target \
    RUST_BACKTRACE=1

WORKDIR /workspace

CMD ["bash"]
