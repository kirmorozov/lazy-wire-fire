# syntax=docker/dockerfile:1.7
#
# Static lazywf image (linux/amd64, linux/arm64), built like lazyview's: the build stages run
# on the build platform and cross-compile with cargo-zigbuild (zig as C toolchain for ring) to
# a fully static musl binary, copied onto distroless static (no shell). TLS uses the Mozilla
# roots compiled into the binary.
#
#   docker buildx build -t lazywf --load .
#   docker run --rm lazywf --version

FROM --platform=$BUILDPLATFORM rust:1.99.1-slim-bookworm AS toolchain
ARG ZIG_VERSION=0.17.0
ARG CARGO_ZIGBUILD_VERSION=0.23.4
ARG BUILDARCH
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl xz-utils \
 && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${BUILDARCH}" in \
      amd64) zarch=x86_64;  zsha=1cbe9df9f27e6b78d14ccbca43b6703a404ef79ef1c463de901d7f088d4e2026 ;; \
      arm64) zarch=aarch64; zsha=9e8d11661d4ae3bd57702a3832781e23ad151dde5798e16a5ccd503f65234ff8 ;; \
      *) echo "unsupported build arch: ${BUILDARCH}" >&2; exit 1 ;; \
    esac; \
    f="zig-${zarch}-linux-${ZIG_VERSION}.tar.xz"; \
    curl -fsSL -o "/tmp/${f}" "https://ziglang.org/download/${ZIG_VERSION}/${f}"; \
    echo "${zsha}  /tmp/${f}" | sha256sum -c -; \
    mkdir -p /opt/zig; tar -xJf "/tmp/${f}" -C /opt/zig --strip-components=1; rm "/tmp/${f}"; \
    ln -s /opt/zig/zig /usr/local/bin/zig
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo install --locked cargo-zigbuild --version "${CARGO_ZIGBUILD_VERSION}" \
 && rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

FROM toolchain AS builder
ARG TARGETARCH
# Builds share hosts; raise with --build-arg CARGO_BUILD_JOBS=… where that is fine.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) t=x86_64-unknown-linux-musl ;; \
      arm64) t=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported target arch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    echo "${t}" > /rust-target
WORKDIR /build
ENV CARGO_TARGET_DIR=/build/target
# Dependencies first (re-run only when Cargo.toml / Cargo.lock change).
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eux; mkdir -p src; echo 'fn main() {}' > src/main.rs; : > src/lib.rs; \
    cargo zigbuild --release --locked --target "$(cat /rust-target)"; rm -rf src
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eux; t="$(cat /rust-target)"; \
    rm -rf "target/${t}/release/.fingerprint/lazy-wire-fire-"* "target/${t}/release/deps/lazy_wire_fire-"* \
           "target/${t}/release/deps/liblazy_wire_fire-"* "target/${t}/release/deps/lazywf-"*; \
    cargo zigbuild --release --locked --target "${t}"; \
    mkdir -p /out; cp "target/${t}/release/lazywf" /out/lazywf

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
LABEL org.opencontainers.image.title="lazywf" \
      org.opencontainers.image.description="Change-triggered actions from the MySQL binlog" \
      org.opencontainers.image.licenses="MIT"
COPY --from=builder --chown=0:0 --chmod=0555 /out/lazywf /lazywf
USER 65532:65532
# The health endpoint listens on 127.0.0.1:8081 by default (LAZYWF_HEALTH_ADDR); set
# LAZYWF_HEALTH_ADDR=0.0.0.0:8081 to probe it from outside the container.
EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD ["/lazywf", "healthcheck"]
ENTRYPOINT ["/lazywf"]
