# The service's image (docs/design.md D33): a static binary at /bin/llm-ratelimit on distroless,
# for linux/amd64 and linux/arm64. /bin/ratelimit, the path upstream's manifests call, is a link
# to it: one binary, whichever name it is called by, with no second copy in the image.
# Cross-compiled with cargo-zigbuild on the build platform. LICENSE, NOTICE and the crates'
# licenses (cargo-about, D34) are shipped in /usr/share/doc/llm-ratelimit.
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1.99.0-slim-bookworm@sha256:2c3a22f0a5533ea2dd5a16627bc841228151faa2d4de2644ac9987e4a2f1f2fa AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && printf '%s\n' \
        'ziglang==0.15.2 --hash=sha256:806316453d1ede7174ed3fefb8e5348154629089c3bb5fe812dfd0e172fac130 --hash=sha256:edc0aa60ec964a4cf462d40f68d7de242ddf37fd9a80f2afaee6397059463230' \
        > /tmp/zig.txt \
    && pip3 install --break-system-packages --no-cache-dir --require-hashes -r /tmp/zig.txt \
    && cargo install --locked cargo-zigbuild@0.24.0 \
    && cargo install --locked --features cli cargo-about@0.9.3 \
    && rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
WORKDIR /src
COPY Cargo.toml Cargo.lock about.toml about.hbs LICENSE NOTICE ./
COPY crates crates
ARG TARGETARCH
# The registry's cache is locked, both platforms' builds running at once. No cache of the
# target directory: cargo would trust its stale files over the sources copied in.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && cargo zigbuild --release --locked -p llm-ratelimit --bin llm-ratelimit --target "$target" \
    && mkdir -p /image/bin \
    && cp "target/$target/release/llm-ratelimit" /image/bin/llm-ratelimit \
    && ln -s llm-ratelimit /image/bin/ratelimit \
    && mkdir /doc \
    && cp LICENSE NOTICE /doc \
    && cargo fetch --locked \
    && cargo about generate --frozen --fail about.hbs -o /doc/THIRD-PARTY-LICENSES.txt

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
# A directory, as a file would be copied through its link.
COPY --from=build /image/bin/ /bin/
COPY --from=build /doc /usr/share/doc/llm-ratelimit
ENTRYPOINT ["/bin/llm-ratelimit"]
