FROM rust:1.94.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/flare-crosscache /usr/local/bin/flare-crosscache
ENTRYPOINT ["/usr/local/bin/flare-crosscache"]
