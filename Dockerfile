FROM rust:1.94.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/flare-cache-manager /usr/local/bin/flare-cache-manager
ENTRYPOINT ["/usr/local/bin/flare-cache-manager"]
