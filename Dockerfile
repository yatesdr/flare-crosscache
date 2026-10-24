FROM rust:1.94.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/selene /usr/local/bin/selene
ENTRYPOINT ["/usr/local/bin/selene"]
