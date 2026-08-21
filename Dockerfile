FROM rust:1-alpine AS chef
RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ARG BINARY=server
RUN cargo build --release --bin ${BINARY}
RUN cp target/release/${BINARY} /app/binary

FROM busybox:stable AS runtime
COPY --from=builder /app/binary /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
