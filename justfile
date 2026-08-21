set dotenv-load

default:
    @just --list

# === Build ===

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

# === Test ===

test:
    cargo test --workspace

test-features:
    cargo test --workspace --features raft
    cargo test --workspace --features standalone

# === Lint ===

lint:
    cargo clippy --workspace -- -W clippy::pedantic \
        -A clippy::module_name_repetitions \
        -A clippy::must_use_candidate \
        -A clippy::missing_errors_doc \
        -A clippy::missing_panics_doc \
        -A clippy::doc_markdown

fmt:
    cargo +nightly fmt --all

fmt-check:
    cargo +nightly fmt --all -- --check

check: fmt-check lint test

# === Coverage ===

coverage:
    cargo llvm-cov --workspace --lcov --output-path lcov.info

coverage-html:
    cargo llvm-cov --workspace --html
    @echo "Open target/llvm-cov/html/index.html"

# === Security ===

audit:
    cargo audit

deny:
    cargo deny check

# === Miri ===

miri:
    cargo +nightly miri test --workspace

# === Docker ===

docker-build binary:
    docker build --build-arg BINARY={{binary}} -t ghcr.io/jmpargana/musil-{{binary}}:latest .

docker-push binary:
    docker push ghcr.io/jmpargana/musil-{{binary}}:latest

docker-build-all:
    just docker-build server
    just docker-build consumer
    just docker-build producer
    just docker-build seeder

# === Compose (local dev cluster) ===

compose-up:
    docker compose up -d

compose-down:
    docker compose down -v

compose-logs:
    docker compose logs -f

integration-test: compose-up
    @echo "Waiting for cluster health..."
    @sleep 5
    cargo test --test integration
    just compose-down

# === OrbStack (macOS containers) ===

orb-up:
    orb start musil-dev

orb-down:
    orb stop musil-dev

# === Release (local) ===

release-local:
    cargo build --release --target x86_64-unknown-linux-gnu
    cargo build --release --target aarch64-unknown-linux-gnu
    cargo build --release --target x86_64-apple-darwin
    cargo build --release --target aarch64-apple-darwin
