FROM mcr.microsoft.com/devcontainers/rust:1

RUN apt-get update && apt-get install -y \
    netcat-openbsd \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Install mise
RUN curl https://mise.run | sh
ENV PATH="/root/.local/bin:${PATH}"

# Install just
RUN cargo install just

# Install nightly toolchain
RUN rustup toolchain install nightly --component rustfmt miri

# Install cargo tools
RUN cargo install cargo-llvm-cov cargo-deny cargo-audit cargo-chef

# Install act (local CI runner)
RUN curl -s https://raw.githubusercontent.com/nektos/act/master/install.sh | bash
