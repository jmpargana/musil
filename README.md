<p align="center">
  <img src="assets/musil_logo.png" alt="Musil" width="200">
</p>

# musil

[![CI](https://github.com/jmpargana/musil/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jmpargana/musil/actions/workflows/ci.yml)
[![Integration](https://github.com/jmpargana/musil/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/jmpargana/musil/actions/workflows/integration.yml)
[![codecov](https://codecov.io/gh/jmpargana/musil/branch/main/graph/badge.svg)](https://codecov.io/gh/jmpargana/musil)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

A partially* Kafka-compatible broker, producer, consumer and seeder implemented in Rust.

KRaft controller implementation in progress.

## Packages

| Package | crates.io | Homebrew |
|---------|-----------|----------|
| musil-proto | [![crates.io](https://img.shields.io/crates/v/musil-proto)](https://crates.io/crates/musil-proto) | — |
| musil-raft | [![crates.io](https://img.shields.io/crates/v/musil-raft)](https://crates.io/crates/musil-raft) | — |
| musil-network | [![crates.io](https://img.shields.io/crates/v/musil-network)](https://crates.io/crates/musil-network) | — |
| musil-storage | [![crates.io](https://img.shields.io/crates/v/musil-storage)](https://crates.io/crates/musil-storage) | — |
| musil-raft-runtime | [![crates.io](https://img.shields.io/crates/v/musil-raft-runtime)](https://crates.io/crates/musil-raft-runtime) | — |
| musil-broker | — | [![homebrew](https://img.shields.io/badge/brew-musil--broker-orange)](https://github.com/jmpargana/homebrew-tools/blob/main/Formula/musil-broker.rb) |
| musil-producer | — | [![homebrew](https://img.shields.io/badge/brew-musil--producer-orange)](https://github.com/jmpargana/homebrew-tools/blob/main/Formula/musil-producer.rb) |
| musil-consumer | — | [![homebrew](https://img.shields.io/badge/brew-musil--consumer-orange)](https://github.com/jmpargana/homebrew-tools/blob/main/Formula/musil-consumer.rb) |
| musil-seeder | — | [![homebrew](https://img.shields.io/badge/brew-musil--seeder-orange)](https://github.com/jmpargana/homebrew-tools/blob/main/Formula/musil-seeder.rb) |

## Install

```bash
brew tap jmpargana/tools
brew install musil-broker musil-producer musil-consumer musil-seeder
```

## Quick Start

```bash
# Install tools
mise install

# Build
just build

# Run tests
just test

# Start local cluster (3 brokers + seeder)
just compose-up

# Run integration tests
just integration-test
```

## Development

```bash
# Lint + format + test
just check

# Coverage report
just coverage-html

# Security audit
just audit
just deny
```

## License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
