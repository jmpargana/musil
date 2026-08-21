<p align="center">
  <img src="assets/musil_logo.png" alt="Musil" width="200">
</p>

# musil

[![CI](https://github.com/jmpargana/musil/actions/workflows/ci.yml/badge.svg)](https://github.com/jmpargana/musil/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jmpargana/musil/branch/main/graph/badge.svg)](https://codecov.io/gh/jmpargana/musil)
[![crates.io](https://img.shields.io/crates/v/musil-proto.svg)](https://crates.io/crates/musil-proto)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

A partially* Kafka-compatible broker, producer, consumer and seeder implemented in Rust.

KRaft controller implementation in progress.

## Components

| Binary | Description |
|--------|-------------|
| `server` | Broker node (supports raft consensus or standalone mode) |
| `consumer` | CLI consumer client |
| `producer` | CLI producer client |
| `seeder` | Topic/partition seeder utility |

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
