# PelicanQ

**PelicanQ** is a distributed, crash-safe message queue built in Rust. Designed for reliability and simplicity, it provides at-least-once delivery with FIFO ordering, embedded persistence, and a clean HTTP API — with clustering (Raft), gRPC, and multi-language SDKs on the roadmap.

## Quick Start

```bash
# Build the project
cargo build

# Run the daemon (once implemented)
cargo run --bin pelicanqd
```

## Project Structure

| Directory | Description |
|---|---|
| `pelicanq-core/` | Core engine: message types, queues, persistence, delivery |
| `pelicanqd/` | Daemon binary with HTTP API |
| `pelicanctl/` | CLI tool for managing PelicanQ |
| `proto/` | Shared protobuf contracts |
| `sdks/` | Client SDKs (Rust, Go, Python, Node, Java) |
| `docs/` | Architecture, deployment, and roadmap docs |
| `examples/` | Runnable examples per SDK |
| `scripts/` | Dev/build/release helper scripts |
| `tests/` | Cross-crate integration tests |

## Documentation

- [Features & Spec](FEATURES.md) — full feature specification
- [Architecture](docs/architecture.md) — storage model, delivery semantics, retention
- [Deployment Tiers](docs/deployment-tiers.md) — Solo vs Flock cluster modes
- [Roadmap](docs/roadmap.md) — upcoming features and build plan

## Maintain

```bash
# Run all tests
cargo test

# Build in release mode
cargo build --release

# Check formatting
cargo fmt --check

# Lint
cargo clippy --all-targets
```

## Contribute

1. Fork the repo and create a branch from `main`.
2. Make your changes — keep commits focused and messages clear.
3. Run tests and linting before opening a PR.
4. Open a pull request describing the change and any relevant issues.

See the [roadmap](docs/roadmap.md) for planned work. Small iterative PRs are preferred over large sweeping changes.

## License

MIT
