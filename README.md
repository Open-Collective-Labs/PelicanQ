# PelicanQ

**PelicanQ** is a distributed, crash-safe message queue built in Rust. It provides at-least-once delivery with FIFO ordering, embedded sled persistence, dual-protocol access (HTTP + gRPC), and Raft-based clustering for high availability.

## Quick Start

```bash
# Build the workspace
cargo build

# Run the daemon (Solo mode — single node, no Raft)
PELICANQ_DATA_DIR=./data cargo run --bin pelicanqd

# In another terminal, use the Rust SDK example:
cargo run -p rust-publish-consume -- http://127.0.0.1:7072

# Or use the HTTP API directly:
curl -X POST http://127.0.0.1:7070/queues/myqueue
curl -X POST http://127.0.0.1:7070/queues/myqueue/publish \
  -H 'content-type: application/json' \
  -d '{"payload_base64":"SGVsbG8=","headers":{}}'
curl -X POST http://127.0.0.1:7070/queues/myqueue/consume
```

## Project Structure

| Directory | Description |
|---|---|
| `pelicanq-core/` | Core engine: message types, queues, persistence, delivery |
| `pelicanqd/` | Daemon binary with HTTP and gRPC API |
| `pelicanq-raft/` | Raft consensus layer (openraft) for clustered mode |
| `sdks/rust/` | Rust client SDK (reference implementation) |
| `pelicanctl/` | CLI tool for managing PelicanQ |
| `proto/` | Canonical protobuf contracts (source of truth) |
| `docs/` | Architecture, clustering, deployment, and roadmap |
| `examples/` | Runnable examples per SDK |
| `scripts/` | Dev/build/release helper scripts |

## Protocol Support

| Protocol | Port (default) | Status |
|---|---|---|
| **HTTP/REST** | `127.0.0.1:7070` | ✅ Complete — all operations |
| **gRPC** | `127.0.0.1:7072` | ✅ Complete — all operations including streaming consume |

Both protocols serve the **same data** through the **same engine**. You can publish over HTTP and consume over gRPC, or vice versa.

## SDKs

| Language | Crate / Package | Status |
|---|---|---|
| **Rust** | `pelicanq` (`sdks/rust/`) | ✅ Reference implementation |
| Go | — | ❌ Planned |
| Python | — | ❌ Planned |
| Node.js | — | ❌ Planned |
| Java | — | ❌ Planned |

## Documentation

- [Features & Spec](FEATURES.md) — full feature specification
- [Architecture](docs/architecture.md) — storage model, delivery semantics, retention, gRPC
- [Clustering](docs/clustering.md) — Raft Flock mode configuration and operations
- [Deployment Tiers](docs/deployment-tiers.md) — Solo vs Flock cluster modes
- [Roadmap](docs/roadmap.md) — upcoming features and build plan

## Maintain

```bash
# Run all tests
cargo test --workspace

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
