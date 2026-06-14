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
| **MQTT 3.1.1** | `127.0.0.1:1883` | ✅ QoS 0/1, topic-to-queue mapping |

All protocols serve the **same data** through the **same engine**. You can publish over HTTP and consume over gRPC, or vice versa.

## SDKs

| Language | Crate / Package | Status |
|---|---|---|
| **Rust** | `pelicanq` (`sdks/rust/`) | ✅ Reference implementation |
| Go | `sdks/go/` | 🚧 In progress |
| Python | `sdks/python/` | 🚧 In progress |
| Node.js | `sdks/node/` | 🚧 In progress |
| Java | `sdks/java/` | 🚧 In progress |

## Documentation

| Section | Contents |
|---------|----------|
| [Getting Started](docs/getting-started/quickstart.md) | Quickstart, installation, configuration |
| [Guides](docs/guides/publish-consume.md) | Publish/consume, batches, scheduling, priorities, dedup, DLQ, MQTT |
| [Architecture](docs/architecture/overview.md) | System design, storage model, delivery semantics, retention, clustering |
| [Deployment](docs/deployment/solo.md) | Solo mode, Flock cluster, configuration reference |
| [Reference](docs/reference/http-api.md) | HTTP API, gRPC API, proto spec, SDK docs |
| [Development](docs/development/building.md) | Building from source, testing, contributing |
| [Roadmap](docs/roadmap.md) | Completed, in-progress, and planned features |
| [Features & Spec](FEATURES.md) | Full feature specification with status

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

We welcome contributions! See the [Contributing Guide](CONTRIBUTING.md) to get started.

Small iterative PRs are preferred over large sweeping changes.

## License

MIT

---

[Changelog](CHANGELOG.md) — [Contributing](CONTRIBUTING.md) — [Code of Conduct](CODE_OF_CONDUCT.md) — [Security](SECURITY.md)
