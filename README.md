# PelicanQ

A distributed, crash-safe message queue built in Rust.

## Project Structure

| Directory | Description |
|---|---|
| `pelicanq-core/` | Core engine: message types, queues, persistence, delivery |
| `pelicanqd/` | Daemon binary with HTTP API |
| `pelicanctl/` | CLI tool for managing PelicanQ |
| `proto/` | Protobuf contracts |
| `sdks/` | Client SDKs (Rust, Go, Python, Node, Java) |
| `docs/` | Architecture, deployment, and roadmap docs |
| `examples/` | Runnable examples per SDK |
| `scripts/` | Dev/build/release helpers |
| `tests/` | Integration tests |

See [FEATURES.md](FEATURES.md) for the full feature spec.
