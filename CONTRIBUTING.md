# Contributing to PelicanQ

Thank you for your interest in contributing to PelicanQ! We welcome contributions of all kinds: bug fixes, feature implementations, documentation improvements, SDKs, and examples.

## Getting Started

### Prerequisites
- Rust 1.75 or later
- `protoc` (Protocol Buffers compiler) installed and in your `PATH`
- `cargo` (comes with Rust)

### Setup
```bash
# Clone the repository
git clone https://github.com/Open-Collective-Labs/PelicanQ.git
cd PelicanQ

# Build the entire workspace
cargo build

# Run tests to verify your setup
cargo test --workspace

# Run the daemon locally
PELICANQ_DATA_DIR=./data cargo run --bin pelicanqd
```

## Development Workflow

1. **Create a branch** from `main` with a descriptive name:
   ```bash
   git checkout -b feature/my-feature
   # or
   git checkout -b fix/issue-description
   ```

2. **Make your changes** with focused, clear commits:
   ```bash
   git add <files>
   git commit -m "Brief description of change"
   ```

3. **Run quality checks** before pushing:
   ```bash
   # Format code
   cargo fmt

   # Run tests
   cargo test --workspace

   # Lint (clippy)
   cargo clippy --all-targets

   # Check documentation
   cargo doc --no-deps --open
   ```

4. **Push and open a Pull Request**:
   - Describe what your change does and why
   - Reference any related issues
   - Keep PRs focused; smaller is better

## What We're Looking For

### High-Value Contributions
- **SDKs**: Go, Python, Node.js, Java, Ruby (see [roadmap](docs/roadmap.md))
- **Examples**: Real-world usage patterns and integrations
- **Documentation**: Architecture clarifications, deployment guides, troubleshooting
- **Performance**: Benchmarks, optimizations, profiling
- **Testing**: Unit tests, integration tests, chaos testing
- **Bug fixes**: Any reproducible issues

### Code Standards
- Follow Rust conventions (rustfmt formatting enforced)
- Add tests for new functionality
- Update documentation if behavior changes
- Keep commits focused and messages clear
- Avoid large sweeping refactors; discuss first

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `pelicanq-core/` | Core engine: queues, persistence, delivery semantics |
| `pelicanqd/` | HTTP + gRPC daemon |
| `pelicanq-raft/` | Raft consensus layer (openraft-based) |
| `sdks/rust/` | Rust client SDK (reference) |
| `pelicanctl/` | CLI tool for cluster management |
| `proto/` | Canonical protobuf contracts (source of truth) |
| `docs/` | Architecture, clustering, deployment, roadmap |
| `examples/` | Runnable examples |
| `scripts/` | Build, test, release helpers |

## Key Architectural Concepts

- **At-least-once delivery**: Messages are retried until acknowledged
- **FIFO ordering**: Within a queue, messages are processed in order
- **Dual protocols**: HTTP/REST and gRPC serve the same core engine
- **Embedded storage**: Built on sled (embedded B+ tree database)
- **Raft clustering**: Multi-node high availability (Flock mode)

See [Architecture](docs/architecture.md) and [Clustering](docs/clustering.md) for deeper details.

## Roadmap & Issues

Review the [roadmap](docs/roadmap.md) to see planned features and help prioritize your work. Open an issue **before** starting significant work to discuss approach and avoid duplicate effort.

## Testing

```bash
# Unit and integration tests
cargo test --workspace

# Run specific test
cargo test --package pelicanq-core queue_behavior

# With logging (RUST_LOG=debug)
RUST_LOG=debug cargo test --workspace -- --nocapture
```

## Commit Messages

Follow this pattern:
```
<type>(<scope>): <subject>

<body>

<footer>
```

Examples:
```
feat(core): add message TTL support
fix(raft): handle network partition correctly
docs: update clustering guide
test(delivery): add redelivery scenario
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

## Questions?

- Check [docs/](docs/) for architecture and design decisions
- Open a [Discussion](https://github.com/Open-Collective-Labs/PelicanQ/discussions) (when enabled)
- Start an issue for design questions

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

**Happy hacking! 🚀**
