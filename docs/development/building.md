# Building from Source

## Prerequisites

- **Rust 1.75+**: [rustup](https://rustup.rs/)
- **protoc** (optional): For gRPC codegen verification

## Build Commands

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Build specific crate
cargo build -p pelicanqd
cargo build -p pelicanq-core
cargo build -p pelicanq-raft
```

## Build Output

The compiled binary is at `./target/debug/pelicanqd` (debug) or `./target/release/pelicanqd` (release).
