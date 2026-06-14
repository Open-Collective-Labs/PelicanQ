# Testing

## Running Tests

```bash
# All unit tests
cargo test --workspace

# Specific crate
cargo test -p pelicanq-core

# Run a specific test
cargo test -p pelicanq-core -- test_deduplication

# Integration tests (requires running daemon, gated by env var)
PELICANQ_INTEGRATION=1 cargo test --workspace -- --ignored
```

## Test Structure

| Crate | Test Count | Type |
|-------|-----------|------|
| `pelicanq-core` | 54 | Unit tests |
| `pelicanq-raft` | 9 | Unit + integration |
| `pelicanqd` | 6 | Integration |
| `pelicanq` (Rust SDK) | 7 | Unit |

## Code Quality

```bash
# Formatting
cargo fmt --check

# Linting
cargo clippy --all-targets

# All checks
cargo test --workspace && cargo fmt --check && cargo clippy --all-targets
```
