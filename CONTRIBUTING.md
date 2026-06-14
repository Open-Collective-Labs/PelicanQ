# Contributing to PelicanQ

Thanks for your interest in contributing! We welcome contributions of all kinds — bug reports, feature requests, documentation, tests, and code changes.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Improving Documentation](#improving-documentation)
  - [Contributing Code](#contributing-code)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Pull Request Process](#pull-request-process)
- [Commit Messages](#commit-messages)
- [Getting Help](#getting-help)

## Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the maintainers.

## Getting Started

1. Fork the repository.
2. Clone your fork: `git clone https://github.com/<your-username>/PelicanQ.git`
3. Add the upstream remote: `git remote add upstream https://github.com/Open-Collective-Labs/PelicanQ.git`
4. Read the [README](README.md) and [documentation](docs/) to understand the project.

## How to Contribute

### Reporting Bugs

Open a [bug report](.github/ISSUE_TEMPLATE/bug_report.md) with:

- A clear title and description.
- Steps to reproduce (code snippets, logs, config).
- Expected vs actual behavior.
- Environment details (OS, Rust version, PelicanQ version).

### Suggesting Features

Open a [feature request](.github/ISSUE_TEMPLATE/feature_request.md) with:

- The problem you're trying to solve.
- Proposed solution or API.
- Alternatives you've considered.

### Improving Documentation

Documentation lives in `docs/` and the root `README.md`. We welcome:

- Fixing typos or unclear sections.
- Adding missing guides or examples.
- Translating documentation.
- Improving docstrings in Rust source files.

### Contributing Code

1. Pick an issue — comment to let others know you're working on it.
2. Create a branch: `git checkout -b my-feature`.
3. Make your changes (see [Development Setup](#development-setup)).
4. Run tests: `cargo test --workspace`.
5. Run linting: `cargo fmt --check && cargo clippy --all-targets`.
6. Commit your changes (see [Commit Messages](#commit-messages)).
7. Push to your fork and open a pull request.

## Development Setup

### Prerequisites

- **Rust**: Install via [rustup](https://rustup.rs/) (minimum version 1.75).
- **Protoc** (optional, for gRPC codegen): Install via your package manager or from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases).

### Build

```bash
# Build the entire workspace
cargo build

# Build in release mode
cargo build --release
```

### Test

```bash
# Run all unit tests
cargo test --workspace

# Run integration tests (requires a running daemon)
PELICANQ_INTEGRATION=1 cargo test --workspace -- --ignored

# Run specific crate tests
cargo test -p pelicanq-core
```

### Run

```bash
# Solo mode (single node)
PELICANQ_DATA_DIR=./data cargo run --bin pelicanqd

# Flock mode (3-node cluster)
./scripts/dev-cluster.sh
```

## Project Structure

```
pelicanq/
├── pelicanq-core/        # Core engine: queues, persistence, delivery
├── pelicanqd/            # Daemon binary (HTTP + gRPC server)
├── pelicanq-raft/        # Raft consensus layer
├── proto/                # Canonical protobuf contracts
├── sdks/                 # Client SDKs
│   ├── rust/             #   Rust (reference implementation)
│   ├── go/               #   Go
│   ├── python/           #   Python
│   ├── node/             #   Node.js / TypeScript
│   └── java/             #   Java
├── docs/                 # Documentation
├── examples/             # Runnable examples per language
├── scripts/              # Dev and CI scripts
└── pelicanctl/           # CLI tool (in progress)
```

## Coding Standards

### Rust

- Format with `cargo fmt` (default rustfmt settings).
- Lint with `cargo clippy --all-targets` — no warnings.
- Follow standard Rust conventions (snake_case, `Result<T, E>`, etc.).
- All public APIs must have doc comments.
- Error types should implement `std::error::Error`.

### Go

- Format with `gofmt` (default settings).
- Follow [Effective Go](https://go.dev/doc/effective_go) conventions.

### Python

- Follow [PEP 8](https://peps.python.org/pep-0008/).
- Type hints required for all public APIs.

### TypeScript / Node.js

- Format with `prettier` (default settings).
- Strict TypeScript mode required.

### Java

- Follow [Google Java Style](https://google.github.io/styleguide/javaguide.html).
- Maven for build, JUnit 4 for tests.

## Pull Request Process

1. Ensure your PR references an existing issue (create one first if needed).
2. Keep PRs focused — one feature or fix per PR.
3. Update documentation and add tests for new functionality.
4. Ensure CI passes (tests, linting, formatting).
5. Squash commits before merging (see below).
6. A maintainer will review your PR within a few days.

### Before Submitting

- [ ] Code compiles without warnings
- [ ] Tests pass: `cargo test --workspace`
- [ ] Linting passes: `cargo fmt --check && cargo clippy --all-targets`
- [ ] Documentation is updated
- [ ] Commit messages follow the guidelines

## Commit Messages

We follow [conventional commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Usage | Example |
|------|-------|---------|
| `feat` | New feature | `feat(queue): add priority queue support` |
| `fix` | Bug fix | `fix(raft): handle split-brain on partition recovery` |
| `docs` | Documentation | `docs(api): add HTTP consume endpoint example` |
| `test` | Tests | `test(core): add concurrency test for publish` |
| `refactor` | Code change without feature/fix | `refactor(engine): extract retention logic` |
| `perf` | Performance improvement | `perf(storage): batch sled writes` |
| `chore` | Maintenance | `chore(deps): update tonic to 0.12` |

### Scope Examples

`core`, `raft`, `grpc`, `http`, `mqtt`, `sdk/rust`, `sdk/go`, `docs`, `proto`

## Getting Help

- **Issues**: Open a GitHub issue for bugs or feature requests.
- **Discussions**: Use GitHub Discussions for questions.
- **Security**: Report vulnerabilities to the maintainers directly (see [SECURITY.md](SECURITY.md)).
