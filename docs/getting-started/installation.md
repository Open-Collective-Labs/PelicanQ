# Installation

## Building from Source

### Prerequisites

- **Rust**: Minimum version 1.75. Install via [rustup](https://rustup.rs/):
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **protoc** (optional, for gRPC codegen): Install via your package manager:
  ```bash
  # Debian/Ubuntu
  apt install protobuf-compiler

  # macOS
  brew install protobuf

  # Arch Linux
  pacman -S protobuf
  ```

### Build

```bash
git clone https://github.com/Open-Collective-Labs/PelicanQ.git
cd PelicanQ
cargo build --release
```

The binary is at `./target/release/pelicanqd`.

## Configuration

PelicanQ is configured entirely through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_DATA_DIR` | `./data` | Directory for persistent storage |
| `PELICANQ_LISTEN_ADDR` | `127.0.0.1:7070` | HTTP API listen address |
| `PELICANQ_GRPC_ADDR` | `127.0.0.1:7072` | gRPC API listen address |
| `PELICANQ_MQTT_ADDR` | `127.0.0.1:1883` | MQTT listener address (empty to disable) |
| `PELICANQ_NODE_ID` | (unset) | Node ID for Flock mode (unset = Solo) |
| `PELICANQ_CLUSTER_MEMBERS` | (unset) | Cluster topology for Flock mode |
| `RUST_LOG` | `info` | Log level (debug, info, warn, error) |

## Running

### Solo Mode (default)

```bash
PELICANQ_DATA_DIR=./data ./target/release/pelicanqd
```

### Flock Mode (clustered)

See the [Flock deployment guide](../deployment/flock.md).

## Docker

A Dockerfile is available at the root of the repository:

```bash
docker build -t pelicanq .
docker run -p 7070:7070 -p 7072:7072 -e PELICANQ_DATA_DIR=/data pelicanq
```
