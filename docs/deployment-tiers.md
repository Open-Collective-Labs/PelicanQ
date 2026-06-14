# Deployment Tiers

## Solo

Single `pelicanqd` process with embedded sled storage. No replication. Suitable for:

- Local development
- CI/CD pipelines
- Low-throughput production (loss of the node is acceptable)

### Ports

| Service | Default | Env Var |
|---|---|---|
| HTTP API | `127.0.0.1:7070` | `PELICANQ_LISTEN_ADDR` |
| gRPC API | `127.0.0.1:7072` | `PELICANQ_GRPC_ADDR` |

### Start

```bash
PELICANQ_DATA_DIR=./data cargo run --bin pelicanqd
```

## Flock

Multi-node cluster using Raft consensus. Every queue is replicated across N nodes. Automatic leader election and failover. Suitable for:

- High-availability production
- Geographic distribution
- Rolling upgrades

### Ports

Each node needs two client-facing ports plus a Raft address (can be same host with different ports):

| Service | Default | Env Var |
|---|---|---|
| HTTP API | Per-node | `PELICANQ_LISTEN_ADDR` |
| gRPC API | Per-node | `PELICANQ_GRPC_ADDR` |
| Raft inter-node | Per-node | From `PELICANQ_CLUSTER_MEMBERS` |

### Quick Start (3-node dev cluster)

```bash
# Terminal 1
PELICANQ_NODE_ID=1 \
  PELICANQ_CLUSTER_MEMBERS="1@127.0.0.1:7071=127.0.0.1:7070,2@127.0.0.1:7081=127.0.0.1:7080,3@127.0.0.1:7091=127.0.0.1:7090" \
  PELICANQ_DATA_DIR=./data/node1 \
  PELICANQ_LISTEN_ADDR=127.0.0.1:7070 \
  PELICANQ_GRPC_ADDR=127.0.0.1:7072 \
  cargo run --bin pelicanqd

# Terminal 2
PELICANQ_NODE_ID=2 \
  PELICANQ_CLUSTER_MEMBERS="1@127.0.0.1:7071=127.0.0.1:7070,2@127.0.0.1:7081=127.0.0.1:7080,3@127.0.0.1:7091=127.0.0.1:7090" \
  PELICANQ_DATA_DIR=./data/node2 \
  PELICANQ_LISTEN_ADDR=127.0.0.1:7080 \
  PELICANQ_GRPC_ADDR=127.0.0.1:7082 \
  cargo run --bin pelicanqd

# Terminal 3
PELICANQ_NODE_ID=3 \
  PELICANQ_CLUSTER_MEMBERS="1@127.0.0.1:7071=127.0.0.1:7070,2@127.0.0.1:7081=127.0.0.1:7080,3@127.0.0.1:7091=127.0.0.1:7090" \
  PELICANQ_DATA_DIR=./data/node3 \
  PELICANQ_LISTEN_ADDR=127.0.0.1:7090 \
  PELICANQ_GRPC_ADDR=127.0.0.1:7092 \
  cargo run --bin pelicanqd
```

Or use the helper script:

```bash
./scripts/dev-cluster.sh
```

### SDK Connection

```rust
// Connect to any node (gRPC)
let mut client = PelicanClient::connect("http://127.0.0.1:7072").await?;

// Mutating operations are forwarded to the leader transparently
// by the SDK (Status::Unavailable triggers redirection).
```
