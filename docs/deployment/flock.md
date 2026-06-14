# Flock Cluster (Raft)

Multi-node cluster using Raft consensus. Every queue is replicated across all nodes with automatic leader election and failover.

## When to Use

- High-availability production
- Rolling upgrades
- Multi-node deployments

## Configuration

Clustering is configured through environment variables. When `PELICANQ_NODE_ID` is unset, the daemon runs in Solo mode.

| Variable | Example | Description |
|----------|---------|-------------|
| `PELICANQ_NODE_ID` | `2` | This node's unique integer ID |
| `PELICANQ_CLUSTER_MEMBERS` | `1@10.0.0.1:7071=10.0.0.1:7070,...` | Comma-separated list of all members |

### Member Format

```
<id>@<raft_addr>=<client_addr>
```

| Part | Description |
|------|-------------|
| `id` | Integer node ID |
| `raft_addr` | Address for inter-node Raft RPC traffic |
| `client_addr` | Address clients use to reach this node's HTTP API |

All nodes must have the identical `PELICANQ_CLUSTER_MEMBERS` list.

## 3-Node Dev Cluster

```bash
# Terminal 1
PELICANQ_NODE_ID=1 \
  PELICANQ_CLUSTER_MEMBERS="1@127.0.0.1:7071=127.0.0.1:7070,2@127.0.0.1:7081=127.0.0.1:7080,3@127.0.0.1:7091=127.0.0.1:7090" \
  PELICANQ_DATA_DIR=./data/node1 \
  PELICANQ_LISTEN_ADDR=127.0.0.1:7070 \
  PELICANQ_GRPC_ADDR=127.0.0.1:7072 \
  cargo run --bin pelicanqd

# Terminal 2 (node 2)
PELICANQ_NODE_ID=2 \
  PELICANQ_CLUSTER_MEMBERS="1@127.0.0.1:7071=127.0.0.1:7070,2@127.0.0.1:7081=127.0.0.1:7080,3@127.0.0.1:7091=127.0.0.1:7090" \
  PELICANQ_DATA_DIR=./data/node2 \
  PELICANQ_LISTEN_ADDR=127.0.0.1:7080 \
  PELICANQ_GRPC_ADDR=127.0.0.1:7082 \
  cargo run --bin pelicanqd

# Terminal 3 (node 3)
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

## Cluster Status

```bash
curl http://127.0.0.1:7070/cluster/status
```

## Leader Redirects

When a mutating request arrives at a follower, the node responds with HTTP 421 and an `X-Pelican-Leader-Addr` header pointing to the current leader.

## Failure Modes

- **Leader failure**: Followers detect timeout (~750-1500ms), hold election, new leader takes over.
- **Follower failure**: Tolerated as long as quorum (majority) remains. Follower catches up on restart.
- **Network partition**: Minority side cannot commit writes. Recovers via log replication when partition heals.

## Limitations

- No dynamic membership changes (restart required to add/remove nodes)
- Reads may be stale on followers (served locally without Raft round-trip)
- No per-queue sharding — every node stores all queues
