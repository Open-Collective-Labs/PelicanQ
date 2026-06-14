# Clustering (Flock mode)

PelicanQ clusters multiple `pelicanqd` nodes into a fault-tolerant
**Flock** using the [Raft](https://raft.github.io/) consensus protocol
([openraft](https://docs.rs/openraft/0.9/)).

Every node in a Flock holds a full copy of all queues (the entire state
machine). Mutating operations (publish, consume, ack, nack, declare
queue) are submitted to the **leader**, which replicates them to a
quorum of followers before responding. Read-only operations (list
queues, depth checks) are served **locally** on each node and may lag
the leader by up to a few hundred milliseconds.

---

## Configuration

Clustering is configured through environment variables. When
`PELICANQ_NODE_ID` is unset the daemon runs in **Solo mode** (single
node, no Raft — the Phase 1–2 behaviour).

### Required variables

| Variable | Example | Description |
|---|---|---|
| `PELICANQ_NODE_ID` | `2` | This node's unique integer ID in the cluster (small positive integer, e.g. 1, 2, 3) |
| `PELICANQ_CLUSTER_MEMBERS` | `1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070` | Comma-separated list of all members |


### `PELICANQ_CLUSTER_MEMBERS` format

Each member entry follows this format:

```
<id>@<raft_addr>=<client_addr>
```

| Part | Description |
|---|---|
| `id` | Integer node ID, must match `PELICANQ_NODE_ID` for this node |
| `raft_addr` | Address (host:port) for inter-node Raft RPC traffic |
| `client_addr` | Address (host:port) clients use to reach this node's HTTP API |

**Important:** Every node must have the same `PELICANQ_CLUSTER_MEMBERS`
list. There is no gossip-based discovery — the full topology is static.

### Example (3-node cluster)

**Node 1:**
```
PELICANQ_NODE_ID=1
PELICANQ_CLUSTER_MEMBERS=1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070,3@10.0.0.3:7071=10.0.0.3:7070
PELICANQ_DATA_DIR=/var/lib/pelicanq
PELICANQ_LISTEN_ADDR=10.0.0.1:7070
```

**Node 2:**
```
PELICANQ_NODE_ID=2
PELICANQ_CLUSTER_MEMBERS=1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070,3@10.0.0.3:7071=10.0.0.3:7070
PELICANQ_DATA_DIR=/var/lib/pelicanq
PELICANQ_LISTEN_ADDR=10.0.0.2:7070
```

**Node 3:**
```
PELICANQ_NODE_ID=3
PELICANQ_CLUSTER_MEMBERS=1@10.0.0.1:7071=10.0.0.1:7070,2@10.0.0.2:7071=10.0.0.2:7070,3@10.0.0.3:7071=10.0.0.3:7070
PELICANQ_DATA_DIR=/var/lib/pelicanq
PELICANQ_LISTEN_ADDR=10.0.0.3:7070
```

---

## Bootstrap convention

On first startup, every node detects that no existing Raft state is
present. The node with the **lowest ID** in `PELICANQ_CLUSTER_MEMBERS`
automatically calls `initialize` to bootstrap the cluster with the full
member list. Other nodes start as followers and wait to be contacted.

On subsequent restarts the persisted Raft state (log entries, term,
vote) is replayed and initialization is skipped — the node resumes
where it left off.

---

## Cluster status endpoint

```
GET /cluster/status
```

Response (Flock mode):
```json
{
  "self_id": 1,
  "members": [
    {"id": 1, "raft_addr": "10.0.0.1:7071", "client_addr": "10.0.0.1:7070"},
    {"id": 2, "raft_addr": "10.0.0.2:7071", "client_addr": "10.0.0.2:7070"},
    {"id": 3, "raft_addr": "10.0.0.3:7071", "client_addr": "10.0.0.3:7070"}
  ],
  "reads_may_lag": true
}
```

The `reads_may_lag` field indicates that list/depth queries are served
locally and may lag the leader by up to a few heartbeats.

In Solo mode this endpoint returns 404.

---

## Leader redirects

When a mutating request (publish, consume, ack, nack, declare queue)
arrives at a **follower**, the node responds with **HTTP 421
Misdirected Request** and an `X-Pelican-Leader-Addr` header pointing to
the current leader's `client_addr`. SDKs and clients should retry the
request against that address.

Example response:
```
HTTP/1.1 421 Misdirected Request
X-Pelican-Leader-Addr: 10.0.0.2:7070
```

If the leader is not yet known (during an election), the header is
omitted and the client should retry with backoff.

---

## Failure modes

### Leader failure

- Followers detect the leader has timed out (no heartbeats for
  ~750–1500 ms) and hold an election.
- One follower wins the election and becomes the new leader.
- The new leader starts accepting client writes.
- Clients that were talking to the dead leader will receive connection
  errors; they should retry against any known node, which will redirect
  them (via 421) to the new leader.

### Follower failure

- The leader continues operating as long as a quorum (majority) of
  nodes is still alive. A single follower failure in a 3-node cluster
  is tolerated.
- When the failed follower restarts, it replays the persisted Raft log
  and catches up. The leader replicates any entries the restarted node
  missed.
- Queue state is verified to match the leader after the restart.

### Network partition

- The side without a majority cannot commit new entries.
- Writes to the minority side will fail with a 421 or a connection
  error.
- When the partition heals, the minority node catches up via log
  replication.

---

## Durability

Raft log entries are stored in a `sled` database at
`<data_dir>/raft/`. Each entry is flushed to disk before the write
operation returns to openraft. The `QueueManager` state machine is also
backed by sled at `<data_dir>`.

After a full cluster restart (all nodes down simultaneously), the
cluster resumes from the last persisted state. The lowest-ID node
detects the existing Raft state and skips initialization.

---

## Limitations (Phase 3)

These limitations are known and acceptable for Phase 3. They may be
addressed in future releases.

| Limitation | Impact |
|---|---|
| **No dynamic membership changes** | Adding or removing a node requires updating `PELICANQ_CLUSTER_MEMBERS` on every node and restarting the entire cluster. |
| **Reads may be stale on followers** | List-queues and depth checks are served from the local state machine without a Raft round-trip. A follower may briefly report an older state. |
| **Every node has a full copy of all queues** | Total storage = sum of all queues × number of nodes. No per-queue sharding or partitioning. |
| **No authentication on cluster endpoints** | The `/cluster/status` endpoint is unauthenticated. |
