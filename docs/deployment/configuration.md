# Configuration Reference

PelicanQ is configured through environment variables.

## General

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_DATA_DIR` | `./data` | Directory for persistent sled storage |
| `RUST_LOG` | `info` | Log level: `debug`, `info`, `warn`, `error` |

## HTTP API

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_LISTEN_ADDR` | `127.0.0.1:7070` | HTTP API listen address |

## gRPC API

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_GRPC_ADDR` | `127.0.0.1:7072` | gRPC API listen address |

## MQTT

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_MQTT_ADDR` | `127.0.0.1:1883` | MQTT listener address. Set empty to disable. |

## Clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_NODE_ID` | (unset = Solo) | Node ID for Flock mode |
| `PELICANQ_CLUSTER_MEMBERS` | (unset) | Comma-separated member list in Flock mode |

## Storage Watermarks

| Variable | Default | Description |
|----------|---------|-------------|
| `PELICANQ_STORAGE_WARN` | `0.75` | Warn threshold (fraction of max bytes) |
| `PELICANQ_STORAGE_THROTTLE` | `0.90` | Throttle threshold |
| `PELICANQ_STORAGE_REJECT` | `0.95` | Reject threshold |

## Data Layout

```
<data_dir>/
├── <queue_name>/     # Queue database
│   ├── msgs          # Message tree
│   ├── inflight      # In-flight messages
│   ├── scheduled     # Scheduled messages
│   ├── dlq           # Dead-letter queue
│   └── dedup         # Dedup key index
└── raft/             # Raft log (Flock mode only)
    ├── log           # Raft log entries
    └── meta          # Raft metadata (term, vote)
```
