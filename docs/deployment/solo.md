# Solo Mode

Single `pelicanqd` process with embedded sled storage. No replication.

## When to Use

- Local development and testing
- CI/CD pipelines
- Low-throughput production (loss of the node is acceptable)

## Start

```bash
PELICANQ_DATA_DIR=./data cargo run --bin pelicanqd
```

## Ports

| Service | Default | Env Var |
|---------|---------|---------|
| HTTP API | `127.0.0.1:7070` | `PELICANQ_LISTEN_ADDR` |
| gRPC API | `127.0.0.1:7072` | `PELICANQ_GRPC_ADDR` |
| MQTT | `127.0.0.1:1883` | `PELICANQ_MQTT_ADDR` |
