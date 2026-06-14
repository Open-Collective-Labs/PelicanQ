# PelicanQ Python SDK

A Python client for [PelicanQ](https://github.com/Open-Collective-Labs/PelicanQ).

## Installation

```bash
pip install pelicanq/
```

## Quickstart

```python
from pelicanq import PelicanClient, ClientMessage, QueueOptions

client = PelicanClient.connect("127.0.0.1:7072")

created = client.declare_queue("my_queue", QueueOptions())
print(f"created: {created}")

msg = ClientMessage(b"Hello, Python!").with_priority(5)
result = client.publish("my_queue", msg)
print(f"published: {result.id}")

delivery = client.consume("my_queue")
if delivery:
    print(f"got: {delivery.message.payload}")
    client.ack("my_queue", delivery.delivery_tag)

client.close()
```

## API

| Method | Description |
|--------|-------------|
| `PelicanClient.connect(addr)` | Connect to a PelicanQ gRPC endpoint |
| `declare_queue(name, opts)` | Create a queue (idempotent) |
| `publish(queue, message)` | Publish a single message |
| `publish_batch(queue, messages)` | Publish multiple messages |
| `consume(queue)` | Consume one message |
| `consume_batch(queue, max)` | Consume up to `max` messages |
| `consume_stream(queue)` | Bidirectional streaming consume |
| `ack(queue, delivery_tag)` | Acknowledge a message |
| `nack(queue, delivery_tag)` | Nack (requeue or dead-letter) |
| `list_queues()` | List all queues |
| `health()` | Check daemon health |
| `cluster_status()` | Get cluster status (Flock mode) |

## Types

### ClientMessage

```python
class ClientMessage:
    payload: bytes
    headers: dict[str, str]
    priority: int       # 0-9
    deliver_at: int | None
    dedup_key: str | None
```

Builder methods: `with_priority(p)`, `with_deliver_at(ms)`, `with_dedup_key(k)`, `with_header(k, v)`.

### QueueOptions

```python
class QueueOptions:
    max_age_secs: int | None
    max_messages: int | None
    max_delivery_attempts: int | None
    dead_letter_queue: str | None
    dedup_window_secs: int | None
```

## Requirements

- Python 3.10+
- `grpcio>=1.60`
- `protobuf>=4.25`
- A running PelicanQ daemon

## Build & Test

```bash
pip install pytest
python -m pytest tests/
```
