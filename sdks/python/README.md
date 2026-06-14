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
| `ack(queue, delivery_tag)` | Acknowledge a message |
| `nack(queue, delivery_tag)` | Nack (requeue or dead-letter) |
| `list_queues()` | List all queues |
| `health()` | Check daemon health |

## Requirements

- Python 3.10+
- `grpcio>=1.60`
- `protobuf>=4.25`
- A running PelicanQ daemon
