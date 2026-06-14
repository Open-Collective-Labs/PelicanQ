# Publish and Consume

The fundamental workflow in PelicanQ is publish-consume-ack.

## Workflow

1. **Declare** a queue (idempotent — safe to call multiple times).
2. **Publish** a message to the queue.
3. **Consume** a message from the queue.
4. **Ack** the message to confirm processing.
5. Optionally **Nack** to requeue or dead-letter.

## HTTP API

```bash
# Declare queue
curl -X POST http://127.0.0.1:7070/queues/myqueue

# Publish
curl -X POST http://127.0.0.1:7070/queues/myqueue/publish \
  -H 'content-type: application/json' \
  -d '{"payload_base64":"SGVsbG8=", "headers": {"content-type": "text/plain"}}'

# Consume
curl -X POST http://127.0.0.1:7070/queues/myqueue/consume
# Response: {"delivery_tag":1,"payload_base64":"SGVsbG8=","headers":{},"id":"...","timestamp":...}

# Ack
curl -X POST http://127.0.0.1:7070/queues/myqueue/ack \
  -H 'content-type: application/json' \
  -d '{"delivery_tag": 1}'

# Nack
curl -X POST http://127.0.0.1:7070/queues/myqueue/nack \
  -H 'content-type: application/json' \
  -d '{"delivery_tag": 1}'
```

## gRPC / SDK

See the [Rust SDK](../reference/sdk-rust.md) reference for language-specific examples.
