# Message Deduplication

PelicanQ supports idempotent message publishing through deduplication keys.

## How It Works

1. Set a `dedup_key` on the message before publishing.
2. If a message with the same dedup key is published within the dedup window, it is marked as `deduplicated` and not stored.
3. The deduplication window is configurable per queue.

## Usage

```rust
let msg = ClientMessage::new(b"idempotent payload")
    .with_dedup_key("order-12345-v1");
let result = client.publish("myqueue", msg).await?;
println!("Deduplicated: {}", result.deduplicated);
```

## Configuration

Set the dedup window when declaring a queue:

```rust
let opts = QueueOptions {
    dedup_window_secs: Some(300),  // 5 minutes
    ..Default::default()
};
client.declare_queue("myqueue", opts).await?;
```

## Notes

- Dedup keys are scoped per queue (same key in different queues is allowed).
- After the window expires, the same key can be reused.
- Deduplication is checked at the engine level, before storage.
