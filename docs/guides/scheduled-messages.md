# Scheduled Messages

Messages can be scheduled for future delivery by setting a `deliver_at` timestamp.

## Usage

```rust
// Schedule a message for 1 hour from now
let future = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_millis() as i64
    + 3600_000;

let msg = ClientMessage::new(b"scheduled payload").with_deliver_at(future);
client.publish("myqueue", msg).await?;
```

The message will not be visible to consumers until the scheduled time passes.

## Behavior

- Messages with `deliver_at` in the past are delivered immediately.
- Messages with `deliver_at` in the future are stored in a separate scheduled tree and promoted to the main queue when their time arrives.
- Scheduled messages are durable across restarts.
- The scheduled queue depth can be queried via `list_queues()`.
