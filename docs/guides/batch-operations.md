# Batch Operations

PelicanQ supports batch publish and batch consume for higher throughput.

## Batch Publish

Publish multiple messages in a single RPC call:

```rust
let msgs = vec![
    ClientMessage::new(b"msg 1"),
    ClientMessage::new(b"msg 2"),
    ClientMessage::new(b"msg 3"),
];
let results = client.publish_batch("myqueue", msgs).await?;
for r in results {
    println!("id={}, deduplicated={}", r.id, r.deduplicated);
}
```

## Batch Consume

Consume up to N messages in a single call:

```rust
let deliveries = client.consume_batch("myqueue", 10).await?;
for d in deliveries {
    println!("tag={}, payload={:?}", d.delivery_tag, d.message.payload());
    client.ack("myqueue", d.delivery_tag).await?;
}
```

## HTTP API

```bash
# Batch consume is available via gRPC only.
# For HTTP, call the consume endpoint repeatedly.
```
