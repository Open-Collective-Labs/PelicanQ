# PelicanQ Go SDK

A Go client for [PelicanQ](https://github.com/anomalyco/pelicanq).

## Installation

```go
import "github.com/anomalyco/pelicanq/sdks/go/pelicanq"
```

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/anomalyco/pelicanq/sdks/go/pelicanq"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    client, err := pelicanq.Connect("127.0.0.1:7072")
    if err != nil { log.Fatal(err) }
    defer client.Close()

    created, _ := client.DeclareQueue(ctx, "q", pelicanq.QueueOptions{})
    fmt.Println("created:", created)

    msg := pelicanq.NewMessage([]byte("hello")).WithPriority(5)
    result, _ := client.Publish(ctx, "q", msg)
    fmt.Println("published:", result.ID)

    d, _ := client.Consume(ctx, "q")
    fmt.Println("got:", string(d.Message.Payload))

    client.Ack(ctx, "q", d.DeliveryTag)
}
```

## API

| Method | Description |
|--------|-------------|
| `Connect(addr)` | Connect to a PelicanQ gRPC endpoint |
| `DeclareQueue(ctx, name, opts)` | Create a queue (idempotent) |
| `Publish(ctx, queue, msg)` | Publish a single message |
| `PublishBatch(ctx, queue, msgs)` | Publish multiple messages |
| `Consume(ctx, queue)` | Consume one message |
| `ConsumeBatch(ctx, queue, max)` | Consume up to `max` messages |
| `Ack(ctx, queue, tag)` | Acknowledge a message |
| `Nack(ctx, queue, tag)` | Nack (requeue or dead-letter) |
| `ListQueues(ctx)` | List all queues |
| `Health(ctx)` | Check daemon health |

## Requirements

- Go 1.21+
- A running PelicanQ daemon

## Types

### ClientMessage

```go
type ClientMessage struct {
    Payload    []byte
    Headers    map[string]string
    Priority   uint8
    DeliverAt  *int64
    DedupKey   *string
}
```

Builder methods: `NewMessage(payload)`, `.WithPriority(p)`, `.WithDeliverAt(ms)`, `.WithDedupKey(k)`, `.WithHeader(k, v)`.

### QueueOptions

```go
type QueueOptions struct {
    MaxAgeSecs          *uint64
    MaxMessages         *uint64
    MaxDeliveryAttempts *uint32
    DeadLetterQueue     *string
    DedupWindowSecs     *uint64
}
```

### Error Handling

All SDK methods return `(result, error)`. Check the error to detect failures.
