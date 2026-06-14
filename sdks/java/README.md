# PelicanQ Java Client

Java client library for [PelicanQ](https://github.com/pelicanq/pelicanq), a high-performance message queue.

## Installation

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.pelicanq</groupId>
    <artifactId>pelicanq-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Build

```bash
# Compile (generates protobuf stubs and compiles Java sources)
mvn compile

# Run tests
mvn test

# Package into a JAR
mvn package

# Install to local Maven repository
mvn install
```

## Quickstart

```java
import com.pelicanq.client.PelicanClient;
import com.pelicanq.client.types.*;

try (PelicanClient client = PelicanClient.forAddress("127.0.0.1", 7072).build()) {
    // Declare a queue
    boolean created = client.declareQueue("my-queue", new QueueOptions());

    // Publish a message
    ClientMessage msg = new ClientMessage("Hello, World!".getBytes())
        .withPriority(5);
    PublishResult result = client.publish("my-queue", msg);
    System.out.println("Published: " + result.getId());

    // Consume a message
    Delivery d = client.consume("my-queue");
    if (d != null) {
        System.out.println("Received: " + new String(d.getMessage().getPayload()));
        client.ack("my-queue", d.getDeliveryTag());
    }
}
```

## Async Usage

```java
AsyncPelicanClient async = client.async();
CompletableFuture<Delivery> future = async.consume("my-queue");
future.thenAccept(d -> {
    System.out.println("Got: " + new String(d.getMessage().getPayload()));
});
```

## API Reference

### `PelicanClient` (blocking)

| Method | Description |
|--------|-------------|
| `declareQueue(name, options)` | Create or ensure a queue exists |
| `publish(queue, message)` | Publish a single message |
| `publishBatch(queue, messages)` | Publish multiple messages |
| `consume(queue)` | Consume one message (returns null if empty) |
| `consumeBatch(queue, max)` | Consume up to `max` messages |
| `ack(queue, deliveryTag)` | Acknowledge a message |
| `nack(queue, deliveryTag)` | Negative acknowledgement |
| `listQueues()` | List all queues |
| `health()` | Check server health |
| `clusterStatus()` | Get Raft cluster status |
| `consumeStream(queue, observer)` | Open a bidirectional streaming consume (returns `StreamObserver` for ack/nack) |
| `async()` | Get async client instance |

### `AsyncPelicanClient` (non-blocking)

Same methods as `PelicanClient` but all return `CompletableFuture<T>`.

### Streaming Consume

```java
StreamObserver<Delivery> deliveryObserver = new StreamObserver<Delivery>() {
    @Override public void onNext(Delivery d) {
        System.out.println("got: " + new String(d.getMessage().getPayload()));
    }
    @Override public void onError(Throwable t) { t.printStackTrace(); }
    @Override public void onCompleted() {}
};
StreamObserver<pelicanq.v1.ConsumeStreamAck> ackStream = client.consumeStream("q", deliveryObserver);
// ack a message:
ackStream.onNext(pelicanq.v1.ConsumeStreamAck.newBuilder().setDeliveryTag(tag).build());
```

### Types

- `ClientMessage` - Message payload with headers, priority, scheduling, and dedup support
- `PublishResult` - Contains message ID and deduplication flag
- `Delivery` - Contains delivery tag and received message
- `QueueOptions` - Queue configuration (TTL, max messages, DLQ, etc.)
- `QueueInfo` - Queue metadata (name, depth)
- `PelicanException` - Exception wrapping gRPC errors

## Requirements

- Java 11+
- Maven 3.6+
