# PelicanQ Node.js SDK

A Node.js client for [PelicanQ](https://github.com/anomalyco/pelicanq).

## Installation

```bash
npm install @pelicanq/client
```

## Quickstart

```typescript
import { PelicanClient, createMessage, QueueOptions } from '@pelicanq/client';

async function main() {
  const client = await PelicanClient.connect('127.0.0.1:7072');

  const created = await client.declareQueue('my-queue', {});
  console.log('created:', created);

  const msg = createMessage(Buffer.from('Hello, PelicanQ!')).withPriority(5);
  const result = await client.publish('my-queue', msg);
  console.log('published:', result.id);

  const delivery = await client.consume('my-queue');
  if (delivery) {
    console.log('got:', delivery.message.payload.toString());
    await client.ack('my-queue', delivery.deliveryTag);
  }

  console.log('Done!');
}

main().catch(console.error);
```

## API

| Method | Description |
|--------|-------------|
| `PelicanClient.connect(addr)` | Connect to a PelicanQ gRPC endpoint |
| `declareQueue(name, opts)` | Create a queue (idempotent) |
| `publish(queue, message)` | Publish a single message |
| `publishBatch(queue, messages)` | Publish multiple messages |
| `consume(queue)` | Consume one message |
| `consumeBatch(queue, max)` | Consume up to `max` messages |
| `ack(queue, deliveryTag)` | Acknowledge a message |
| `nack(queue, deliveryTag)` | Nack (requeue or dead-letter) |
| `listQueues()` | List all queues |
| `health()` | Check daemon health |

## Types

### ClientMessage

```typescript
class ClientMessage {
  payload: Buffer;
  headers: Record<string, string>;
  priority: number;     // 0–9
  deliverAt?: number;
  dedupKey?: string;
}
```

Builder methods: `createMessage(payload)`, `.withPriority(p)`, `.withDeliverAt(ms)`, `.withDedupKey(k)`, `.withHeader(k, v)`.

### QueueOptions

```typescript
interface QueueOptions {
  maxAgeSecs?: number;
  maxMessages?: number;
  maxDeliveryAttempts?: number;
  deadLetterQueue?: string;
  dedupWindowSecs?: number;
}
```

### Error Handling

All SDK methods throw `PelicanError` on failure.

## Requirements

- Node.js 20+
- A running PelicanQ daemon
