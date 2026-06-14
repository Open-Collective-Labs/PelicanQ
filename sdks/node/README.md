# PelicanQ Node.js SDK

A TypeScript client for [PelicanQ](https://github.com/Open-Collective-Labs/PelicanQ).

## Installation

```bash
npm install @pelicanq/client
```

## Quickstart

```typescript
import { PelicanClient, createMessage } from '@pelicanq/client';

const client = await PelicanClient.connect('127.0.0.1:7072');

await client.declareQueue('my-queue', {});
const msg = createMessage(Buffer.from('Hello!')).withPriority(5);
const result = await client.publish('my-queue', msg);
console.log('published:', result.id);

const delivery = await client.consume('my-queue');
if (delivery) {
  console.log('got:', delivery.message.payload.toString());
  await client.ack('my-queue', delivery.deliveryTag);
}
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
| `clusterStatus()` | Get Raft cluster status |
| `consumeStream(queue)` | Open a bidirectional streaming consume (returns `ClientDuplexStream`) |

### Streaming Consume

```typescript
const stream = client.consumeStream('my-queue');
stream.on('data', (msg: any) => {
  console.log('got:', Buffer.from(msg.message.payload).toString());
  stream.write({ deliveryTag: msg.deliveryTag }); // ack
});
```

```typescript
class PelicanClient {
  static connect(addr: string): Promise<PelicanClient>;

  declareQueue(name: string, opts?: QueueOptions): Promise<boolean>;
  publish(queue: string, msg: ClientMessage): Promise<PublishResult>;
  publishBatch(queue: string, msgs: ClientMessage[]): Promise<PublishResult[]>;
  consume(queue: string): Promise<Delivery | null>;
  consumeBatch(queue: string, max: number): Promise<Delivery[]>;
  ack(queue: string, deliveryTag: number): Promise<void>;
  nack(queue: string, deliveryTag: number): Promise<void>;
  listQueues(): Promise<QueueInfo[]>;
  health(): Promise<string>;
  clusterStatus(): Promise<Record<string, unknown>>;
  consumeStream(queue: string, onMessage: (d: Delivery) => void, onError?: (err: Error) => void): Promise<void>;
}
```

### Types

#### ClientMessage

```typescript
class ClientMessage {
  payload: Buffer;
  headers: Record<string, string>;
  priority: number;       // 0-9, clamped
  deliverAt?: number;     // ms since epoch
  dedupKey?: string;

  constructor(payload: Buffer);
  withPriority(p: number): this;
  withDeliverAt(ms: number): this;
  withDedupKey(key: string): this;
  withHeader(k: string, v: string): this;
}

function createMessage(payload: Buffer): ClientMessage;
```

#### Other Types

```typescript
interface PublishResult { id: string; deduplicated: boolean; }
interface Delivery { deliveryTag: number; message: ClientMessage; id: string; timestamp: number; deliveryAttempts: number; }
interface QueueOptions { maxAgeSecs?: number; maxMessages?: number; maxDeliveryAttempts?: number; deadLetterQueue?: string; dedupWindowSecs?: number; }
interface QueueInfo { name: string; depth: number; scheduledDepth: number; }
```

### Error Handling

All SDK methods throw `PelicanError` on failure.

## Requirements

- Node.js 20+
- TypeScript 5+
- A running PelicanQ daemon (see repo root README)

## Build & Test

```bash
npm install
npm run build
npm test
```
