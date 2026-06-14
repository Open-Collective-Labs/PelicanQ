import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

import {
  ClientMessage,
  Delivery,
  PelicanError,
  PublishResult,
  QueueInfo,
  QueueOptions,
} from './types';

const PROTO_DIR = path.resolve(__dirname, '../../../proto');

let _proto: Record<string, unknown> | null = null;

function getProto(): Record<string, unknown> {
  if (!_proto) {
    const protoFiles = [
      path.join(PROTO_DIR, 'pelicanq/v1/queue.proto'),
      path.join(PROTO_DIR, 'pelicanq/v1/admin.proto'),
    ];
    const packageDef = protoLoader.loadSync(protoFiles, {
      includeDirs: [PROTO_DIR],
      longs: Number,
      defaults: true,
      oneofs: true,
    });
    _proto = grpc.loadPackageDefinition(packageDef) as Record<string, unknown>;
  }
  return _proto;
}

function promisify<Res>(
  client: unknown,
  method: string,
  req: Record<string, unknown>,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    const fn = (client as Record<string, unknown>)[method] as (
      req: Record<string, unknown>,
      cb: (err: grpc.ServiceError | null, res: Res) => void,
    ) => void;
    fn(
      req,
      (err: grpc.ServiceError | null, res: Res) => {
        if (err) {
          reject(new PelicanError(err.details || err.message));
        } else {
          resolve(res);
        }
      },
    );
  });
}

function buildRequest(fields: Record<string, unknown>): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v !== undefined) {
      req[k] = v;
    }
  }
  return req;
}

function clientMessageToProto(msg: ClientMessage): Record<string, unknown> {
  return {
    payload: msg.payload,
    headers: msg.headers,
    priority: msg.priority,
    deliverAt: msg.deliverAt ?? null,
    dedupKey: msg.dedupKey ?? null,
  };
}

function clientMessageFromProto(msg: Record<string, unknown>): ClientMessage {
  const cm = new ClientMessage(msg.payload as Buffer);
  cm.headers = (msg.headers as Record<string, string>) ?? {};
  cm.priority = (msg.priority as number) ?? 0;
  cm.deliverAt = msg.deliverAt as number | undefined;
  cm.dedupKey = msg.dedupKey as string | undefined;
  return cm;
}

function deliveryFromProto(cm: Record<string, unknown>): Delivery {
  const inner = cm.message as Record<string, unknown> | null | undefined;
  let msg: ClientMessage;
  let id = '';
  let timestamp = 0;
  let deliveryAttempts = 0;
  if (inner) {
    msg = clientMessageFromProto(inner);
    id = (inner.id as string) ?? '';
    timestamp = (inner.timestamp as number) ?? 0;
    deliveryAttempts = (inner.deliveryAttempts as number) ?? 0;
  } else {
    msg = new ClientMessage(Buffer.alloc(0));
  }
  return {
    deliveryTag: cm.deliveryTag as number,
    message: msg,
    id,
    timestamp,
    deliveryAttempts,
  };
}

function publishResultFromProto(pr: Record<string, unknown>): PublishResult {
  return {
    id: pr.id as string,
    deduplicated: pr.deduplicated as boolean,
  };
}

function queueInfoFromProto(qi: Record<string, unknown>): QueueInfo {
  return {
    name: qi.name as string,
    depth: qi.depth as number,
    scheduledDepth: qi.scheduledDepth as number,
  };
}

export class PelicanClient {
  private queueClient: Record<string, unknown>;
  private adminClient: Record<string, unknown>;

  private constructor(
    queueClient: Record<string, unknown>,
    adminClient: Record<string, unknown>,
  ) {
    this.queueClient = queueClient;
    this.adminClient = adminClient;
  }

  static async connect(addr: string): Promise<PelicanClient> {
    const proto = getProto();
    const credentials = grpc.credentials.createInsecure();
    const pkg = proto.pelicanq as Record<string, Record<string, unknown>>;
    const v1 = pkg.v1 as Record<string, unknown>;
    const QueueService = v1.QueueService as new (
      addr: string,
      cred: grpc.ChannelCredentials,
    ) => Record<string, unknown>;
    const AdminService = v1.AdminService as new (
      addr: string,
      cred: grpc.ChannelCredentials,
    ) => Record<string, unknown>;
    const queueClient = new QueueService(addr, credentials);
    const adminClient = new AdminService(addr, credentials);
    return new PelicanClient(queueClient, adminClient);
  }

  async declareQueue(name: string, opts: QueueOptions = {}): Promise<boolean> {
    const res = await promisify<{ created: boolean }>(
      this.queueClient,
      'declareQueue',
      buildRequest({
        name,
        maxAgeSecs: opts.maxAgeSecs,
        maxMessages: opts.maxMessages,
        maxDeliveryAttempts: opts.maxDeliveryAttempts,
        deadLetterQueue: opts.deadLetterQueue,
        dedupWindowSecs: opts.dedupWindowSecs,
      }),
    );
    return res.created;
  }

  async publish(queue: string, msg: ClientMessage): Promise<PublishResult> {
    const res = await promisify<Record<string, unknown>>(
      this.queueClient,
      'publish',
      {
        queue,
        message: clientMessageToProto(msg),
      },
    );
    return publishResultFromProto(res);
  }

  async publishBatch(
    queue: string,
    msgs: ClientMessage[],
  ): Promise<PublishResult[]> {
    const res = await promisify<{ results: Record<string, unknown>[] }>(
      this.queueClient,
      'publishBatch',
      {
        queue,
        messages: msgs.map(clientMessageToProto),
      },
    );
    return res.results.map(publishResultFromProto);
  }

  async consume(queue: string): Promise<Delivery | null> {
    const res = await promisify<{ message: Record<string, unknown> | null }>(
      this.queueClient,
      'consume',
      { queue },
    );
    if (!res.message) {
      return null;
    }
    return deliveryFromProto(res.message);
  }

  async consumeBatch(queue: string, max: number): Promise<Delivery[]> {
    const res = await promisify<{ messages: Record<string, unknown>[] }>(
      this.queueClient,
      'consumeBatch',
      { queue, max },
    );
    return res.messages.map(deliveryFromProto);
  }

  async ack(queue: string, deliveryTag: number): Promise<void> {
    await promisify<Record<string, never>>(this.queueClient, 'ack', {
      queue,
      deliveryTag,
    });
  }

  async nack(queue: string, deliveryTag: number): Promise<void> {
    await promisify<Record<string, never>>(this.queueClient, 'nack', {
      queue,
      deliveryTag,
    });
  }

  async listQueues(): Promise<QueueInfo[]> {
    const res = await promisify<{ queues: Record<string, unknown>[] }>(
      this.queueClient,
      'listQueues',
      {},
    );
    return res.queues.map(queueInfoFromProto);
  }

  async health(): Promise<void> {
    const res = await promisify<{ status: string }>(
      this.adminClient,
      'health',
      {},
    );
    if (res.status !== 'ok') {
      throw new PelicanError(`unhealthy: ${res.status}`);
    }
  }
}
