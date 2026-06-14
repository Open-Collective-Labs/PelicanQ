export class PelicanError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PelicanError';
  }
}

export class ClientMessage {
  payload: Buffer;
  headers: Record<string, string>;
  priority: number;
  deliverAt?: number;
  dedupKey?: string;

  constructor(payload: Buffer) {
    this.payload = payload;
    this.headers = {};
    this.priority = 0;
  }

  withPriority(p: number): ClientMessage {
    this.priority = Math.min(Math.max(0, Math.floor(p)), 9);
    return this;
  }

  withDeliverAt(ms: number): ClientMessage {
    this.deliverAt = ms;
    return this;
  }

  withDedupKey(key: string): ClientMessage {
    this.dedupKey = key;
    return this;
  }

  withHeader(key: string, value: string): ClientMessage {
    this.headers[key] = value;
    return this;
  }
}

export function createMessage(payload: Buffer): ClientMessage {
  return new ClientMessage(payload);
}

export interface PublishResult {
  id: string;
  deduplicated: boolean;
}

export interface Delivery {
  deliveryTag: number;
  message: ClientMessage;
  id: string;
  timestamp: number;
  deliveryAttempts: number;
}

export interface QueueOptions {
  maxAgeSecs?: number;
  maxMessages?: number;
  maxDeliveryAttempts?: number;
  deadLetterQueue?: string;
  dedupWindowSecs?: number;
}

export interface QueueInfo {
  name: string;
  depth: number;
  scheduledDepth: number;
}
