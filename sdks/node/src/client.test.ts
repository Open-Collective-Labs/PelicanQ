import { describe, it } from 'node:test';
import assert from 'node:assert';

import {
  ClientMessage,
  Delivery,
  PelicanError,
  PublishResult,
  QueueInfo,
  QueueOptions,
  createMessage,
} from './types';

describe('createMessage', () => {
  it('creates a message with the given payload', () => {
    const msg = createMessage(Buffer.from('hello'));
    assert.strictEqual(msg.payload.toString(), 'hello');
  });

  it('defaults priority to 0', () => {
    const msg = createMessage(Buffer.from('test'));
    assert.strictEqual(msg.priority, 0);
  });

  it('defaults headers to empty object', () => {
    const msg = createMessage(Buffer.from('test'));
    assert.deepStrictEqual(msg.headers, {});
  });
});

describe('ClientMessage builder', () => {
  it('withPriority clamps above 9', () => {
    const msg = new ClientMessage(Buffer.from('x')).withPriority(15);
    assert.strictEqual(msg.priority, 9);
  });

  it('withPriority clamps below 0', () => {
    const msg = new ClientMessage(Buffer.from('x')).withPriority(-5);
    assert.strictEqual(msg.priority, 0);
  });

  it('withPriority accepts normal values', () => {
    const msg = new ClientMessage(Buffer.from('x')).withPriority(5);
    assert.strictEqual(msg.priority, 5);
  });

  it('withPriority floors floats', () => {
    const msg = new ClientMessage(Buffer.from('x')).withPriority(3.9);
    assert.strictEqual(msg.priority, 3);
  });

  it('withDeliverAt sets the field', () => {
    const msg = new ClientMessage(Buffer.from('x')).withDeliverAt(1000);
    assert.strictEqual(msg.deliverAt, 1000);
  });

  it('withDedupKey sets the field', () => {
    const msg = new ClientMessage(Buffer.from('x')).withDedupKey('k1');
    assert.strictEqual(msg.dedupKey, 'k1');
  });

  it('withHeader adds a header', () => {
    const msg = new ClientMessage(Buffer.from('x')).withHeader(
      'ct',
      'text/plain',
    );
    assert.strictEqual(msg.headers['ct'], 'text/plain');
  });

  it('withHeader supports chaining', () => {
    const msg = new ClientMessage(Buffer.from('x'))
      .withPriority(3)
      .withHeader('a', '1')
      .withHeader('b', '2');
    assert.strictEqual(msg.priority, 3);
    assert.strictEqual(msg.headers['a'], '1');
    assert.strictEqual(msg.headers['b'], '2');
  });
});

describe('PelicanError', () => {
  it('is an instance of Error', () => {
    const err = new PelicanError('test error');
    assert(err instanceof Error);
    assert(err instanceof PelicanError);
  });

  it('has the correct name', () => {
    const err = new PelicanError('test error');
    assert.strictEqual(err.name, 'PelicanError');
  });

  it('preserves the message', () => {
    const err = new PelicanError('something went wrong');
    assert.strictEqual(err.message, 'something went wrong');
  });
});

describe('TypeScript interfaces', () => {
  it('PublishResult can be constructed', () => {
    const pr: PublishResult = { id: 'abc', deduplicated: false };
    assert.strictEqual(pr.id, 'abc');
    assert.strictEqual(pr.deduplicated, false);
  });

  it('Delivery can be constructed', () => {
    const msg = new ClientMessage(Buffer.from('data'));
    const d: Delivery = {
      deliveryTag: 42,
      message: msg,
      id: 'msg-1',
      timestamp: 1234567890,
      deliveryAttempts: 1,
    };
    assert.strictEqual(d.deliveryTag, 42);
    assert.strictEqual(d.message.payload.toString(), 'data');
    assert.strictEqual(d.id, 'msg-1');
    assert.strictEqual(d.deliveryAttempts, 1);
  });

  it('QueueInfo can be constructed', () => {
    const qi: QueueInfo = { name: 'q', depth: 10, scheduledDepth: 2 };
    assert.strictEqual(qi.name, 'q');
    assert.strictEqual(qi.depth, 10);
    assert.strictEqual(qi.scheduledDepth, 2);
  });

  it('QueueOptions default all undefined', () => {
    const opts: QueueOptions = {};
    assert.strictEqual(opts.maxAgeSecs, undefined);
    assert.strictEqual(opts.maxMessages, undefined);
    assert.strictEqual(opts.maxDeliveryAttempts, undefined);
    assert.strictEqual(opts.deadLetterQueue, undefined);
    assert.strictEqual(opts.dedupWindowSecs, undefined);
  });
});
