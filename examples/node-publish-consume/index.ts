import { PelicanClient, createMessage, QueueOptions } from '@pelicanq/client';

async function main() {
  const addr = process.argv[2] || '127.0.0.1:7072';
  console.log(`Connecting to PelicanQ at ${addr} ...`);

  const client = await PelicanClient.connect(addr);

  await client.health();
  console.log('Health check OK');

  const queueName = 'example-queue';

  const created = await client.declareQueue(queueName, {});
  console.log(
    `Queue '${queueName}' ${created ? 'created' : 'already exists'}`,
  );

  const msg = createMessage(Buffer.from('Hello, PelicanQ!'))
    .withPriority(5)
    .withHeader('content-type', 'text/plain');
  const result = await client.publish(queueName, msg);
  console.log(`Published message id=${result.id}`);

  const delivery = await client.consume(queueName);
  if (delivery) {
    console.log(
      `Consumed message: payload="${delivery.message.payload.toString()}" tag=${delivery.deliveryTag}`,
    );
    await client.ack(queueName, delivery.deliveryTag);
    console.log('Acknowledged message');
  } else {
    console.log('No message available');
  }

  const queues = await client.listQueues();
  console.log('Queues:');
  for (const q of queues) {
    console.log(`  ${q.name} (depth=${q.depth})`);
  }

  console.log('Done!');
}

main().catch(console.error);
