import { PelicanClient, createMessage } from '../../sdks/node/src/index.js';

async function main() {
  const addr = process.argv[2] || '127.0.0.1:7072';
  const client = await PelicanClient.connect(addr);

  const status = await client.health();
  console.log(`Health: ${status}`);

  const queueName = 'node-example';
  await client.declareQueue(queueName, {});

  const msg = createMessage(Buffer.from('Hello from Node.js!')).withPriority(5);
  const pub = await client.publish(queueName, msg);
  console.log(`Published: ${pub.id}`);

  const delivery = await client.consume(queueName);
  if (delivery) {
    console.log(`Got: ${delivery.message.payload.toString()}`);
    await client.ack(queueName, delivery.deliveryTag);
  }
}

main().catch(console.error);
