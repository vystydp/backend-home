import mqtt from 'mqtt';
import { config } from './lib/config';
import { messageToEvent } from './lib/parse';
import { createPublisher } from './lib/queue';

/**
 * Collector: subscribe to the car's MQTT topics, normalize each message into a
 * domain event (stateless transforms only), and publish it to RabbitMQ. All the
 * stateful work — resampling, weighted SoC, persistence — lives in the writer.
 */
async function main(): Promise<void> {
  const publisher = await createPublisher();
  const client = mqtt.connect(config.mqttUrl);

  let lastMessageAt = Date.now();
  const watchdog = setInterval(() => {
    const idleMs = Date.now() - lastMessageAt;
    if (idleMs > config.idleTimeoutMs) {
      console.warn(
        `[collector] no MQTT data for ${Math.round(idleMs / 1000)}s — source may have stopped`,
      );
    }
  }, config.idleTimeoutMs);

  client.on('connect', () => {
    const topic = `car/${config.carId}/#`;
    client.subscribe(topic, { qos: 1 }, (err) => {
      if (err) {
        console.error('[collector] subscribe failed', err);
        process.exit(1);
      }
      console.log(`[collector] subscribed to ${topic}`);
    });
  });

  client.on('message', (topic, payload) => {
    lastMessageAt = Date.now();
    const event = messageToEvent(topic, payload);
    if (event === null) return; // unknown topic or malformed payload — dropped at the edge
    publisher.publish(event).catch((err) => console.error('[collector] publish failed', err));
  });

  client.on('error', (err) => console.error('[collector] mqtt error', err));

  const shutdown = async () => {
    clearInterval(watchdog);
    client.end();
    await publisher.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[collector] fatal', err);
  process.exit(1);
});
