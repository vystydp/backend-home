import amqp from 'amqplib';
import { config } from './config';
import type { TelemetryEvent } from '../types';

/**
 * RabbitMQ adapter. One durable queue, persistent messages and publisher
 * confirms on the way in; manual ack and prefetch=1 on the way out. Combined
 * with the writer's idempotent upsert this gives effectively-once persistence,
 * and prefetch=1 keeps a single ordered stream for the resampler.
 */

export interface Publisher {
  publish(event: TelemetryEvent): Promise<void>;
  close(): Promise<void>;
}

export async function createPublisher(): Promise<Publisher> {
  const connection = await amqp.connect(config.rabbitUrl);
  const channel = await connection.createConfirmChannel();
  await channel.assertQueue(config.queueName, { durable: true });

  return {
    publish(event) {
      return new Promise<void>((resolve, reject) => {
        channel.sendToQueue(
          config.queueName,
          Buffer.from(JSON.stringify(event)),
          { persistent: true },
          (err) => (err ? reject(err) : resolve()),
        );
      });
    },
    async close() {
      await channel.close();
      await connection.close();
    },
  };
}

export interface Consumer {
  close(): Promise<void>;
}

/**
 * Consume events one at a time. `onEvent` must resolve before the message is
 * acked. A parse failure drops the (poison) message; a processing failure
 * requeues it so a transient DB blip is retried rather than lost.
 */
export async function createConsumer(
  onEvent: (event: TelemetryEvent) => Promise<void>,
): Promise<Consumer> {
  const connection = await amqp.connect(config.rabbitUrl);
  const channel = await connection.createChannel();
  await channel.assertQueue(config.queueName, { durable: true });
  await channel.prefetch(1);

  await channel.consume(config.queueName, async (msg) => {
    if (msg === null) return;

    let event: TelemetryEvent;
    try {
      event = JSON.parse(msg.content.toString()) as TelemetryEvent;
    } catch (err) {
      console.error('[writer] dropping unparseable message', err);
      channel.ack(msg); // poison message — don't requeue
      return;
    }

    try {
      await onEvent(event);
      channel.ack(msg);
    } catch (err) {
      console.error('[writer] processing failed, requeuing', err);
      channel.nack(msg, false, true); // transient — retry
    }
  });

  return {
    async close() {
      await channel.close();
      await connection.close();
    },
  };
}
