import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, describe, it } from 'node:test';
import { bootstrap } from '../scripts/bootstrap.mjs';
import { consumers } from '../scripts/config.mjs';
import { readTopology, verifyTopology } from '../scripts/management.mjs';
import { confirmChannel, eventually, publish, testBroker } from './helpers.mjs';

describe('Real RabbitMQ topology and service permissions', () => {
  let broker;
  let admin;
  before(async () => {
    broker = await testBroker();
    admin = await confirmChannel(await broker.open());
  });
  after(async () => { if (broker) await broker.cleanup(); });

  it('pre-provisions all resources with no service processes and supports repeat bootstrap', async () => {
    const counts = await verifyTopology(broker.api, broker.config, await readTopology());
    assert.deepEqual(counts, { exchanges: 3, queues: 12, bindings: 14 });
    await bootstrap(broker.config);
  });

  it('allows the existing Booking declarations with its restricted account', async () => {
    const channel = await confirmChannel(await broker.open('booking'));
    const topology = await readTopology();
    for (const exchange of topology.exchanges) await channel.assertExchange(exchange.name, exchange.type, { durable: true });
    for (const queue of topology.queues.filter(item => item.name.startsWith('booking.'))) {
      const args = Object.fromEntries(Object.entries(queue.arguments).filter(([key]) => key !== 'x-queue-type'));
      await channel.assertQueue(queue.name, { durable: true, arguments: args });
    }
    for (const binding of topology.bindings.filter(item => item.destination.startsWith('booking.'))) {
      await channel.bindQueue(binding.destination, binding.source, binding.routing_key);
    }
    await channel.close();
  });

  it('routes requested and cancel-requested to Operations while its consumer is offline', async () => {
    const channel = await confirmChannel(await broker.open('booking'));
    for (const eventType of consumers.booking.publish) {
      await publish(channel, 'restaurant.events', eventType, { eventId: randomUUID(), eventType });
    }
    const operations = await confirmChannel(await broker.open('operations'));
    const received = [];
    for (let i = 0; i < 2; i++) {
      const message = await operations.get('operations.requests.q');
      assert.ok(message); assert.equal(message.properties.deliveryMode, 2);
      received.push(JSON.parse(message.content).eventType); operations.ack(message);
    }
    assert.deepEqual(received.sort(), [...consumers.booking.publish].sort());
  });

  it('fans out state independently to Booking, Tracking and Notification; rejection reaches Booking only', async () => {
    const channel = await confirmChannel(await broker.open('operations'));
    const event = { eventId: randomUUID(), eventType: 'booking.state-changed.v1' };
    await publish(channel, 'restaurant.events', event.eventType, event);
    for (const role of ['booking', 'tracking', 'notification']) {
      const reader = await confirmChannel(await broker.open(role));
      const message = await reader.get(consumers[role].queue);
      assert.ok(message); assert.deepEqual(JSON.parse(message.content), event); reader.ack(message);
      await reader.close();
    }
    await publish(channel, 'restaurant.events', 'booking.cancellation-rejected.v1', { eventId: randomUUID() });
    for (const role of ['tracking', 'notification']) assert.equal((await admin.checkQueue(consumers[role].queue)).messageCount, 0);
    const message = await admin.get(consumers.booking.queue); assert.ok(message); admin.ack(message);
  });

  it('returns mandatory unroutable events to the publisher', async () => {
    await assert.rejects(publish(admin, 'restaurant.events', 'unknown.event.v1', {}), /UNROUTABLE/);
  });

  it('rejects publishing another services events and consuming foreign queues', async () => {
    const book = await broker.open('booking');
    const forbidden = await confirmChannel(book);
    await assert.rejects(publish(forbidden, 'restaurant.events', 'booking.state-changed.v1', {}));
    const foreign = await confirmChannel(book);
    await assert.rejects(foreign.get('operations.requests.q'), error => error.fields?.replyCode === 403 || error.code === 403);
    const injected = await confirmChannel(book);
    await assert.rejects(publish(injected, '', 'tracking.lifecycle.q', {}));
    const tracking = await confirmChannel(await broker.open('tracking'));
    await assert.rejects(publish(tracking, 'restaurant.events', 'booking.state-changed.v1', {}));
  });

  it('routes a rejected delivery to the consumer-specific DLQ', async () => {
    await publish(admin, 'restaurant.events', 'booking.cancellation-rejected.v1', { eventId: randomUUID() });
    const book = await confirmChannel(await broker.open('booking'));
    const message = await book.get('booking.lifecycle.q'); assert.ok(message);
    book.nack(message, false, false);
    await eventually(async () => (await admin.checkQueue('booking.lifecycle.q.dlq')).messageCount === 1);
    const dead = await admin.get('booking.lifecycle.q.dlq'); assert.ok(dead);
    assert.equal(dead.properties.headers['x-death'][0].reason, 'rejected');
    admin.ack(dead);
  });

  it('returns retry deliveries after TTL to the correct queue and preserves event identity', async () => {
    const book = await confirmChannel(await broker.open('booking'));
    const event = { eventId: randomUUID(), eventType: 'booking.state-changed.v1' };
    const started = Date.now();
    await publish(book, 'restaurant.retry', 'booking.lifecycle.q', event, { headers: { 'x-booking-retry-count': 1 } });
    assert.equal((await admin.checkQueue('tracking.lifecycle.q')).messageCount, 0);
    await eventually(async () => (await admin.checkQueue('booking.lifecycle.q')).messageCount === 1);
    assert.ok(Date.now() - started >= 4500);
    const retried = await book.get('booking.lifecycle.q'); assert.ok(retried);
    assert.equal(JSON.parse(retried.content).eventId, event.eventId);
    assert.equal(retried.properties.headers['x-booking-retry-count'], 1);
    assert.equal(retried.properties.headers['x-death'][0].reason, 'expired');
    book.ack(retried);
  });
});
