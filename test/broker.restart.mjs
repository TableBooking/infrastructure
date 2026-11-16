import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { root } from '../scripts/config.mjs';
import { confirmChannel, eventually, publish, testBroker } from './helpers.mjs';

test('persistent messages, queues and service credentials survive restarting this Compose broker', { timeout: 60000 }, async () => {
  const broker = await testBroker();
  try {
    const channel = await confirmChannel(await broker.open('booking'));
    const event = { eventId: randomUUID(), eventType: 'booking.requested.v1' };
    await publish(channel, 'restaurant.events', event.eventType, event);
    await broker.closeConnections();
    const result = spawnSync('docker', ['compose', '-f', 'compose.yaml', '-f', 'compose.dev.yaml', 'restart', 'rabbitmq'], {
      cwd: fileURLToPath(root), encoding: 'utf8', timeout: 30000,
    });
    assert.equal(result.status, 0, 'The infrastructure broker must restart successfully');
    await eventually(async () => { await broker.api('overview'); return true; }, 30000);
    const operations = await confirmChannel(await broker.open('operations'));
    const message = await operations.get('operations.requests.q');
    assert.ok(message); assert.equal(JSON.parse(message.content).eventId, event.eventId);
    operations.ack(message);
  } finally { await broker.cleanup(); }
});
