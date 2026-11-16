import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseEnv } from 'node:util';
import { test } from 'node:test';
import { connectionUrl, consumers, roles, root, validateConfig } from '../scripts/config.mjs';
import { prepareEnvironment } from '../scripts/setup.mjs';
import { checkCompatibility, readTopology } from '../scripts/management.mjs';
import { servicePermissions, topicPermissions } from '../scripts/permissions.mjs';

const example = await readFile(new URL('.env.example', root), 'utf8');
const valid = Object.fromEntries(Object.entries(parseEnv(example)).map(([key, value]) => [key, key.endsWith('_PASSWORD') ? `${key}_long_test_secret_12345` : value]));

test('setup generates distinct secrets, preserves existing values and writes encoded connection files', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'restaurant-infra-test-'));
  const url = pathToFileURL(`${directory}/`);
  try {
    await writeFile(new URL('.env.example', url), example);
    const first = await prepareEnvironment(url);
    const original = await readFile(new URL('.env', url), 'utf8');
    assert.equal(new Set(Object.values(first.users).map(user => user.password)).size, 5);
    const second = await prepareEnvironment(url);
    assert.deepEqual(second, first);
    assert.equal(await readFile(new URL('.env', url), 'utf8'), original);
    for (const role of roles) {
      const host = parseEnv(await readFile(new URL(`generated/${role}.host.env`, url), 'utf8'));
      const docker = parseEnv(await readFile(new URL(`generated/${role}.docker.env`, url), 'utf8'));
      assert.equal(new URL(host.RABBITMQ_URL).hostname, '127.0.0.1');
      assert.equal(new URL(docker.RABBITMQ_URL).hostname, 'rabbitmq');
      assert.equal(host.RABBIT_RETRY_DELAY_MS, '5000');
    }
  } finally {
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('restaurant-infra-test-'));
    await rm(directory, { recursive: true, force: true });
  }
});

for (const [name, change] of [
  ['weak password', { RABBITMQ_BOOKING_PASSWORD: 'short' }],
  ['guest account', { RABBITMQ_ADMIN_USER: 'guest' }],
  ['duplicate usernames', { RABBITMQ_BOOKING_USER: valid.RABBITMQ_OPERATIONS_USER }],
  ['invalid vhost', { RABBITMQ_VHOST: 'bad/vhost' }],
  ['invalid port', { RABBITMQ_AMQP_PORT: '0' }],
  ['duplicate ports', { RABBITMQ_AMQP_PORT: valid.RABBITMQ_MANAGEMENT_PORT }],
]) test(`configuration rejects ${name}`, () => {
  assert.throws(() => validateConfig({ ...valid, ...change }));
});

test('AMQP URLs encode special password characters', () => {
  const config = validateConfig({ ...valid, RABBITMQ_BOOKING_PASSWORD: 'a-long-test-password:@/#?%' });
  const url = new URL(connectionUrl(config, 'booking'));
  assert.equal(decodeURIComponent(url.password), config.users.booking.password);
  assert.equal(url.pathname, '/restaurant');
});

test('topology defines independent fanout, retry and DLQ for every consumer', async () => {
  const topology = await readTopology();
  assert.equal(topology.exchanges.length, 3);
  assert.equal(topology.queues.length, 12);
  assert.equal(topology.bindings.length, 14);
  for (const { queue, keys } of Object.values(consumers)) {
    const main = topology.queues.find(item => item.name === queue);
    const retry = topology.queues.find(item => item.name === `${queue}.retry`);
    assert.equal(main.durable, true);
    assert.equal(main.arguments['x-queue-type'], 'classic');
    assert.equal(main.arguments['x-dead-letter-routing-key'], queue);
    assert.equal(retry.arguments['x-message-ttl'], 5000);
    assert.equal(retry.arguments['x-dead-letter-exchange'], '');
    assert.equal(retry.arguments['x-dead-letter-routing-key'], queue);
    assert.deepEqual(topology.bindings.filter(item => item.source === 'restaurant.events' && item.destination === queue).map(item => item.routing_key), keys);
  }
});

test('permissions isolate queues, disallow default exchange injection and enforce event producers', () => {
  for (const role of roles) {
    const permissions = servicePermissions(role);
    assert.ok(new RegExp(permissions.read).test(consumers[role].queue));
    assert.equal(new RegExp(permissions.write).test('amq.default'), false);
    for (const other of roles.filter(item => item !== role)) {
      assert.equal(new RegExp(permissions.read).test(consumers[other].queue), false);
      assert.equal(new RegExp(permissions.configure).test(consumers[other].queue), false);
    }
    for (const key of ['booking.requested.v1', 'booking.cancel-requested.v1', 'booking.state-changed.v1', 'booking.cancellation-rejected.v1']) {
      assert.equal(new RegExp(topicPermissions(role).write).test(key), consumers[role].publish.includes(key));
    }
  }
});

test('bootstrap refuses immutable queue/exchange conflicts without deleting resources', async () => {
  const topology = await readTopology();
  const actual = structuredClone(topology);
  actual.queues[0].arguments['x-dead-letter-exchange'] = 'incorrect';
  assert.throws(() => checkCompatibility(topology, actual), /Incompatible queue/);
  actual.queues = structuredClone(topology.queues);
  actual.exchanges[0].type = 'direct';
  assert.throws(() => checkCompatibility(topology, actual), /Incompatible exchange/);
});
