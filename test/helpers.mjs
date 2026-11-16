import { randomBytes, randomUUID } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';
import { connect } from 'amqplib';
import { bootstrap } from '../scripts/bootstrap.mjs';
import { connectionUrl, loadConfig, roles } from '../scripts/config.mjs';
import { encode, managementClient } from '../scripts/management.mjs';

export async function eventually(work, timeout = 15000) {
  const until = Date.now() + timeout;
  while (Date.now() < until) {
    try { if (await work()) return; } catch { /* Broker may be restarting. */ }
    await setTimeout(100);
  }
  throw new Error('Timed out waiting for RabbitMQ state');
}

export async function testBroker() {
  const suffix = randomUUID().replaceAll('-', '');
  const base = loadConfig();
  const config = { ...base, users: { ...base.users }, vhost: `infra-test-${suffix}` };
  for (const role of roles) config.users[role] = { username: `${role}_test_${suffix}`, password: randomBytes(32).toString('base64url') };
  const api = managementClient(config);
  const connections = new Set();
  const open = async (role = 'admin') => {
    const connection = await connect(connectionUrl(config, role), { timeout: 5000 });
    connection.on('error', () => undefined);
    connection.on('close', () => connections.delete(connection));
    connections.add(connection);
    return connection;
  };
  try { await bootstrap(config); } catch (error) {
    await api(`vhosts/${encode(config.vhost)}`, { method: 'DELETE', allowMissing: true });
    for (const role of roles) await api(`users/${encode(config.users[role].username)}`, { method: 'DELETE', allowMissing: true });
    throw error;
  }
  return {
    config, api, open,
    async closeConnections() {
      await Promise.allSettled([...connections].map(connection => connection.close()));
    },
    async cleanup() {
      await Promise.allSettled([...connections].map(connection => connection.close()));
      await api(`vhosts/${encode(config.vhost)}`, { method: 'DELETE', allowMissing: true });
      for (const role of roles) await api(`users/${encode(config.users[role].username)}`, { method: 'DELETE', allowMissing: true });
    },
  };
}

export async function confirmChannel(connection) {
  const channel = await connection.createConfirmChannel();
  channel.on('error', () => undefined);
  return channel;
}

export function publish(channel, exchange, routingKey, body, properties = {}) {
  return new Promise((resolve, reject) => {
    let returned = false;
    const onReturn = () => { returned = true; };
    const onClose = () => done(new Error('Channel closed before publisher confirm'));
    const timer = globalThis.setTimeout(() => done(new Error('Publisher confirm timed out')), 5000);
    function done(error) {
      clearTimeout(timer);
      channel.off('return', onReturn); channel.off('close', onClose);
      if (error) reject(error); else if (returned) reject(new Error('UNROUTABLE')); else resolve();
    }
    channel.on('return', onReturn); channel.once('close', onClose);
    try {
      channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(body)), {
        contentType: 'application/json', persistent: true, mandatory: true,
        messageId: randomUUID(), ...properties,
      }, done);
    } catch (error) { done(error); }
  });
}
