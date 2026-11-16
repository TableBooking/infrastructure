import { readFile } from 'node:fs/promises';
import { root } from './config.mjs';

export const encode = encodeURIComponent;

export function managementClient(config) {
  const { username, password } = config.users.admin;
  const auth = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
  return async (path, { method = 'GET', body, allowMissing = false } = {}) => {
    const response = await fetch(`${config.managementUrl}/api/${path}`, {
      method, headers: { Authorization: auth, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(5000),
    });
    if (allowMissing && response.status === 404) return undefined;
    if (!response.ok) throw new Error(`RabbitMQ management ${method} ${path.split('/')[0]} failed (${response.status})`);
    return response.status === 204 || response.headers.get('content-length') === '0' ? undefined : response.text().then(text => text ? JSON.parse(text) : undefined);
  };
}

export async function readTopology() {
  return JSON.parse(await readFile(new URL('rabbitmq/topology.json', root), 'utf8'));
}

export function checkCompatibility(topology, actual) {
  const argumentsEqual = (left, right) => JSON.stringify(Object.entries(left ?? {}).sort()) === JSON.stringify(Object.entries(right ?? {}).sort());
  for (const expected of topology.exchanges) {
    const found = actual.exchanges.find(item => item.name === expected.name);
    if (found && (['type', 'durable', 'auto_delete', 'internal'].some(key => (found[key] ?? false) !== (expected[key] ?? false))
      || !argumentsEqual(found.arguments, expected.arguments))) {
      throw new Error(`Incompatible exchange ${expected.name}; inspect the existing resource before changing topology`);
    }
  }
  for (const expected of topology.queues) {
    const found = actual.queues.find(item => item.name === expected.name);
    if (!found) continue;
    const argumentsMatch = argumentsEqual(found.arguments, expected.arguments);
    if (found.durable !== expected.durable || found.auto_delete !== expected.auto_delete || !argumentsMatch) {
      throw new Error(`Incompatible queue ${expected.name}; migrate it explicitly without deleting pending messages`);
    }
  }
}

export async function verifyTopology(api, config, topology) {
  const vhost = encode(config.vhost);
  const [exchanges, queues, bindings] = await Promise.all([
    api(`exchanges/${vhost}`), api(`queues/${vhost}`), api(`bindings/${vhost}`),
  ]);
  checkCompatibility(topology, { exchanges, queues });
  for (const expected of topology.exchanges) {
    if (!exchanges.some(item => item.name === expected.name)) throw new Error(`Missing exchange ${expected.name}`);
  }
  for (const expected of topology.queues) {
    if (!queues.some(item => item.name === expected.name)) throw new Error(`Missing queue ${expected.name}`);
  }
  for (const expected of topology.bindings) {
    if (!bindings.some(item => item.source === expected.source && item.destination === expected.destination
      && item.destination_type === expected.destination_type && item.routing_key === expected.routing_key)) {
      throw new Error(`Missing binding ${expected.source} -> ${expected.destination}`);
    }
  }
  return { exchanges: topology.exchanges.length, queues: topology.queues.length, bindings: topology.bindings.length };
}
