import { readFileSync } from 'node:fs';
import { parseEnv } from 'node:util';
import { pathToFileURL } from 'node:url';

export const root = new URL('../', import.meta.url);
export const roles = ['booking', 'operations', 'tracking', 'notification'];
export const consumers = {
  booking: { queue: 'booking.lifecycle.q', keys: ['booking.state-changed.v1', 'booking.cancellation-rejected.v1'], publish: ['booking.requested.v1', 'booking.cancel-requested.v1'] },
  operations: { queue: 'operations.requests.q', keys: ['booking.requested.v1', 'booking.cancel-requested.v1'], publish: ['booking.state-changed.v1', 'booking.cancellation-rejected.v1'] },
  tracking: { queue: 'tracking.lifecycle.q', keys: ['booking.state-changed.v1'], publish: [] },
  notification: { queue: 'notification.lifecycle.q', keys: ['booking.state-changed.v1'], publish: [] },
};

export function validateConfig(env) {
  const required = name => {
    if (!env[name]) throw new Error(`Missing ${name}; run npm run setup`);
    return env[name];
  };
  const names = ['ADMIN', ...roles.map(role => role.toUpperCase())];
  const users = {};
  for (const name of names) {
    const username = required(`RABBITMQ_${name}_USER`);
    const password = required(`RABBITMQ_${name}_PASSWORD`);
    if (!/^[a-zA-Z0-9_-]{1,64}$/.test(username) || username === 'guest') throw new Error(`Invalid RABBITMQ_${name}_USER`);
    if (password.length < 20 || /[\r\n]/.test(password)) throw new Error(`RABBITMQ_${name}_PASSWORD must have at least 20 characters and no newline`);
    users[name.toLowerCase()] = { username, password };
  }
  if (new Set(Object.values(users).map(user => user.username)).size !== names.length) throw new Error('RabbitMQ usernames must be distinct');
  const vhost = required('RABBITMQ_VHOST');
  if (!/^[a-zA-Z0-9_.-]{1,64}$/.test(vhost)) throw new Error('Invalid RABBITMQ_VHOST');
  const port = (key, fallback) => {
    const value = Number(env[key] ?? fallback);
    if (!Number.isInteger(value) || value < 1 || value > 65535) throw new Error(`Invalid ${key}`);
    return value;
  };
  const amqpPort = port('RABBITMQ_AMQP_PORT', 5672);
  const managementPort = port('RABBITMQ_MANAGEMENT_PORT', 15672);
  if (amqpPort === managementPort) throw new Error('RabbitMQ ports must be distinct');
  const network = env.RABBITMQ_NETWORK ?? 'restaurant-backend';
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(network)) throw new Error('Invalid RABBITMQ_NETWORK');
  const managementUrl = env.RABBITMQ_MANAGEMENT_URL ?? `http://127.0.0.1:${managementPort}`;
  if (!['http:', 'https:'].includes(new URL(managementUrl).protocol)) throw new Error('Invalid RABBITMQ_MANAGEMENT_URL');
  return { users, vhost, amqpPort, managementPort, network, managementUrl };
}

export function loadConfig() {
  let file = {};
  try { file = parseEnv(readFileSync(new URL('.env', root), 'utf8')); } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  return validateConfig({ ...file, ...process.env });
}

export function connectionUrl(config, role, docker = false) {
  const { username, password } = config.users[role];
  const encode = encodeURIComponent;
  return `amqp://${encode(username)}:${encode(password)}@${docker ? 'rabbitmq:5672' : `127.0.0.1:${config.amqpPort}`}/${encode(config.vhost)}`;
}

export function isMainModule(meta) {
  return !!process.argv[1] && pathToFileURL(process.argv[1]).href === meta.url;
}
