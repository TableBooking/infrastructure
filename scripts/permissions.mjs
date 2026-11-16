import { consumers, roles } from './config.mjs';

const escapeRegex = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export function servicePermissions(role) {
  if (!roles.includes(role)) throw new Error('Unknown service role');
  const own = `${escapeRegex(consumers[role].queue)}(\\.(retry|dlq))?`;
  const shared = 'restaurant\\.(events|retry|dlx)';
  return {
    configure: `^(${shared}|${own})$`,
    write: `^(${consumers[role].publish.length ? shared : 'restaurant\\.(retry|dlx)'}|${own})$`,
    read: `^(${shared}|${own})$`,
  };
}

export function topicPermissions(role) {
  const consumer = consumers[role];
  return {
    exchange: 'restaurant.events',
    write: consumer.publish.length ? `^(${consumer.publish.map(escapeRegex).join('|')})$` : '^$',
    read: `^(${consumer.keys.map(escapeRegex).join('|')})$`,
  };
}
