import { setTimeout } from 'node:timers/promises';
import { isMainModule, loadConfig, roles } from './config.mjs';
import { checkCompatibility, encode, managementClient, readTopology, verifyTopology } from './management.mjs';
import { servicePermissions, topicPermissions } from './permissions.mjs';

export async function bootstrap(config = loadConfig()) {
  const api = managementClient(config);
  let available = false;
  for (let attempt = 0; attempt < 30; attempt++) {
    try { await api('overview'); available = true; break; } catch {
      await setTimeout(1000);
    }
  }
  if (!available) throw new Error('RabbitMQ management unavailable or admin credentials rejected; inspect broker logs and .env');
  const vhost = encode(config.vhost);
  await api(`vhosts/${vhost}`, { method: 'PUT' });
  await api(`permissions/${vhost}/${encode(config.users.admin.username)}`, {
    method: 'PUT', body: { configure: '.*', write: '.*', read: '.*' },
  });
  const topology = await readTopology();
  const [exchanges, queues] = await Promise.all([api(`exchanges/${vhost}`), api(`queues/${vhost}`)]);
  checkCompatibility(topology, { exchanges, queues });
  await api(`definitions/${vhost}`, { method: 'POST', body: topology });
  for (const role of roles) {
    const { username, password } = config.users[role];
    await api(`users/${encode(username)}`, { method: 'PUT', body: { password, tags: '' } });
    await api(`permissions/${vhost}/${encode(username)}`, { method: 'PUT', body: servicePermissions(role) });
    await api(`topic-permissions/${vhost}/${encode(username)}`, { method: 'PUT', body: topicPermissions(role) });
  }
  const counts = await verifyTopology(api, config, topology);
  console.log(`RabbitMQ topology ready: ${counts.exchanges} exchanges, ${counts.queues} queues, ${counts.bindings} bindings; 4 service accounts configured.`);
  return counts;
}

if (isMainModule(import.meta)) {
  bootstrap().catch(error => { console.error(error.message); process.exitCode = 1; });
}
