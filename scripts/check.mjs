import { loadConfig } from './config.mjs';
import { managementClient, readTopology, verifyTopology } from './management.mjs';

try {
  const config = loadConfig();
  const counts = await verifyTopology(managementClient(config), config, await readTopology());
  console.log(`RabbitMQ topology verified: ${counts.exchanges} exchanges, ${counts.queues} queues, ${counts.bindings} bindings.`);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
