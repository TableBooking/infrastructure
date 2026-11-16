import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { prepareEnvironment } from './setup.mjs';
import { root } from './config.mjs';

try {
  const config = await prepareEnvironment();
  const compose = ['compose', '-f', 'compose.yaml', '-f', 'compose.dev.yaml'];
  for (const args of [
    ['up', '-d', '--wait', 'rabbitmq'],
    ['up', '-d', '--no-deps', '--force-recreate', 'rabbitmq-init'],
    ['wait', 'rabbitmq-init'],
  ]) {
    const result = spawnSync('docker', [...compose, ...args], { cwd: fileURLToPath(root), stdio: 'inherit' });
    if (result.error || result.status !== 0) throw new Error('Infrastructure startup failed; inspect the Docker output above');
  }
  console.log(`RabbitMQ ready. Management: http://127.0.0.1:${config.managementPort}. Credentials remain in .env.`);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
