const Redis = require('ioredis');

async function createRedis(url, { logger } = {}) {
  const redis = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 3 });
  redis.on('error', (err) => logger?.error({ err }, 'redis error'));
  await redis.connect();
  logger?.info('redis connected');
  return redis;
}

module.exports = { createRedis };
