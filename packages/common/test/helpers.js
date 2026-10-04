const { MongoMemoryReplSet } = require('mongodb-memory-server');
const { mongoose, connectMongo, disconnectMongo } = require('../src/mongo');
const { createLogger } = require('../src/logger');

const silentLogger = createLogger({ service: 'test', level: 'silent' });

/** Single-node replica set: transactions need one. Require models before calling. */
async function startMongo() {
  const replSet = await MongoMemoryReplSet.create({
    replSet: { count: 1, storageEngine: 'wiredTiger' },
  });
  await connectMongo({ uri: replSet.getUri(), dbName: 'common_test', logger: silentLogger });
  // Create collections + unique/TTL indexes up front; the tests rely on them.
  await Promise.all(Object.values(mongoose.models).map((model) => model.init()));

  return {
    async clear() {
      const collections = Object.values(mongoose.connection.collections);
      await Promise.all(collections.map((c) => c.deleteMany({})));
    },
    async stop() {
      await disconnectMongo();
      await replSet.stop();
    },
  };
}

/** Minimal in-memory stand-in for the ioredis calls we use (SET [EX] [NX], GET, DEL). */
function fakeRedis() {
  const store = new Map();
  return {
    store,
    async set(key, value, ...args) {
      if (args.includes('NX') && store.has(key)) return null;
      store.set(key, value);
      return 'OK';
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async del(key) {
      return store.delete(key) ? 1 : 0;
    },
  };
}

module.exports = { silentLogger, startMongo, fakeRedis };
