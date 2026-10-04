const os = require('node:os');
const mongoose = require('mongoose');

mongoose.set('strictQuery', true);

async function connectMongo({ uri, dbName, logger }) {
  await mongoose.connect(uri, {
    dbName,
    serverSelectionTimeoutMS: 10_000,
    // The driver otherwise loads `os` via dynamic import(), which fails inside
    // Jest's VM; it then sends empty handshake metadata and the server rejects it.
    runtimeAdapters: { os },
  });
  logger?.info({ dbName }, 'mongo connected');
  return mongoose.connection;
}

async function disconnectMongo() {
  await mongoose.disconnect();
}

/**
 * Runs `fn(session)` in a multi-document transaction and returns its result.
 * The driver retries the whole callback on TransientTransactionError, so `fn`
 * must be safe to re-run (only DB writes through `session`, no side effects).
 */
async function withTransaction(fn, { connection = mongoose.connection } = {}) {
  const session = await connection.startSession();
  try {
    let result;
    await session.withTransaction(async () => {
      result = await fn(session);
    });
    return result;
  } finally {
    await session.endSession();
  }
}

module.exports = { mongoose, connectMongo, disconnectMongo, withTransaction };
