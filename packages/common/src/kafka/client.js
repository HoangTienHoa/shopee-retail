const { Kafka, Partitioners, logLevel } = require('kafkajs');

const PINO_LEVEL = {
  [logLevel.ERROR]: 'error',
  [logLevel.WARN]: 'warn',
  [logLevel.INFO]: 'info',
  [logLevel.DEBUG]: 'debug',
};

/** Routes kafkajs' internal logs into our pino logger. */
const pinoLogCreator =
  (logger) =>
  () =>
  // Drop kafkajs' own timestamp/logger fields: pino already records time and service.
  ({ namespace, level, log: { message, timestamp: _ts, logger: _name, ...extra } }) => {
    const method = PINO_LEVEL[level] ?? 'info';
    logger?.[method]({ namespace, ...extra }, message);
  };

function createKafka({ clientId, brokers, logger, level = logLevel.WARN }) {
  return new Kafka({ clientId, brokers, logLevel: level, logCreator: pinoLogCreator(logger) });
}

/**
 * Idempotent producer: the broker de-duplicates retries of the same batch, so a
 * network retry cannot write a message twice. Requires acks = -1 (all ISR).
 */
async function createProducer(kafka) {
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
    // Explicit (murmur2, Java-compatible) to silence the v2 partitioner warning.
    // Messages are keyed by orderId, so each order sticks to one partition.
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  return producer;
}

module.exports = { createKafka, createProducer, pinoLogCreator };
