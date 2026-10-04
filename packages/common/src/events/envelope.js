const { randomUUID } = require('node:crypto');
const { z } = require('zod');
const { NonRetryableError } = require('../errors');

const envelopeSchema = z.object({
  eventId: z.uuid(),
  eventType: z.string().min(1),
  version: z.number().int().positive(),
  aggregateId: z.string().min(1),
  occurredAt: z.iso.datetime(),
  correlationId: z.string().min(1),
  causationId: z.string().nullable(),
  payload: z.record(z.string(), z.unknown()),
});

/**
 * Builds an event envelope. Pass `causedBy` (the event being handled) so the
 * new event inherits its correlationId and records it as causation — that is
 * what lets us reconstruct a whole saga from Kafka.
 */
function createEvent({ eventType, aggregateId, payload = {}, causedBy, version = 1 }) {
  return envelopeSchema.parse({
    eventId: randomUUID(),
    eventType,
    version,
    aggregateId,
    occurredAt: new Date().toISOString(),
    correlationId: causedBy?.correlationId ?? aggregateId,
    causationId: causedBy?.eventId ?? null,
    payload,
  });
}

/** Parses a Kafka message value. Malformed input is not retryable — it goes to the DLQ. */
function parseEvent(raw) {
  let data;
  try {
    data = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : raw);
  } catch (_err) {
    throw new NonRetryableError('Message is not valid JSON');
  }
  const result = envelopeSchema.safeParse(data);
  if (!result.success) {
    throw new NonRetryableError('Invalid event envelope', z.flattenError(result.error));
  }
  return result.data;
}

/** Kafka key = aggregateId keeps every event of one order on one partition, in order. */
function toKafkaMessage(event) {
  return {
    key: event.aggregateId,
    value: JSON.stringify(event),
    headers: {
      eventId: event.eventId,
      eventType: event.eventType,
      correlationId: event.correlationId,
    },
  };
}

module.exports = { envelopeSchema, createEvent, parseEvent, toKafkaMessage };
