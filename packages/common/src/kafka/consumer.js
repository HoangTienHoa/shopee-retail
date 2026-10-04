const { setTimeout: sleep } = require('node:timers/promises');
const { parseEvent } = require('../events/envelope');
const { isRetryable } = require('../errors');
const { dlqTopic } = require('./topics');

/**
 * Kafka consumer that dispatches events by `eventType` to `handlers`.
 *
 * Delivery is at-least-once: the offset is committed only after a handler
 * succeeds or the message has been parked in `<topic>.dlq`. Handlers must
 * therefore be idempotent — wrap them with `idempotent()`.
 *
 * Failure policy:
 *   - malformed message or non-retryable error -> DLQ immediately
 *   - other errors -> retry with exponential backoff, then DLQ
 *   - event types without a handler are skipped (other services' events)
 */
function createEventConsumer({
  kafka,
  groupId,
  topics,
  handlers,
  producer,
  logger,
  metrics,
  retry: { retries = 3, initialDelayMs = 200, maxDelayMs = 5_000 } = {},
}) {
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });

  const record = (topic, eventType, result) =>
    metrics?.kafkaMessagesConsumed.inc({ topic, eventType, result });

  async function sendToDlq({ topic, partition, message }, err) {
    await producer.send({
      topic: dlqTopic(topic),
      acks: -1,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'x-original-topic': topic,
            'x-original-partition': String(partition),
            'x-original-offset': String(message.offset),
            'x-consumer-group': groupId,
            'x-error': String(err?.message ?? err).slice(0, 1000),
            'x-failed-at': new Date().toISOString(),
          },
        },
      ],
    });
  }

  async function handleMessage({ topic, partition, message }) {
    let event;
    try {
      event = parseEvent(message.value);
    } catch (err) {
      logger.error({ err, topic, partition, offset: message.offset }, 'malformed message -> DLQ');
      await sendToDlq({ topic, partition, message }, err);
      record(topic, 'unknown', 'dlq');
      return;
    }

    const { eventType } = event;
    const handler = handlers[eventType];
    if (!handler) {
      record(topic, eventType, 'ignored');
      return;
    }

    const log = logger.child({
      eventId: event.eventId,
      eventType,
      correlationId: event.correlationId,
      topic,
      partition,
      offset: message.offset,
    });
    const endTimer = metrics?.kafkaHandlerDuration.startTimer({ topic, eventType });

    for (let attempt = 0; ; attempt += 1) {
      try {
        const result = await handler(event, { log, groupId, topic, partition });
        record(topic, eventType, result?.duplicate ? 'duplicate' : 'success');
        endTimer?.();
        return;
      } catch (err) {
        if (!isRetryable(err) || attempt >= retries) {
          log.error({ err, attempt }, 'handler failed -> DLQ');
          await sendToDlq({ topic, partition, message }, err);
          record(topic, eventType, 'dlq');
          endTimer?.();
          return;
        }
        const delay = Math.min(initialDelayMs * 2 ** attempt, maxDelayMs);
        log.warn({ err, attempt, delay }, 'handler failed, retrying');
        record(topic, eventType, 'retry');
        await sleep(delay);
      }
    }
  }

  async function start() {
    await consumer.connect();
    // fromBeginning only applies to a brand-new group (no committed offsets), so
    // a service deployed after events were produced still sees them.
    await consumer.subscribe({ topics, fromBeginning: true });
    await consumer.run({ eachMessage: handleMessage });
    logger.info({ groupId, topics }, 'kafka consumer started');
  }

  async function stop() {
    await consumer.disconnect();
  }

  return { start, stop, handleMessage, consumer };
}

module.exports = { createEventConsumer };
