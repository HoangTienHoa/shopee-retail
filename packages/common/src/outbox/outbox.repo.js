const { Outbox } = require('./outbox.model');
const { topicFor } = require('../kafka/topics');

/**
 * Stages one or more events for publishing. Must be called with the same session
 * as the business write, so the state change and its events commit atomically.
 */
async function addToOutbox(session, events) {
  if (!session) throw new Error('addToOutbox must be called inside a transaction session');
  const list = Array.isArray(events) ? events : [events];
  if (list.length === 0) return;

  await Outbox.insertMany(
    list.map((event) => ({
      eventId: event.eventId,
      eventType: event.eventType,
      topic: topicFor(event.eventType),
      key: event.aggregateId,
      event,
    })),
    { session },
  );
}

module.exports = { addToOutbox };
