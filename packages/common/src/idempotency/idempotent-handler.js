const { ProcessedEvent } = require('./processed-event.model');
const { withTransaction } = require('../mongo');
const { isDuplicateKeyError } = require('../errors');

/**
 * Wraps a consumer handler so each event is applied exactly once per consumer group.
 *
 * The processed-marker insert, the handler's business writes and any outbox rows
 * all share one transaction: either everything commits, or nothing does and the
 * event can be retried. The marker is inserted first so a duplicate fails fast.
 *
 *   handlers: { OrderCreated: idempotent(async (event, { session, log }) => { ... }) }
 */
function idempotent(fn) {
  return async (event, ctx) => {
    const markerId = `${ctx.groupId}:${event.eventId}`;
    try {
      const result = await withTransaction(async (session) => {
        await ProcessedEvent.create([{ _id: markerId, eventType: event.eventType }], { session });
        return fn(event, { ...ctx, session });
      });
      return { duplicate: false, result };
    } catch (err) {
      // Only our own marker counts as "already processed"; a duplicate key on a
      // business collection is a real error and must surface.
      if (isDuplicateKeyError(err) && err.keyValue?._id === markerId) {
        ctx.log?.info('duplicate event skipped');
        return { duplicate: true };
      }
      throw err;
    }
  };
}

module.exports = { idempotent };
