const { createEvent, parseEvent, toKafkaMessage } = require('../src/events/envelope');
const { NonRetryableError } = require('../src/errors');
const { EVENT_TYPES, TOPICS, topicFor, dlqTopic } = require('../src/kafka/topics');

describe('event envelope', () => {
  it('creates a root event correlated by its aggregate', () => {
    const event = createEvent({
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 'order-1',
      payload: { totalAmount: 100 },
    });

    expect(event).toMatchObject({
      eventType: 'OrderCreated',
      version: 1,
      aggregateId: 'order-1',
      correlationId: 'order-1',
      causationId: null,
      payload: { totalAmount: 100 },
    });
    expect(event.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(new Date(event.occurredAt).toISOString()).toBe(event.occurredAt);
  });

  it('inherits correlation and records causation from the triggering event', () => {
    const cause = createEvent({ eventType: EVENT_TYPES.ORDER_CREATED, aggregateId: 'order-1' });
    const effect = createEvent({
      eventType: EVENT_TYPES.INVENTORY_RESERVED,
      aggregateId: 'order-1',
      causedBy: cause,
    });

    expect(effect.correlationId).toBe(cause.correlationId);
    expect(effect.causationId).toBe(cause.eventId);
    expect(effect.eventId).not.toBe(cause.eventId);
  });

  it('round-trips through a Kafka message', () => {
    const event = createEvent({ eventType: EVENT_TYPES.PAYMENT_COMPLETED, aggregateId: 'order-2' });
    const message = toKafkaMessage(event);

    expect(message.key).toBe('order-2');
    expect(message.headers).toEqual({
      eventId: event.eventId,
      eventType: 'PaymentCompleted',
      correlationId: 'order-2',
    });
    expect(parseEvent(Buffer.from(message.value))).toEqual(event);
  });

  it('rejects non-JSON and invalid envelopes as non-retryable', () => {
    expect(() => parseEvent('not json')).toThrow(NonRetryableError);
    expect(() => parseEvent(JSON.stringify({ eventType: 'X' }))).toThrow('Invalid event envelope');
  });

  it('maps every event type to its owner topic', () => {
    expect(topicFor(EVENT_TYPES.ORDER_CANCELLED)).toBe(TOPICS.ORDER);
    expect(topicFor(EVENT_TYPES.SHIPMENT_DELIVERED)).toBe(TOPICS.SHIPPING);
    expect(() => topicFor('Nope')).toThrow('No topic registered');
    expect(dlqTopic(TOPICS.PAYMENT)).toBe('payment.events.dlq');
    for (const type of Object.values(EVENT_TYPES)) expect(() => topicFor(type)).not.toThrow();
  });
});
