const { createEventConsumer } = require('../src/kafka/consumer');
const { createEvent, toKafkaMessage } = require('../src/events/envelope');
const { NonRetryableError } = require('../src/errors');
const { createMetrics } = require('../src/metrics');
const { createKafka, createProducer, pinoLogCreator } = require('../src/kafka/client');
const { EVENT_TYPES, TOPICS } = require('../src/kafka/topics');
const { silentLogger } = require('./helpers');

function setup(handlers, retry = { retries: 2, initialDelayMs: 1 }) {
  const kafkaConsumer = {
    connect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
    disconnect: jest.fn(),
  };
  const kafka = { consumer: jest.fn(() => kafkaConsumer) };
  const producer = { send: jest.fn().mockResolvedValue([]) };
  const metrics = createMetrics({ service: 'test', collectDefault: false });
  const sut = createEventConsumer({
    kafka,
    groupId: 'inventory-service',
    topics: [TOPICS.ORDER],
    handlers,
    producer,
    logger: silentLogger,
    metrics,
    retry,
  });
  return { sut, kafka, kafkaConsumer, producer, metrics };
}

const orderCreated = createEvent({ eventType: EVENT_TYPES.ORDER_CREATED, aggregateId: 'o-1' });
const asMessage = (event, offset = '7') => {
  const m = toKafkaMessage(event);
  return { key: Buffer.from(m.key), value: Buffer.from(m.value), headers: m.headers, offset };
};
const deliver = (sut, message) => sut.handleMessage({ topic: TOPICS.ORDER, partition: 1, message });

async function counter(metrics, labels) {
  const { values } = await metrics.kafkaMessagesConsumed.get();
  return values.find((v) => Object.entries(labels).every(([k, val]) => v.labels[k] === val))?.value;
}

describe('createEventConsumer', () => {
  it('subscribes from the beginning and starts the run loop', async () => {
    const { sut, kafka, kafkaConsumer } = setup({});
    await sut.start();
    expect(kafka.consumer).toHaveBeenCalledWith({
      groupId: 'inventory-service',
      allowAutoTopicCreation: false,
    });
    expect(kafkaConsumer.subscribe).toHaveBeenCalledWith({
      topics: [TOPICS.ORDER],
      fromBeginning: true,
    });
    expect(kafkaConsumer.run).toHaveBeenCalledWith({ eachMessage: sut.handleMessage });
    await sut.stop();
    expect(kafkaConsumer.disconnect).toHaveBeenCalled();
  });

  it('dispatches by eventType with a context', async () => {
    const handler = jest.fn().mockResolvedValue({ duplicate: false });
    const { sut, metrics } = setup({ [EVENT_TYPES.ORDER_CREATED]: handler });

    await deliver(sut, asMessage(orderCreated));

    expect(handler).toHaveBeenCalledWith(
      orderCreated,
      expect.objectContaining({ groupId: 'inventory-service', topic: TOPICS.ORDER, partition: 1 }),
    );
    expect(await counter(metrics, { eventType: 'OrderCreated', result: 'success' })).toBe(1);
  });

  it('counts duplicates reported by the idempotent wrapper', async () => {
    const { sut, metrics } = setup({
      [EVENT_TYPES.ORDER_CREATED]: async () => ({ duplicate: true }),
    });
    await deliver(sut, asMessage(orderCreated));
    expect(await counter(metrics, { result: 'duplicate' })).toBe(1);
  });

  it('skips event types it has no handler for', async () => {
    const { sut, producer, metrics } = setup({});
    await deliver(sut, asMessage(orderCreated));
    expect(producer.send).not.toHaveBeenCalled();
    expect(await counter(metrics, { result: 'ignored' })).toBe(1);
  });

  it('retries transient failures and succeeds', async () => {
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error('mongo timeout'))
      .mockResolvedValueOnce(undefined);
    const { sut, producer, metrics } = setup({ [EVENT_TYPES.ORDER_CREATED]: handler });

    await deliver(sut, asMessage(orderCreated));

    expect(handler).toHaveBeenCalledTimes(2);
    expect(producer.send).not.toHaveBeenCalled();
    expect(await counter(metrics, { result: 'retry' })).toBe(1);
    expect(await counter(metrics, { result: 'success' })).toBe(1);
  });

  it('sends to the DLQ with diagnostics once retries are exhausted', async () => {
    const handler = jest.fn().mockRejectedValue(new Error('still down'));
    const { sut, producer } = setup({ [EVENT_TYPES.ORDER_CREATED]: handler });

    await deliver(sut, asMessage(orderCreated, '42'));

    expect(handler).toHaveBeenCalledTimes(3); // 1 + 2 retries
    const [{ topic, messages }] = producer.send.mock.calls[0];
    expect(topic).toBe('order.events.dlq');
    expect(messages[0].key.toString()).toBe('o-1');
    expect(messages[0].headers).toMatchObject({
      eventType: 'OrderCreated',
      'x-original-topic': 'order.events',
      'x-original-partition': '1',
      'x-original-offset': '42',
      'x-consumer-group': 'inventory-service',
      'x-error': 'still down',
    });
  });

  it('does not retry non-retryable errors', async () => {
    const handler = jest.fn().mockRejectedValue(new NonRetryableError('invariant broken'));
    const { sut, producer } = setup({ [EVENT_TYPES.ORDER_CREATED]: handler });
    await deliver(sut, asMessage(orderCreated));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(producer.send).toHaveBeenCalledTimes(1);
  });

  it('parks malformed messages in the DLQ', async () => {
    const { sut, producer, metrics } = setup({});
    await deliver(sut, { key: null, value: Buffer.from('garbage'), headers: {}, offset: '1' });
    expect(producer.send.mock.calls[0][0].messages[0].headers['x-error']).toBe(
      'Message is not valid JSON',
    );
    expect(await counter(metrics, { eventType: 'unknown', result: 'dlq' })).toBe(1);
  });

  it('propagates DLQ failures so the offset is not committed', async () => {
    const { sut, producer } = setup({
      [EVENT_TYPES.ORDER_CREATED]: async () => {
        throw new NonRetryableError('x');
      },
    });
    producer.send.mockRejectedValue(new Error('kafka down'));
    await expect(deliver(sut, asMessage(orderCreated))).rejects.toThrow('kafka down');
  });
});

describe('kafka client', () => {
  it('routes kafkajs logs to pino levels', () => {
    const logger = { error: jest.fn(), warn: jest.fn(), info: jest.fn(), debug: jest.fn() };
    const log = pinoLogCreator(logger)();
    log({ namespace: 'Consumer', level: 1, log: { message: 'boom', broker: 'k:9092' } });
    log({ namespace: 'Producer', level: 99, log: { message: 'odd level' } });
    expect(logger.error).toHaveBeenCalledWith({ namespace: 'Consumer', broker: 'k:9092' }, 'boom');
    expect(logger.info).toHaveBeenCalledWith({ namespace: 'Producer' }, 'odd level');
  });

  it('creates a connected idempotent producer', async () => {
    const producer = { connect: jest.fn() };
    const kafka = { producer: jest.fn(() => producer) };
    await expect(createProducer(kafka)).resolves.toBe(producer);
    expect(kafka.producer).toHaveBeenCalledWith(expect.objectContaining({ idempotent: true }));
    expect(producer.connect).toHaveBeenCalled();
  });

  it('builds a Kafka client', () => {
    const kafka = createKafka({ clientId: 'x', brokers: ['localhost:9092'], logger: silentLogger });
    expect(typeof kafka.producer).toBe('function');
  });
});
