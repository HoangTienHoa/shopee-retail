const { startMongo, silentLogger } = require('./helpers');
const { mongoose, withTransaction } = require('../src/mongo');
const { Outbox, OUTBOX_STATUS } = require('../src/outbox/outbox.model');
const { addToOutbox } = require('../src/outbox/outbox.repo');
const { OutboxRelay } = require('../src/outbox/relay');
const { createEvent } = require('../src/events/envelope');
const { createMetrics } = require('../src/metrics');
const { EVENT_TYPES } = require('../src/kafka/topics');

// A business collection to write alongside the outbox. Registered before startMongo.
const Thing = mongoose.model('Thing', new mongoose.Schema({ name: String }));

jest.setTimeout(60_000);

let mongo;
beforeAll(async () => {
  mongo = await startMongo();
});
afterEach(() => mongo.clear());
afterAll(() => mongo.stop());

const event = (eventType, orderId) =>
  createEvent({ eventType, aggregateId: orderId, payload: { orderId } });

async function stage(...events) {
  await withTransaction((session) => addToOutbox(session, events));
}

const fakeProducer = () => ({ sendBatch: jest.fn().mockResolvedValue([]) });

describe('addToOutbox', () => {
  it('commits the business write and its events atomically', async () => {
    await withTransaction(async (session) => {
      await Thing.create([{ name: 'a' }], { session });
      await addToOutbox(session, [
        event(EVENT_TYPES.ORDER_CREATED, 'o1'),
        event(EVENT_TYPES.ORDER_CANCELLED, 'o1'),
      ]);
    });

    expect(await Thing.countDocuments()).toBe(1);
    const rows = await Outbox.find().sort({ _id: 1 }).lean();
    expect(rows.map((r) => r.eventType)).toEqual(['OrderCreated', 'OrderCancelled']);
    expect(rows[0]).toMatchObject({
      topic: 'order.events',
      key: 'o1',
      status: 'PENDING',
      attempts: 0,
    });
  });

  it('rolls back the events together with the business write', async () => {
    await expect(
      withTransaction(async (session) => {
        await Thing.create([{ name: 'a' }], { session });
        await addToOutbox(session, event(EVENT_TYPES.ORDER_CREATED, 'o1'));
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');

    expect(await Thing.countDocuments()).toBe(0);
    expect(await Outbox.countDocuments()).toBe(0);
  });

  it('refuses to write outside a transaction and ignores empty lists', async () => {
    await expect(addToOutbox(undefined, [])).rejects.toThrow('inside a transaction');
    await withTransaction((session) => addToOutbox(session, []));
    expect(await Outbox.countDocuments()).toBe(0);
  });

  it('keeps empty payloads', async () => {
    const e = createEvent({ eventType: EVENT_TYPES.ORDER_COMPLETED, aggregateId: 'o1' });
    await stage(e);
    expect((await Outbox.findOne().lean()).event.payload).toEqual({});
  });
});

describe('OutboxRelay', () => {
  it('publishes in write order, grouped by topic, then marks rows published', async () => {
    await stage(
      event(EVENT_TYPES.ORDER_CREATED, 'o1'),
      event(EVENT_TYPES.PAYMENT_COMPLETED, 'o1'),
      event(EVENT_TYPES.ORDER_CREATED, 'o2'),
    );
    const producer = fakeProducer();
    const metrics = createMetrics({ service: 'test', collectDefault: false });
    const relay = new OutboxRelay({ producer, logger: silentLogger, metrics });

    expect(await relay.tick()).toBe(3);

    const { acks, topicMessages } = producer.sendBatch.mock.calls[0][0];
    expect(acks).toBe(-1);
    const summary = topicMessages.map(({ topic, messages }) => [
      topic,
      messages.map((m) => `${JSON.parse(m.value).eventType}:${m.key}`),
    ]);
    expect(summary).toEqual([
      ['order.events', ['OrderCreated:o1', 'OrderCreated:o2']],
      ['payment.events', ['PaymentCompleted:o1']],
    ]);

    const rows = await Outbox.find().lean();
    expect(rows.every((r) => r.status === OUTBOX_STATUS.PUBLISHED && r.publishedAt)).toBe(true);
    expect(rows.some((r) => r.lockToken)).toBe(false);

    expect(await relay.tick()).toBe(0);
    expect(producer.sendBatch).toHaveBeenCalledTimes(1);
    expect((await metrics.outboxPublished.get()).values.map((v) => v.value)).toEqual([2, 1]);
    expect((await metrics.outboxPendingEvents.get()).values[0].value).toBe(0);
  });

  it('backs off exponentially and gives up after maxAttempts', async () => {
    await stage(event(EVENT_TYPES.ORDER_CREATED, 'o1'));
    const producer = { sendBatch: jest.fn().mockRejectedValue(new Error('broker down')) };
    const metrics = createMetrics({ service: 'test', collectDefault: false });
    const relay = new OutboxRelay({ producer, logger: silentLogger, metrics, maxAttempts: 2 });
    const t0 = new Date();

    expect(await relay.tick(t0)).toBe(1);
    let row = await Outbox.findOne().lean();
    expect(row).toMatchObject({ status: 'PENDING', attempts: 1, lastError: 'broker down' });
    expect(row.lockedUntil.getTime()).toBe(t0.getTime() + 2_000); // 2^1 s
    expect(row.lockToken).toBeUndefined();

    expect(await relay.tick(t0)).toBe(0); // not due yet

    expect(await relay.tick(new Date(t0.getTime() + 2_000))).toBe(1);
    row = await Outbox.findOne().lean();
    expect(row).toMatchObject({ status: 'FAILED', attempts: 2 });

    expect(await relay.tick(new Date(t0.getTime() + 3_600_000))).toBe(0);
    expect((await metrics.outboxPublishFailures.get()).values[0].value).toBe(2);
  });

  it('recovers once the broker is back', async () => {
    await stage(event(EVENT_TYPES.ORDER_CREATED, 'o1'));
    const producer = fakeProducer();
    producer.sendBatch.mockRejectedValueOnce(new Error('blip'));
    const relay = new OutboxRelay({ producer, logger: silentLogger });
    const t0 = new Date();

    await relay.tick(t0);
    await relay.tick(new Date(t0.getTime() + 5_000));

    expect((await Outbox.findOne().lean()).status).toBe('PUBLISHED');
  });

  it('never lets two relays claim the same row within a lease', async () => {
    await stage(...Array.from({ length: 20 }, (_, i) => event(EVENT_TYPES.ORDER_CREATED, `o${i}`)));
    const a = new OutboxRelay({ producer: fakeProducer(), logger: silentLogger, lockMs: 1_000 });
    const b = new OutboxRelay({ producer: fakeProducer(), logger: silentLogger, lockMs: 1_000 });
    const now = new Date();

    const [claimedA, claimedB] = await Promise.all([a.claimBatch(now), b.claimBatch(now)]);
    const idsA = claimedA.map((d) => String(d._id));
    const idsB = new Set(claimedB.map((d) => String(d._id)));

    expect(idsA.length + idsB.size).toBe(20);
    expect(idsA.filter((id) => idsB.has(id))).toEqual([]);

    // After the lease expires, an abandoned claim is picked up again.
    expect(await b.claimBatch(new Date(now.getTime() + 1_001))).toHaveLength(20);
  });

  it('runs in the background until stopped', async () => {
    await stage(event(EVENT_TYPES.ORDER_CREATED, 'o1'));
    const relay = new OutboxRelay({
      producer: fakeProducer(),
      logger: silentLogger,
      intervalMs: 10,
    });

    relay.start();
    relay.start(); // no-op when already running
    for (let i = 0; i < 100; i += 1) {
      if ((await Outbox.findOne().lean()).status === 'PUBLISHED') break;
      await new Promise((r) => setTimeout(r, 20));
    }
    await relay.stop();

    expect((await Outbox.findOne().lean()).status).toBe('PUBLISHED');
  });

  it('logs and keeps polling when a tick throws', async () => {
    const logger = { info: jest.fn(), debug: jest.fn(), error: jest.fn() };
    const relay = new OutboxRelay({ producer: fakeProducer(), logger, intervalMs: 10 });
    relay.claimBatch = jest.fn().mockRejectedValue(new Error('mongo down'));

    relay.start();
    await new Promise((r) => setTimeout(r, 60));
    await relay.stop();

    expect(relay.claimBatch.mock.calls.length).toBeGreaterThan(1);
    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(Error) }),
      'outbox relay tick failed',
    );
  });
});
