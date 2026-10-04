const { startMongo, silentLogger } = require('./helpers');
const { mongoose } = require('../src/mongo');
const { idempotent } = require('../src/idempotency/idempotent-handler');
const { ProcessedEvent } = require('../src/idempotency/processed-event.model');
const { Outbox } = require('../src/outbox/outbox.model');
const { addToOutbox } = require('../src/outbox/outbox.repo');
const { createEvent } = require('../src/events/envelope');
const { EVENT_TYPES } = require('../src/kafka/topics');

const Stock = mongoose.model('Stock', new mongoose.Schema({ _id: String, qty: Number }));
const Reservation = mongoose.model(
  'Reservation',
  new mongoose.Schema({ orderId: { type: String, unique: true } }),
);

jest.setTimeout(60_000);

let mongo;
beforeAll(async () => {
  mongo = await startMongo();
});
beforeEach(() => Stock.create({ _id: 'p1', qty: 10 }));
afterEach(() => mongo.clear());
afterAll(() => mongo.stop());

const ctx = { groupId: 'inventory-service', log: silentLogger };
const orderCreated = () => createEvent({ eventType: EVENT_TYPES.ORDER_CREATED, aggregateId: 'o1' });

/** Reserve one unit and emit InventoryReserved; optionally fail at the end. */
const reserve = (shouldFail = () => false) =>
  idempotent(async (event, { session }) => {
    await Stock.updateOne({ _id: 'p1' }, { $inc: { qty: -1 } }, { session });
    await addToOutbox(
      session,
      createEvent({
        eventType: EVENT_TYPES.INVENTORY_RESERVED,
        aggregateId: event.aggregateId,
        causedBy: event,
      }),
    );
    if (shouldFail()) throw new Error('crash mid-handler');
    return 'reserved';
  });

const qty = async () => (await Stock.findById('p1').lean()).qty;

describe('idempotent()', () => {
  it('applies an event exactly once per consumer group', async () => {
    const handler = reserve();
    const event = orderCreated();

    await expect(handler(event, ctx)).resolves.toEqual({ duplicate: false, result: 'reserved' });
    await expect(handler(event, ctx)).resolves.toEqual({ duplicate: true });

    expect(await qty()).toBe(9);
    expect(await Outbox.countDocuments()).toBe(1);
    expect(await ProcessedEvent.exists({ _id: `inventory-service:${event.eventId}` })).toBeTruthy();
  });

  it('tracks consumer groups independently', async () => {
    const handler = reserve();
    const event = orderCreated();
    await handler(event, ctx);
    await expect(handler(event, { ...ctx, groupId: 'payment-service' })).resolves.toMatchObject({
      duplicate: false,
    });
  });

  it('rolls back everything on failure so the retry starts clean', async () => {
    let fail = true;
    const handler = reserve(() => fail);
    const event = orderCreated();

    await expect(handler(event, ctx)).rejects.toThrow('crash mid-handler');
    expect(await qty()).toBe(10);
    expect(await ProcessedEvent.countDocuments()).toBe(0);
    expect(await Outbox.countDocuments()).toBe(0);

    fail = false;
    await expect(handler(event, ctx)).resolves.toMatchObject({ duplicate: false });
    expect(await qty()).toBe(9);
  });

  it('surfaces duplicate keys from business collections as errors', async () => {
    await Reservation.create({ orderId: 'o1' });
    const handler = idempotent((event, { session }) =>
      Reservation.create([{ orderId: event.aggregateId }], { session }),
    );

    await expect(handler(orderCreated(), ctx)).rejects.toMatchObject({ code: 11000 });
    expect(await ProcessedEvent.countDocuments()).toBe(0);
  });

  it('handles concurrent redelivery of the same event once', async () => {
    const handler = reserve();
    const event = orderCreated();

    const results = await Promise.all([handler(event, ctx), handler(event, ctx)]);

    expect(results.filter((r) => r.duplicate)).toHaveLength(1);
    expect(await qty()).toBe(9);
  });
});
