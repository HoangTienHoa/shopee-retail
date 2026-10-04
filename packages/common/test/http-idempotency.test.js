const express = require('express');
const request = require('supertest');
const { fakeRedis, silentLogger } = require('./helpers');
const { httpIdempotency } = require('../src/idempotency/http-idempotency');
const { errorMiddleware } = require('../src/errors');

function buildApp(redis, handler) {
  const app = express();
  app.use(express.json());
  app.post('/orders', httpIdempotency({ redis }), handler);
  app.use(errorMiddleware(silentLogger));
  return app;
}

const body = { userId: 'u1', items: [{ productId: 'p1', qty: 1 }] };
const post = (app, key = 'k', payload = body) =>
  request(app).post('/orders').set('Idempotency-Key', key).send(payload);

describe('httpIdempotency', () => {
  it('runs the handler once and replays the stored response', async () => {
    const redis = fakeRedis();
    let calls = 0;
    const app = buildApp(redis, (_req, res) => {
      calls += 1;
      res.status(201).json({ orderId: `o-${calls}` });
    });

    const first = await post(app, 'k1');
    const second = await post(app, 'k1');

    expect(calls).toBe(1);
    expect(first.status).toBe(201);
    expect(second.status).toBe(201);
    expect(second.body).toEqual(first.body);
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect([...redis.store.keys()]).toEqual(['idem:u1:k1']);
  });

  it('scopes keys per user', async () => {
    let calls = 0;
    const app = buildApp(fakeRedis(), (_req, res) => res.status(201).json({ n: ++calls }));
    await post(app);
    await post(app, 'k', { ...body, userId: 'u2' });
    expect(calls).toBe(2);
  });

  it('requires a reasonably sized header', async () => {
    const app = buildApp(fakeRedis(), (_req, res) => res.json({}));
    expect((await request(app).post('/orders').send(body)).status).toBe(400);
    expect((await post(app, 'x'.repeat(256))).status).toBe(400);
  });

  it('rejects the same key with a different body', async () => {
    const app = buildApp(fakeRedis(), (_req, res) => res.status(201).json({}));
    await post(app);
    const res = await post(app, 'k', { ...body, items: [] });
    expect(res.status).toBe(422);
    expect(res.body.error.code).toBe('IDEMPOTENCY_KEY_REUSED');
  });

  it('answers 409 while the first request is still running', async () => {
    let release;
    const gate = new Promise((resolve) => (release = resolve));
    const app = buildApp(fakeRedis(), async (_req, res) => {
      await gate;
      res.status(201).json({ ok: true });
    });

    const first = post(app).then((r) => r); // .then() fires the request now
    await new Promise((r) => setTimeout(r, 50));
    const concurrent = await post(app);
    release();

    expect(concurrent.status).toBe(409);
    expect(concurrent.headers['retry-after']).toBe('1');
    expect((await first).status).toBe(201);
  });

  it('caches 4xx but forgets 5xx so the client can retry', async () => {
    const redis = fakeRedis();
    let fail = true;
    const app = buildApp(redis, (_req, res) =>
      fail ? res.status(503).json({ error: 'down' }) : res.status(201).json({ ok: true }),
    );

    expect((await post(app)).status).toBe(503);
    expect(redis.store.size).toBe(0);
    fail = false;
    expect((await post(app)).status).toBe(201);
  });

  it('still responds if persisting the result fails', async () => {
    const redis = fakeRedis();
    const app = buildApp(redis, (_req, res) => {
      redis.set = async () => {
        throw new Error('redis down');
      };
      res.status(201).json({ ok: true });
    });
    expect((await post(app)).status).toBe(201);
  });

  it('forwards Redis errors to the error handler', async () => {
    const redis = fakeRedis();
    redis.set = async () => {
      throw new Error('ECONNREFUSED');
    };
    const app = buildApp(redis, (_req, res) => res.json({}));
    expect((await post(app)).status).toBe(500);
  });
});
