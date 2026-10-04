const express = require('express');
const request = require('supertest');
const { z } = require('zod');
const { silentLogger } = require('./helpers');
const { httpLogger } = require('../src/logger');
const { validate } = require('../src/http/validate');
const { createMetrics } = require('../src/metrics');
const {
  AppError,
  NotFoundError,
  isRetryable,
  errorMiddleware,
  notFoundMiddleware,
  NonRetryableError,
} = require('../src/errors');

function buildApp() {
  const metrics = createMetrics({ service: 'test', collectDefault: false });
  const app = express();
  app.use(express.json());
  app.use(httpLogger(silentLogger));
  app.use(metrics.httpMiddleware());
  app.get('/metrics', metrics.handler);
  app.post('/items', validate(z.object({ qty: z.number().int().positive() })), (req, res) =>
    res.status(201).json(req.validated.body),
  );
  app.get('/items/:id', (req, res, next) =>
    next(req.params.id === 'boom' ? new Error('db password leaked') : new NotFoundError()),
  );
  app.use(notFoundMiddleware);
  app.use(errorMiddleware(silentLogger));
  return { app, metrics };
}

describe('http helpers', () => {
  const { app } = buildApp();

  it('passes valid bodies through req.validated', async () => {
    const res = await request(app).post('/items').send({ qty: 2 });
    expect(res.status).toBe(201);
    expect(res.body).toEqual({ qty: 2 });
  });

  it('returns 400 with field errors for invalid bodies', async () => {
    const res = await request(app).post('/items').send({ qty: -1 });
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
    expect(res.body.error.details.fieldErrors.qty).toBeDefined();
  });

  it('maps AppErrors and unknown routes to 4xx', async () => {
    expect((await request(app).get('/items/1')).body.error.code).toBe('NOT_FOUND');
    const res = await request(app).get('/nope');
    expect(res.status).toBe(404);
    expect(res.body.error.message).toContain('GET /nope');
  });

  it('hides internals of 5xx errors', async () => {
    const res = await request(app).get('/items/boom');
    expect(res.status).toBe(500);
    expect(res.body).toEqual({
      error: { code: 'INTERNAL_ERROR', message: 'Internal Server Error' },
    });
  });

  it('labels malformed JSON as a client error', async () => {
    const res = await request(app)
      .post('/items')
      .set('Content-Type', 'application/json')
      .send('{bad');
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('BAD_REQUEST');
  });

  it('propagates or generates x-request-id', async () => {
    const given = await request(app).get('/items/1').set('x-request-id', 'abc');
    expect(given.headers['x-request-id']).toBe('abc');
    const generated = await request(app).get('/items/1');
    expect(generated.headers['x-request-id']).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('exposes http metrics labelled by route template', async () => {
    await request(app).get('/items/42');
    const res = await request(app).get('/metrics');
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.text).toMatch(/http_request_duration_seconds_count\{[^}]*route="\/items\/:id"/);
    expect(res.text).toMatch(/route="unmatched"/);
  });

  it('classifies retryability', () => {
    expect(isRetryable(new Error('timeout'))).toBe(true);
    expect(isRetryable(new NonRetryableError('bad'))).toBe(false);
    expect(new AppError('x').status).toBe(500);
  });

  it('collects default process metrics when enabled', async () => {
    const metrics = createMetrics({ service: 'svc' });
    expect(await metrics.registry.metrics()).toMatch(
      /process_cpu_user_seconds_total\{service="svc"\}/,
    );
  });
});
