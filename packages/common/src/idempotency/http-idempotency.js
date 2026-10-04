const { createHash } = require('node:crypto');
const { AppError, ConflictError, ValidationError } = require('../errors');

const IN_PROGRESS = 'IN_PROGRESS';
const DONE = 'DONE';

const defaultScope = (req) => req.get('x-user-id') ?? req.body?.userId ?? 'anonymous';

/**
 * Express middleware implementing the Idempotency-Key header (Stripe-style).
 * Mount after express.json(); responses must be sent with res.json().
 *
 *   first request   -> SET NX lock (IN_PROGRESS), run handler, store response (DONE)
 *   retry, DONE     -> replay the stored response (header Idempotent-Replayed: true)
 *   retry, running  -> 409 + Retry-After
 *   same key, other body -> 422
 *
 * 5xx responses are not stored, so the client can retry them with the same key.
 * The IN_PROGRESS lock has a short TTL so a crashed request does not block forever.
 */
function httpIdempotency({
  redis,
  ttlSeconds = 24 * 3600,
  lockTtlSeconds = 60,
  headerName = 'Idempotency-Key',
  scope = defaultScope,
  keyPrefix = 'idem',
}) {
  return async (req, res, next) => {
    const key = req.get(headerName);
    if (!key) return next(new ValidationError(`Missing ${headerName} header`));
    if (key.length > 255) return next(new ValidationError(`${headerName} is too long`));

    const redisKey = `${keyPrefix}:${scope(req)}:${key}`;
    const fingerprint = createHash('sha256')
      .update(`${req.method} ${req.originalUrl} ${JSON.stringify(req.body ?? {})}`)
      .digest('hex');

    try {
      const acquired = await redis.set(
        redisKey,
        JSON.stringify({ state: IN_PROGRESS, fingerprint }),
        'EX',
        lockTtlSeconds,
        'NX',
      );
      if (acquired) {
        captureResponse({ req, res, redis, redisKey, fingerprint, ttlSeconds });
        return next();
      }

      const raw = await redis.get(redisKey);
      const stored = raw ? JSON.parse(raw) : null;
      if (!stored || stored.state === IN_PROGRESS) {
        // !stored: lock expired between SET and GET; treat it as still running.
        res.set('Retry-After', '1');
        return next(new ConflictError(`A request with this ${headerName} is still in progress`));
      }
      if (stored.fingerprint !== fingerprint) {
        return next(
          new AppError(`${headerName} was already used with a different request`, {
            status: 422,
            code: 'IDEMPOTENCY_KEY_REUSED',
            retryable: false,
          }),
        );
      }
      res.set('Idempotent-Replayed', 'true');
      return res.status(stored.statusCode).json(stored.body);
    } catch (err) {
      return next(err);
    }
  };
}

/** Stores the response in Redis before it is sent, so an immediate retry sees DONE. */
function captureResponse({ req, res, redis, redisKey, fingerprint, ttlSeconds }) {
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    const { statusCode } = res;
    const persist =
      statusCode >= 500
        ? redis.del(redisKey)
        : redis.set(
            redisKey,
            JSON.stringify({ state: DONE, fingerprint, statusCode, body }),
            'EX',
            ttlSeconds,
          );
    persist
      .catch((err) => req.log?.error({ err, redisKey }, 'failed to persist idempotent response'))
      .finally(() => originalJson(body));
    return res;
  };
}

module.exports = { httpIdempotency };
