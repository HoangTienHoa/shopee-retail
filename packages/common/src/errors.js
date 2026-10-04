/**
 * Base error. `retryable` is read by the Kafka consumer: business errors set it
 * to false so they go straight to the DLQ instead of being retried.
 */
class AppError extends Error {
  constructor(message, { status = 500, code = 'INTERNAL_ERROR', details, retryable = true } = {}) {
    super(message);
    this.name = this.constructor.name;
    this.status = status;
    this.code = code;
    this.details = details;
    this.retryable = retryable;
  }
}

class ValidationError extends AppError {
  constructor(message = 'Validation failed', details) {
    super(message, { status: 400, code: 'VALIDATION_ERROR', details, retryable: false });
  }
}

class NotFoundError extends AppError {
  constructor(message = 'Resource not found') {
    super(message, { status: 404, code: 'NOT_FOUND', retryable: false });
  }
}

class ConflictError extends AppError {
  constructor(message = 'Conflict', details) {
    super(message, { status: 409, code: 'CONFLICT', details, retryable: false });
  }
}

/** Thrown from consumer handlers when retrying cannot help (bad payload, broken invariant). */
class NonRetryableError extends AppError {
  constructor(message, details) {
    super(message, { status: 422, code: 'NON_RETRYABLE', details, retryable: false });
  }
}

const isRetryable = (err) => err?.retryable !== false;

const isDuplicateKeyError = (err) => err?.code === 11000;

function notFoundMiddleware(req, _res, next) {
  next(new NotFoundError(`Route ${req.method} ${req.originalUrl} not found`));
}

function errorMiddleware(logger) {
  // Express recognises error middleware by its 4-arg signature, so _next must stay.
  return (err, req, res, _next) => {
    const status = err.status ?? err.statusCode ?? 500;
    const isClientError = status < 500;
    if (!isClientError) (req.log ?? logger)?.error({ err }, 'request failed');

    // Never leak internals of 5xx errors to the client.
    res.status(status).json({
      error: isClientError
        ? { code: err.code ?? 'BAD_REQUEST', message: err.message, details: err.details }
        : { code: 'INTERNAL_ERROR', message: 'Internal Server Error' },
    });
  };
}

module.exports = {
  AppError,
  ValidationError,
  NotFoundError,
  ConflictError,
  NonRetryableError,
  isRetryable,
  isDuplicateKeyError,
  notFoundMiddleware,
  errorMiddleware,
};
