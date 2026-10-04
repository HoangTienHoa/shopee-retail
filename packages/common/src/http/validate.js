const { z } = require('zod');
const { ValidationError } = require('../errors');

/**
 * Validates req[source] with a zod schema; the parsed value lands in
 * req.validated[source] (Express 5's req.query is read-only, so we never overwrite).
 */
function validate(schema, source = 'body') {
  return (req, _res, next) => {
    const result = schema.safeParse(req[source]);
    if (!result.success) {
      return next(new ValidationError('Invalid request', z.flattenError(result.error)));
    }
    req.validated = { ...req.validated, [source]: result.data };
    return next();
  };
}

module.exports = { validate };
