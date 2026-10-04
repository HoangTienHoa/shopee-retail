const { randomUUID } = require('node:crypto');
const pino = require('pino');
const pinoHttp = require('pino-http');

function createLogger({ service, level = 'info' } = {}) {
  return pino({
    level,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: ['req.headers.authorization', 'req.headers.cookie'],
  });
}

/** Request logger; reuses an incoming x-request-id so a request can be traced across hops. */
function httpLogger(logger) {
  return pinoHttp({
    logger,
    genReqId: (req, res) => {
      const id = req.headers['x-request-id'] || randomUUID();
      res.setHeader('x-request-id', id);
      return id;
    },
    autoLogging: { ignore: (req) => req.url === '/metrics' || req.url.startsWith('/health') },
  });
}

module.exports = { createLogger, httpLogger };
