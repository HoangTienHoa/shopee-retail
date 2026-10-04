const client = require('@prometheus-io/client');

/**
 * Per-service Prometheus registry with the metrics shared by every service.
 * Services register their own (e.g. saga_*) on `metrics.registry`.
 */
function createMetrics({ service, collectDefault = true }) {
  const registry = new client.Registry();
  registry.setDefaultLabels({ service });
  if (collectDefault) client.collectDefaultMetrics({ register: registry });
  const registers = [registry];

  const metrics = {
    client,
    registry,

    httpRequestDuration: new client.Histogram({
      name: 'http_request_duration_seconds',
      help: 'HTTP request duration',
      labelNames: ['method', 'route', 'status'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers,
    }),

    kafkaMessagesConsumed: new client.Counter({
      name: 'kafka_messages_consumed_total',
      help: 'Kafka messages consumed, by outcome',
      labelNames: ['topic', 'eventType', 'result'], // success|duplicate|retry|dlq|ignored
      registers,
    }),
    kafkaHandlerDuration: new client.Histogram({
      name: 'kafka_handler_duration_seconds',
      help: 'Time spent handling one Kafka message, retries included',
      labelNames: ['topic', 'eventType'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers,
    }),

    outboxPendingEvents: new client.Gauge({
      name: 'outbox_pending_events',
      help: 'Outbox rows waiting to be published',
      registers,
    }),
    outboxPublished: new client.Counter({
      name: 'outbox_events_published_total',
      help: 'Outbox events published to Kafka',
      labelNames: ['eventType'],
      registers,
    }),
    outboxPublishFailures: new client.Counter({
      name: 'outbox_publish_failures_total',
      help: 'Outbox events whose publish attempt failed',
      registers,
    }),
    outboxPublishLatency: new client.Histogram({
      name: 'outbox_publish_latency_seconds',
      help: 'Delay between writing an event to the outbox and publishing it',
      buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
      registers,
    }),
  };

  // Label by route template (/orders/:id), never by raw URL, to bound cardinality.
  metrics.httpMiddleware = () => (req, res, next) => {
    const end = metrics.httpRequestDuration.startTimer({ method: req.method });
    res.on('finish', () => {
      const route = req.route ? `${req.baseUrl}${req.route.path}` : 'unmatched';
      end({ route, status: res.statusCode });
    });
    next();
  };

  metrics.handler = async (_req, res) => {
    res.set('Content-Type', registry.contentType);
    res.end(await registry.metrics());
  };

  return metrics;
}

module.exports = { createMetrics };
