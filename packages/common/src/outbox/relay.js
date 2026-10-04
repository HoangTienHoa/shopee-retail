const { randomUUID } = require('node:crypto');
const { Outbox, OUTBOX_STATUS } = require('./outbox.model');
const { toKafkaMessage } = require('../events/envelope');

/**
 * Polls the outbox and publishes pending events to Kafka.
 *
 * Guarantees: at-least-once, in write order per service instance. If the process
 * dies after Kafka acked but before the rows were marked PUBLISHED, the lock
 * expires and the batch is sent again — consumers de-duplicate by eventId.
 *
 * Claiming uses a lock token + lease (`lockedUntil`), so running several relay
 * instances never double-sends within a lease. Note that two instances may still
 * interleave batches, which can reorder events of one aggregate; run one relay
 * per service if strict ordering matters.
 */
class OutboxRelay {
  constructor({
    producer,
    logger,
    metrics,
    intervalMs = 500,
    batchSize = 100,
    lockMs = 30_000,
    maxAttempts = 10,
    maxBackoffMs = 60_000,
  }) {
    Object.assign(this, { producer, logger, metrics, intervalMs, batchSize, lockMs });
    Object.assign(this, { maxAttempts, maxBackoffMs });
    this.running = false;
    this.timer = null;
    this.inFlight = null;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.#schedule(0);
    this.logger.info({ intervalMs: this.intervalMs }, 'outbox relay started');
  }

  async stop() {
    this.running = false;
    clearTimeout(this.timer);
    await this.inFlight;
  }

  #schedule(delayMs) {
    this.timer = setTimeout(() => {
      this.inFlight = this.#loop();
    }, delayMs);
  }

  async #loop() {
    let claimed = 0;
    try {
      claimed = await this.tick();
    } catch (err) {
      this.logger.error({ err }, 'outbox relay tick failed');
    }
    // A full batch means there is probably a backlog: drain it without waiting.
    if (this.running) this.#schedule(claimed === this.batchSize ? 0 : this.intervalMs);
  }

  /** One poll cycle. Returns the number of events claimed. Public for tests. */
  async tick(now = new Date()) {
    const batch = await this.claimBatch(now);
    if (batch.length > 0) await this.publishBatch(batch, now);
    if (this.metrics) {
      this.metrics.outboxPendingEvents.set(
        await Outbox.countDocuments({ status: OUTBOX_STATUS.PENDING }),
      );
    }
    return batch.length;
  }

  async claimBatch(now) {
    const candidates = await Outbox.find({
      status: OUTBOX_STATUS.PENDING,
      lockedUntil: { $lte: now },
    })
      .sort({ _id: 1 })
      .limit(this.batchSize)
      .select('_id')
      .lean();
    if (candidates.length === 0) return [];

    // Re-checking the filter in the update makes the claim atomic per document:
    // a row another instance grabbed in between simply won't match.
    const lockToken = randomUUID();
    await Outbox.updateMany(
      {
        _id: { $in: candidates.map((c) => c._id) },
        status: OUTBOX_STATUS.PENDING,
        lockedUntil: { $lte: now },
      },
      { $set: { lockToken, lockedUntil: new Date(now.getTime() + this.lockMs) } },
    );
    return Outbox.find({ lockToken }).sort({ _id: 1 }).lean();
  }

  async publishBatch(batch, now) {
    const ids = batch.map((doc) => doc._id);

    // Group by topic, keeping order inside each topic.
    const byTopic = new Map();
    for (const doc of batch) {
      if (!byTopic.has(doc.topic)) byTopic.set(doc.topic, []);
      byTopic.get(doc.topic).push(toKafkaMessage(doc.event));
    }

    try {
      await this.producer.sendBatch({
        acks: -1,
        topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
      });
    } catch (err) {
      await this.#markFailedAttempt(ids, err, now);
      this.metrics?.outboxPublishFailures.inc(batch.length);
      this.logger.error({ err, count: batch.length }, 'outbox publish failed, will retry');
      return;
    }

    await Outbox.updateMany(
      { _id: { $in: ids } },
      {
        $set: { status: OUTBOX_STATUS.PUBLISHED, publishedAt: new Date() },
        $unset: { lockToken: 1 },
      },
    );

    if (this.metrics) {
      for (const doc of batch) {
        this.metrics.outboxPublished.inc({ eventType: doc.eventType });
        this.metrics.outboxPublishLatency.observe((Date.now() - doc.createdAt.getTime()) / 1000);
      }
    }
    this.logger.debug({ count: batch.length }, 'outbox batch published');
  }

  /** attempts += 1, exponential backoff via lockedUntil, FAILED after maxAttempts. */
  async #markFailedAttempt(ids, err, now) {
    await Outbox.updateMany(
      { _id: { $in: ids } },
      [
        { $set: { attempts: { $add: ['$attempts', 1] }, lastError: String(err?.message ?? err) } },
        {
          $set: {
            status: {
              $cond: [
                { $gte: ['$attempts', this.maxAttempts] },
                OUTBOX_STATUS.FAILED,
                OUTBOX_STATUS.PENDING,
              ],
            },
            lockedUntil: {
              $add: [
                now,
                { $min: [this.maxBackoffMs, { $multiply: [1000, { $pow: [2, '$attempts'] }] }] },
              ],
            },
          },
        },
        { $unset: 'lockToken' },
      ],
      { updatePipeline: true },
    );
  }
}

module.exports = { OutboxRelay };
