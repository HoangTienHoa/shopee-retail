const { mongoose } = require('../mongo');

const RETENTION_SECONDS = 7 * 24 * 3600;

// One row per (consumer group, event). Kafka redelivery older than the
// retention window is not expected; business unique indexes are the last line.
const processedEventSchema = new mongoose.Schema(
  {
    _id: String, // `${groupId}:${eventId}`
    eventType: { type: String, required: true },
    processedAt: { type: Date, default: Date.now },
  },
  { collection: 'processed_events', versionKey: false },
);

processedEventSchema.index({ processedAt: 1 }, { expireAfterSeconds: RETENTION_SECONDS });

const ProcessedEvent =
  mongoose.models.ProcessedEvent ?? mongoose.model('ProcessedEvent', processedEventSchema);

module.exports = { ProcessedEvent };
