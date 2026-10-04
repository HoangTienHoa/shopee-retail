const { mongoose } = require('../mongo');

const OUTBOX_STATUS = Object.freeze({
  PENDING: 'PENDING',
  PUBLISHED: 'PUBLISHED',
  FAILED: 'FAILED',
});

const PUBLISHED_RETENTION_SECONDS = 7 * 24 * 3600;

// _id stays an ObjectId on purpose: ObjectIds created by one process are
// monotonic, so sorting by _id preserves the order events were written in —
// even several events inserted in the same millisecond / transaction.
const outboxSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true, unique: true },
    eventType: { type: String, required: true },
    topic: { type: String, required: true },
    key: { type: String, required: true },
    event: { type: mongoose.Schema.Types.Mixed, required: true },
    status: {
      type: String,
      enum: Object.values(OUTBOX_STATUS),
      default: OUTBOX_STATUS.PENDING,
    },
    attempts: { type: Number, default: 0 },
    lastError: String,
    lockedUntil: { type: Date, default: () => new Date(0) },
    lockToken: String,
    publishedAt: Date,
  },
  {
    collection: 'outbox',
    timestamps: { createdAt: true, updatedAt: false },
    versionKey: false,
    minimize: false, // keep empty payload objects
  },
);

outboxSchema.index({ status: 1, _id: 1 });
outboxSchema.index({ lockToken: 1 }, { sparse: true });
outboxSchema.index({ publishedAt: 1 }, { expireAfterSeconds: PUBLISHED_RETENTION_SECONDS });

const Outbox = mongoose.models.Outbox ?? mongoose.model('Outbox', outboxSchema);

module.exports = { Outbox, OUTBOX_STATUS };
