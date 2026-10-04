const { z } = require('zod');

const baseSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.string().default('info'),
  SERVICE_NAME: z.string().min(1),
  PORT: z.coerce.number().int().positive(),
  MONGO_URI: z.string().default('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'),
  MONGO_DB: z.string().min(1),
  KAFKA_BROKERS: z
    .string()
    .default('localhost:9092')
    .transform((s) =>
      s
        .split(',')
        .map((b) => b.trim())
        .filter(Boolean),
    ),
  REDIS_URL: z.string().default('redis://localhost:6379'),
});

/**
 * Validates env vars against the shared schema plus service-specific fields.
 * Services typically pass defaults for SERVICE_NAME / PORT / MONGO_DB:
 *   loadConfig({ PORT: z.coerce.number().default(3001), ... })
 */
function loadConfig(extension = {}, env = process.env) {
  const result = baseSchema.extend(extension).safeParse(env);
  if (!result.success) {
    throw new Error(`Invalid configuration:\n${z.prettifyError(result.error)}`);
  }
  return Object.freeze(result.data);
}

module.exports = { loadConfig, baseSchema, z };
