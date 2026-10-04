const { loadConfig, z } = require('../src/config');

const serviceDefaults = {
  SERVICE_NAME: z.string().default('order-service'),
  PORT: z.coerce.number().default(3001),
  MONGO_DB: z.string().default('order_db'),
};

describe('loadConfig', () => {
  it('applies shared and service defaults', () => {
    const config = loadConfig(serviceDefaults, {});
    expect(config).toMatchObject({
      NODE_ENV: 'development',
      SERVICE_NAME: 'order-service',
      PORT: 3001,
      MONGO_DB: 'order_db',
      KAFKA_BROKERS: ['localhost:9092'],
    });
    expect(Object.isFrozen(config)).toBe(true);
  });

  it('parses env values', () => {
    const config = loadConfig(serviceDefaults, {
      PORT: '4000',
      KAFKA_BROKERS: 'k1:9092, k2:9092,',
      NODE_ENV: 'production',
    });
    expect(config.PORT).toBe(4000);
    expect(config.KAFKA_BROKERS).toEqual(['k1:9092', 'k2:9092']);
  });

  it('fails fast with a readable message', () => {
    expect(() => loadConfig({}, { NODE_ENV: 'staging' })).toThrow(/Invalid configuration/);
  });
});
