module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/?(*.)+(test).js'],
  // E2E runs against the full docker-compose stack via its own script (Phase 10).
  testPathIgnorePatterns: ['/node_modules/', '/tests/e2e/'],
  collectCoverageFrom: ['packages/*/src/**/*.js', 'services/*/src/**/*.js'],
  passWithNoTests: true,
};
