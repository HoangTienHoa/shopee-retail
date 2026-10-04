const js = require('@eslint/js');
const globals = require('globals');
const prettier = require('eslint-config-prettier');

module.exports = [
  { ignores: ['**/node_modules/**', '**/coverage/**', '**/dist/**'] },
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'commonjs',
      globals: { ...globals.node },
    },
    rules: {
      'no-unused-vars': ['error', { argsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' }],
    },
  },
  {
    files: ['**/*.test.js', 'tests/**/*.js'],
    languageOptions: { globals: { ...globals.jest } },
  },
  {
    // Scripts executed inside mongosh, not Node.
    files: ['infra/mongo/**/*.js'],
    languageOptions: { globals: { rs: 'readonly', db: 'readonly', quit: 'readonly' } },
  },
  // Disables stylistic rules that conflict with Prettier; keep last.
  prettier,
];
