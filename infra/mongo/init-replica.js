// Run by the mongo healthcheck: initiates the single-node replica set on first boot,
// then reports healthy only once this node is a writable primary (transactions need it).
try {
  rs.status();
} catch (_e) {
  rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
}

if (!db.hello().isWritablePrimary) {
  quit(1);
}
