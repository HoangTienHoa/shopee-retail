/**
 * Declarative state machine for aggregates updated by saga events.
 *
 *   const orderStates = defineStateMachine({ PENDING: ['STOCK_RESERVED', 'CANCELLED'], ... });
 *   Order.updateOne({ orderId, status: { $in: orderStates.allowedFrom('PAID') } }, ...)
 *
 * The conditional update makes duplicate and out-of-order events harmless: if the
 * current status cannot move to the target, nothing matches and nothing changes.
 */
function defineStateMachine(transitions) {
  const states = new Set(Object.keys(transitions));
  for (const [from, targets] of Object.entries(transitions)) {
    for (const to of targets) {
      if (!states.has(to)) throw new Error(`Unknown target state "${to}" from "${from}"`);
    }
  }

  const canTransition = (from, to) => transitions[from]?.includes(to) ?? false;

  const allowedFrom = (to) => {
    if (!states.has(to)) throw new Error(`Unknown state "${to}"`);
    return [...states].filter((from) => canTransition(from, to));
  };

  const isTerminal = (state) => (transitions[state]?.length ?? 0) === 0;

  return Object.freeze({ states: [...states], canTransition, allowedFrom, isTerminal });
}

module.exports = { defineStateMachine };
