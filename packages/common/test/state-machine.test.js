const { defineStateMachine } = require('../src/saga/state-machine');

const order = defineStateMachine({
  PENDING: ['STOCK_RESERVED', 'CANCELLED'],
  STOCK_RESERVED: ['PAID', 'CANCELLED'],
  PAID: ['SHIPPING', 'CANCELLED'],
  SHIPPING: ['COMPLETED', 'CANCELLED'],
  COMPLETED: [],
  CANCELLED: [],
});

describe('defineStateMachine', () => {
  it('answers whether a transition is allowed', () => {
    expect(order.canTransition('PENDING', 'STOCK_RESERVED')).toBe(true);
    expect(order.canTransition('PENDING', 'PAID')).toBe(false);
    expect(order.canTransition('COMPLETED', 'CANCELLED')).toBe(false);
    expect(order.canTransition('UNKNOWN', 'PAID')).toBe(false);
  });

  it('lists source states for a conditional update', () => {
    expect(order.allowedFrom('CANCELLED')).toEqual([
      'PENDING',
      'STOCK_RESERVED',
      'PAID',
      'SHIPPING',
    ]);
    expect(order.allowedFrom('PAID')).toEqual(['STOCK_RESERVED']);
    expect(order.allowedFrom('PENDING')).toEqual([]);
    expect(() => order.allowedFrom('NOPE')).toThrow('Unknown state');
  });

  it('detects terminal states', () => {
    expect(order.isTerminal('COMPLETED')).toBe(true);
    expect(order.isTerminal('PAID')).toBe(false);
  });

  it('rejects transitions to undeclared states', () => {
    expect(() => defineStateMachine({ A: ['B'] })).toThrow('Unknown target state "B"');
  });
});
