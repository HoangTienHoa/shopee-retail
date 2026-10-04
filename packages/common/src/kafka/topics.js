const TOPICS = Object.freeze({
  ORDER: 'order.events',
  INVENTORY: 'inventory.events',
  PAYMENT: 'payment.events',
  SHIPPING: 'shipping.events',
});

const dlqTopic = (topic) => `${topic}.dlq`;

const EVENT_TYPES = Object.freeze({
  ORDER_CREATED: 'OrderCreated',
  ORDER_CANCELLED: 'OrderCancelled',
  ORDER_COMPLETED: 'OrderCompleted',

  INVENTORY_RESERVED: 'InventoryReserved',
  INVENTORY_RESERVATION_FAILED: 'InventoryReservationFailed',
  INVENTORY_RELEASED: 'InventoryReleased',

  PAYMENT_COMPLETED: 'PaymentCompleted',
  PAYMENT_FAILED: 'PaymentFailed',
  PAYMENT_REFUNDED: 'PaymentRefunded',

  SHIPMENT_CREATED: 'ShipmentCreated',
  SHIPMENT_FAILED: 'ShipmentFailed',
  SHIPMENT_DELIVERED: 'ShipmentDelivered',
});

// Each event type is owned by exactly one service and lives on that service's topic.
const EVENT_TOPIC = Object.freeze({
  [EVENT_TYPES.ORDER_CREATED]: TOPICS.ORDER,
  [EVENT_TYPES.ORDER_CANCELLED]: TOPICS.ORDER,
  [EVENT_TYPES.ORDER_COMPLETED]: TOPICS.ORDER,
  [EVENT_TYPES.INVENTORY_RESERVED]: TOPICS.INVENTORY,
  [EVENT_TYPES.INVENTORY_RESERVATION_FAILED]: TOPICS.INVENTORY,
  [EVENT_TYPES.INVENTORY_RELEASED]: TOPICS.INVENTORY,
  [EVENT_TYPES.PAYMENT_COMPLETED]: TOPICS.PAYMENT,
  [EVENT_TYPES.PAYMENT_FAILED]: TOPICS.PAYMENT,
  [EVENT_TYPES.PAYMENT_REFUNDED]: TOPICS.PAYMENT,
  [EVENT_TYPES.SHIPMENT_CREATED]: TOPICS.SHIPPING,
  [EVENT_TYPES.SHIPMENT_FAILED]: TOPICS.SHIPPING,
  [EVENT_TYPES.SHIPMENT_DELIVERED]: TOPICS.SHIPPING,
});

function topicFor(eventType) {
  const topic = EVENT_TOPIC[eventType];
  if (!topic) throw new Error(`No topic registered for event type "${eventType}"`);
  return topic;
}

module.exports = { TOPICS, EVENT_TYPES, dlqTopic, topicFor };
