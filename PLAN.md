# Plan: Shopee-like Backend (Microservices + Choreography Saga)

## Context

Dự án học tập: xây backend giống Shopee cho luồng mua hàng
**Tạo order → Kiểm tra tồn kho → Thanh toán → Giao hàng → Thông báo**, dùng MongoDB làm DB chính.
Mục tiêu là thực hành các pattern phân tán: Saga (choreography), Outbox, Idempotency, Kafka, Redis, cùng observability (Prometheus/Grafana) và CI/CD (GitHub Actions + Docker).
Thư mục `shopee-retail/` hiện đang trống, nên đây là dự án greenfield.

**Quyết định đã chốt:** microservices trong 1 monorepo · Saga **choreography** · **JavaScript + Express** · Payment/Notification dùng **mock provider**.

---

## 1. Kiến trúc tổng quan

```
Client ──HTTP──► order-service ──(outbox)──► Kafka ◄──► inventory / payment / shipping / notification
                     │                                       │
                  MongoDB (orders_db)              mỗi service có DB riêng trên cùng Mongo cluster
                  Redis (idempotency key, cache)
```

| Service              | Port | DB                | Trách nhiệm                                                                                        |
| -------------------- | ---- | ----------------- | -------------------------------------------------------------------------------------------------- |
| order-service        | 3001 | `order_db`        | API tạo/xem order, giữ trạng thái order, phát `OrderCreated` / `OrderCancelled` / `OrderCompleted` |
| inventory-service    | 3002 | `inventory_db`    | Quản lý sản phẩm và tồn kho, reserve/release stock                                                 |
| payment-service      | 3003 | `payment_db`      | Thanh toán qua mock gateway, refund                                                                |
| shipping-service     | 3004 | `shipping_db`     | Tạo shipment, giả lập vận chuyển → delivered                                                       |
| notification-service | 3005 | `notification_db` | Gửi email/SMS mock, lưu log thông báo                                                              |

**Kafka topics** (key = `orderId` để giữ thứ tự theo từng order, 3 partitions):
`order.events`, `inventory.events`, `payment.events`, `shipping.events`, cộng `<topic>.dlq` cho mỗi topic.

**Event envelope** (dùng chung):

```js
{ eventId, eventType, aggregateId /*orderId*/, version: 1, occurredAt,
  correlationId /*= orderId*/, causationId /*eventId gây ra event này*/, payload }
```

### Luồng choreography (happy path)

```
POST /orders ─► [order] OrderCreated
  ─► [inventory] reserve ─► InventoryReserved
    ─► [payment] charge ─► PaymentCompleted
      ─► [shipping] create shipment ─► ShipmentCreated ─► (job giả lập) ShipmentDelivered
        ─► [order] COMPLETED ─► OrderCompleted
[notification] lắng nghe OrderCreated, PaymentCompleted, ShipmentCreated, OrderCompleted, OrderCancelled
[order] lắng nghe tất cả *.events để cập nhật trạng thái
```

### Compensation (failure path)

Mọi event lỗi (`InventoryReservationFailed`, `PaymentFailed`, `ShipmentFailed`, timeout) đều đi về **order-service**. Order chuyển sang `CANCELLED` và phát `OrderCancelled{reason}`. Các service tự bù trừ phần việc của mình:

- inventory: nếu có reservation `RESERVED` thì release stock và phát `InventoryReleased`
- payment: nếu payment `COMPLETED` thì refund và phát `PaymentRefunded`
- Mọi compensation đều **idempotent**: nếu không có gì để bù trừ thì không làm gì.
- **Xử lý race:** `OrderCancelled` có thể đến inventory trước `OrderCreated` vì khác topic. Khi đó inventory ghi một reservation có trạng thái `RELEASED` (tombstone), để event reserve đến sau sẽ bị bỏ qua.

### State machine của Order

`PENDING → STOCK_RESERVED → PAID → SHIPPING → COMPLETED`
Từ bất kỳ trạng thái nào trước `COMPLETED` đều có thể chuyển sang `CANCELLED`.
Chuyển trạng thái bằng conditional update: `updateOne({orderId, status: {$in: allowedFrom}}, {$set, $push: statusHistory})`. Event đến sai thứ tự hoặc bị trùng sẽ không làm hỏng state.

---

## 2. Cấu trúc monorepo (npm workspaces)

```
shopee-retail/
├── package.json                 # workspaces: ["packages/*", "services/*"]
├── .eslintrc / .prettierrc / .nvmrc (Node 22 LTS)
├── packages/
│   └── common/                  # @shopee/common, dùng chung cho mọi service
│       ├── config.js            # đọc env + validate (zod)
│       ├── logger.js            # pino, gắn correlationId
│       ├── mongo.js             # connect mongoose, helper withTransaction(session => ...)
│       ├── kafka/
│       │   ├── producer.js      # kafkajs, idempotent: true, acks: -1
│       │   ├── consumer.js      # wrapper: retry + backoff, chuyển DLQ, metrics
│       │   └── topics.js        # tên topic, eventType constants
│       ├── events/envelope.js   # createEvent(), validate schema
│       ├── outbox/
│       │   ├── outbox.model.js  # schema Outbox
│       │   ├── outbox.repo.js   # addToOutbox(session, event)
│       │   └── relay.js         # polling relay: claim → publish → mark PUBLISHED
│       ├── idempotency/
│       │   ├── processed-event.model.js
│       │   ├── idempotent-handler.js   # bọc handler consumer
│       │   └── http-idempotency.js     # middleware Express dùng Redis
│       ├── redis.js             # ioredis client
│       ├── metrics.js           # prom-client registry, middleware HTTP, /metrics
│       └── errors.js            # AppError, error middleware
├── services/
│   ├── order-service/
│   │   └── src/{app.js, server.js, routes/, controllers/, models/, handlers/, saga/stateMachine.js, jobs/timeout.job.js}
│   ├── inventory-service/  (src/{models/product, models/reservation, handlers/, routes/products})
│   ├── payment-service/    (src/{models/payment, providers/mockGateway.js, handlers/})
│   ├── shipping-service/   (src/{models/shipment, providers/mockCarrier.js, jobs/deliverySimulator.js})
│   └── notification-service/ (src/{models/notification, providers/mockMailer.js, templates/})
├── infra/
│   ├── docker-compose.yml       # hạ tầng + services
│   ├── mongo/init-replica.js    # rs.initiate cho single-node replica set
│   ├── kafka/create-topics.sh
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/{datasources,dashboards}/ + dashboards/*.json
├── scripts/seed-products.js
├── tests/e2e/                   # kịch bản saga end-to-end
└── .github/workflows/{ci.yml, cd.yml}
```

Mỗi service dùng cùng một cấu trúc: `app.js` (Express app, chưa listen, để supertest dùng) và `server.js` (bootstrap: connect Mongo/Redis/Kafka, start consumer, start outbox relay, graceful shutdown khi nhận SIGTERM).

**Thư viện:** express, mongoose, kafkajs, ioredis, prom-client, pino/pino-http, zod, uuid, jest, supertest, testcontainers.

---

## 3. Các thiết kế cốt lõi

### 3.1 Outbox pattern (MongoDB)

- MongoDB **bắt buộc chạy replica set** (single node `rs0` trong Docker) để dùng multi-document transaction.
- Ghi business data và outbox trong **cùng một transaction**:
  ```js
  await withTransaction(async (session) => {
    await Order.create([order], { session });
    await addToOutbox(session, createEvent('OrderCreated', order.orderId, payload));
  });
  ```
- Schema `outbox`: `{ _id: eventId, topic, key, eventType, payload, headers, status: PENDING|PUBLISHED|FAILED, attempts, lockedUntil, createdAt, publishedAt }`. Index `{status:1, createdAt:1}`, TTL index trên `publishedAt` (7 ngày) để tự dọn.
- **Relay (polling, chu kỳ 500ms):** `findOneAndUpdate({status:'PENDING', lockedUntil<now}, {lockedUntil: now+30s})` để claim theo batch, gửi Kafka, rồi set `PUBLISHED`. Lỗi thì tăng `attempts`; quá N lần thì chuyển `FAILED` và bắn alert. Cơ chế claim bằng `lockedUntil` cho phép chạy nhiều instance an toàn.
- Hướng nâng cấp (tùy chọn): thay polling bằng **Change Streams** có lưu resume token.

### 3.2 Idempotency (3 tầng)

1. **HTTP API:** header `Idempotency-Key` bắt buộc cho `POST /orders`. Middleware Redis:
   `SET idem:{userId}:{key} IN_PROGRESS NX EX 86400`. Đã tồn tại với status `IN_PROGRESS` thì trả 409; đã `DONE` thì trả lại response đã cache. Hash request body; cùng key nhưng body khác thì trả 422.
   Lưới an toàn: unique index `orders.idempotencyKey` trong Mongo.
2. **Kafka producer:** `idempotent: true`, `maxInFlightRequests: 1` (hoặc ≤5 với idempotent).
3. **Kafka consumer** (at-least-once): collection `processed_events` với `_id = "${consumerGroup}:${eventId}"`, TTL 7 ngày. `idempotentHandler` insert record này **trong cùng transaction** với thay đổi business và outbox. Gặp duplicate key thì bỏ qua và commit offset.
   Thêm lớp an toàn: unique index nghiệp vụ (`reservations.orderId`, `payments.orderId`, `shipments.orderId`).

### 3.3 Consumer: retry và DLQ

- Retry in-process với exponential backoff (3 lần). Vẫn lỗi thì gửi message sang `<topic>.dlq` kèm header `x-error`, `x-original-topic` rồi commit offset.
- Phân loại lỗi: business error (ví dụ hết hàng) **không retry**, phát event failure. Lỗi tạm thời (Mongo/Kafka timeout) thì retry.
- Script `scripts/replay-dlq.js` để đẩy lại message từ DLQ.

### 3.4 Inventory: reserve an toàn

Trong transaction, với từng item:
`updateOne({_id: productId, 'stock.available': {$gte: qty}}, {$inc: {'stock.available': -qty, 'stock.reserved': qty}})`.
Nếu có item nào có `matchedCount === 0` thì abort transaction và phát `InventoryReservationFailed`. Tạo `reservations{orderId(unique), items, status}`.
Khi order `COMPLETED`, commit reservation (giảm `reserved`). Khi `OrderCancelled`, release stock.

### 3.5 Redis

- Lưu idempotency key (mục 3.2).
- Cache `GET /products/:id` theo cache-aside, TTL 60s, invalidate khi stock thay đổi.
- Rate limit `POST /orders` theo userId (sliding window, có thể dùng `rate-limiter-flexible`).

### 3.6 Mock providers

- `mockGateway.charge()`: env `PAYMENT_FAIL_RATE`, `PAYMENT_LATENCY_MS`. Thêm trigger xác định cho test: order có `metadata.simulate = 'PAYMENT_FAIL' | 'SHIPPING_FAIL' | 'PAYMENT_TIMEOUT'`, chỉ có hiệu lực khi `NODE_ENV !== 'production'`.
- `mockCarrier`: tạo trackingNo. `deliverySimulator` job sau X giây phát `ShipmentDelivered`.
- `mockMailer`: log ra console và lưu vào collection `notifications`.

### 3.7 Saga timeout

Job trong order-service chạy mỗi 30s: tìm order ở trạng thái chưa kết thúc mà `updatedAt < now - SAGA_TIMEOUT` (ví dụ 5 phút), chuyển `CANCELLED` với reason `TIMEOUT` và ghi `OrderCancelled` vào outbox. Đây là điểm yếu cố hữu của choreography, nên cần có lưới an toàn này.

---

## 4. Các phase triển khai

### Phase 0: Khởi tạo monorepo

- `git init`, npm workspaces, ESLint + Prettier, `.nvmrc`, `.editorconfig`, `.env.example`, Jest config dùng chung.
- **Xong khi:** `npm run lint` và `npm test` chạy được (chưa có test).

### Phase 1: Hạ tầng local (docker-compose)

- `mongo:7` chạy replica set `rs0` (dùng healthcheck để `rs.initiate`), `apache/kafka` chế độ KRaft (không cần Zookeeper), `redis:7`, `kafka-ui` (provectus) để xem topic/message, cùng job `create-topics`.
- **Xong khi:** `docker compose up -d` lên đủ service, Kafka UI hiện đủ topics, `rs.status()` OK.

### Phase 2: Package `@shopee/common`

- config, logger, mongo (`withTransaction`), redis, kafka producer/consumer wrapper, event envelope, outbox (model, repo, relay), idempotent handler, HTTP idempotency middleware, metrics, errors.
- Unit test cho envelope, state transition helper và idempotent handler (dùng mongodb-memory-server ở chế độ replica set).
- **Xong khi:** coverage của common ≥ 80%.

### Phase 3: order-service

- `POST /orders` (validate bằng zod, Idempotency-Key, lấy giá từ inventory qua HTTP hoặc snapshot giá trong request cho đơn giản), `GET /orders/:id`, `GET /orders?userId=`.
- Model `Order { orderId, userId, items[{productId, qty, price}], totalAmount, currency, status, statusHistory[], idempotencyKey, cancelReason, metadata, timestamps }`.
- Consumer cho mọi `*.events`, áp state machine, phát `OrderCancelled` / `OrderCompleted` qua outbox. Thêm timeout job.
- **Xong khi:** tạo order trả 201, event `OrderCreated` xuất hiện trên Kafka UI, gửi lại cùng key thì nhận cùng response.

### Phase 4: inventory-service

- CRUD products tối thiểu, `GET /products/:id` có cache Redis, `scripts/seed-products.js`.
- Handlers: `OrderCreated` → reserve. `OrderCancelled` → release (kèm tombstone). `OrderCompleted` → commit.
- **Xong khi:** stock giảm và reserved tăng đúng; đặt quá tồn kho thì nhận `InventoryReservationFailed` và order chuyển `CANCELLED`.

### Phase 5: payment-service

- Handlers: `InventoryReserved` → charge, phát `PaymentCompleted` hoặc `PaymentFailed`. `OrderCancelled` → refund nếu đã `COMPLETED`.
- Model `Payment { orderId(unique), amount, status: PENDING|COMPLETED|FAILED|REFUNDED, providerTxnId, attempts }`.
- **Xong khi:** đơn có `simulate=PAYMENT_FAIL` bị cancel và stock được trả về.

### Phase 6: shipping-service

- Handler `PaymentCompleted` → tạo shipment, phát `ShipmentCreated` hoặc `ShipmentFailed`. Delivery simulator phát `ShipmentDelivered`.
- **Xong khi:** đơn có `simulate=SHIPPING_FAIL` được refund, stock được release và order chuyển `CANCELLED`.

### Phase 7: notification-service

- Chỉ consume, không cần outbox. Gửi qua mockMailer, idempotent theo eventId, template theo eventType.
- `GET /notifications?orderId=` để kiểm tra.
- **Xong khi:** mỗi order có đúng chuỗi thông báo, không bị trùng khi replay event.

### Phase 8: Độ bền (resilience)

- Hoàn thiện retry, DLQ, script replay DLQ, graceful shutdown (dừng consumer, flush producer, đóng kết nối), healthcheck `/health/live` và `/health/ready` (kiểm tra Mongo, Redis, Kafka).
- Chaos test thủ công: kill một service giữa saga rồi bật lại, saga phải chạy tiếp đến trạng thái cuối.

### Phase 9: Observability

- `prom-client` trong mỗi service, expose `/metrics`:
  - `http_request_duration_seconds{method,route,status}`
  - `kafka_messages_consumed_total{topic,eventType,result=success|retry|dlq|duplicate}`
  - `outbox_pending_events` (gauge), `outbox_publish_latency_seconds`
  - `saga_orders_total{status}`, `saga_duration_seconds` (từ lúc tạo đến khi COMPLETED hoặc CANCELLED)
  - `payment_attempts_total{result}`, `inventory_reservation_total{result}`
- Exporters: `kafka-exporter` (consumer lag), `percona/mongodb_exporter`, `oliver006/redis_exporter`.
- Grafana provisioning: datasource Prometheus, cùng 2 dashboard **Saga Overview** (throughput, tỉ lệ cancel theo reason, p95 saga duration) và **Infra** (consumer lag, outbox backlog, HTTP p95, DLQ count).
- Alert rules Prometheus: outbox backlog > 100 kéo dài 2 phút, DLQ > 0, consumer lag cao.
- (Tùy chọn) Tracing bằng OpenTelemetry + Jaeger, truyền `traceparent` qua Kafka headers.

### Phase 10: Testing

- **Unit:** state machine, handlers (mock repo), middleware.
- **Integration** (testcontainers chạy Mongo rs, Kafka, Redis): outbox ghi rồi relay publish; consumer gặp duplicate eventId thì chỉ xử lý một lần.
- **E2E** (`tests/e2e`, chạy trên docker-compose): 4 kịch bản gồm happy path, hết hàng, payment fail, shipping fail. Thêm test gửi cùng Idempotency-Key 2 lần song song và test 50 order đồng thời cho cùng một sản phẩm stock=10 (đúng 10 đơn thành công, không oversell).
- (Tùy chọn) Load test bằng k6.

### Phase 11: Docker và CI/CD

- `Dockerfile` multi-stage cho mỗi service (dùng `node:22-alpine`, `npm ci --omit=dev`, chạy non-root user, `HEALTHCHECK`), kèm `.dockerignore`.
- `docker-compose.yml` có profile `infra` (chỉ hạ tầng, để dev chạy service bằng `npm run dev`) và profile `full` (chạy tất cả).
- **`.github/workflows/ci.yml`** (khi có PR hoặc push):
  1. Bật cache npm, chạy `npm ci`, lint, unit test (matrix theo workspace hoặc chạy chung).
  2. Integration test bằng testcontainers (runner ubuntu có sẵn Docker).
  3. E2E: `docker compose --profile full up -d --build`, chạy e2e, rồi `down`.
  4. Build image theo matrix service. Dùng path filter để chỉ build service có thay đổi.
- **`.github/workflows/cd.yml`** (khi push `main` hoặc tag): build và push image lên **GHCR** với tag `sha` và `latest`. (Tùy chọn) deploy lên VPS qua SSH bằng `docker compose pull && up -d`.
- Thêm Dependabot và `npm audit` trong CI.

---

## 5. Thứ tự ưu tiên

Phase 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 tạo ra luồng chạy end-to-end. Sau đó làm 8 → 9 → 10 → 11. Có thể đưa phần CI cơ bản (lint và unit test) lên làm ngay sau Phase 2.

## 6. Verification (end-to-end)

1. `docker compose --profile infra up -d`, sau đó `npm run seed` rồi `npm run dev` (chạy tất cả service qua `concurrently`).
2. Happy path:
   ```bash
   curl -X POST localhost:3001/orders -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
     -d '{"userId":"u1","items":[{"productId":"p1","qty":2}]}'
   ```
   Sau vài giây, `GET /orders/:id` trả về `COMPLETED`, `statusHistory` đủ các bước, notification có 4 bản ghi.
3. Gửi lại cùng `Idempotency-Key: k1` phải nhận cùng response và không tạo order mới.
4. Chạy lại với `metadata.simulate` = `PAYMENT_FAIL` hoặc `SHIPPING_FAIL`: order chuyển `CANCELLED`, stock quay về như cũ, payment `REFUNDED` (với case shipping fail).
5. Dùng Kafka UI xem chuỗi event theo key `orderId`. Tắt payment-service, tạo order rồi bật lại: order vẫn hoàn tất.
6. Grafana (`localhost:3000`) hiển thị số liệu trên dashboard Saga Overview, Prometheus targets đều `UP`.
7. `npm test` và `npm run test:e2e` đều pass. Workflow CI trên GitHub chạy xanh.
