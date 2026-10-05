# Kafka Design - Distributed Food Delivery Platform

## Pattern
Event-driven **choreography**: services publish and consume Kafka events rather than making synchronous calls to each other. The Order Service owns the order state machine and updates its view as lifecycle events arrive.

State machine: `CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED` (or `CANCELLED`)

## Topics

| Topic | Producer | Consumers |
|---|---|---|
| `orders.created` | Order | Restaurant, Payment, Notification, Admin |
| `orders.confirmed` | Restaurant | Order, Notification, Admin |
| `orders.preparing` | Restaurant | Order, Notification, Admin |
| `orders.ready` | Restaurant | Order, Delivery, Notification, Admin |
| `orders.cancelled` | Order | Order, Restaurant, Payment, Delivery, Notification, Admin |
| `payments.completed` | Payment | Order, Notification, Admin |
| `delivery.assigned` | Delivery | Order, Notification, Admin |
| `delivery.completed` | Delivery | Order, Notification, Admin |
| `orders.rejected`, `payments.failed`, `delivery.picked_up` | reserved | reserved |
| `*.dlq` | reserved | manual inspection; dead-letter publishing is not implemented |

Admin Service may additionally consume any topic for reporting.

## Partitioning
- **Key = `orderId`** on every order-related message. Same key -> same partition -> per-order ordering is guaranteed.
- **3 partitions per topic**: allows up to 3 parallel instances per consumer group.
- **Replication factor 1** locally (single broker). Production: 3, with `min.insync.replicas=2`.
- **One consumer group per service** (`customer-service`, `payment-service`, ...): every service sees every event; instances of one service share the load.

## Reliability
- Producer: `acks=all`; records are keyed by `orderId` to preserve per-order ordering within a partition.
- Consumers use separate groups and commit offsets after processing each batch. Handlers keep local duplicate checks for the events they apply.
- Customer, order, restaurant, payment, delivery, notification, and admin state is persisted in PostgreSQL. Each service writes to its own schema; the local setup stores service records as keyed JSONB payloads.
- Dead-letter topics exist for development exploration, but automatic retries, dead-letter publishing, a transactional outbox, and payment compensation are not implemented yet.

## Event payload
Services currently exchange this common order payload as JSON. `eventType` identifies the event and `orderId` is also used as the Kafka record key:
```json
{
  "eventType": "orders.created",
  "orderId": "ORD-1",
  "customerId": "CUST-1",
  "restaurantId": "REST-1",
  "items": [],
  "totalAmount": 25.00,
  "status": "CREATED",
  "deliveryId": "",
  "driverId": ""
}
```
This is a development payload; persistent event IDs, timestamps, and formal event schemas are still needed for production-grade recovery.

## Running and testing (Windows)
```powershell
docker compose up --build -d
docker compose ps
docker compose logs kafka-init
```

PostgreSQL is exposed on `localhost:5432`; services connect to it internally as `database:5432`. The local-only default password is `food_delivery_dev`. Set `DB_PASSWORD` before starting the stack to override it. The database is initialized from `database/init.sql` when its data volume is first created.

Run the end-to-end test from PowerShell:
```powershell
.\scripts\test-system.ps1
```

It creates a customer and order, waits for restaurant and payment consumers, moves the restaurant order through preparation and ready, assigns a driver, marks the delivery complete, and checks the order, notification, and admin services. Expected order progression: `CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED`.

To inspect Kafka topics and messages:
```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic orders.created --from-beginning
```

To inspect persisted state:
```powershell
docker compose exec database psql -U food_delivery -d food_delivery
```
For example, inside `psql`:
```sql
SELECT key, payload FROM order_service.state;
SELECT key, payload FROM customer_service.state;
```

`docker compose down` stops the stack but preserves the PostgreSQL named volume. **Do not use `docker compose down -v` unless you intend to permanently delete all stored data.** Kafka is still a single broker without a persistent volume, and this configuration uses one shared database user; it is for development/testing, not production.

HTTP endpoints use `localhost` and their mapped service ports: Customer `9091`, Order `9093`, Restaurant `9098`, Payment `9094`, Delivery `9095`, Notification `9096`, Admin `9097`.

Connection strings:
- Ballerina services in Docker: `kafka:9092`
- Anything on your laptop: `localhost:29092`
