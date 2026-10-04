# Kafka Design - Distributed Food Delivery Platform

## Pattern
Event-driven **choreography**: services never call each other directly. The Order Service owns the order state machine and reacts to events published by the other services.

State machine: `CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED` (or `CANCELLED`)

## Topics

| Topic | Producer | Consumers |
|---|---|---|
| `orders.created` | Order | Restaurant, Payment, Notification |
| `orders.confirmed` | Restaurant | Order, Notification |
| `orders.rejected` | Restaurant | Order, Notification |
| `payments.completed` | Payment | Order, Notification |
| `payments.failed` | Payment | Order, Notification |
| `orders.preparing` | Restaurant | Order, Notification |
| `orders.ready` | Restaurant | Order, Delivery, Notification |
| `delivery.assigned` | Delivery | Order, Notification |
| `delivery.picked_up` | Delivery | Order, Notification |
| `delivery.completed` | Delivery | Order, Notification, Admin |
| `orders.cancelled` | Order | Restaurant, Payment, Delivery, Notification |
| `*.dlq` | any consumer | manual inspection |

Admin Service may additionally consume any topic for reporting.

## Partitioning
- **Key = `orderId`** on every order-related message. Same key -> same partition -> per-order ordering is guaranteed.
- **3 partitions per topic**: allows up to 3 parallel instances per consumer group.
- **Replication factor 1** locally (single broker). Production: 3, with `min.insync.replicas=2`.
- **One consumer group per service** (`customer-service`, `payment-service`, ...): every service sees every event; instances of one service share the load.

## Reliability
- Producer: `acks=all`, idempotence enabled.
- Consumer: at-least-once (commit offset after processing) -> handlers must be **idempotent** (track processed `eventId`, or check current order state before acting).
- Failures: bounded retries, then publish to the matching `*.dlq` topic.
- Compensation (saga): payment failure after restaurant confirmation -> `orders.cancelled` -> inventory released.

## Event envelope (agree on this as a group first)
```json
{
  "eventId": "uuid",
  "eventType": "orders.created",
  "orderId": "ord-1001",
  "timestamp": "2026-10-04T10:15:30Z",
  "payload": { }
}
```

## Running
```bash
docker compose up -d
docker compose logs kafka-init      # should list all topics
./scripts/verify-topics.sh          # shows partitions per topic
# Kafka UI: http://localhost:8080
```
Connection strings:
- Ballerina services in Docker: `kafka:9092`
- Anything on your laptop: `localhost:29092`
