# Goodfood local platform

Goodfood is a local food-delivery demo with Ballerina services, PostgreSQL, Kafka, and a browser-based operations dashboard.

## Run it

From the repository root:

```powershell
docker compose up --build -d
docker compose ps
```

Open [http://localhost:8080](http://localhost:8080). The dashboard can register a customer, place an order, move it through restaurant preparation, assign a driver, complete the delivery, and view order notifications and admin metrics. It refreshes service state every five seconds.

The frontend and API share one origin. Nginx serves the static dashboard and proxies `/api/...` requests to their corresponding services, so browser CORS configuration is not needed.

Run the end-to-end service check from PowerShell:

```powershell
.\scripts\test-system.ps1
```

## Kafka flow

Kafka topics are provisioned by `kafka-init` before the event-consuming services start. The services use `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`; clients outside Docker use `localhost:29092`.

The order service publishes `orders.created`. Restaurant and payment services consume it and publish their outcomes; restaurant status changes publish `orders.preparing` and `orders.ready`; delivery consumes ready orders and publishes assignment/completion events. Order, notification, and admin services consume the relevant events in their own consumer groups. Messages are keyed by `orderId`, topics have three partitions locally, and producers request `acks=all`.

Kafka broker data and PostgreSQL state use named Docker volumes. `docker compose down` preserves both; `docker compose down -v` permanently removes them.

This is a local demonstration, not an exactly-once or production deployment. It uses a single Kafka broker with replication factor 1, and does not yet have a transactional outbox, automated retries/dead-letter publishing, or payment compensation. Consumers manually commit after database processing and use persisted duplicate guards, which provides useful retry behavior but does not make a database write and Kafka publish atomic.

## Useful commands

```powershell
docker compose logs -f order-service restaurant-service payment-service delivery-service
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic orders.created --from-beginning
```

PostgreSQL is exposed on `localhost:5432`. The development password defaults to `food_delivery_dev`; set `DB_PASSWORD` to override it before starting the stack.
