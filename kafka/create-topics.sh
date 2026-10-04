#!/bin/bash

# Wait for Kafka to be ready
echo "Creating Kafka topics for Distributed Food Delivery Platform..."

# Topic 1: Emitted when customer places an order
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic orders.created

# Topic 2: Emitted when payment service processes payment
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic payments.completed

# Topic 3: Emitted when restaurant updates kitchen order status
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic restaurant.order-status

# Topic 4: Emitted when driver is assigned to a delivery
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic delivery.assigned

# Topic 5: Emitted when order delivery is finalized
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic delivery.completed

echo "All Kafka topics created successfully!"
