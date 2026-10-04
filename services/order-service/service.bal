import ballerina/http;
import ballerinax/kafka;

// Order Status Enum State Machine
public enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED
}

public type OrderItem record {|
    string itemId;
    string name;
    decimal price;
    int quantity;
|};

public type Order record {|
    readonly string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    OrderStatus status;
|};

public type CreateOrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
|};

public type StatusUpdateRequest record {|
    OrderStatus newStatus;
|};

// Top-level Kafka Producer initialization
kafka:ProducerConfiguration producerConfigs = {
    clientId: "order-service-producer",
    acks: "all"
};

kafka:Producer orderKafkaProducer = checkpanic new (kafka:DEFAULT_URL, producerConfigs);

// In-memory table for active order tracking
table<Order> key(orderId) orderTable = table [];
int orderCounter = 1;

service /orders on new http:Listener(9093) {

    // 1. Create a new order (Initial state: CREATED)
    resource function post . (CreateOrderRequest req) returns Order|http:BadRequest|error {
        if req.items.length() == 0 {
            return <http:BadRequest>{body: "Order must contain at least one item."};
        }

        decimal total = 0;
        foreach var item in req.items {
            total += item.price * <decimal>item.quantity;
        }

        string id = "ORD-" + orderCounter.toString();
        orderCounter += 1;

        Order newOrder = {
            orderId: id,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            totalAmount: total,
            status: CREATED
        };

        orderTable.add(newOrder);

        // Publish event to Kafka orders.created topic (INSIDE the resource function)
        _ = check orderKafkaProducer->send({
            topic: "orders.created",
            value: newOrder.toString().toBytes()
        });

        return newOrder;
    }

    // 2. Get details of an order
    resource function get [string orderId]() returns Order|http:NotFound {
        Order? ord = orderTable[orderId];
        if ord is () {
            return <http:NotFound>{body: "Order not found."};
        }
        return ord;
    }

    // 3. Update order state machine status
    resource function put [string orderId]/status(StatusUpdateRequest req) returns Order|http:BadRequest|http:NotFound {
        Order? ord = orderTable[orderId];
        if ord is () {
            return <http:NotFound>{body: "Order not found."};
        }

        Order updatedOrder = ord;
        
        if updatedOrder.status == DELIVERED || updatedOrder.status == CANCELLED {
            return <http:BadRequest>{body: "Cannot change status of a completed or cancelled order."};
        }

        updatedOrder.status = req.newStatus;
        orderTable.put(updatedOrder);
        return updatedOrder;
    }

    // 4. List all orders
    resource function get . () returns Order[] {
        return orderTable.toArray();
    }
}