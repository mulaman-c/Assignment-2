import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

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

public type OrderEvent record {|
    string eventType;
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    string deliveryId;
    string driverId;
|};

function getKafkaBootstrapServers() returns string {
    string bootstrapServers = os:getEnv("KAFKA_BOOTSTRAP_SERVERS");
    if bootstrapServers == "" {
        return kafka:DEFAULT_URL;
    }
    return bootstrapServers;
}

function getStatusRank(OrderStatus status) returns int {
    if status == CREATED {
        return 0;
    } else if status == CONFIRMED {
        return 1;
    } else if status == PREPARING {
        return 2;
    } else if status == READY {
        return 3;
    } else if status == OUT_FOR_DELIVERY {
        return 4;
    } else if status == DELIVERED {
        return 5;
    }
    return 6;
}

string kafkaBootstrapServers = getKafkaBootstrapServers();

// Top-level Kafka Producer initialization
kafka:ProducerConfiguration producerConfigs = {
    clientId: "order-service-producer",
    acks: "all"
};

kafka:Producer orderKafkaProducer = checkpanic new (kafkaBootstrapServers, producerConfigs);

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "order-service",
    topics: ["orders.confirmed", "orders.preparing", "orders.ready", "orders.cancelled",
        "payments.completed", "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};

listener kafka:Listener orderKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

// In-memory table for active order tracking
table<Order> key(orderId) orderTable = table [];
int orderCounter = 1;

type PersistedState record {|
    string key;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO order_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, payload::text AS payload FROM order_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        Order storedOrder = check content.cloneWithType();
        orderTable.add(storedOrder);
    };
    check records.close();
    orderCounter = orderTable.toArray().length() + 1;
    return true;
}

boolean stateLoaded = checkpanic loadState();

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

        check saveState("order:" + id, newOrder.toString());
        orderTable.add(newOrder);

        OrderEvent createdEvent = {
            eventType: "orders.created",
            orderId: newOrder.orderId,
            customerId: newOrder.customerId,
            restaurantId: newOrder.restaurantId,
            items: newOrder.items,
            totalAmount: newOrder.totalAmount,
            status: newOrder.status.toString(),
            deliveryId: "",
            driverId: ""
        };

        _ = check orderKafkaProducer->send({
            topic: "orders.created",
            key: newOrder.orderId.toBytes(),
            value: createdEvent.toString().toBytes()
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
    resource function put [string orderId]/status(StatusUpdateRequest req) returns Order|http:BadRequest|http:NotFound|error {
        Order? ord = orderTable[orderId];
        if ord is () {
            return <http:NotFound>{body: "Order not found."};
        }

        Order updatedOrder = ord;
        
        if updatedOrder.status == DELIVERED || updatedOrder.status == CANCELLED {
            return <http:BadRequest>{body: "Cannot change status of a completed or cancelled order."};
        }
        if req.newStatus != CANCELLED &&
                getStatusRank(req.newStatus) != getStatusRank(updatedOrder.status) + 1 {
            return <http:BadRequest>{body: "Order status must advance one step at a time."};
        }
        updatedOrder.status = req.newStatus;
        updatedOrder.status = req.newStatus;

        string eventType = "orders.created";
        if req.newStatus == CONFIRMED {
            eventType = "orders.confirmed";
        } else if req.newStatus == PREPARING {
            eventType = "orders.preparing";
        } else if req.newStatus == READY {
            eventType = "orders.ready";
        } else if req.newStatus == OUT_FOR_DELIVERY {
            eventType = "delivery.assigned";
        } else if req.newStatus == DELIVERED {
            eventType = "delivery.completed";
        } else if req.newStatus == CANCELLED {
            eventType = "orders.cancelled";
        }
        OrderEvent statusEvent = {
            eventType,
            orderId: updatedOrder.orderId,
            customerId: updatedOrder.customerId,
            restaurantId: updatedOrder.restaurantId,
            items: updatedOrder.items,
            totalAmount: updatedOrder.totalAmount,
            status: updatedOrder.status.toString(),
            deliveryId: "",
            driverId: ""
        };
        _ = check orderKafkaProducer->send({
            topic: eventType,
            key: updatedOrder.orderId.toBytes(),
            value: statusEvent.toString().toBytes()
        });
        check saveState("order:" + orderId, updatedOrder.toString());
        orderTable.put(updatedOrder);
        return updatedOrder;
    }

    // 4. List all orders
    resource function get . () returns Order[] {
        return orderTable.toArray();
    }
}

service kafka:Service on orderKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent event = check content.cloneReadOnly().ensureType();

            Order? currentOrder = orderTable[event.orderId];
            if currentOrder is () {
                continue;
            }

            Order updatedOrder = currentOrder;
            OrderStatus? nextStatus = ();
            if event.eventType == "orders.confirmed" {
                nextStatus = CONFIRMED;
            } else if event.eventType == "orders.preparing" {
                nextStatus = PREPARING;
            } else if event.eventType == "orders.ready" {
                nextStatus = READY;
            } else if event.eventType == "delivery.assigned" {
                nextStatus = OUT_FOR_DELIVERY;
            } else if event.eventType == "delivery.completed" {
                nextStatus = DELIVERED;
            } else if event.eventType == "orders.cancelled" {
                nextStatus = CANCELLED;
            }
            if nextStatus is OrderStatus && currentOrder.status != DELIVERED &&
                    currentOrder.status != CANCELLED &&
                    getStatusRank(nextStatus) > getStatusRank(currentOrder.status) {
                updatedOrder.status = nextStatus;
                check saveState("order:" + event.orderId, updatedOrder.toString());
                orderTable.put(updatedOrder);
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}