import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public type OrderItem record {|
    string itemId;
    string name;
    decimal price;
    int quantity;
|};

public type OrderEvent record {|
    string eventType;
    readonly string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    string deliveryId;
    string driverId;
|};

public type StatusUpdateRequest record {|
    string newStatus;
|};

function getKafkaBootstrapServers() returns string {
    string bootstrapServers = os:getEnv("KAFKA_BOOTSTRAP_SERVERS");
    if bootstrapServers == "" {
        return kafka:DEFAULT_URL;
    }
    return bootstrapServers;
}

string kafkaBootstrapServers = getKafkaBootstrapServers();
kafka:Producer restaurantProducer = checkpanic new (kafkaBootstrapServers);

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "restaurant-service",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};
listener kafka:Listener restaurantKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

table<OrderEvent> key(orderId) restaurantOrders = table [];

type PersistedState record {|
    string key;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO restaurant_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, payload::text AS payload FROM restaurant_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        OrderEvent orderEvent = check content.cloneWithType();
        restaurantOrders.add(orderEvent);
    };
    check records.close();
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /restaurant on new http:Listener(9098) {
    resource function get orders() returns OrderEvent[] {
        return restaurantOrders.toArray();
    }

    resource function get orders/[string orderId]() returns OrderEvent|http:NotFound {
        OrderEvent? restaurantOrder = restaurantOrders[orderId];
        if restaurantOrder is () {
            return <http:NotFound>{body: "Restaurant order not found."};
        }
        return restaurantOrder;
    }

    resource function put orders/[string orderId]/status(StatusUpdateRequest req)
            returns OrderEvent|http:BadRequest|http:NotFound|error {
        OrderEvent? current = restaurantOrders[orderId];
        if current is () {
            return <http:NotFound>{body: "Restaurant order not found."};
        }
        if (current.status == "CONFIRMED" && req.newStatus != "PREPARING") ||
                (current.status == "PREPARING" && req.newStatus != "READY") {
            return <http:BadRequest>{body: "Allowed transitions are CONFIRMED to PREPARING to READY."};
        }

        OrderEvent updated = current;
        updated.status = req.newStatus;
        if req.newStatus == "PREPARING" {
            updated.eventType = "orders.preparing";
        } else if req.newStatus == "READY" {
            updated.eventType = "orders.ready";
        } else {
            return <http:BadRequest>{body: "Status must be PREPARING or READY."};
        }
        _ = check restaurantProducer->send({
            topic: updated.eventType,
            key: updated.orderId.toBytes(),
            value: updated.toString().toBytes()
        });
        check saveState("restaurant_order:" + orderId, updated.toString());
        restaurantOrders.put(updated);
        return updated;
    }
}

service kafka:Service on restaurantKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent incoming = check content.cloneWithType();
            if restaurantOrders[incoming.orderId] is () {
                incoming.eventType = "orders.confirmed";
                incoming.status = "CONFIRMED";
                check restaurantProducer->send({
                    topic: "orders.confirmed",
                    key: incoming.orderId.toBytes(),
                    value: incoming.toString().toBytes()
                });
                check saveState("restaurant_order:" + incoming.orderId, incoming.toString());
                restaurantOrders.add(incoming);
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}
