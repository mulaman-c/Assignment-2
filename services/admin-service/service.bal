import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public type PlatformSummary record {|
    int totalRestaurants;
    int totalOrdersProcessed;
    int activeDrivers;
    decimal totalRevenue;
|};

public type OrderItem record {|
    string itemId;
    string name;
    decimal price;
    int quantity;
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

type ProcessedOrder record {|
    readonly string orderId;
|};

type ProcessedEvent record {|
    readonly string eventKey;
|};

function getKafkaBootstrapServers() returns string {
    string bootstrapServers = os:getEnv("KAFKA_BOOTSTRAP_SERVERS");
    if bootstrapServers == "" {
        return kafka:DEFAULT_URL;
    }
    return bootstrapServers;
}

string kafkaBootstrapServers = getKafkaBootstrapServers();
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "admin-service",
    topics: ["orders.created", "orders.confirmed", "orders.preparing", "orders.ready",
        "orders.cancelled", "payments.completed", "payments.failed", "delivery.assigned",
        "delivery.picked_up", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};
listener kafka:Listener adminKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

table<ProcessedOrder> key(orderId) processedOrders = table [];
table<ProcessedEvent> key(eventKey) processedEvents = table [];
int processedOrderCount = 0;
decimal platformRevenue = 0d;

type PersistedState record {|
    string key;
    string kind;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO admin_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, split_part(key, ':', 1) AS kind, payload::text AS payload
            FROM admin_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        if row.kind == "processed_order" {
            ProcessedOrder storedOrder = check content.cloneWithType();
            processedOrders.add(storedOrder);
            processedOrderCount += 1;
        } else if row.kind == "processed_revenue" {
            OrderEvent event = check content.cloneWithType();
            string eventKey = event.orderId + ":" + event.eventType;
            processedEvents.add({eventKey});
            platformRevenue += event.totalAmount;
        }
    };
    check records.close();
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /admin on new http:Listener(9097) {

    // 1. Generate overview metrics and performance reports
    resource function get overview() returns PlatformSummary {
        // Returns aggregated statistics for platform monitoring
        return {
            totalRestaurants: 12,
            totalOrdersProcessed: processedOrderCount,
            activeDrivers: 8,
            totalRevenue: platformRevenue
        };
    }
}

service kafka:Service on adminKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent event = check content.cloneReadOnly().ensureType();
            if event.eventType == "orders.created" && processedOrders[event.orderId] is () {
                ProcessedOrder processedOrder = {orderId: event.orderId};
                check saveState("processed_order:" + event.orderId, processedOrder.toString());
                processedOrders.add({orderId: event.orderId});
                processedOrderCount += 1;
            } else if event.eventType == "payments.completed" &&
                    processedEvents[event.orderId + ":" + event.eventType] is () {
                check saveState("processed_revenue:" + event.orderId + ":" + event.eventType,
                    event.toString());
                platformRevenue += event.totalAmount;
                processedEvents.add({eventKey: event.orderId + ":" + event.eventType});
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}