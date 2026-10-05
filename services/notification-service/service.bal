import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public type NotificationRequest record {|
    string recipientId;
    string recipientType;
    string message;
|};

public type NotificationLog record {|
    readonly string notificationId;
    string recipientId;
    string recipientType;
    string message;
    string sentAt;
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
    groupId: "notification-service",
    topics: ["orders.created", "orders.confirmed", "orders.preparing", "orders.ready",
        "orders.cancelled", "payments.completed", "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};
listener kafka:Listener notificationKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

table<NotificationLog> key(notificationId) notificationTable = table [];
table<ProcessedEvent> key(eventKey) processedEvents = table [];
int notificationCounter = 1;

type PersistedState record {|
    string key;
    string kind;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO notification_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, split_part(key, ':', 1) AS kind, payload::text AS payload
            FROM notification_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        if row.kind == "notification" {
            NotificationLog entry = check content.cloneWithType();
            notificationTable.add(entry);
        } else if row.kind == "processed_event" {
            ProcessedEvent event = check content.cloneWithType();
            processedEvents.add(event);
        }
    };
    check records.close();
    notificationCounter = notificationTable.toArray().length() + 1;
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /notifications on new http:Listener(9096) {
    resource function post send(NotificationRequest req) returns NotificationLog|http:BadRequest|error {
        if req.recipientId == "" || req.message == "" {
            return <http:BadRequest>{body: "Recipient ID and message cannot be empty."};
        }

        string notifId = "NOTIF-" + notificationCounter.toString();
        notificationCounter += 1;
        NotificationLog logEntry = {
            notificationId: notifId,
            recipientId: req.recipientId,
            recipientType: req.recipientType,
            message: req.message,
            sentAt: "2026-10-05T00:00:00Z"
        };
        check saveState("notification:" + notifId, logEntry.toString());
        notificationTable.add(logEntry);
        return logEntry;
    }

    resource function get recipient/[string recipientId]() returns NotificationLog[] {
        NotificationLog[] result = [];
        foreach var entry in notificationTable {
            if entry.recipientId == recipientId {
                result.push(entry);
            }
        }
        return result;
    }
}

service kafka:Service on notificationKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent orderEvent = check content.cloneReadOnly().ensureType();
            string eventKey = orderEvent.orderId + ":" + orderEvent.eventType;
            if processedEvents[eventKey] is () {
                string recipientId = orderEvent.customerId;
                string recipientType = "CUSTOMER";
                if orderEvent.eventType == "delivery.assigned" || orderEvent.eventType == "delivery.completed" {
                    recipientId = orderEvent.driverId;
                    recipientType = "DRIVER";
                } else if orderEvent.eventType == "orders.confirmed" ||
                        orderEvent.eventType == "orders.preparing" || orderEvent.eventType == "orders.ready" {
                    recipientId = orderEvent.restaurantId;
                    recipientType = "RESTAURANT";
                }
                NotificationLog logEntry = {
                    notificationId: "NOTIF-" + notificationCounter.toString(),
                    recipientId,
                    recipientType,
                    message: "Order " + orderEvent.orderId + " event: " + orderEvent.eventType,
                    sentAt: "2026-10-05T00:00:00Z"
                };
                notificationCounter += 1;
                check saveState("notification:" + logEntry.notificationId, logEntry.toString());
                ProcessedEvent processedEvent = {eventKey};
                check saveState("processed_event:" + eventKey, processedEvent.toString());
                notificationTable.add(logEntry);
                processedEvents.add({eventKey});
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}
