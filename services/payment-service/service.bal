import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public enum PaymentStatus {
    PENDING,
    SUCCESS,
    FAILED
}

public type PaymentRequest record {|
    string orderId;
    decimal amount;
    string paymentMethod;
|};

public type PaymentReceipt record {|
    readonly string transactionId;
    string orderId;
    decimal amount;
    PaymentStatus status;
|};

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

function getKafkaBootstrapServers() returns string {
    string bootstrapServers = os:getEnv("KAFKA_BOOTSTRAP_SERVERS");
    if bootstrapServers == "" {
        return kafka:DEFAULT_URL;
    }
    return bootstrapServers;
}

string kafkaBootstrapServers = getKafkaBootstrapServers();
kafka:Producer paymentProducer = checkpanic new (kafkaBootstrapServers);
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "payment-service",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};
listener kafka:Listener paymentKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

table<PaymentReceipt> key(transactionId) paymentTable = table [];
table<OrderEvent> key(orderId) processedOrders = table [];
int paymentCounter = 100;

type PersistedState record {|
    string key;
    string kind;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO payment_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, split_part(key, ':', 1) AS kind, payload::text AS payload
            FROM payment_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        if row.kind == "receipt" {
            PaymentReceipt receipt = check content.cloneWithType();
            paymentTable.add(receipt);
        } else if row.kind == "processed_order" {
            OrderEvent orderEvent = check content.cloneWithType();
            processedOrders.add(orderEvent);
        }
    };
    check records.close();
    paymentCounter = paymentTable.toArray().length() + 100;
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /payments on new http:Listener(9094) {

    // 1. Process payment for an order
    resource function post process(PaymentRequest req) returns PaymentReceipt|http:BadRequest|error {
        // Compare decimal with 0d or 0 (not 0.0 float)
        if req.amount <= 0d {
            return <http:BadRequest>{body: "Payment amount must be greater than zero."};
        }

        string txnId = "TXN-" + paymentCounter.toString();
        paymentCounter += 1;

        // Simulate successful payment transaction
        PaymentReceipt receipt = {
            transactionId: txnId,
            orderId: req.orderId,
            amount: req.amount,
            status: SUCCESS
        };

        check saveState("receipt:" + txnId, receipt.toString());
        paymentTable.add(receipt);
        return receipt;
    }

    // 2. Fetch transaction details by transaction ID
    resource function get [string transactionId]() returns PaymentReceipt|http:NotFound {
        PaymentReceipt? receipt = paymentTable[transactionId];
        if receipt is () {
            return <http:NotFound>{body: "Payment record not found."};
        }
        return receipt;
    }
}

service kafka:Service on paymentKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent orderEvent = check content.cloneWithType();
            if processedOrders[orderEvent.orderId] is () {
                PaymentReceipt receipt = {
                    transactionId: "TXN-" + paymentCounter.toString(),
                    orderId: orderEvent.orderId,
                    amount: orderEvent.totalAmount,
                    status: SUCCESS
                };
                orderEvent.eventType = "payments.completed";
                orderEvent.status = "SUCCESS";
                check paymentProducer->send({
                    topic: "payments.completed",
                    key: orderEvent.orderId.toBytes(),
                    value: orderEvent.toString().toBytes()
                });
                check saveState("receipt:" + receipt.transactionId, receipt.toString());
                check saveState("processed_order:" + orderEvent.orderId, orderEvent.toString());
                paymentCounter += 1;
                paymentTable.add(receipt);
                processedOrders.add(orderEvent);
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}