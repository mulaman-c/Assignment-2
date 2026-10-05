import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public enum DriverStatus {
    AVAILABLE,
    BUSY,
    OFFLINE
}

public type Driver record {|
    readonly string driverId;
    string name;
    string phone;
    DriverStatus status;
|};

public type DeliveryAssignment record {|
    readonly string deliveryId;
    string orderId;
    string driverId;
    string status;
|};

public type AssignDriverRequest record {|
    string orderId;
    string driverId;
|};

public type DeliveryStatusRequest record {|
    string status;
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
kafka:Producer deliveryProducer = checkpanic new (kafkaBootstrapServers);
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "delivery-service",
    topics: ["orders.ready"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
};
listener kafka:Listener deliveryKafkaListener = new (kafkaBootstrapServers, consumerConfigs);

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, 5432,
    connectionPool = {maxOpenConnections: 5});

table<Driver> key(driverId) driverTable = table [];
table<DeliveryAssignment> key(deliveryId) deliveryTable = table [];
table<OrderEvent> key(orderId) readyOrders = table [];
int deliveryCounter = 500;

type PersistedState record {|
    string key;
    string kind;
    string payload;
|};

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO delivery_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, split_part(key, ':', 1) AS kind, payload::text AS payload
            FROM delivery_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        if row.kind == "driver" {
            Driver driver = check content.cloneWithType();
            driverTable.add(driver);
        } else if row.kind == "delivery" {
            DeliveryAssignment assignment = check content.cloneWithType();
            deliveryTable.add(assignment);
        } else if row.kind == "ready_order" {
            OrderEvent orderEvent = check content.cloneWithType();
            readyOrders.add(orderEvent);
        }
    };
    check records.close();
    deliveryCounter = deliveryTable.toArray().length() + 500;
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /delivery on new http:Listener(9095) {

    resource function get readyOrders() returns OrderEvent[] {
        return readyOrders.toArray();
    }

    // 1. Register a delivery driver
    resource function post drivers(Driver driver) returns Driver|error {
        check saveState("driver:" + driver.driverId, driver.toString());
        driverTable.add(driver);
        return driver;
    }

    // 2. Assign driver to an order
    resource function post assign(AssignDriverRequest req)
            returns DeliveryAssignment|http:NotFound|http:BadRequest|error {
        OrderEvent? readyOrder = readyOrders[req.orderId];
        if readyOrder is () {
            return <http:BadRequest>{body: "Order is not ready for delivery."};
        }

        Driver? driver = driverTable[req.driverId];
        if driver is () {
            return <http:NotFound>{body: "Driver not found."};
        }

        if driver.status != AVAILABLE {
            return <http:BadRequest>{body: "Driver is not available for assignment."};
        }

        string delId = "DEL-" + deliveryCounter.toString();
        deliveryCounter += 1;

        DeliveryAssignment assignment = {
            deliveryId: delId,
            orderId: req.orderId,
            driverId: req.driverId,
            status: "ASSIGNED"
        };

        readyOrder.eventType = "delivery.assigned";
        readyOrder.status = "OUT_FOR_DELIVERY";
        readyOrder.deliveryId = delId;
        readyOrder.driverId = driver.driverId;
        check saveState("ready_order:" + readyOrder.orderId, readyOrder.toString());
        _ = check deliveryProducer->send({
            topic: "delivery.assigned",
            key: readyOrder.orderId.toBytes(),
            value: readyOrder.toString().toBytes()
        });
        Driver updatedDriver = driver;
        updatedDriver.status = BUSY;
        check saveState("delivery:" + delId, assignment.toString());
        check saveState("driver:" + driver.driverId, updatedDriver.toString());
        deliveryTable.add(assignment);
        driverTable.put(updatedDriver);
        return assignment;
    }

    // 3. Get delivery status by delivery ID
    resource function get [string deliveryId]() returns DeliveryAssignment|http:NotFound {
        DeliveryAssignment? assignment = deliveryTable[deliveryId];
        if assignment is () {
            return <http:NotFound>{body: "Delivery record not found."};
        }
        return assignment;
    }

    resource function put [string deliveryId]/status(DeliveryStatusRequest req)
            returns DeliveryAssignment|http:NotFound|http:BadRequest|error {
        DeliveryAssignment? current = deliveryTable[deliveryId];
        if current is () {
            return <http:NotFound>{body: "Delivery record not found."};
        }
        if req.status != "DELIVERED" || current.status != "ASSIGNED" {
            return <http:BadRequest>{body: "Only an assigned delivery can be marked DELIVERED."};
        }

        OrderEvent? readyOrder = readyOrders[current.orderId];
        if readyOrder is () {
            return <http:NotFound>{body: "Order event not found; cannot publish delivery completion."};
        }
        readyOrder.eventType = "delivery.completed";
        readyOrder.status = "DELIVERED";
        readyOrder.deliveryId = current.deliveryId;
        readyOrder.driverId = current.driverId;
        check saveState("ready_order:" + readyOrder.orderId, readyOrder.toString());
        _ = check deliveryProducer->send({
            topic: "delivery.completed",
            key: readyOrder.orderId.toBytes(),
            value: readyOrder.toString().toBytes()
        });

        DeliveryAssignment updated = current;
        updated.status = "DELIVERED";
        check saveState("delivery:" + deliveryId, updated.toString());
        deliveryTable.put(updated);
        Driver? currentDriver = driverTable[current.driverId];
        if currentDriver is Driver {
            Driver availableDriver = currentDriver;
            availableDriver.status = AVAILABLE;
            check saveState("driver:" + currentDriver.driverId, availableDriver.toString());
            driverTable.put(availableDriver);
        }
        return updated;
    }
}

service kafka:Service on deliveryKafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord 'record in records {
            string message = check string:fromBytes('record.value);
            json content = check value:fromJsonString(message);
            OrderEvent orderEvent = check content.cloneWithType();
            if readyOrders[orderEvent.orderId] is () {
                check saveState("ready_order:" + orderEvent.orderId, orderEvent.toString());
                readyOrders.add(orderEvent);
            }
        }
        kafka:Error? commitResult = caller->commit();
        if commitResult is error {
            return commitResult;
        }
    }
}