import ballerina/http;

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

table<Driver> key(driverId) driverTable = table [];
table<DeliveryAssignment> key(deliveryId) deliveryTable = table [];
int deliveryCounter = 500;

service /delivery on new http:Listener(9095) {

    // 1. Register a delivery driver
    resource function post drivers(Driver driver) returns Driver {
        driverTable.add(driver);
        return driver;
    }

    // 2. Assign driver to an order
    resource function post assign(AssignDriverRequest req) returns DeliveryAssignment|http:NotFound|http:BadRequest {
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

        deliveryTable.add(assignment);

        // Update driver availability status
        Driver updatedDriver = driver;
        updatedDriver.status = BUSY;
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
}