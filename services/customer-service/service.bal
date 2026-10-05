import ballerina/http;
import ballerina/lang.value;
import ballerina/os;
import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

public type Address record {|
    string street;
    string city;
    string postalCode;
|};

public type Customer record {|
    readonly string id;
    string name;
    string email;
    string phone;
    Address deliveryAddress;
|};

public type CustomerRegistration record {|
    string name;
    string email;
    string phone;
    Address deliveryAddress;
|};

type PersistedState record {|
    string key;
    string payload;
|};

string dbHost = os:getEnv("DB_HOST");
string dbUser = os:getEnv("DB_USER");
string dbPassword = os:getEnv("DB_PASSWORD");
string dbName = os:getEnv("DB_NAME");
int dbPort = 5432;
postgresql:Client dbClient = checkpanic new (dbHost, dbUser, dbPassword, dbName, dbPort,
    connectionPool = {maxOpenConnections: 5});

// In-memory data store for local development
table<Customer> key(id) customerTable = table [];
int idCounter = 1;

function saveState(string key, string payload) returns error? {
    _ = check dbClient->execute(`INSERT INTO customer_service.state (key, payload)
        VALUES (${key}, ${payload}::jsonb)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload`);
}

function loadState() returns boolean|error {
    stream<PersistedState, sql:Error?> records =
        dbClient->query(`SELECT key, payload::text AS payload FROM customer_service.state`);
    check from PersistedState row in records
    do {
        json content = check value:fromJsonString(row.payload);
        Customer customer = check content.cloneWithType();
        customerTable.add(customer);
    };
    check records.close();
    idCounter = customerTable.toArray().length() + 1;
    return true;
}

boolean stateLoaded = checkpanic loadState();

service /customers on new http:Listener(9091) {

    // 1. Create new customer account
    resource function post register(CustomerRegistration req) returns Customer|http:BadRequest|error {
        if req.name == "" || req.email == "" {
            return <http:BadRequest>{body: "Name and email are required fields."};
        }

        string newId = "CUST-" + idCounter.toString();
        idCounter += 1;

        Customer newCustomer = {
            id: newId,
            name: req.name,
            email: req.email,
            phone: req.phone,
            deliveryAddress: req.deliveryAddress
        };

        check saveState("customer:" + newId, newCustomer.toString());
        customerTable.add(newCustomer);
        return newCustomer;
    }

    // 2. Fetch customer profile by ID
    resource function get [string id]() returns Customer|http:NotFound {
        Customer? customer = customerTable[id];
        if customer is () {
            return <http:NotFound>{body: "Customer not found."};
        }
        return customer;
    }

    // 3. Update delivery address
    resource function put [string id]/address(Address newAddress) returns Customer|http:NotFound|error {
        Customer? customer = customerTable[id];
        if customer is () {
            return <http:NotFound>{body: "Customer not found."};
        }

        Customer updatedCustomer = customer;
        updatedCustomer.deliveryAddress = newAddress;
        check saveState("customer:" + id, updatedCustomer.toString());
        customerTable.put(updatedCustomer);
        return updatedCustomer;
    }

    // 4. List all customers
    resource function get .() returns Customer[] {
        return customerTable.toArray();
    }
}