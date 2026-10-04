import ballerina/http;

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

// In-memory data store for local development
table<Customer> key(id) customerTable = table [];
int idCounter = 1;

service /customers on new http:Listener(9091) {

    // 1. Create new customer account
    resource function post register(CustomerRegistration req) returns Customer|http:BadRequest {
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
    resource function put [string id]/address(Address newAddress) returns Customer|http:NotFound {
        Customer? customer = customerTable[id];
        if customer is () {
            return <http:NotFound>{body: "Customer not found."};
        }

        Customer updatedCustomer = customer;
        updatedCustomer.deliveryAddress = newAddress;
        customerTable.put(updatedCustomer);
        return updatedCustomer;
    }

    // 4. List all customers
    resource function get .() returns Customer[] {
        return customerTable.toArray();
    }
}