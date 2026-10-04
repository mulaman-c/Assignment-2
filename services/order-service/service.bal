import ballerina/http;
import ballerinax/kafka;

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


kafka:ProducerConfiguration producerConfigs = {
    clientId: "order-service-producer",
    acks: "all"
};

kafka:Producer orderKafkaProducer = checkpanic new (kafka:DEFAULT_URL, producerConfigs);


table<Order> key(orderId) orderTable = table [];
int orderCounter = 1;

service /orders on new http:Listener(9093) {

  
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

 
        _ = check orderKafkaProducer->send({
            topic: "orders.created",
            value: newOrder.toString().toBytes()
        });

        return newOrder;
    }

    
    resource function get [string orderId]() returns Order|http:NotFound {
        Order? ord = orderTable[orderId];
        if ord is () {
            return <http:NotFound>{body: "Order not found."};
        }
        return ord;
    }

    
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

  
    resource function get . () returns Order[] {
        return orderTable.toArray();
    }
}
