import ballerina/http;

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

table<PaymentReceipt> key(transactionId) paymentTable = table [];
int paymentCounter = 100;

service /payments on new http:Listener(9094) {

    // 1. Process payment for an order
    resource function post process(PaymentRequest req) returns PaymentReceipt|http:BadRequest {
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