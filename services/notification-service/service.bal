import ballerina/http;

public type NotificationRequest record {|
    string recipientId;
    string recipientType; // CUSTOMER, RESTAURANT, DRIVER
    string message;
|};

public type NotificationLog record {|
    readonly string notificationId;
    string recipientId;
    string recipientType;
    string message;
    string sentAt;
|};

table<NotificationLog> key(notificationId) notificationTable = table [];
int notificationCounter = 1;

service /notifications on new http:Listener(9096) {

    // 1. Send multi-channel alert
    resource function post send(NotificationRequest req) returns NotificationLog|http:BadRequest {
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
            sentAt: "2026-10-03T03:30:00Z"
        };

        notificationTable.add(logEntry);
        return logEntry;
    }

    // 2. Fetch notification history for a recipient
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
