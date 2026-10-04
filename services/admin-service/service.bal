import ballerina/http;

public type PlatformSummary record {|
    int totalRestaurants;
    int totalOrdersProcessed;
    int activeDrivers;
    decimal totalRevenue;
|};

service /admin on new http:Listener(9097) {

    resource function get overview() returns PlatformSummary {
        
        return {
            totalRestaurants: 12,
            totalOrdersProcessed: 145,
            activeDrivers: 8,
            totalRevenue: 3450.50d
        };
    }
}
