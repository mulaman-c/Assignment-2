$ErrorActionPreference = "Stop"
$base = "http://localhost"

function Send-Json($method, $uri, $body) {
    $json = ConvertTo-Json -InputObject $body -Depth 10
    return Invoke-RestMethod -Method $method -Uri $uri -ContentType "application/json" -Body $json
}

function Wait-ForOrderStatus($orderId, $expectedStatus) {
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $order = Invoke-RestMethod -Uri "$base`:9093/orders/$orderId"
        if ($order.status -eq $expectedStatus) {
            return $order
        }
        Start-Sleep -Seconds 1
    }
    throw "Timed out waiting for order $orderId to reach $expectedStatus."
}

$customer = Send-Json "Post" "$base`:9091/customers/register" @{
    name = "Kafka Test Customer"
    email = "kafka-test@example.test"
    phone = "555-0100"
    deliveryAddress = @{ street = "1 Test Street"; city = "Testville"; postalCode = "00000" }
}

$order = Send-Json "Post" "$base`:9093/orders" @{
    customerId = $customer.id
    restaurantId = "REST-1"
    items = @(@{ itemId = "ITEM-1"; name = "Test meal"; price = 12.50; quantity = 2 })
}
Write-Host "Created $($order.orderId); waiting for restaurant/payment events..."
$null = Wait-ForOrderStatus $order.orderId "CONFIRMED"

$driverId = "DRIVER-TEST-" + (Get-Date -Format "yyyyMMddHHmmssfff")
$null = Send-Json "Post" "$base`:9095/delivery/drivers" @{
    driverId = $driverId
    name = "Kafka Test Driver"
    phone = "555-0101"
    status = "AVAILABLE"
}

$null = Send-Json "Put" "$base`:9098/restaurant/orders/$($order.orderId)/status" @{
    newStatus = "PREPARING"
}
$null = Send-Json "Put" "$base`:9098/restaurant/orders/$($order.orderId)/status" @{
    newStatus = "READY"
}
$null = Wait-ForOrderStatus $order.orderId "READY"

$ready = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $readyOrders = Invoke-RestMethod -Uri "$base`:9095/delivery/readyOrders"
    if (@($readyOrders | Where-Object { $_.orderId -eq $order.orderId }).Count -gt 0) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 1
}
if (-not $ready) {
    throw "Delivery Service did not consume the orders.ready event for $($order.orderId)."
}

$assignment = Send-Json "Post" "$base`:9095/delivery/assign" @{
    orderId = $order.orderId
    driverId = $driverId
}
$null = Wait-ForOrderStatus $order.orderId "OUT_FOR_DELIVERY"

$null = Send-Json "Put" "$base`:9095/delivery/$($assignment.deliveryId)/status" @{
    status = "DELIVERED"
}
$finalOrder = Wait-ForOrderStatus $order.orderId "DELIVERED"

$notifications = @()
$overview = $null
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $notifications = @(Invoke-RestMethod -Uri "$base`:9096/notifications/recipient/$($customer.id)")
    $overview = Invoke-RestMethod -Uri "$base`:9097/admin/overview"
    if ($notifications.Count -gt 0 -and $overview.totalOrdersProcessed -ge 1 -and
            $overview.totalRevenue -ge $order.totalAmount) {
        break
    }
    Start-Sleep -Seconds 1
}

if ($overview.totalOrdersProcessed -lt 1 -or $overview.totalRevenue -lt $order.totalAmount) {
    throw "Admin Service did not consume order/payment events. Overview: $($overview | ConvertTo-Json -Compress)"
}
if ($notifications.Count -lt 1) {
    throw "Notification Service did not consume the order events for customer $($customer.id)."
}

Write-Host "PASS: order reached $($finalOrder.status), delivery $($assignment.deliveryId) completed."
Write-Host "PASS: Notification Service recorded $(@($notifications).Count) notifications."
Write-Host "PASS: Admin Service reports $($overview.totalOrdersProcessed) orders and revenue $($overview.totalRevenue)."
