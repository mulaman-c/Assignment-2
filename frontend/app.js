const $ = (selector) => document.querySelector(selector);
const state = { customers: [], orders: [], restaurantOrders: [], readyOrders: [], drivers: [], deliveries: [], notifications: [] };

function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    })[char]);
}

function showNotice(message, kind = "error") {
    const notice = $("#notice");
    notice.textContent = message;
    notice.className = `notice${kind === "success" ? " success" : ""}`;
    notice.hidden = false;
    window.clearTimeout(showNotice.timer);
    showNotice.timer = window.setTimeout(() => { notice.hidden = true; }, 7000);
}

async function api(path, options = {}) {
    const response = await fetch(path, {
        ...options,
        headers: { "Content-Type": "application/json", ...options.headers },
        body: options.body === undefined ? undefined : JSON.stringify(options.body)
    });
    const text = await response.text();
    let payload;
    try { payload = text ? JSON.parse(text) : null; } catch { payload = text; }
    if (!response.ok) {
        const reason = typeof payload === "string" ? payload : payload?.message || JSON.stringify(payload);
        throw new Error(`${response.status}: ${reason || response.statusText}`);
    }
    return payload;
}

function money(amount) {
    return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD" }).format(Number(amount || 0));
}

function orderStatus(status) {
    const value = String(status || "UNKNOWN").toUpperCase();
    const modifier = value === "DELIVERED" ? " delivered" : value === "CANCELLED" ? " cancelled" : "";
    return `<span class="status-pill${modifier}">${escapeHtml(value.replaceAll("_", " "))}</span>`;
}

function renderCustomers() {
    const selectedOrderCustomer = $("#customer-select").value;
    const options = state.customers.map((customer) =>
        `<option value="${escapeHtml(customer.id)}">${escapeHtml(customer.name)} · ${escapeHtml(customer.id)}</option>`
    ).join("");
    $("#customer-select").innerHTML = `<option value="">Select a customer</option>${options}`;
    if (state.customers.some((customer) => customer.id === selectedOrderCustomer)) {
        $("#customer-select").value = selectedOrderCustomer;
    }
    const selected = $("#notification-customer").value;
    $("#notification-customer").innerHTML = `<option value="">Choose a customer</option>${options}`;
    if (state.customers.some((customer) => customer.id === selected)) {
        $("#notification-customer").value = selected;
    }
    $("#metric-customers").textContent = state.customers.length;
}

function renderOrders() {
    const rows = [...state.orders].sort((a, b) => String(b.orderId).localeCompare(String(a.orderId), undefined, { numeric: true }));
    $("#orders-body").innerHTML = rows.length ? rows.map((order) => {
        const customer = state.customers.find((item) => item.id === order.customerId);
        return `<tr>
            <td><span class="order-id">${escapeHtml(order.orderId)}</span></td>
            <td>${escapeHtml(customer?.name || order.customerId)}</td>
            <td>${escapeHtml(order.restaurantId)}</td>
            <td>${orderStatus(order.status)}</td>
            <td>${money(order.totalAmount)}</td>
        </tr>`;
    }).join("") : '<tr><td colspan="5" class="empty-state">No orders yet. Create one above to start the event flow.</td></tr>';
}

function renderRestaurantQueue() {
    const active = state.restaurantOrders.filter((order) => order.status === "CONFIRMED" || order.status === "PREPARING");
    $("#restaurant-queue").innerHTML = active.length ? active.map((order) => {
        const next = order.status === "CONFIRMED" ? "PREPARING" : "READY";
        return `<div class="queue-item">
            <div class="queue-main"><strong>${escapeHtml(order.orderId)} · ${escapeHtml(order.status)}</strong><span>${escapeHtml(order.restaurantId)} · ${money(order.totalAmount)}</span></div>
            <div class="queue-actions"><button class="button button-outline" data-restaurant-order="${escapeHtml(order.orderId)}" data-next-status="${next}">Mark ${next.toLowerCase()}</button></div>
        </div>`;
    }).join("") : '<p class="empty-state">New orders appear here after the restaurant consumes <code>orders.created</code>.</p>';
}

function renderDelivery() {
    const availableDrivers = state.drivers.filter((driver) => driver.status === "AVAILABLE");
    const ready = state.readyOrders.filter((order) => order.status === "READY");
    $("#delivery-queue").innerHTML = ready.length ? ready.map((order) => `
        <div class="queue-item">
            <div class="queue-main"><strong>${escapeHtml(order.orderId)} · READY</strong><span>${money(order.totalAmount)} · ${escapeHtml(order.restaurantId)}</span></div>
            <div class="queue-actions">
                <select aria-label="Driver for ${escapeHtml(order.orderId)}" data-driver-for="${escapeHtml(order.orderId)}">
                    <option value="">Choose driver</option>${availableDrivers.map((driver) => `<option value="${escapeHtml(driver.driverId)}">${escapeHtml(driver.name)}</option>`).join("")}
                </select>
                <button class="button button-outline" data-assign-order="${escapeHtml(order.orderId)}" ${availableDrivers.length ? "" : "disabled"}>Assign</button>
            </div>
        </div>`).join("") : '<p class="empty-state">Ready orders will be available after the restaurant publishes <code>orders.ready</code>.</p>';

    const active = state.deliveries.filter((delivery) => delivery.status !== "DELIVERED");
    $("#active-deliveries").innerHTML = active.length ? active.map((delivery) => `
        <div class="queue-item">
            <div class="queue-main"><strong>${escapeHtml(delivery.orderId)} · ${escapeHtml(delivery.status)}</strong><span>${escapeHtml(delivery.deliveryId)} · ${escapeHtml(delivery.driverId)}</span></div>
            <button class="button button-outline" data-complete-delivery="${escapeHtml(delivery.deliveryId)}">Mark delivered</button>
        </div>`).join("") : '<p class="empty-state">Assigned deliveries appear here.</p>';
}

function renderNotifications() {
    $("#notifications-list").innerHTML = state.notifications.length ? [...state.notifications].reverse().map((entry) => `
        <article class="notification-item"><strong>${escapeHtml(entry.message)}</strong><span>${escapeHtml(entry.recipientType)} · ${escapeHtml(entry.notificationId)} · ${escapeHtml(entry.sentAt)}</span></article>
    `).join("") : '<p class="empty-state">No notifications for this customer yet.</p>';
}

function renderOverview(overview) {
    $("#metric-orders").textContent = overview.totalOrdersProcessed;
    $("#metric-revenue").textContent = money(overview.totalRevenue);
    $("#metric-drivers").textContent = overview.activeDrivers;
}

async function refreshData() {
    $("#refresh-button").disabled = true;
    try {
        const [customers, orders, restaurantOrders, readyOrders, drivers, deliveries, overview] = await Promise.all([
            api("/api/customers/"), api("/api/orders/"), api("/api/restaurant/orders"),
            api("/api/delivery/readyOrders"), api("/api/delivery/drivers"),
            api("/api/delivery/deliveries"), api("/api/admin/overview")
        ]);
        Object.assign(state, { customers, orders, restaurantOrders, readyOrders, drivers, deliveries });
        renderCustomers();
        renderOrders();
        renderRestaurantQueue();
        renderDelivery();
        renderOverview(overview);
        const customerId = $("#notification-customer").value;
        if (customerId) {
            state.notifications = await api(`/api/notifications/recipient/${encodeURIComponent(customerId)}`);
            renderNotifications();
        }
    } catch (error) {
        showNotice(`Could not refresh platform data. Check that the Docker Compose services are running. ${error.message}`);
    } finally {
        $("#refresh-button").disabled = false;
    }
}

$("#customer-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    try {
        const customer = await api("/api/customers/register", {
            method: "POST",
            body: {
                name: form.get("name"), email: form.get("email"), phone: form.get("phone"),
                deliveryAddress: { street: form.get("street"), city: form.get("city"), postalCode: form.get("postalCode") }
            }
        });
        event.currentTarget.reset();
        await refreshData();
        $("#customer-select").value = customer.id;
        $("#notification-customer").value = customer.id;
        showNotice(`Customer ${customer.name} is ready to order.`, "success");
    } catch (error) { showNotice(`Customer could not be created. ${error.message}`); }
});

$("#order-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    try {
        const order = await api("/api/orders", {
            method: "POST",
            body: {
                customerId: form.get("customerId"),
                restaurantId: form.get("restaurantId"),
                items: [{
                    itemId: `ITEM-${Date.now()}`, name: form.get("itemName"),
                    price: Number(form.get("price")), quantity: Number(form.get("quantity"))
                }]
            }
        });
        await refreshData();
        showNotice(`Order ${order.orderId} placed. Kafka is moving it through the services.`, "success");
    } catch (error) { showNotice(`Order could not be placed. ${error.message}`); }
});

$("#driver-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const driverId = `DRIVER-${Date.now()}`;
    try {
        const driver = await api("/api/delivery/drivers", {
            method: "POST",
            body: { driverId, name: form.get("name"), phone: form.get("phone"), status: "AVAILABLE" }
        });
        event.currentTarget.reset();
        await refreshData();
        showNotice(`${driver.name} is available for delivery.`, "success");
    } catch (error) { showNotice(`Driver could not be added. ${error.message}`); }
});

$("#restaurant-queue").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-restaurant-order]");
    if (!button) return;
    try {
        await api(`/api/restaurant/orders/${encodeURIComponent(button.dataset.restaurantOrder)}/status`, {
            method: "PUT", body: { newStatus: button.dataset.nextStatus }
        });
        await refreshData();
        showNotice(`Restaurant published the next order event.`, "success");
    } catch (error) { showNotice(`Restaurant status could not be updated. ${error.message}`); }
});

$("#delivery-queue").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-assign-order]");
    if (!button) return;
    const orderId = button.dataset.assignOrder;
    const driverId = $(`[data-driver-for="${CSS.escape(orderId)}"]`)?.value;
    if (!driverId) {
        showNotice("Choose an available driver before assigning this order.");
        return;
    }
    try {
        const delivery = await api("/api/delivery/assign", { method: "POST", body: { orderId, driverId } });
        await refreshData();
        showNotice(`Delivery ${delivery.deliveryId} assigned.`, "success");
    } catch (error) { showNotice(`Delivery could not be assigned. ${error.message}`); }
});

$("#active-deliveries").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-complete-delivery]");
    if (!button) return;
    try {
        await api(`/api/delivery/${encodeURIComponent(button.dataset.completeDelivery)}/status`, {
            method: "PUT", body: { status: "DELIVERED" }
        });
        await refreshData();
        showNotice("Delivery completed and the order status has been updated.", "success");
    } catch (error) { showNotice(`Delivery could not be completed. ${error.message}`); }
});

$("#notification-customer").addEventListener("change", async (event) => {
    if (!event.target.value) {
        state.notifications = [];
        renderNotifications();
        return;
    }
    try {
        state.notifications = await api(`/api/notifications/recipient/${encodeURIComponent(event.target.value)}`);
        renderNotifications();
    } catch (error) { showNotice(`Notifications could not be loaded. ${error.message}`); }
});

$("#refresh-button").addEventListener("click", refreshData);
refreshData();
window.setInterval(refreshData, 5000);
