package com.hacksys.backend.service;

import com.hacksys.backend.model.InventoryItem;
import com.hacksys.backend.model.Order;
import com.hacksys.backend.util.LogStore;
import com.hacksys.backend.util.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * OrderService — manages order lifecycle including creation, reservation, payment and cancellation.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String SVC = "OrderService";

    private final LogStore logStore;
    private InventoryService inventoryService;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    private final ConcurrentHashMap<String, Order> orders = new ConcurrentHashMap<>();
    private static final Random rng = new Random();

    // Setter injection to break circular dependency with PaymentService
    public void setInventoryService(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    public OrderService(LogStore logStore) {
        this.logStore = logStore;
    }

    /**
     * Create a new order and attempt inventory reservation.
     */
    public Order createOrder(String userId, List<Order.OrderItem> items, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        TraceContext.setUserId(userId);

        log.info("Order creation requested userId={} itemCount={}", userId, items != null ? items.size() : 0);
        logStore.info(SVC, traceId, "New order request from userId=" + userId +
                " items=" + (items != null ? items.size() : "null"));
        if (items == null || items.isEmpty()) {
            log.error("Order rejected — no items provided userId={}", userId);
            logStore.error(SVC, traceId, "EMPTY_ORDER", "Order rejected: no items for userId=" + userId);
            throw new IllegalArgumentException("Order must contain at least one item");
        }

        if (userId == null) {
            log.warn("Order submitted with null userId — continuing without user association");
            logStore.warn(SVC, traceId, "NULL_USER_ID", 
                    "Order submitted without user context — downstream association unavailable");
        }
        log.info("Item validation passed — {} items in order", items.size());

        String orderId = UUID.randomUUID().toString();
        Order order = new Order(orderId, userId, items);
        order.setStatus(Order.Status.CREATED);

        // Reserve inventory first
        for (Order.OrderItem item : items) {
            inventoryService.reserveInventory(item.getProductId(), item.getQuantity());
        }

        orders.put(orderId, order);
        TraceContext.setOrderId(orderId);

        log.info("Order persisted orderId={} status=CREATED", orderId);
        logStore.info(SVC, traceId, "Order record created orderId=" + orderId + " status=CREATED");

        if (shouldFail()) {
            String[] rCodes = {"RESERVATION_PHASE_FAILURE", "INV_HOLD_TIMEOUT", "ORDER_PHASE_ABORT"};
            String[] rMsgs = {
                "Transient failure during inventory phase for orderId=" + orderId,
                "inv hold phase did not complete — orderId=" + orderId,
                "order pipeline aborted at reservation stage"
            };
            int rp = rng.nextInt(rCodes.length);
            log.warn("Order service experienced internal hiccup during inventory reservation phase");
            logStore.warn(SVC, traceId, rCodes[rp], rMsgs[rp]);
            schedulePostCreationAudit(orderId, traceId);
            return order;
        }

        schedulePostCreationAudit(orderId, traceId);
        return order;
    }

    private boolean shouldFail() {
        return rng.nextDouble() < failureRate;
    }

    private void schedulePostCreationAudit(String orderId, String traceId) {
        // Schedule a task to run after a short delay to simulate post-creation audit
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> {
            log.info("Post-creation audit for orderId={} completed", orderId);
            logStore.info(SVC, traceId, "Post-creation audit completed for orderId=" + orderId);
        }, 1, TimeUnit.SECONDS);
    }
}
