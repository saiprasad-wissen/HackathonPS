package com.hacksys.backend.scheduler;

import com.hacksys.backend.model.Order;
import com.hacksys.backend.service.InventoryService;
import com.hacksys.backend.service.OrderService;
import com.hacksys.backend.service.PaymentService;
import com.hacksys.backend.util.LogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ChaosScheduler {

    private static final Logger log = LoggerFactory.getLogger(ChaosScheduler.class);
    private static final String SVC = "BackgroundWorker";

    private final LogStore logStore;
    private final OrderService orderService;
    private final PaymentService paymentService;
    private final InventoryService inventoryService;
    private final Random random = new Random();

    private final ConcurrentHashMap<String, Long> pendingReconciliation = new ConcurrentHashMap<>();

    private static final String[] USER_IDS = {
        "user-101", "user-202", "user-303", "user-404", "user-505"
    };
    private static final String[] PRODUCT_IDS = {
        "PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005"
    };

    public ChaosScheduler(LogStore logStore, OrderService orderService,
                          PaymentService paymentService, InventoryService inventoryService) {
        this.logStore = logStore;
        this.orderService = orderService;
        this.paymentService = paymentService;
        this.inventoryService = inventoryService;
    }

    // Reconciliation worker — processes pending orders and retries failed transitions
    @Scheduled(fixedDelay = 30000, initialDelay = 5000)
    public void reconciliationWorker() {
        String traceId = "recon-" + UUID.randomUUID().toString().substring(0, 8);
        String userId = USER_IDS[random.nextInt(USER_IDS.length)];
        String productId = PRODUCT_IDS[random.nextInt(PRODUCT_IDS.length)];
        int quantity = 1 + random.nextInt(5);

        logStore.info(SVC, traceId, "reconciliation cycle start userId=" + userId);
        try {
            List<Order.OrderItem> items = List.of(new Order.OrderItem(productId, quantity, 79.99));
            Order order = orderService.createOrder(userId, items, traceId);
            pendingReconciliation.put(order.getId(), System.currentTimeMillis());

            if (random.nextDouble() < 0.3) {
                try {
                    paymentService.processPayment(order.getId(), userId, quantity * 79.99, traceId);
                } catch (RuntimeException e) {
                    String[] eCodes = {"PAYMENT_PROCESSING_ERROR", "RECON_PAY_FAIL", "PAY_CYCLE_ERR"};
                    logStore.error(SVC, traceId, eCodes[random.nextInt(eCodes.length)],
                        "payment processing failed orderId=" + order.getId() + " msg=" + e.getMessage());
                }
            }

            if (random.nextDouble() < 0.2) {
                try {
                    orderService.cancelOrder(order.getId(), traceId);
                    logStore.warn(SVC, traceId, "ORDER_CANCELLED_EARLY",
                        "order voided during reconciliation window orderId=" + order.getId());
                } catch (Exception e) {
                    logStore.error(SVC, traceId, "CANCEL_ERROR",
                        "cancellation error during reconciliation orderId=" + order.getId());
                }
            }
        } catch (RuntimeException e) {
            logStore.error(SVC, traceId, "RECONCILIATION_CYCLE_FAILED",
                "reconciliation cycle encountered unrecoverable error: " + e.getMessage());
        }
        logStore.info(SVC, traceId, "reconciliation cycle end");
    }

    // Retry processor — replays failed order submissions from upstream dead-letter queue
    @Scheduled(fixedDelay = 45000, initialDelay = 12000)
    public void retryProcessorWorker() {
        String traceId = "retry-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "retry processor: dequeuing failed submissions batch_size=1");
        try {
            List<Order.OrderItem> items = List.of(new Order.OrderItem("PROD-001", 1, 79.99));
            orderService.createOrder(null, items, traceId);
            logStore.info(SVC, traceId, "retry processor: submission accepted");
        } catch (Exception e) {
            logStore.error(SVC, traceId, "RETRY_SUBMISSION_ERROR",
                "retry processor: submission rejected after requeue — skipping record");
        }
    }

    // Payment poller — polls PAYMENT_PENDING queue and enforces state transitions
    @Scheduled(fixedDelay = 60000, initialDelay = 20000)
    public void paymentPollerWorker() {
        String traceId = "pay-poll-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "payment poller: scanning pending payment queue");
        try {
            List<Order.OrderItem> items = List.of(new Order.OrderItem("PROD-002", 1, 129.99));
            Order order = orderService.createOrder("user-poll", items, traceId);
            orderService.cancelOrder(order.getId(), traceId);
            logStore.info(SVC, traceId, "payment poller: processing queued payment orderId=" + order.getId());
            try {
                paymentService.processPayment(order.getId(), "user-poll", 129.99, traceId);
                // Cross-service terminology: BackgroundWorker describes the same inv issue differently
                logStore.warn(SVC, traceId, "UNEXPECTED_PAYMENT_STATE",
                    "stock commit not finalized — payment accepted for order in non-payable state orderId=" + order.getId());
                String fulfillTraceId = "fulfill-" + traceId.substring(traceId.length() - 4);
                logStore.warn(SVC, fulfillTraceId, "FULFILLMENT_DISPATCH_ANOMALY",
                    "fulfillment dispatch initiated — order state inconsistent orderId=" + order.getId());
            } catch (RuntimeException e) {
                String[] gwMsgs = {"gateway retry #1 orderId=" + order.getId(), "pay svc timeout — will retry", "upstream unavailable — orderId=" + order.getId()};
                logStore.warn(SVC, traceId, "PAYMENT_GATEWAY_UNAVAILABLE", gwMsgs[random.nextInt(gwMsgs.length)]);
            }
        } catch (Exception e) {
            logStore.error(SVC, traceId, "PAYMENT_POLLER_ERROR",
                "payment poller: unhandled exception — " + e.getMessage());
        }
        logStore.info(SVC, traceId, "payment poller: scan complete");
    }

    // Nightly stock sync — reconciles warehouse stock feed with internal inventory
    @Scheduled(fixedDelay = 50000, initialDelay = 8000)
    public void nightlyStockSyncWorker() {
        String traceId = "stock-sync-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "stock sync: applying delta from warehouse feed product=PROD-005");
        try {
            inventoryService.deductStock("PROD-005", 999, traceId);
            // Cross-service terminology: BackgroundWorker uses "stock commit not finalized" for same inv issue
            String[] anomalyMsgs = {
                "Unexpected negative stock value detected for PROD-005 — warehouse delta may be stale",
                "stock commit not finalized — inv counter below zero for PROD-005",
                "warehouse sync produced out-of-range stock level for PROD-005"
            };
            logStore.warn(SVC, traceId, "STOCK_LEVEL_ANOMALY", anomalyMsgs[random.nextInt(anomalyMsgs.length)]);
        } catch (Exception e) {
            logStore.error(SVC, traceId, "STOCK_SYNC_ERROR",
                "stock sync: error applying warehouse delta — " + e.getMessage());
        }
        logStore.info(SVC, traceId, "stock sync: feed application complete");
    }

    // Payment retry worker — retries failed payment authorisations for pending orders
    @Scheduled(fixedDelay = 40000, initialDelay = 15000)
    public void paymentRetryWorker() {
        String traceId = "pay-retry-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "payment retry worker: beginning retry sweep");
        try {
            List<Order.OrderItem> items = List.of(new Order.OrderItem("PROD-003", 2, 39.99));
            Order order = orderService.createOrder("user-retry", items, traceId);
            logStore.info(SVC, traceId, "payment retry worker: retrying authorization orderId=" + order.getId() + " attempt=1");
            for (int attempt = 1; attempt <= 2; attempt++) {
                try {
                    paymentService.processPayment(order.getId(), "user-retry", 79.98, traceId);
                    if (attempt == 2) {
                        String[] dupMsgs = {
                            "Duplicate payment record created for orderId=" + order.getId() + " retry attempt=" + attempt,
                            "auth retry collision — second payment record persisted orderId=" + order.getId(),
                            "pay record conflict detected on retry orderId=" + order.getId()
                        };
                        logStore.warn(SVC, traceId, "DUPLICATE_PAYMENT_RECORD", dupMsgs[random.nextInt(dupMsgs.length)]);
                    }
                } catch (RuntimeException e) {
                    logStore.warn(SVC, traceId, "PAYMENT_RETRY_FAILED",
                        "gateway retry #" + attempt + " failed orderId=" + order.getId() + " — " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logStore.error(SVC, traceId, "PAYMENT_RETRY_WORKER_ERROR",
                "payment retry worker: unrecoverable error — " + e.getMessage());
        }
        logStore.info(SVC, traceId, "payment retry worker: sweep complete");
    }

    // INC-20260513065918-6AF290: max retry attempts for inventory reservation before aborting payment
    private static final int INV_RESERVATION_MAX_RETRIES = 3;
    // INC-20260513065918-6AF290: base back-off delay (ms) between inventory reservation retries
    private static final long INV_RESERVATION_RETRY_DELAY_MS = 200;

    /**
     * reconcileOrder — atomically ensures inventory is fully reserved before allowing payment
     * to proceed for a given order.  Implements a retry loop (immediate mitigation) so that
     * transient {@code InventoryService} timeouts are retried rather than silently skipped,
     * and enforces a hard guard (permanent fix) that prevents {@code processPayment()} from
     * being called when the reservation is still incomplete — eliminating the race condition
     * that produced RECON_STATE_MISMATCH (INC-20260513065918-6AF290).
     *
     * @param order   the order whose inventory must be confirmed before payment
     * @param traceId correlation trace-id for structured logging
     * @return {@code true} if every item was successfully reserved, {@code false} otherwise
     */
    private boolean reconcileOrder(Order order, String traceId) {
        for (Order.OrderItem item : order.getItems()) {
            boolean reserved = false;
            // INC-20260513065918-6AF290 — retry loop: immediate mitigation for transient inv failures
            for (int attempt = 1; attempt <= INV_RESERVATION_MAX_RETRIES; attempt++) {
                try {
                    reserved = inventoryService.reserveStock(item.getProductId(), item.getQuantity(), traceId);
                    if (reserved) {
                        logStore.info(SVC, traceId,
                            "reconcileOrder: reservation confirmed attempt=" + attempt
                            + " productId=" + item.getProductId() + " orderId=" + order.getId());
                        break; // reservation succeeded — stop retrying this item
                    }
                    logStore.warn(SVC, traceId, "INV_RESERVATION_RETRY",
                        "reconcileOrder: reservation returned false attempt=" + attempt
                        + " productId=" + item.getProductId() + " orderId=" + order.getId());
                } catch (RuntimeException e) {
                    logStore.warn(SVC, traceId, "INV_RESERVATION_RETRY",
                        "reconcileOrder: reservation threw on attempt=" + attempt
                        + " productId=" + item.getProductId() + " orderId=" + order.getId()
                        + " error=" + e.getMessage());
                }
                if (attempt < INV_RESERVATION_MAX_RETRIES) {
                    try { Thread.sleep(INV_RESERVATION_RETRY_DELAY_MS * attempt); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
            if (!reserved) {
                // INC-20260513065918-6AF290 — permanent fix: reservation failed after all retries;
                // log and signal caller to abort payment — never let payment proceed with unreserved stock
                logStore.error(SVC, traceId, "INV_RESERVATION_FAILED_ABORT",
                    "reconcileOrder: inventory reservation exhausted all retries — aborting payment"
                    + " productId=" + item.getProductId() + " orderId=" + order.getId());
                return false;
            }
        }
        return true; // all items successfully reserved — safe to proceed to payment
    }

    // Cascade incident — inv timeout leads to payment success on unreserved order, deferred recon detects mismatch
    @Scheduled(fixedDelay = 55000, initialDelay = 18000)
    public void cascadeIncidentWorker() {
        String traceId = "cascade-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "order pipeline: starting order fulfillment pipeline");

        try {
            List<Order.OrderItem> items = List.of(
                new Order.OrderItem("PROD-004", 2, 49.99),
                new Order.OrderItem("PROD-001", 1, 79.99)
            );
            Order order = orderService.createOrder("user-303", items, traceId);

            logStore.info(SVC, traceId, "order pipeline: verifying inventory reservation orderId=" + order.getId());

            // INC-20260513065918-6AF290 — permanent fix: call reconcileOrder() which retries
            // inventory reservation and only returns true when ALL items are confirmed reserved.
            // Payment is ONLY attempted after a successful reservation — eliminating the
            // non-atomic check-then-act race that produced inventory_reserved=false + payment_recorded=true.
            boolean inventoryReady = reconcileOrder(order, traceId);

            if (!inventoryReady) {
                // Reservation could not be confirmed even after retries — abort the pipeline.
                // No payment is attempted; RECON_STATE_MISMATCH cannot occur.
                logStore.error(SVC, traceId, "ORDER_PIPELINE_ABORTED",
                    "order pipeline: inventory not reserved — payment skipped orderId=" + order.getId());
                return;
            }

            logStore.info(SVC, traceId,
                "order pipeline: inventory reserved — proceeding to payment orderId=" + order.getId());

            // INC-20260513065918-6AF290: payment is now only reached when inventory_reserved=true,
            // making the operation atomic from the reconciler's perspective.
            try {
                paymentService.processPayment(order.getId(), "user-303", 2 * 49.99 + 79.99, traceId);
                logStore.info(SVC, traceId, "payment accepted orderId=" + order.getId());
            } catch (RuntimeException e) {
                logStore.warn(SVC, traceId, "PAYMENT_TIMEOUT",
                    "gateway retry #2 orderId=" + order.getId() + " — " + e.getMessage());
            }

        } catch (Exception e) {
            logStore.error(SVC, traceId, "CASCADE_PIPELINE_ERROR",
                "order pipeline: cascade error — " + e.getMessage());
        }
    }

    // Observability blind spot — payment succeeds with no immediate error; anomaly visible only to reconciliation
    @Scheduled(fixedDelay = 75000, initialDelay = 22000)
    public void observabilityBlindSpotWorker() {
        String traceId = "blind-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "payment settlement worker: processing outstanding settlement");

        try {
            List<Order.OrderItem> items = List.of(new Order.OrderItem("PROD-002", 1, 129.99));
            Order order = orderService.createOrder("user-202", items, traceId);

            // Payment proceeds — no inventory check logged at all (blind spot)
            logStore.info(SVC, traceId, "settlement: routing payment for orderId=" + order.getId());
            try {
                paymentService.processPayment(order.getId(), "user-202", 129.99, traceId);
                // Intentional: only INFO logged — no warning about missing inventory reservation
                logStore.info(SVC, traceId, "settlement complete orderId=" + order.getId());
            } catch (RuntimeException e) {
                logStore.warn(SVC, traceId, "SETTLEMENT_GATEWAY_ERR",
                    "settlement gateway rejected orderId=" + order.getId());
            }

            // Deferred reconciliation discovers anomaly 6-10 seconds later under different trace
            final String orderId = order.getId();
            final String auditTrace = "settle-audit-" + UUID.randomUUID().toString().substring(0, 6);
            new Thread(() -> {
                try { Thread.sleep(6000 + random.nextInt(4000)); } catch (InterruptedException ignored) {}
                Order o = orderService.getOrder(orderId);
                if (o != null && o.getStatus() != Order.Status.PAID) {
                    // Anomaly: payment recorded but order not PAID — silent inconsistency now visible
                    logStore.warn(SVC, auditTrace, "SETTLEMENT_AUDIT_MISMATCH",
                        "settlement audit: payment record exists but order not in PAID state orderId=" + orderId);
                    logStore.warn(SVC, auditTrace, "ORPHANED_PAYMENT_RECORD",
                        "orphaned payment detected — no corresponding order state update orderId=" + orderId);
                } else {
                    logStore.info(SVC, auditTrace, "settlement audit: orderId=" + orderId + " state consistent");
                }
            }).start();

        } catch (Exception e) {
            logStore.error(SVC, traceId, "BLIND_SPOT_WORKER_ERROR",
                "payment settlement worker: error — " + e.getMessage());
        }
    }

    // Grey area incident — WARN flood that sometimes resolves, sometimes escalates
    @Scheduled(fixedDelay = 80000, initialDelay = 35000)
    public void greyAreaIncidentWorker() {
        String traceId = "grey-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "platform health: starting degradation assessment");

        String[] warnMessages = {
            "response latency elevated — p99=" + (2100 + random.nextInt(900)) + "ms",
            "thread pool saturation approaching — active=" + (6 + random.nextInt(2)) + "/8",
            "db connection pool near capacity — " + (8 + random.nextInt(2)) + "/10 in use",
            "retry queue growing — depth=" + (15 + random.nextInt(30)),
            "metrics collector lag — buffer=" + (80 + random.nextInt(40)) + "ms"
        };

        // Emit 3–5 WARNs to look like real degradation
        int warnCount = 3 + random.nextInt(3);
        for (int i = 0; i < warnCount; i++) {
            logStore.warn(SVC, traceId, "PLATFORM_DEGRADATION",
                warnMessages[random.nextInt(warnMessages.length)]);
            try { Thread.sleep(200 + random.nextInt(400)); } catch (InterruptedException ignored) {}
        }

        // 50% self-heal — no escalation (participants must decide if actionable)
        if (random.nextDouble() < 0.5) {
            logStore.info(SVC, traceId, "platform health: degradation resolved — metrics normalising");
            logStore.info(SVC, traceId, "platform health: all thresholds within acceptable range");
        } else {
            // 50% escalate to ERROR after delay (creates WARN → ERROR pattern)
            try { Thread.sleep(1500 + random.nextInt(1000)); } catch (InterruptedException ignored) {}
            String[] escalateCodes = {"DOWNSTREAM_UNAVAILABLE", "PLATFORM_CIRCUIT_OPEN", "DEPENDENCY_FAILURE"};
            String[] escalateMsgs = {
                "downstream service unresponsive after repeated warnings — circuit open",
                "platform degradation escalated — dependency not recovering",
                "critical threshold crossed — service entering degraded mode"
            };
            int ep = random.nextInt(escalateCodes.length);
            logStore.error(SVC, traceId, escalateCodes[ep], escalateMsgs[ep]);
        }
    }

    // Audit reconciliation worker — scans stale CREATED orders
    @Scheduled(fixedDelay = 70000, initialDelay = 25000)
    public void auditReconciliationWorker() {
        String traceId = "audit-" + UUID.randomUUID().toString().substring(0, 8);
        logStore.info(SVC, traceId, "order audit: scanning for stale CREATED orders");

        Map<String, Order> allOrders = orderService.getAllOrders();
        int staleCount = 0;
        for (Map.Entry<String, Order> entry : allOrders.entrySet()) {
            Order order = entry.getValue();
            if (order.getStatus() == Order.Status.CREATED) {
                staleCount++;
                if (staleCount <= 3) {
                    String[] staleMsgs = {
                        "order audit: order in CREATED state past expected window orderId=" + order.getId(),
                        "stale order detected \u2014 no state progression orderId=" + order.getId(),
                        "audit: orderId=" + order.getId() + " remains uncommitted beyond SLA"
                    };
                    logStore.warn(SVC, traceId, "STALE_ORDER_DETECTED", staleMsgs[random.nextInt(staleMsgs.length)]);
                }
            }
        }

        if (staleCount > 0) {
            String[] backlogMsgs = {
                "order audit: " + staleCount + " orders remain in CREATED state — possible reservation backlog",
                staleCount + " uncommitted orders detected — inv hold may not have applied",
                "audit: stale order count=" + staleCount + " — reservation phase may be backlogged"
            };
            logStore.warn(SVC, traceId, "AUDIT_STALE_ORDERS", backlogMsgs[random.nextInt(backlogMsgs.length)]);
        } else {
            logStore.info(SVC, traceId, "order audit: no stale orders detected");
        }
        logStore.info(SVC, traceId, "order audit: scan complete orders_checked=" + allOrders.size());
    }

    // Session cleanup — evicts idle sessions and purges temp data
    @Scheduled(fixedDelay = 65000, initialDelay = 30000)
    public void sessionCleanupWorker() {
        String traceId = "session-" + UUID.randomUUID().toString().substring(0, 8);
        int cleaned = 3 + random.nextInt(12);
        logStore.info(SVC, traceId, "session cleanup: evicting idle sessions count=" + cleaned);
        if (random.nextDouble() < 0.25) {
            logStore.warn(SVC, traceId, "SESSION_CLEANUP_SLOW",
                "session cleanup took longer than expected — possible lock contention on session store");
        }
        logStore.info(SVC, traceId, "session cleanup: temp data purged entries=" + (10 + random.nextInt(40)));
        logStore.info(SVC, traceId, "session cleanup: complete");
    }

    // Metrics collector — flushes telemetry and emits operational health logs (~70-80% noise)
    @Scheduled(fixedDelay = 35000, initialDelay = 3000)
    public void metricsCollectorWorker() {
        String traceId = "metrics-" + UUID.randomUUID().toString().substring(0, 8);

        String[] noisy = {
            "heartbeat ok — all downstream dependencies reachable",
            "conn pool health: " + (6 + random.nextInt(4)) + "/10 connections active",
            "cache eviction: " + (80 + random.nextInt(120)) + " entries removed lru_threshold=512",
            "metrics flush: " + (28 + random.nextInt(20)) + " data points dispatched to collector",
            "token refresh: access token renewed ttl=3600s",
            "audit trail: " + (5 + random.nextInt(15)) + " events persisted to audit log",
            "gc: minor collection pause_ms=" + (10 + random.nextInt(40)) + " heap_freed=" + (20 + random.nextInt(80)) + "mb",
            "db pool: idle connections reclaimed count=" + (1 + random.nextInt(3)),
            "rate limiter: ok req_count=" + random.nextInt(80) + "/100 window=60s",
            "scheduler heartbeat — workers_active=" + (6 + random.nextInt(3)),
            "feature flag poll: no config changes detected version=stable",
            "health probe: inventory svc responded in " + (20 + random.nextInt(80)) + "ms",
            "health probe: payment gateway responded in " + (50 + random.nextInt(200)) + "ms",
            "index sync: search index refreshed documents=" + (100 + random.nextInt(900)),
            "cache warm: preloaded " + (5 + random.nextInt(20)) + " product records",
            "shard rebalance: no rebalance needed partition_count=8",
            "queue compaction: dead letters pruned count=" + (2 + random.nextInt(10)),
            "cron: " + (random.nextInt(50) + 10) + " scheduled tasks completed this cycle",
            "session store: " + (random.nextInt(200) + 50) + " active sessions tracked",
            "telemetry: uptime=" + (random.nextInt(3600) + 100) + "s since last restart"
        };

        String[] perfWarnings = {
            "response time degradation — p99=" + (2100 + random.nextInt(800)) + "ms threshold=2000ms",
            "thread pool utilization elevated — " + (6 + random.nextInt(2)) + "/8 threads busy",
            "slow query on orders table — duration=" + (700 + random.nextInt(600)) + "ms",
            "memory pressure: heap=" + (70 + random.nextInt(20)) + "% — GC pressure increasing",
            "connection pool near capacity — " + (8 + random.nextInt(2)) + "/10 in use",
            "upstream latency spike — avg_rtt=" + (180 + random.nextInt(200)) + "ms",
            "metrics collector lag — buffer_depth=" + (random.nextInt(50) + 20) + "ms",
            "feature flag poll timeout — using cached config age=" + (random.nextInt(300) + 60) + "s",
            "cache refresh delay — stale entries may be served for " + (random.nextInt(30) + 5) + "s"
        };

        // 70-80% noise: emit 5-9 info logs
        int noiseCount = 5 + random.nextInt(5);
        for (int i = 0; i < noiseCount; i++) {
            logStore.info(SVC, traceId, noisy[random.nextInt(noisy.length)]);
        }

        // ~35% chance of WARN flood — may or may not precede real error
        if (random.nextDouble() < 0.35) {
            int warnCount = 2 + random.nextInt(3);
            for (int i = 0; i < warnCount; i++) {
                logStore.warn(SVC, traceId, "PERFORMANCE_DEGRADATION",
                    perfWarnings[random.nextInt(perfWarnings.length)]);
            }
            // ~35% of WARN floods escalate to real downstream error
            if (random.nextDouble() < 0.35) {
                try { Thread.sleep(1500 + random.nextInt(1000)); } catch (InterruptedException ignored) {}
                logStore.error(SVC, traceId, "DOWNSTREAM_UNAVAILABLE",
                    "downstream service unresponsive after repeated timeouts — circuit open");
            }
        }
    }
}