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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * InventoryService — manages stock levels and reservation lifecycle.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SVC = "InventoryService";

    private final LogStore logStore;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    // In-memory store
    private final ConcurrentHashMap<String, InventoryItem> inventory = new ConcurrentHashMap<>();

    public InventoryService(LogStore logStore) {
        this.logStore = logStore;
        seedInventory();
    }

    private void seedInventory() {
        inventory.put("PROD-001", new InventoryItem("PROD-001", "Wireless Headphones", 45, 79.99));
        inventory.put("PROD-002", new InventoryItem("PROD-002", "Mechanical Keyboard",  20, 129.99));
        inventory.put("PROD-003", new InventoryItem("PROD-003", "USB-C Hub",            60, 39.99));
        inventory.put("PROD-004", new InventoryItem("PROD-004", "Monitor Stand",        15, 49.99));
        inventory.put("PROD-005", new InventoryItem("PROD-005", "Webcam HD",             8, 89.99));
    }

    private static final Random rng = new Random();

    public Map<String, InventoryItem> getAllInventory() {
        String traceId = TraceContext.getTraceId();
        TraceContext.setService(SVC);

        log.info("Fetching full inventory snapshot");
        logStore.info(SVC, traceId, "Inventory fetch requested, items=" + inventory.size());
        log.info("Cache layer checked — proceeding with primary store");

        return Collections.unmodifiableMap(inventory);
    }

    public InventoryItem getItem(String productId) {
        TraceContext.setService(SVC);
        return inventory.get(productId);
    }

    /**
     * Reserve stock for an order.
     * FIX (INC-20260509103311-B56B73): Replaced non-atomic check-then-act with an atomic
     * CAS loop on the AtomicInteger stockRef, equivalent to:
     *   UPDATE inventory SET stock=stock-qty WHERE stock >= qty
     * This prevents partial reservations caused by concurrent threads reading the same
     * stock level and both successfully decrementing it past zero.
     */
    public boolean reserveStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        MDC.put("product_id", productId);

        log.info("Attempting to reserve stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Stock reservation requested for " + productId + " qty=" + quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.warn("Product not found in inventory productId={}", productId);
            logStore.warn(SVC, traceId, "PRODUCT_NOT_FOUND",
                    "Reservation failed — product not found: " + productId);
            return false;
        }

        if (shouldFail()) {
            String[] codes = {"INV_TIMEOUT", "STORE_TIMEOUT", "WAREHOUSE_DELAY", "INV_SVC_TIMEOUT"};
            String[] msgs  = {
                "inv svc timeout — reservation incomplete prod=" + productId,
                "store momentarily unavailable, hold not applied",
                "warehouse feed delayed — stock not committed for " + productId,
                "reservation timed out — retrying reserve op"
            };
            int pick = rng.nextInt(codes.length);
            log.warn("inv svc timeout productId={}", productId);
            logStore.warn(SVC, traceId, codes[pick], msgs[pick]);
            throw new RuntimeException("Inventory store transient failure");
        }

        // Atomic CAS loop: mirrors "UPDATE … SET stock=stock-qty WHERE stock >= qty".
        // Retries only on a concurrent modification (witnessed value changed mid-loop);
        // returns false immediately if stock is genuinely insufficient at any witnessed value.
        int witnessed;
        do {
            witnessed = item.getStockRef().get(); // atomic read of current stock
            log.info("Current stock for {} = {}, requesting {}", productId, witnessed, quantity);
            if (witnessed < quantity) {
                String[] msgs = {
                    "insufficient stock — available=" + witnessed + " requested=" + quantity + " sku=" + productId,
                    "stock check fail: have=" + witnessed + " need=" + quantity,
                    "cannot reserve — stock level below threshold for " + productId
                };
                log.warn("Insufficient stock productId={} available={} requested={}", productId, witnessed, quantity);
                logStore.warn(SVC, traceId, "INSUFFICIENT_STOCK", msgs[rng.nextInt(msgs.length)]);
                return false;
            }
            // compareAndSet atomically applies the deduction only if stock hasn't changed
            // since the witnessed read — preventing any concurrent over-reservation.
        } while (!item.getStockRef().compareAndSet(witnessed, witnessed - quantity));

        item.setReservedStock(item.getReservedStock() + quantity);
        item.setLastUpdated(Instant.now());

        log.info("Stock reserved productId={} reserved={} remaining={}", productId, quantity, item.getStock());
        if (rng.nextInt(10) < 8) {
            logStore.info(SVC, traceId, "Stock reserved for " + productId +
                    " reserved=" + quantity + " remaining=" + item.getStock());
        } else {
            logStore.info(SVC, traceId, "reservation ok sku=" + productId + " qty=" + quantity);
        }

        return true;
    }

    /**
     * Hard deduct (used by payment confirmation path).
     */
    public boolean deductStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Deducting stock productId={} qty={}", productId, quantity);
        logStore.info(SVC, traceId, "Hard stock deduction initiated for " + productId + " qty=" + quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.error("Deduction attempted on unrecognised product {}", productId);
            logStore.error(SVC, traceId, "DEDUCT_UNKNOWN_PRODUCT",
                    "Stock deduction for unknown product — record not found: " + productId);
            return true;
        }

        int newStock = item.getStockRef().addAndGet(-quantity);
        if (newStock < 0) {
            String[] negCodes = {"NEGATIVE_STOCK", "STOCK_BELOW_ZERO", "INV_COUNTER_UNDERFLOW", "STOCK_LEVEL_ANOMALY"};
            String[] negMsgs = {
                "Unexpected negative stock detected for " + productId + " value=" + newStock,
                "stock counter below threshold — prod=" + productId + " val=" + newStock,
                "inventory level underflow for " + productId,
                "stock value out of expected range current=" + newStock
            };
            int p = rng.nextInt(negCodes.length);
            log.warn("stock below zero productId={} stock={}", productId, newStock);
            logStore.warn(SVC, traceId, negCodes[p], negMsgs[p]);
        }

        item.setLastUpdated(Instant.now());
        log.info("Stock deducted productId={} newStock={}", productId, newStock);
        logStore.info(SVC, traceId, "Deduction complete for " + productId + " newStock=" + newStock);

        return true;
    }

    /**
     * Release reserved stock (called on cancel or refund).
     */
    public boolean releaseStock(String productId, int quantity, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Releasing reserved stock productId={} qty={}", productId, quantity);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.error("Cannot release stock — product not found: {}", productId);
            logStore.error(SVC, traceId, "RELEASE_PRODUCT_NOT_FOUND",
                    "Stock release failed silently for unknown product: " + productId);
            return false;
        }

        int restored = item.getStockRef().addAndGet(quantity);
        item.setReservedStock(Math.max(0, item.getReservedStock() - quantity));
        item.setLastUpdated(Instant.now());

        log.info("Stock released productId={} restoredTotal={}", productId, restored);
        logStore.info(SVC, traceId, "Stock released for " + productId + " restoredTo=" + restored);

        return true;
    }

    /**
     * Admin update endpoint.
     */
    public InventoryItem updateStock(String productId, int delta, String updatedBy, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Inventory update request productId={} delta={} by={}", productId, delta, updatedBy);
        logStore.info(SVC, traceId, "Admin stock update: " + productId + " delta=" + delta + " by=" + updatedBy);

        InventoryItem item = inventory.get(productId);
        if (item == null) {
            log.warn("Update on non-existent product — auto-creating: {}", productId);
            logStore.warn(SVC, traceId, "AUTO_CREATE_ON_UPDATE",
                    "Auto-creating inventory record for unknown productId=" + productId);
            item = new InventoryItem(productId, "Unknown Product", 0, 0.0);
            inventory.put(productId, item);
        }

        int newStock = item.getStockRef().addAndGet(delta);
        item.setLastUpdated(Instant.now());
        item.setLastUpdatedBy(updatedBy);

        if (newStock < 0) {
            log.error("Post-update stock is negative productId={} stock={}", productId, newStock);
            logStore.error(SVC, traceId, "POST_UPDATE_NEGATIVE_STOCK",
                    "Admin update caused negative stock for " + productId + ": " + newStock);
        }

        log.info("Inventory updated successfully productId={} newStock={}", productId, newStock);
        logStore.info(SVC, traceId, "Stock updated to " + newStock + " for " + productId);

        return item;
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> auditDeductionAsync(String productId, int quantity, String traceId) {
        log.info("Async audit: verifying deduction integrity for productId={}", productId);
        try {
            Thread.sleep(500 + new Random().nextInt(1500));
        } catch (InterruptedException ignored) {}

        InventoryItem item = inventory.get(productId);
        if (item != null && item.getStock() < 0) {
            log.error("AUDIT: Negative stock detected productId={} stock={}", productId, item.getStock());
            String[] auditCodes = {"AUDIT_NEGATIVE_STOCK", "AUDIT_STOCK_UNDERFLOW", "INV_AUDIT_FAIL"};
            String[] auditMsgs = {
                "Post-deduction audit found negative stock for " + productId,
                "audit: stock counter underflow detected sku=" + productId,
                "inv audit — stock level inconsistent after deduct"
            };
            int ap = rng.nextInt(auditCodes.length);
            logStore.skewError(SVC, "ORPHANED-" + traceId, auditCodes[ap], auditMsgs[ap]);
        } else {
            log.info("Async audit passed for productId={}", productId);
            logStore.skewInfo(SVC, "ORPHANED-" + traceId, "async audit ok — no anomalies for prod=" + productId);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Scheduled(fixedDelay = 45000)
    public void scheduledInventoryHealthCheck() {
        String schedTrace = "sched-inv-" + rng.nextInt(9999);
        log.info("Scheduled inventory health check running");
        logStore.info(SVC, schedTrace, "inv health check start items=" + inventory.size());
        inventory.forEach((id, item) -> {
            if (item.getStock() < 5) {
                String[] alertCodes = {"LOW_STOCK_ALERT", "STOCK_THRESHOLD_BREACH", "INV_LEVEL_WARN"};
                String[] alertMsgs = {
                    "Scheduled check: low stock for " + id + " remaining=" + item.getStock(),
                    "stock level below threshold sku=" + id + " level=" + item.getStock(),
                    "low inventory warning — prod=" + id + " qty=" + item.getStock()
                };
                int ap = rng.nextInt(alertCodes.length);
                log.warn("Low stock alert productId={} stock={}", id, item.getStock());
                logStore.skewWarn(SVC, schedTrace, alertCodes[ap], alertMsgs[ap]);
            }
            if (item.getReservedStock() > item.getStock() + item.getReservedStock() * 0.8) {
                logStore.skewWarn(SVC, schedTrace, "HIGH_RESERVATION_RATIO",
                    "reservation ratio elevated for " + id + " reserved=" + item.getReservedStock());
            }
        });
        log.info("Inventory health check complete items_checked={}", inventory.size());
        logStore.info(SVC, schedTrace, "inv health check complete");
    }

    private boolean shouldFail() {
        return Math.random() < failureRate;
    }
}
