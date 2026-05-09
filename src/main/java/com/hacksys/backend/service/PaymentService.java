package com.hacksys.backend.service;

import com.hacksys.backend.model.Order;
import com.hacksys.backend.model.Payment;
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
 * PaymentService — handles payment processing and refund lifecycle.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String SVC = "PaymentService";

    private final LogStore logStore;
    private final OrderService orderService;

    @Value("${app.payment.timeout-ms:3000}")
    private long paymentTimeoutMs;

    @Value("${app.chaos.intermittent-failure-rate:0.25}")
    private double failureRate;

    private final ConcurrentHashMap<String, Payment> paymentsById    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Payment>> paymentsByOrder = new ConcurrentHashMap<>();
    private static final Random rng = new Random();

    public PaymentService(LogStore logStore, OrderService orderService) {
        this.logStore = logStore;
        this.orderService = orderService;
    }

    /**
     * Process payment for an order.
     */
    public Payment processPayment(String orderId, String userId, double amount, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);
        MDC.put("order_id", orderId);

        log.info("Payment processing initiated orderId={} userId={} amount={}", orderId, userId, amount);
        logStore.info(SVC, traceId, "Payment initiated for orderId=" + orderId + " amount=" + amount);

        String effectiveUserId = (userId != null && !userId.trim().isEmpty()) ? userId : "anonymous";
        if (!effectiveUserId.equals(userId)) {
            log.warn("Payment initiated without valid userId — orderId={}", orderId);
            logStore.warn(SVC, traceId, "NULL_USER_ID",
                    "userId missing for orderId=" + orderId + " — proceeding as anonymous");
        }

        Order order = orderService.getOrder(orderId);
        if (order == null) {
            log.error("Payment rejected — order not found orderId={}", orderId);
            logStore.error(SVC, traceId, "ORDER_NOT_FOUND",
                    "Cannot process payment — order does not exist: " + orderId);
            throw new IllegalArgumentException("Order not found: " + orderId);
        }

        if (order.getStatus() != Order.Status.RESERVED && order.getStatus() != Order.Status.CREATED) {
            String[] warnCodes = {"UNEXPECTED_ORDER_STATUS", "ORDER_STATE_MISMATCH", "AUTH_ON_TERMINAL_ORDER"};
            String[] warnMsgs = {
                "payment auth continuing despite terminal order state",
                "order state mismatch during auth phase — status=" + order.getStatus(),
                "retry auth accepted — order not in payable state orderId=" + orderId,
                "Payment proceeding for order status=" + order.getStatus() + " orderId=" + orderId
            };
            log.warn("Payment initiated for order in non-standard state orderId={} status={}",
                    orderId, order.getStatus());
            logStore.warn(SVC, traceId, warnCodes[rng.nextInt(warnCodes.length)],
                    warnMsgs[rng.nextInt(warnMsgs.length)]);
        }

        log.info("Routing payment through primary processor gateway");

        if (shouldFail()) {
            String[] gwCodes = {"GTWY_TMO", "GATEWAY_TIMEOUT", "PAY_GATEWAY_ERR", "PAYMENT_SVC_TIMEOUT"};
            String[] gwMsgs  = {
                "payment gateway did not respond within SLA orderId=" + orderId,
                "gateway timeout — orderId=" + orderId,
                "pay svc unreachable — request not processed",
                "upstream gateway timeout on auth attempt"
            };
            int g = rng.nextInt(gwCodes.length);
            log.warn("Payment gateway timeout — orderId={}", orderId);
            logStore.warn(SVC, traceId, gwCodes[g], gwMsgs[g]);
            throw new RuntimeException("Payment gateway timeout");
        }

        // Simulate processing latency
        try {
            long processingTime = 200 + new Random().nextInt(800);
            Thread.sleep(processingTime);
            log.info("Payment processor responded in {}ms", processingTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Payment processing interrupted orderId={}", orderId, e);
            logStore.error(SVC, traceId, "PROCESSING_INTERRUPTED",
                    "Thread interrupted during payment for orderId=" + orderId);
        }

        // Create payment record
        String paymentId = UUID.randomUUID().toString();
        Payment payment = new Payment(paymentId, orderId, effectiveUserId, amount);
        payment.setStatus(Payment.Status.SUCCESS);

        paymentsById.put(paymentId, payment);
        paymentsByOrder.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>()).add(payment);

        log.info("Payment record created paymentId={} orderId={}", paymentId, orderId);
        logStore.info(SVC, traceId, "Payment record persisted paymentId=" + paymentId);

        try { Thread.sleep(50); } catch (InterruptedException ignored) {}

        boolean orderUpdated = orderService.markOrderPaid(orderId, paymentId, traceId);
        if (!orderUpdated) {
            String[] pCodes = {"ORDER_UPDATE_FAILURE", "PAY_PARTIAL_WRITE", "ORDER_SYNC_FAILURE", "PARTIAL_COMMIT"};
            String[] pMsgs  = {
                "Payment=" + paymentId + " persisted but orderId=" + orderId + " not updated to PAID",
                "partial write — payment committed but order state not synced",
                "order sync failed post-payment — orderId=" + orderId,
                "pay record created, order update skipped — paymentId=" + paymentId
            };
            int pp = rng.nextInt(pCodes.length);
            log.error("Payment recorded but order status update failed orderId={} paymentId={}",
                    orderId, paymentId);
            logStore.error(SVC, traceId, pCodes[pp], pMsgs[pp]);
        }

        schedulePaymentConfirmation(paymentId, orderId, traceId);

        log.info("Payment completed successfully paymentId={}", paymentId);
        logStore.info(SVC, traceId, "Payment flow complete paymentId=" + paymentId + " orderId=" + orderId);

        return payment;
    }

    /**
     * Refund a payment.
     */
    public Payment refundPayment(String orderId, String traceId) {
        TraceContext.setService(SVC);
        TraceContext.bindTrace(traceId);

        log.info("Refund requested for orderId={}", orderId);
        logStore.info(SVC, traceId, "Refund initiated for orderId=" + orderId);

        List<Payment> payments = paymentsByOrder.get(orderId);
        if (payments == null || payments.isEmpty()) {
            log.error("Refund failed — no payment found for orderId={}", orderId);
            logStore.error(SVC, traceId, "REFUND_NO_PAYMENT",
                    "Cannot refund — no payment records for orderId=" + orderId);
            throw new IllegalStateException("No payment found for order: " + orderId);
        }

        Payment latest = payments.get(payments.size() - 1);

        if (latest.getStatus() == Payment.Status.REFUNDED) {
            String[] dupCodes = {"DUPLICATE_REFUND", "REFUND_ALREADY_PROCESSED", "WARN_DUP_REFUND"};
            String[] dupMsgs  = {
                "Refund attempt on already-refunded paymentId=" + latest.getId(),
                "duplicate refund — payment already in REFUNDED state",
                "refund re-issued for paymentId=" + latest.getId() + " — state not checked"
            };
            int dr = rng.nextInt(dupCodes.length);
            log.warn("Refund requested on payment already in REFUNDED state paymentId={}", latest.getId());
            logStore.warn(SVC, traceId, dupCodes[dr], dupMsgs[dr]);
        }

        latest.setStatus(Payment.Status.REFUNDED);
        log.info("Refund processed paymentId={} orderId={}", latest.getId(), orderId);
        logStore.info(SVC, traceId, "Refund successful paymentId=" + latest.getId());

        orderService.markOrderRefunded(orderId, traceId);

        return latest;
    }

    public List<Payment> getPaymentsForOrder(String orderId) {
        return paymentsByOrder.getOrDefault(orderId, Collections.emptyList());
    }

    public Payment getPayment(String paymentId) {
        return paymentsById.get(paymentId);
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> schedulePaymentConfirmation(String paymentId, String orderId, String callerTraceId) {
        try {
            Thread.sleep(1000 + new Random().nextInt(2000));
        } catch (InterruptedException ignored) {}

        Payment p = paymentsById.get(paymentId);
        if (p == null) {
            log.error("Async confirmation: payment not found paymentId={}", paymentId);
            logStore.error(SVC, "ASYNC-ORPHAN", "CONFIRM_PAYMENT_NOT_FOUND",
                    "Async job could not find payment record paymentId=" + paymentId);
            return CompletableFuture.completedFuture(null);
        }

        // Simulate occasional async confirmation failure
        if (Math.random() < 0.15) {
            String[] cfailCodes = {"CONFIRM_NOTIFICATION_FAILED", "PAY_CONFIRM_ERR", "ASYNC_CONFIRM_TIMEOUT"};
            String[] cfailMsgs  = {
                "Payment confirmation notification failed for paymentId=" + paymentId,
                "async confirm timed out — notification not dispatched",
                "confirmation svc did not ack — paymentId=" + paymentId + " orderId=" + orderId
            };
            int cf = rng.nextInt(cfailCodes.length);
            log.warn("Async payment confirmation failed — notification not sent paymentId={}", paymentId);
            logStore.skewWarn(SVC, "ASYNC-ORPHAN", cfailCodes[cf], cfailMsgs[cf]);
        } else {
            log.info("Async confirmation sent paymentId={}", paymentId);
            logStore.skewInfo(SVC, "ASYNC-" + callerTraceId,
                    "Payment confirmation dispatched for paymentId=" + paymentId);
        }

        return CompletableFuture.completedFuture(null);
    }

    private boolean shouldFail() {
        return Math.random() < failureRate;
    }
}
