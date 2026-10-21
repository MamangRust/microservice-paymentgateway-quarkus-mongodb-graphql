package com.sanedge.common.adapter.fraud;

import io.smallrye.mutiny.Uni;

/**
 * Port for the ai-security service: fraud detection before a debit and
 * domain-level security verification before value moves. Mirrors
 * {@code pb.ai_security.AISecurityService} without leaking proto types into
 * consumer services.
 */
public interface FraudPort {

    /**
     * Scores a pending transaction for fraud. {@code is_fraudulent() == true}
     * means the caller must reject the transaction before debiting.
     */
    Uni<FraudResult> detectFraud(FraudCheck check);

    /**
     * Verifies a domain-scoped operation (e.g. WITHDRAW, TRANSFER).
     * {@code action()} of {@code "DENY"} means the caller must reject.
     */
    Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount);

    /** Metadata carried into {@link #detectFraud(FraudCheck)}. */
    record FraudCheck(
            String transactionId,
            int merchantId,
            int userId,
            double amount,
            String currency,
            String paymentMethod,
            String deviceId,
            String ipAddress,
            long timestamp) {
    }

    record FraudResult(
            String transactionId,
            double riskScore,
            boolean fraudulent,
            String reason) {
    }

    record SecurityResult(
            boolean safe,
            double riskScore,
            String reason,
            String action) {
    }

    /** Domains understood by ai-security (mirrors {@code pb.ai_security.SecurityDomain}). */
    enum SecurityDomain {
        UNKNOWN,
        CARD,
        MERCHANT,
        SALDO,
        TOPUP,
        TRANSACTION,
        TRANSFER,
        WITHDRAW
    }
}
