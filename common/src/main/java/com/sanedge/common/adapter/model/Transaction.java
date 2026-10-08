package com.sanedge.common.adapter.model;

import java.time.Instant;

/**
 * Domain view of a transaction owned by the transaction service.
 */
public record Transaction(
        String id,
        String cardNumber,
        String transactionNo,
        int amount,
        String paymentMethod,
        String merchantId,
        Instant transactionTime,
        Instant createdAt,
        Instant updatedAt) {
}
