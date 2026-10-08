package com.sanedge.common.adapter.model;

import java.time.Instant;

/**
 * Domain view of a card owned by the card service.
 */
public record Card(
        String id,
        String userId,
        String cardNumber,
        String cardType,
        String expireDate,
        String cvv,
        String cardProvider,
        String email,
        Instant createdAt,
        Instant updatedAt) {
}
