package com.sanedge.card.repository;

import java.time.Instant;

import com.sanedge.card.entity.CardEventLog;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardEventLogRepository implements ReactivePanacheMongoRepository<CardEventLog> {

    public Uni<CardEventLog> findByTopicReference(String topic, String referenceId) {
        return find("{ 'topic': ?1, 'referenceId': ?2 }", topic, referenceId).firstResult();
    }

    /**
     * Idempotent append for at-least-once Kafka delivery.
     *
     * <p>Topics with a stable per-entity reference ({@code card.payment.posted}
     * → paymentId, {@code card.fraud.alert} → authTxnId) are deduplicated by the
     * unique index {@code (topic, referenceId)}: a redelivery hits the duplicate
     * key error and is resolved to the existing row. Topics without a stable
     * reference ({@code card.statement.generated}) always insert — its
     * cycle-day key repeats every month, so full dedup would wrongly drop valid
     * events.</p>
     */
    public Uni<CardEventLog> appendIfAbsent(String topic, String eventType, String cardNumber,
            String referenceId, String payload) {
        if (referenceId == null || referenceId.isBlank()) {
            return persist(newLog(topic, eventType, cardNumber, referenceId, payload));
        }
        return persist(newLog(topic, eventType, cardNumber, referenceId, payload))
                .onFailure().recoverWithUni(e -> {
                    if (!isDuplicateKey(e)) {
                        return Uni.createFrom().failure(e);
                    }
                    return findByTopicReference(topic, referenceId)
                            .map(existing -> {
                                if (existing == null) {
                                    throw new IllegalStateException("Event log not found after duplicate key");
                                }
                                return existing;
                            });
                });
    }

    private static boolean isDuplicateKey(Throwable e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof com.mongodb.MongoWriteException
                    || t instanceof com.mongodb.MongoBulkWriteException) {
                return true;
            }
            if (t.getMessage() != null && t.getMessage().contains("E11000")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private CardEventLog newLog(String topic, String eventType, String cardNumber,
            String referenceId, String payload) {
        CardEventLog log = new CardEventLog();
        log.topic = topic;
        log.eventType = eventType;
        log.cardNumber = cardNumber;
        log.referenceId = referenceId;
        log.payload = payload;
        log.setCreatedAt(Instant.now());
        log.setUpdatedAt(Instant.now());
        return log;
    }
}
