package com.sanedge.card.repository;

import java.math.BigDecimal;
import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.card.entity.Card;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardCommandRepository implements ReactivePanacheMongoRepository<Card> {

    public Uni<Card> trashed(String cardId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card != null) {
                        card.setDeletedAt(Instant.now());
                        return card.update().map(v -> card);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Card> restore(String cardId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card != null) {
                        card.setDeletedAt(null);
                        return card.update().map(v -> card);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Card> deletePermanent(String cardId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card != null) {
                        return card.delete().map(v -> card);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Boolean> restoreAllDeleted() {
        return update("{ '$set': { 'deletedAt': null } }")
                .where("{ 'deletedAt': { '$ne': null } }")
                .map(updatedCount -> updatedCount > 0);
    }

    public Uni<Boolean> deleteAllDeleted() {
        return delete("{ 'deletedAt': { '$ne': null } }")
                .map(deletedCount -> deletedCount > 0);
    }

    public Uni<Card> toggleStatus(String cardId) {
        return find("{ '_id': ?1 }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card != null) {
                        if (card.status == Card.CardStatus.ACTIVE) {
                            card.status = Card.CardStatus.SUSPENDED;
                        } else if (card.status == Card.CardStatus.SUSPENDED) {
                            card.status = Card.CardStatus.ACTIVE;
                        } else {
                            return Uni.createFrom().nullItem();
                        }
                        card.setUpdatedAt(Instant.now());
                        return card.update().map(v -> card);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Card> update(Card card) {
        return card.update().replaceWith(card);
    }

    public Uni<Card> updateCreditLimit(String cardId, BigDecimal newLimit) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card != null) {
                        card.setCreditLimit(newLimit);
                        card.setUpdatedAt(Instant.now());
                        return card.update().map(v -> card);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Card> redeemPoints(String cardId, BigDecimal points) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(cardId)).firstResult()
                .chain(card -> {
                    if (card == null) {
                        return Uni.createFrom().nullItem();
                    }
                    BigDecimal available = card.getPoints() != null ? card.getPoints() : BigDecimal.ZERO;
                    if (available.compareTo(points) < 0) {
                        return Uni.createFrom().nullItem();
                    }
                    card.setPoints(available.subtract(points));
                    card.setUpdatedAt(Instant.now());
                    return card.update().map(v -> card);
                });
    }
}
