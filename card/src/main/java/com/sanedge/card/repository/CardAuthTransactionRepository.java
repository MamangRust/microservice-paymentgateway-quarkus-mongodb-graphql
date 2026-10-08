package com.sanedge.card.repository;

import java.time.Instant;
import java.util.List;

import org.bson.types.ObjectId;

import com.sanedge.card.entity.CardAuthTransaction;
import com.sanedge.card.entity.CardAuthTransaction.AuthTxnStatus;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardAuthTransactionRepository implements ReactivePanacheMongoRepository<CardAuthTransaction> {

    public Uni<CardAuthTransaction> findByIdempotencyKey(String key) {
        return find("{ 'idempotencyKey': ?1 }", key).firstResult();
    }

    public Uni<CardAuthTransaction> findByAuthTxnId(String authTxnId) {
        return find("_id", new ObjectId(authTxnId)).firstResult();
    }

    public Uni<List<CardAuthTransaction>> findByCardNumber(String cardNumber, int page, int size) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .page(page > 0 ? page - 1 : 0, size > 0 ? size : 10)
                .list();
    }

    public Uni<Long> countByCardNumber(String cardNumber) {
        return count("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber);
    }

    public Uni<Long> countRecentByCardNumber(String cardNumber, Instant since) {
        return count("{ 'cardNumber': ?1, 'createdAt': { '$gte': ?2 }, 'deletedAt': null }", cardNumber, since);
    }

    public Uni<CardAuthTransaction> updateStatus(String authTxnId, AuthTxnStatus status) {
        return find("{ '_id': ?1 }", new ObjectId(authTxnId)).firstResult()
                .chain(txn -> {
                    if (txn == null) {
                        return Uni.createFrom().nullItem();
                    }
                    txn.status = status;
                    if (status == AuthTxnStatus.APPROVED) {
                        txn.authorizedAt = Instant.now();
                    } else if (status == AuthTxnStatus.REVERSED) {
                        txn.reversedAt = Instant.now();
                    }
                    txn.setUpdatedAt(Instant.now());
                    return txn.update().map(v -> txn);
                });
    }

    public Uni<CardAuthTransaction> updateRiskScore(String authTxnId, int score) {
        return find("{ '_id': ?1 }", new ObjectId(authTxnId)).firstResult()
                .chain(txn -> {
                    if (txn == null) {
                        return Uni.createFrom().nullItem();
                    }
                    txn.riskScore = score;
                    txn.setUpdatedAt(Instant.now());
                    return txn.update().map(v -> txn);
                });
    }
}
