package com.sanedge.transaction.repository;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.common.enums.Status;
import com.sanedge.transaction.entity.Transaction;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransactionCommandRepository implements ReactivePanacheMongoRepository<Transaction> {

    public Uni<Transaction> updateTransactionStatus(String transactionId, String status) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transactionId)).firstResult()
                .chain(transaction -> {
                    if (transaction != null) {
                        try {
                            transaction.status = Status.valueOf(status.toUpperCase());
                            transaction.setUpdatedAt(Instant.now());
                            return transaction.update().map(v -> transaction);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().item(transaction);
                        }
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Transaction> trashed(String transactionId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transactionId)).firstResult()
                .chain(transaction -> {
                    if (transaction != null) {
                        transaction.setDeletedAt(Instant.now());
                        return transaction.update().map(v -> transaction);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Transaction> restore(String transactionId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(transactionId)).firstResult()
                .chain(transaction -> {
                    if (transaction != null) {
                        transaction.setDeletedAt(null);
                        return transaction.update().map(v -> transaction);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Boolean> deletePermanent(String transactionId) {
        return find("{ '_id': ?1 }", new ObjectId(transactionId)).firstResult()
                .chain(transaction -> {
                    if (transaction != null) {
                        return transaction.delete().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> restoreAllDeleted() {
        return update("{ '$set': { 'deletedAt': null } }")
                .where("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }

    public Uni<Boolean> deleteAllDeleted() {
        return delete("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }

    public Uni<Transaction> markCompensationRequired(String transactionId, String reason) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transactionId)).firstResult()
                .chain(txn -> {
                    if (txn != null) {
                        txn.compensationReason = reason;
                        txn.setUpdatedAt(Instant.now());
                        return txn.update().map(v -> txn);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    /**
     * Atomically claims the compensation lease for a transaction using a
     * single-document compare-and-set: the update only applies while no live
     * lease is held ({@code compensationWorkerId} null or the lease expired) and
     * the attempt budget is not exhausted. The attempt counter is incremented in
     * the same operation.
     */
    public Uni<Boolean> claimCompensation(String transactionId, String workerId, String claimToken,
            Instant now, Instant leaseUntil, int maxAttempts) {
        return update("{ '$set': { 'compensationWorkerId': ?1, 'compensationClaimToken': ?2, "
                + "'compensationLeaseUntil': ?3, 'updatedAt': ?4 }, "
                + "'$inc': { 'compensationAttempts': 1 } }",
                workerId, claimToken, leaseUntil, now)
                .where("{ '_id': ?1, 'deletedAt': null, 'compensationAttempts': { '$lt': ?2 }, "
                        + "'$or': [ { 'compensationWorkerId': null }, { 'compensationLeaseUntil': { '$lt': ?3 } } ] }",
                        new ObjectId(transactionId), maxAttempts, now)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> completeCompensation(String transactionId, String workerId, String claimToken) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': null, 'status': ?1, 'updatedAt': ?2 } }",
                Status.SUCCESS.name(), Instant.now())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(transactionId), claimToken, workerId)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> releaseCompensation(String transactionId, String workerId, String claimToken,
            Instant leaseUntil, String reason) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': ?1, 'compensationReason': ?2, 'updatedAt': ?3 } }",
                leaseUntil, reason, Instant.now())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(transactionId), claimToken, workerId)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> exhaustCompensation(String transactionId, int maxAttempts, String reason) {
        return update("{ '$set': { 'compensationAttempts': ?1, 'compensationReason': ?2, "
                + "'status': ?3, 'updatedAt': ?4 } }",
                maxAttempts, reason, Status.FAILED.name(), Instant.now())
                .where("{ '_id': ?1 }", new ObjectId(transactionId))
                .map(updated -> updated > 0);
    }
}
