package com.sanedge.withdraw.repository;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.withdraw.entity.Withdraw;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WithdrawCommandRepository implements ReactivePanacheMongoRepository<Withdraw> {

    public Uni<Withdraw> updateStatus(String withdrawId, String status) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(withdrawId)).firstResult()
                .chain(withdraw -> {
                    if (withdraw != null) {
                        try {
                            withdraw.status = com.sanedge.common.enums.Status.valueOf(status.toUpperCase());
                            withdraw.setUpdatedAt(Instant.now());
                            return withdraw.update().map(v -> withdraw);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().item(withdraw);
                        }
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Withdraw> trashed(String withdrawId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(withdrawId)).firstResult()
                .chain(withdraw -> {
                    if (withdraw != null) {
                        withdraw.setDeletedAt(Instant.now());
                        return withdraw.update().map(v -> withdraw);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Withdraw> restore(String withdrawId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(withdrawId)).firstResult()
                .chain(withdraw -> {
                    if (withdraw != null) {
                        withdraw.setDeletedAt(null);
                        return withdraw.update().map(v -> withdraw);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Boolean> deletePermanent(String withdrawId) {
        return find("{ '_id': ?1 }", new ObjectId(withdrawId)).firstResult()
                .chain(withdraw -> {
                    if (withdraw != null) {
                        return withdraw.delete().map(v -> true);
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

    public Uni<Boolean> markCompensationRequired(String withdrawId, String reason) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(withdrawId)).firstResult()
                .chain(w -> {
                    if (w != null) {
                        w.compensationAttempts = 1;
                        w.compensationReason = reason;
                        w.compensationWorkerId = null;
                        w.compensationClaimToken = null;
                        w.compensationLeaseUntil = null;
                        w.setUpdatedAt(Instant.now());
                        return w.update().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> claimCompensation(String withdrawId, String workerId, String claimToken,
            Instant now, Instant leaseUntil, int maxAttempts) {
        return find("{ '_id': ?1, '$or': [ { 'compensationWorkerId': null }, "
                + "{ 'compensationLeaseUntil': { '$lt': ?2 } } ] }",
                new ObjectId(withdrawId), now).firstResult()
                .chain(w -> {
                    if (w != null) {
                        int attempts = w.compensationAttempts != null ? w.compensationAttempts : 0;
                        if (attempts >= maxAttempts) return Uni.createFrom().item(false);
                        // Only bump the attempt counter when reclaiming an already-claimed
                        // record (i.e. its previous lease expired). The first claim merely
                        // establishes the lease and keeps the counter set by markCompensationRequired.
                        boolean reclaim = w.compensationWorkerId != null;
                        w.compensationWorkerId = workerId;
                        w.compensationClaimToken = claimToken;
                        w.compensationLeaseUntil = leaseUntil;
                        if (reclaim) {
                            w.compensationAttempts = attempts + 1;
                        } else if (attempts < 1) {
                            w.compensationAttempts = 1;
                        }
                        w.setUpdatedAt(Instant.now());
                        return w.update().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> completeCompensation(String withdrawId, String workerId, String claimToken) {
        return find("{ '_id': ?1, 'compensationClaimToken': ?2 }",
                new ObjectId(withdrawId), claimToken).firstResult()
                .chain(w -> {
                    if (w != null && workerId.equals(w.compensationWorkerId)) {
                        w.compensationWorkerId = null;
                        w.compensationClaimToken = null;
                        w.compensationLeaseUntil = null;
                        w.status = com.sanedge.common.enums.Status.COMPENSATED;
                        w.setUpdatedAt(Instant.now());
                        return w.update().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> releaseCompensation(String withdrawId, String workerId, String claimToken,
            Instant leaseUntil, String reason) {
        return find("{ '_id': ?1, 'compensationClaimToken': ?2 }",
                new ObjectId(withdrawId), claimToken).firstResult()
                .chain(w -> {
                    if (w != null && workerId.equals(w.compensationWorkerId)) {
                        w.compensationWorkerId = null;
                        w.compensationClaimToken = null;
                        w.compensationLeaseUntil = leaseUntil;
                        w.compensationReason = reason;
                        w.setUpdatedAt(Instant.now());
                        return w.update().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> exhaustCompensation(String withdrawId, int maxAttempts, String reason) {
        return find("{ '_id': ?1 }", new ObjectId(withdrawId)).firstResult()
                .chain(w -> {
                    if (w != null) {
                        w.compensationAttempts = maxAttempts;
                        w.compensationReason = reason;
                        w.status = com.sanedge.common.enums.Status.FAILED;
                        w.setUpdatedAt(Instant.now());
                        return w.update().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }
}
