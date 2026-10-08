package com.sanedge.topup.repository;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.common.enums.Status;
import com.sanedge.topup.entity.Topup;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TopupCommandRepository implements ReactivePanacheMongoRepository<Topup> {

    public Uni<Topup> updateTopupAmount(String topupId, Long amount) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(topupId)).firstResult()
                .chain(topup -> {
                    if (topup != null) {
                        topup.topupAmount = amount != null ? amount.intValue() : 0;
                        topup.setUpdatedAt(Instant.now());
                        return topup.update().map(v -> topup);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Topup> updateTopupStatus(String topupId, String status) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(topupId)).firstResult()
                .chain(topup -> {
                    if (topup != null) {
                        try {
                            topup.status = Status.valueOf(status.toUpperCase());
                            topup.setUpdatedAt(Instant.now());
                            return topup.update().map(v -> topup);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().item(topup);
                        }
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Topup> trashed(String topupId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(topupId)).firstResult()
                .chain(topup -> {
                    if (topup != null) {
                        topup.setDeletedAt(Instant.now());
                        return topup.update().map(v -> topup);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Topup> restore(String topupId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(topupId)).firstResult()
                .chain(topup -> {
                    if (topup != null) {
                        topup.setDeletedAt(null);
                        return topup.update().map(v -> topup);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Boolean> deletePermanent(String topupId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(topupId)).firstResult()
                .chain(topup -> {
                    if (topup != null) {
                        return topup.delete().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
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

    /**
     * Single-document compare-and-swap claim of a compensation record. Only one
     * worker can win the lease: the update is applied only when the record is not
     * deleted, has not exhausted its attempts, and is either unclaimed or its
     * lease has expired. {@code compensationAttempts} is incremented atomically.
     */
    public Uni<Boolean> claimCompensation(String topupId, String workerId, String claimToken,
            Instant now, Instant leaseUntil, int maxAttempts) {
        return update("{ '$set': { 'compensationWorkerId': ?1, 'compensationClaimToken': ?2, "
                + "'compensationLeaseUntil': ?3, 'updatedAt': ?4 }, '$inc': { 'compensationAttempts': 1 } }",
                workerId, claimToken, leaseUntil, now)
                .where("{ '_id': ?1, 'deletedAt': null, 'compensationAttempts': { '$lt': ?2 }, '$or': [ "
                        + "{ 'compensationWorkerId': null }, "
                        + "{ 'compensationLeaseUntil': { '$lt': ?3 } } ] }",
                        new ObjectId(topupId), maxAttempts, now)
                .map(updatedCount -> updatedCount > 0);
    }

    public Uni<Boolean> completeCompensation(String topupId, String workerId, String claimToken) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': null, 'status': ?2, 'updatedAt': ?1 } }",
                Instant.now(), Status.SUCCESS.name())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(topupId), claimToken, workerId)
                .map(updatedCount -> updatedCount > 0);
    }

    public Uni<Boolean> releaseCompensation(String topupId, String workerId, String claimToken,
            Instant leaseUntil, String reason) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': ?1, 'compensationReason': ?2, 'updatedAt': ?3 } }",
                leaseUntil, reason, Instant.now())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(topupId), claimToken, workerId)
                .map(updatedCount -> updatedCount > 0);
    }

    public Uni<Boolean> exhaustCompensation(String topupId, int maxAttempts, String reason) {
        return update("{ '$set': { 'compensationAttempts': ?1, 'compensationReason': ?2, 'status': ?3, "
                + "'updatedAt': ?4 } }",
                maxAttempts, reason, Status.FAILED.name(), Instant.now())
                .where("{ '_id': ?1 }", new ObjectId(topupId))
                .map(updatedCount -> updatedCount > 0);
    }
}
