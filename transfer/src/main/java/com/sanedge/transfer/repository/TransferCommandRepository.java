package com.sanedge.transfer.repository;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.common.enums.Status;
import com.sanedge.transfer.entity.Transfer;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransferCommandRepository implements ReactivePanacheMongoRepository<Transfer> {

    public Uni<Transfer> updateTransferAmount(String transferId, Long amount) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transferId)).firstResult()
                .chain(transfer -> {
                    if (transfer != null) {
                        transfer.transferAmount = amount != null ? amount.intValue() : 0;
                        Instant now = Instant.now();
                        transfer.transferTime = now;
                        transfer.setUpdatedAt(now);
                        return transfer.update().map(v -> transfer);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Transfer> updateTransferStatus(String transferId, String status) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transferId)).firstResult()
                .chain(transfer -> {
                    if (transfer != null) {
                        try {
                            transfer.status = Status.valueOf(status.toUpperCase());
                            transfer.setUpdatedAt(Instant.now());
                            return transfer.update().map(v -> transfer);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().item(transfer);
                        }
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Transfer> updateTransfer(Transfer transfer) {
        return transfer.update().map(v -> transfer);
    }

    public Uni<Transfer> trashed(String transferId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transferId)).firstResult()
                .chain(transfer -> {
                    if (transfer != null) {
                        transfer.setDeletedAt(Instant.now());
                        return transfer.update().map(v -> transfer);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Transfer> restore(String transferId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(transferId)).firstResult()
                .chain(transfer -> {
                    if (transfer != null) {
                        transfer.setDeletedAt(null);
                        return transfer.update().map(v -> transfer);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Boolean> deletePermanent(String transferId) {
        return find("{ '_id': ?1 }", new ObjectId(transferId)).firstResult()
                .chain(transfer -> {
                    if (transfer != null) {
                        return transfer.delete().map(v -> true);
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

    public Uni<Boolean> markCompensationRequired(String transferId, String reason) {
        return update("{ '$set': { 'compensationReason': ?1, 'updatedAt': ?2 } }",
                reason, Instant.now())
                .where("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transferId))
                .map(updated -> updated > 0);
    }

    /**
     * Atomically claims the compensation lease for a transfer using a
     * single-document compare-and-set: the update only applies while no live
     * lease is held ({@code compensationWorkerId} null or the lease expired) and
     * the attempt budget is not exhausted. The attempt counter is incremented in
     * the same operation.
     */
    public Uni<Boolean> claimCompensation(String transferId, String workerId, String claimToken,
            Instant now, Instant leaseUntil, int maxAttempts) {
        return update("{ '$set': { 'compensationWorkerId': ?1, 'compensationClaimToken': ?2, "
                + "'compensationLeaseUntil': ?3, 'updatedAt': ?4 }, "
                + "'$inc': { 'compensationAttempts': 1 } }",
                workerId, claimToken, leaseUntil, now)
                .where("{ '_id': ?1, 'deletedAt': null, 'compensationAttempts': { '$lt': ?2 }, "
                        + "'$or': [ { 'compensationWorkerId': null }, { 'compensationLeaseUntil': { '$lt': ?3 } } ] }",
                        new ObjectId(transferId), maxAttempts, now)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> completeCompensation(String transferId, String workerId, String claimToken) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': null, 'status': ?1, 'updatedAt': ?2 } }",
                Status.SUCCESS.name(), Instant.now())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(transferId), claimToken, workerId)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> releaseCompensation(String transferId, String workerId, String claimToken,
            Instant leaseUntil, String reason) {
        return update("{ '$set': { 'compensationWorkerId': null, 'compensationClaimToken': null, "
                + "'compensationLeaseUntil': ?1, 'compensationReason': ?2, 'updatedAt': ?3 } }",
                leaseUntil, reason, Instant.now())
                .where("{ '_id': ?1, 'compensationClaimToken': ?2, 'compensationWorkerId': ?3 }",
                        new ObjectId(transferId), claimToken, workerId)
                .map(updated -> updated > 0);
    }

    public Uni<Boolean> exhaustCompensation(String transferId, int maxAttempts, String reason) {
        return update("{ '$set': { 'compensationAttempts': ?1, 'compensationReason': ?2, "
                + "'status': ?3, 'updatedAt': ?4 } }",
                maxAttempts, reason, Status.FAILED.name(), Instant.now())
                .where("{ '_id': ?1 }", new ObjectId(transferId))
                .map(updated -> updated > 0);
    }
}
