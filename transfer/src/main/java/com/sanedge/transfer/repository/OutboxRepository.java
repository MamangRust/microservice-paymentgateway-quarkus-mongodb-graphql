package com.sanedge.transfer.repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.sanedge.transfer.entity.Outbox;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutboxRepository implements ReactivePanacheMongoRepository<Outbox> {

    @ConfigProperty(name = "outbox.lease.seconds", defaultValue = "300")
    long leaseSeconds;

    public Uni<Long> countPending(String domain) {
        return count("{ 'status': 'PENDING', 'domain': ?1 }", domain);
    }

    public Uni<List<Outbox>> findDue(String domain, int limit) {
        Instant now = Instant.now();
        Instant leaseCutoff = now.minusSeconds(Math.max(1, leaseSeconds));
        return find("{ 'status': 'PENDING', 'domain': ?1, '$and': [ "
                + "{ '$or': [ { 'nextAttemptAt': null }, { 'nextAttemptAt': { '$lte': ?2 } } ] }, "
                + "{ '$or': [ { 'claimedAt': null }, { 'claimedAt': { '$lt': ?3 } } ] } ] }",
                Sort.ascending("createdAt"), domain, now, leaseCutoff)
                .page(0, limit)
                .list();
    }

    public Uni<Outbox> claim(String domain, Outbox event) {
        Instant now = Instant.now();
        Instant leaseCutoff = now.minusSeconds(Math.max(1, leaseSeconds));
        String claimToken = UUID.randomUUID().toString();
        return update("{ '$set': { 'claimedAt': ?1, 'claimToken': ?2 }, '$inc': { 'attempts': 1 } }",
                now, claimToken)
                .where("{ '_id': ?1, 'domain': ?2, 'status': 'PENDING', '$and': [ "
                        + "{ '$or': [ { 'claimedAt': null }, { 'claimedAt': { '$lt': ?3 } } ] }, "
                        + "{ '$or': [ { 'nextAttemptAt': null }, { 'nextAttemptAt': { '$lte': ?4 } } ] } ] }",
                        event.id, domain, leaseCutoff, now)
                .chain(updated -> updated > 0 ? findById(event.id) : Uni.<Outbox>createFrom().nullItem());
    }

    public Uni<Void> markSent(Outbox event) {
        return update("{ '$set': { 'status': 'SENT', 'claimedAt': null, 'lastError': null } }")
                .where("{ '_id': ?1, 'status': 'PENDING', 'claimToken': ?2 }", event.id, event.getClaimToken())
                .replaceWithVoid();
    }

    public Uni<Void> markRetry(Outbox event, Throwable failure, long delaySeconds) {
        return update("{ '$set': { 'status': 'PENDING', 'claimedAt': null, 'lastError': ?1, 'nextAttemptAt': ?2 } }",
                failure == null ? "unknown failure" : failure.getMessage(),
                Instant.now().plusSeconds(delaySeconds))
                .where("{ '_id': ?1, 'status': 'PENDING', 'claimToken': ?2 }", event.id, event.getClaimToken())
                .replaceWithVoid();
    }
}
