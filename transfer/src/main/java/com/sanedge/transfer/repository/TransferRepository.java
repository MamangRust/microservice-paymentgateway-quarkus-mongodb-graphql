package com.sanedge.transfer.repository;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.transfer.entity.Transfer;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransferRepository implements ReactivePanacheMongoRepository<Transfer> {

    private static final String SEARCH_OR = " { 'transferFrom': { '$regex': ?1, '$options': 'i' } },"
            + " { 'transferTo': { '$regex': ?1, '$options': 'i' } },"
            + " { 'status': { '$regex': ?1, '$options': 'i' } }";

    public Uni<PagedResult<Transfer>> findTransfers(String search, int page, int size) {
        String kw = (search != null && !search.trim().isEmpty()) ? search.trim() : null;
        ReactivePanacheQuery<Transfer> panacheQuery = kw == null
                ? find("{ 'deletedAt': null }", Sort.descending("transferTime"))
                : find("{ 'deletedAt': null, '$or': [" + SEARCH_OR + "] }",
                        Sort.descending("transferTime"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<Transfer>> findActiveTransfers(String search, int page, int size) {
        return findTransfers(search, page, size);
    }

    public Uni<PagedResult<Transfer>> findTrashedTransfers(String search, int page, int size) {
        String kw = (search != null && !search.trim().isEmpty()) ? search.trim() : null;
        ReactivePanacheQuery<Transfer> panacheQuery = kw == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("transferTime"))
                : find("{ 'deletedAt': { '$ne': null }, '$or': [" + SEARCH_OR + "] }",
                        Sort.descending("transferTime"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<Transfer> findTransferById(String id) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(id)).firstResult();
    }

    public Uni<List<Transfer>> findTransfersByCardNumber(String cardNumber) {
        return list("{ 'deletedAt': null, '$or': [ { 'transferFrom': ?1 }, { 'transferTo': ?1 } ] }",
                Sort.descending("transferTime"), cardNumber);
    }

    public Uni<List<Transfer>> findTransfersBySourceCard(String cardNumber) {
        return list("{ 'deletedAt': null, 'transferFrom': ?1 }", Sort.descending("transferTime"), cardNumber);
    }

    public Uni<List<Transfer>> findTransfersByDestinationCard(String cardNumber) {
        return list("{ 'deletedAt': null, 'transferTo': ?1 }", Sort.descending("transferTime"), cardNumber);
    }

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
                            transfer.status = com.sanedge.common.enums.Status.valueOf(status.toUpperCase());
                            transfer.setUpdatedAt(Instant.now());
                            return transfer.update().map(v -> transfer);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().item(transfer);
                        }
                    }
                    return Uni.createFrom().nullItem();
                });
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
}
