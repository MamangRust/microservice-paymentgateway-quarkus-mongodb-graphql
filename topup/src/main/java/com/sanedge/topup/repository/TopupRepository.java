package com.sanedge.topup.repository;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.common.enums.Status;
import com.sanedge.topup.entity.Topup;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TopupRepository implements ReactivePanacheMongoRepository<Topup> {

    private static String searchFilter() {
        return "'$or': [ "
                + "{ 'cardNumber': { '$regex': ?1, '$options': 'i' } }, "
                + "{ 'topupMethod': { '$regex': ?1, '$options': 'i' } }, "
                + "{ 'status': { '$regex': ?1, '$options': 'i' } } ]";
    }

    private int pageIndex(int page) {
        return page > 0 ? page - 1 : 0;
    }

    private int pageSize(int size) {
        return size > 0 ? size : 10;
    }

    private String keyword(String search) {
        return (search != null && !search.trim().isEmpty()) ? search.trim() : null;
    }

    private Uni<PagedResult<Topup>> pagedResult(ReactivePanacheQuery<Topup> panacheQuery) {
        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(t -> new PagedResult<>(t.getItem1(), t.getItem2().intValue()));
    }

    public Uni<PagedResult<Topup>> findTopups(String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.descending("topupTime"))
                : find("{ 'deletedAt': null, " + searchFilter() + " }", Sort.descending("topupTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(page), pageSize(size)));
    }

    public Uni<PagedResult<Topup>> findTopupByCard(String cardNumber, String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("topupTime"), cardNumber)
                : find("{ 'deletedAt': null, 'cardNumber': ?1, "
                        + "'$or': [ { 'topupMethod': { '$regex': ?2, '$options': 'i' } }, "
                        + "{ 'status': { '$regex': ?2, '$options': 'i' } } ] }",
                        Sort.descending("topupTime"), cardNumber, Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(page), pageSize(size)));
    }

    public Uni<PagedResult<Topup>> findActiveTopups(String search, int page, int size) {
        return findTopups(search, page, size);
    }

    public Uni<PagedResult<Topup>> findTrashedTopups(String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("topupTime"))
                : find("{ 'deletedAt': { '$ne': null }, " + searchFilter() + " }", Sort.descending("topupTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(page), pageSize(size)));
    }

    public Uni<List<Topup>> findByCardNumber(String cardNumber) {
        return list("{ 'cardNumber': ?1, 'deletedAt': null }", Sort.descending("topupTime"), cardNumber);
    }

    public Uni<Topup> findTopupById(String id) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(id)).firstResult();
    }

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
        return find("{ '_id': ?1 }", new ObjectId(topupId)).firstResult()
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
                .map(count -> count > 0);
    }

    public Uni<Boolean> deleteAllDeleted() {
        return delete("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }
}
