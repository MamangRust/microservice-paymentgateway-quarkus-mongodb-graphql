package com.sanedge.withdraw.repository;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.withdraw.entity.Withdraw;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WithdrawRepository implements ReactivePanacheMongoRepository<Withdraw> {

    private static String searchFilter(String param) {
        return "'$or': [ "
                + "{ 'cardNumber': { '$regex': " + param + ", '$options': 'i' } }, "
                + "{ 'status': { '$regex': " + param + ", '$options': 'i' } } ]";
    }

    private String keyword(String search) {
        return (search != null && !search.trim().isEmpty()) ? search.trim() : null;
    }

    private Uni<PagedResult<Withdraw>> pagedResult(ReactivePanacheQuery<Withdraw> panacheQuery, int page, int size) {
        return Uni.combine().all().unis(
                panacheQuery.page(page, size).list(),
                panacheQuery.count())
                .asTuple()
                .map(t -> new PagedResult<>(t.getItem1(), t.getItem2().intValue()));
    }

    public Uni<PagedResult<Withdraw>> findAllWithdraws(String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.descending("withdrawTime"))
                : find("{ 'deletedAt': null, " + searchFilter("?1") + " }", Sort.descending("withdrawTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery, page, size);
    }

    public Uni<PagedResult<Withdraw>> findActiveWithdraws(String search, int page, int size) {
        return findAllWithdraws(search, page, size);
    }

    public Uni<PagedResult<Withdraw>> findTrashedWithdraws(String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("withdrawTime"))
                : find("{ 'deletedAt': { '$ne': null }, " + searchFilter("?1") + " }", Sort.descending("withdrawTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery, page, size);
    }

    public Uni<List<Withdraw>> findByCardNumber(String cardNumber) {
        return list("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("withdrawTime"), cardNumber);
    }

    public Uni<PagedResult<Withdraw>> findAllByCardNumber(String cardNumber, String search, int page, int size) {
        String keyword = keyword(search);

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("withdrawTime"), cardNumber)
                : find("{ 'deletedAt': null, 'cardNumber': ?1, " + searchFilter("?2") + " }",
                        Sort.descending("withdrawTime"), cardNumber, Pattern.quote(keyword));

        return pagedResult(panacheQuery, page, size);
    }

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
}
