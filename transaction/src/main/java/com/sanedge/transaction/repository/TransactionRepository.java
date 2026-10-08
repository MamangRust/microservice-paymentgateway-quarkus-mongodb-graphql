package com.sanedge.transaction.repository;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.transaction.entity.Transaction;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransactionRepository implements ReactivePanacheMongoRepository<Transaction> {

    private static final String SEARCH_OR = "{ '$or': ["
            + " { 'cardNumber': { '$regex': ?1, '$options': 'i' } },"
            + " { 'paymentMethod': { '$regex': ?1, '$options': 'i' } },"
            + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }";

    public Uni<PagedResult<Transaction>> findTransactions(String search, int page, int size) {
        String kw = (search != null && !search.trim().isEmpty()) ? search.trim() : null;
        ReactivePanacheQuery<Transaction> panacheQuery = kw == null
                ? find("{ 'deletedAt': null }", Sort.descending("transactionTime"))
                : find("{ 'deletedAt': null, '$or': ["
                        + " { 'cardNumber': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'paymentMethod': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.descending("transactionTime"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<Transaction>> findActiveTransactions(String search, int page, int size) {
        return findTransactions(search, page, size);
    }

    public Uni<PagedResult<Transaction>> findTrashedTransactions(String search, int page, int size) {
        String kw = (search != null && !search.trim().isEmpty()) ? search.trim() : null;
        ReactivePanacheQuery<Transaction> panacheQuery = kw == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("transactionTime"))
                : find("{ 'deletedAt': { '$ne': null }, '$or': ["
                        + " { 'cardNumber': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'paymentMethod': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.descending("transactionTime"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<Transaction> findTransactionById(String id) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(id)).firstResult();
    }

    public Uni<PagedResult<Transaction>> findTransactionsByCardNumber(String cardNumber, String filter, int page, int size) {
        String kw = (filter != null && !filter.trim().isEmpty()) ? filter.trim() : null;
        ReactivePanacheQuery<Transaction> panacheQuery = kw == null
                ? find("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("transactionTime"), cardNumber)
                : find("{ 'deletedAt': null, 'cardNumber': ?1, '$or': ["
                        + " { 'paymentMethod': { '$regex': ?2, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?2, '$options': 'i' } } ] }",
                        Sort.descending("transactionTime"), cardNumber, Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<List<Transaction>> findTransactionsByMerchantId(String merchantId) {
        if (merchantId == null) {
            return Uni.createFrom().item(List.of());
        }
        return list("{ 'merchantId': ?1, 'deletedAt': null }", Sort.descending("transactionTime"),
                new ObjectId(merchantId));
    }

    public Uni<Transaction> updateTransactionStatus(String transactionId, String status) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(transactionId)).firstResult()
                .chain(transaction -> {
                    if (transaction != null) {
                        try {
                            transaction.status = com.sanedge.common.enums.Status.valueOf(status.toUpperCase());
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
}
