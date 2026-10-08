package com.sanedge.transaction.repository;

import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.transaction.domain.requests.FindAllTransactionCardNumber;
import com.sanedge.transaction.domain.requests.FindAllTransactions;
import com.sanedge.transaction.entity.Transaction;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransactionQueryRepository implements ReactivePanacheMongoRepository<Transaction> {

    public Uni<PagedResult<Transaction>> findTransactions(FindAllTransactions req) {
        String search = req.getSearch();
        int page = req.getPage();
        int size = req.getPageSize();
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

    public Uni<PagedResult<Transaction>> findActiveTransactions(FindAllTransactions req) {
        return findTransactions(req);
    }

    public Uni<PagedResult<Transaction>> findTrashedTransactions(FindAllTransactions req) {
        String search = req.getSearch();
        int page = req.getPage();
        int size = req.getPageSize();
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

    public Uni<PagedResult<Transaction>> findTransactionsByCardNumber(FindAllTransactionCardNumber req) {
        String cardNumber = req.getCardNumber();
        String filter = req.getSearch();
        int page = req.getPage();
        int size = req.getPageSize();
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

    public Uni<List<Transaction>> findPendingCompensation(int maxAttempts) {
        return find("{ 'compensationAttempts': { '$lt': ?1 }, 'deletedAt': null }", maxAttempts).list();
    }
}
