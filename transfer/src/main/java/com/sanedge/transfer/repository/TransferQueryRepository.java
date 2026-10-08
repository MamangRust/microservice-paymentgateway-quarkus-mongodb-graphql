package com.sanedge.transfer.repository;

import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.transfer.domain.requests.FindAllTransfers;
import com.sanedge.transfer.entity.Transfer;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransferQueryRepository implements ReactivePanacheMongoRepository<Transfer> {

    private static final String SEARCH_OR = " { 'transferFrom': { '$regex': ?1, '$options': 'i' } },"
            + " { 'transferTo': { '$regex': ?1, '$options': 'i' } },"
            + " { 'status': { '$regex': ?1, '$options': 'i' } }";

    public Uni<PagedResult<Transfer>> findTransfers(FindAllTransfers req) {
        String search = req.getSearch() != null && !req.getSearch().isEmpty() ? req.getSearch() : null;
        int page = req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() > 0 ? req.getPageSize() : 10;
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

    public Uni<PagedResult<Transfer>> findActiveTransfers(FindAllTransfers req) {
        return findTransfers(req);
    }

    public Uni<PagedResult<Transfer>> findTrashedTransfers(FindAllTransfers req) {
        String search = req.getSearch() != null && !req.getSearch().isEmpty() ? req.getSearch() : null;
        int page = req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() > 0 ? req.getPageSize() : 10;
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

    public Uni<List<Transfer>> findPendingCompensation(int maxAttempts) {
        return find("{ 'compensationAttempts': { '$lt': ?1 }, 'deletedAt': null }", maxAttempts).list();
    }
}
