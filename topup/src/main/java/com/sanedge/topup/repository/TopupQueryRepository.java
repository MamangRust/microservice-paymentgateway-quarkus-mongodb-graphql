package com.sanedge.topup.repository;

import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.topup.domain.requests.FindAllTopups;
import com.sanedge.topup.domain.requests.FindAllTopupsByCardNumber;
import com.sanedge.topup.entity.Topup;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TopupQueryRepository implements ReactivePanacheMongoRepository<Topup> {

    private static String searchFilter(String param) {
        return "'$or': [ "
                + "{ 'cardNumber': { '$regex': " + param + ", '$options': 'i' } }, "
                + "{ 'topupMethod': { '$regex': " + param + ", '$options': 'i' } }, "
                + "{ 'status': { '$regex': " + param + ", '$options': 'i' } } ]";
    }

    private int pageIndex(Integer page) {
        return (page != null && page > 0) ? page - 1 : 0;
    }

    private int pageSize(Integer size) {
        return (size != null && size > 0) ? size : 10;
    }

    private String keyword(String search) {
        return (search != null && !search.trim().isEmpty()) ? search.trim() : null;
    }

    private Uni<PagedResult<Topup>> pagedResult(ReactivePanacheQuery<Topup> panacheQuery) {
        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(t -> new PagedResult<>(t.getItem1(), t.getItem2().intValue()));
    }

    public Uni<PagedResult<Topup>> findTopups(FindAllTopups req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.descending("topupTime"))
                : find("{ 'deletedAt': null, " + searchFilter("?1") + " }", Sort.descending("topupTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<PagedResult<Topup>> findTopupByCard(FindAllTopupsByCardNumber req) {
        String cardNumber = req.getCardNumber();
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("topupTime"), cardNumber)
                : find("{ 'deletedAt': null, 'cardNumber': ?1, " + searchFilter("?2") + " }",
                        Sort.descending("topupTime"), cardNumber, Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<PagedResult<Topup>> findActiveTopups(FindAllTopups req) {
        return findTopups(req);
    }

    public Uni<PagedResult<Topup>> findTrashedTopups(FindAllTopups req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Topup> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("topupTime"))
                : find("{ 'deletedAt': { '$ne': null }, " + searchFilter("?1") + " }", Sort.descending("topupTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<List<Topup>> findByCardNumber(String cardNumber) {
        return list("{ 'cardNumber': ?1, 'deletedAt': null }", Sort.descending("topupTime"), cardNumber);
    }

    public Uni<Topup> findTopupById(String id) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(id)).firstResult();
    }

    public Uni<List<Topup>> findPendingCompensation(int maxAttempts) {
        return find("{ 'deletedAt': null, '$or': [ { 'compensationAttempts': null }, "
                + "{ 'compensationAttempts': { '$lt': ?1 } } ] }", maxAttempts).list();
    }
}
