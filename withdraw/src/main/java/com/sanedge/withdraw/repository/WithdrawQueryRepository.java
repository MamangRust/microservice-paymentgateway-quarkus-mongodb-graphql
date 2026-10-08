package com.sanedge.withdraw.repository;

import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.withdraw.domain.requests.FindAllWithdrawCardNumber;
import com.sanedge.withdraw.domain.requests.FindAllWithdraws;
import com.sanedge.withdraw.entity.Withdraw;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WithdrawQueryRepository implements ReactivePanacheMongoRepository<Withdraw> {

    private static String searchFilter(String param) {
        return "'$or': [ "
                + "{ 'cardNumber': { '$regex': " + param + ", '$options': 'i' } }, "
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

    private Uni<PagedResult<Withdraw>> pagedResult(ReactivePanacheQuery<Withdraw> panacheQuery) {
        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(t -> new PagedResult<>(t.getItem1(), t.getItem2().intValue()));
    }

    public Uni<PagedResult<Withdraw>> findAllWithdraws(FindAllWithdraws req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.descending("withdrawTime"))
                : find("{ 'deletedAt': null, " + searchFilter("?1") + " }", Sort.descending("withdrawTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<PagedResult<Withdraw>> findActiveWithdraws(FindAllWithdraws req) {
        return findAllWithdraws(req);
    }

    public Uni<PagedResult<Withdraw>> findTrashedWithdraws(FindAllWithdraws req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("withdrawTime"))
                : find("{ 'deletedAt': { '$ne': null }, " + searchFilter("?1") + " }", Sort.descending("withdrawTime"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<List<Withdraw>> findByCardNumber(String cardNumber) {
        return list("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("withdrawTime"), cardNumber);
    }

    public Uni<PagedResult<Withdraw>> findAllByCardNumber(FindAllWithdrawCardNumber req) {
        String cardNumber = req.getCardNumber();
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Withdraw> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null, 'cardNumber': ?1 }", Sort.descending("withdrawTime"), cardNumber)
                : find("{ 'deletedAt': null, 'cardNumber': ?1, " + searchFilter("?2") + " }",
                        Sort.descending("withdrawTime"), cardNumber, Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<Withdraw> findById(String withdrawId) {
        return find("{ '_id': ?1 }", new ObjectId(withdrawId)).firstResult();
    }

    public Uni<List<Withdraw>> findPendingCompensation(int maxAttempts) {
        // Candidates are records that have been marked for compensation (attempts >= 1),
        // are not yet terminal (still PENDING), and have not been hard-deleted.
        // Exhausted records are flipped to FAILED by exhaustCompensation, so they are
        // excluded by the status filter regardless of the per-record max attempts.
        return find("{ 'compensationAttempts': { '$gte': 1 }, 'status': ?1, 'deletedAt': null }",
                com.sanedge.common.enums.Status.PENDING.name()).list();
    }
}
