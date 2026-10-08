package com.sanedge.saldo.repository;

import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.saldo.domain.requests.FindAllSaldos;
import com.sanedge.saldo.entity.Saldo;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SaldoQueryRepository implements ReactivePanacheMongoRepository<Saldo> {

    private String searchFilter(String param) {
        return "'cardNumber': { '$regex': " + param + ", '$options': 'i' }";
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

    private Uni<PagedResult<Saldo>> pagedResult(ReactivePanacheQuery<Saldo> panacheQuery) {
        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(t -> new PagedResult<>(t.getItem1(), t.getItem2().intValue()));
    }

    public Uni<PagedResult<Saldo>> findSaldos(FindAllSaldos req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Saldo> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.ascending("_id"))
                : find("{ 'deletedAt': null, " + searchFilter("?1") + " }", Sort.ascending("_id"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }

    public Uni<Saldo> findByCardNumber(String cardNumber) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber).firstResult();
    }

    public Uni<Saldo> findById(String saldoId) {
        return find("{ '_id': ?1 }", new ObjectId(saldoId)).firstResult();
    }

    public Uni<PagedResult<Saldo>> findActiveSaldos(FindAllSaldos req) {
        return findSaldos(req);
    }

    public Uni<PagedResult<Saldo>> findTrashedSaldos(FindAllSaldos req) {
        String keyword = keyword(req.getSearch());

        ReactivePanacheQuery<Saldo> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.ascending("_id"))
                : find("{ 'deletedAt': { '$ne': null }, " + searchFilter("?1") + " }", Sort.ascending("_id"),
                        Pattern.quote(keyword));

        return pagedResult(panacheQuery.page(pageIndex(req.getPage()), pageSize(req.getPageSize())));
    }
}
