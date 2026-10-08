package com.sanedge.merchant.repository;

import java.util.List;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.merchant.entity.Merchant;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MerchantRepository implements ReactivePanacheMongoRepository<Merchant> {

    private static final String SEARCH_OR = "{ '$or': ["
            + " { 'name': { '$regex': ?1, '$options': 'i' } },"
            + " { 'apiKey': { '$regex': ?1, '$options': 'i' } },"
            + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }";

    public Uni<Boolean> existsByName(String name) {
        return count("{ 'name': { '$regex': ?1, '$options': 'i' }, 'deletedAt': null }",
                "^" + Pattern.quote(name) + "$").map(c -> c > 0);
    }

    public Uni<PagedResult<Merchant>> findMerchants(String keyword, int page, int size) {
        String kw = (keyword != null && !keyword.trim().isEmpty()) ? keyword.trim() : null;
        ReactivePanacheQuery<Merchant> panacheQuery = kw == null
                ? findAll(Sort.ascending("createdAt"))
                : find(SEARCH_OR, Sort.ascending("createdAt"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<Merchant>> findActiveMerchants(String keyword, int page, int size) {
        String kw = (keyword != null && !keyword.trim().isEmpty()) ? keyword.trim() : null;
        ReactivePanacheQuery<Merchant> panacheQuery = kw == null
                ? find("{ 'deletedAt': null }", Sort.ascending("createdAt"))
                : find("{ 'deletedAt': null, '$or': ["
                        + " { 'name': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'apiKey': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.ascending("createdAt"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<Merchant>> findTrashedMerchants(String keyword, int page, int size) {
        String kw = (keyword != null && !keyword.trim().isEmpty()) ? keyword.trim() : null;
        ReactivePanacheQuery<Merchant> panacheQuery = kw == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("deletedAt"))
                : find("{ 'deletedAt': { '$ne': null }, '$or': ["
                        + " { 'name': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'apiKey': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.descending("deletedAt"), Pattern.quote(kw));
        panacheQuery.page(page, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<Merchant> findMerchantById(String merchantId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(merchantId)).firstResult();
    }

    public Uni<Merchant> findByApiKey(String apiKey) {
        return find("{ 'apiKey': ?1, 'deletedAt': null }", apiKey).firstResult();
    }

    public Uni<Merchant> findByName(String name) {
        return find("{ 'name': ?1, 'deletedAt': null }", name).firstResult();
    }

    public Uni<List<Merchant>> findByUserId(String userId) {
        if (userId == null) {
            return Uni.createFrom().item(List.of());
        }
        return list("{ 'userId': ?1, 'deletedAt': null }", new ObjectId(userId));
    }
}
