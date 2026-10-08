package com.sanedge.merchant.repository;

import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.merchant.domain.requests.FindAllMerchantDocuments;
import com.sanedge.merchant.entity.MerchantDocument;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MerchantDocumentQueryRepository implements ReactivePanacheMongoRepository<MerchantDocument> {

    private static final String SEARCH_OR = "{ '$or': ["
            + " { 'documentType': { '$regex': ?1, '$options': 'i' } },"
            + " { 'documentUrl': { '$regex': ?1, '$options': 'i' } },"
            + " { 'status': { '$regex': ?1, '$options': 'i' } },"
            + " { 'note': { '$regex': ?1, '$options': 'i' } } ] }";

    public Uni<PagedResult<MerchantDocument>> findDocuments(FindAllMerchantDocuments req) {
        int pageIndex = req.getPage() != null && req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() != null && req.getPageSize() > 0 ? req.getPageSize() : 10;
        String keyword = (req.getSearch() != null && !req.getSearch().trim().isEmpty()) ? req.getSearch().trim() : null;

        ReactivePanacheQuery<MerchantDocument> panacheQuery = keyword == null
                ? findAll(Sort.ascending("createdAt"))
                : find(SEARCH_OR, Sort.ascending("createdAt"), Pattern.quote(keyword));
        panacheQuery.page(pageIndex, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<MerchantDocument>> findActiveDocuments(FindAllMerchantDocuments req) {
        int pageIndex = req.getPage() != null && req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() != null && req.getPageSize() > 0 ? req.getPageSize() : 10;
        String keyword = (req.getSearch() != null && !req.getSearch().trim().isEmpty()) ? req.getSearch().trim() : null;

        ReactivePanacheQuery<MerchantDocument> panacheQuery = keyword == null
                ? find("{ 'deletedAt': null }", Sort.ascending("createdAt"))
                : find("{ 'deletedAt': null, '$or': ["
                        + " { 'documentType': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'documentUrl': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'note': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.ascending("createdAt"), Pattern.quote(keyword));
        panacheQuery.page(pageIndex, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<PagedResult<MerchantDocument>> findTrashedDocuments(FindAllMerchantDocuments req) {
        int pageIndex = req.getPage() != null && req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() != null && req.getPageSize() > 0 ? req.getPageSize() : 10;
        String keyword = (req.getSearch() != null && !req.getSearch().trim().isEmpty()) ? req.getSearch().trim() : null;

        ReactivePanacheQuery<MerchantDocument> panacheQuery = keyword == null
                ? find("{ 'deletedAt': { '$ne': null } }", Sort.descending("deletedAt"))
                : find("{ 'deletedAt': { '$ne': null }, '$or': ["
                        + " { 'documentType': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'documentUrl': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'status': { '$regex': ?1, '$options': 'i' } },"
                        + " { 'note': { '$regex': ?1, '$options': 'i' } } ] }",
                        Sort.descending("deletedAt"), Pattern.quote(keyword));
        panacheQuery.page(pageIndex, size);

        return Uni.combine().all().unis(panacheQuery.list(), panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<MerchantDocument> findDocumentById(String documentId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(documentId)).firstResult();
    }
}
