package com.sanedge.card.repository;

import java.util.Optional;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.card.domain.requests.FindAllCards;
import com.sanedge.card.entity.Card;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardQueryRepository implements ReactivePanacheMongoRepository<Card> {

    public Uni<PagedResult<Card>> findCards(FindAllCards req) {
        return findPage(false, req);
    }

    public Uni<PagedResult<Card>> findActiveCards(FindAllCards req) {
        return findPage(false, req);
    }

    public Uni<PagedResult<Card>> findTrashedCards(FindAllCards req) {
        return findPage(true, req);
    }

    private Uni<PagedResult<Card>> findPage(boolean trashed, FindAllCards req) {
        String keyword = (req.getSearch() != null && !req.getSearch().isEmpty()) ? req.getSearch() : null;
        int page = req.getPage() != null && req.getPage() > 0 ? req.getPage() - 1 : 0;
        int size = req.getPageSize() != null && req.getPageSize() > 0 ? req.getPageSize() : 10;

        String deletedFilter = trashed ? "'deletedAt': { '$ne': null }" : "'deletedAt': null";

        ReactivePanacheQuery<Card> panacheQuery;
        if (keyword == null) {
            panacheQuery = find("{ " + deletedFilter + " }");
        } else {
            String quoted = Pattern.quote(keyword);
            panacheQuery = find("{ " + deletedFilter
                    + ", '$or': [ { 'cardNumber': { '$regex': ?1, '$options': 'i' } }, "
                    + "{ 'cardType': { '$regex': ?2, '$options': 'i' } }, "
                    + "{ 'cardProvider': { '$regex': ?3, '$options': 'i' } } ] }",
                    quoted, quoted, quoted);
        }

        return Uni.combine().all().unis(
                panacheQuery.page(page, size).list(),
                panacheQuery.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    public Uni<Optional<Card>> findCardById(String cardId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(cardId)).firstResult()
                .map(Optional::ofNullable);
    }

    public Uni<Optional<Card>> findCardByUserId(String userId) {
        if (userId == null) {
            return Uni.createFrom().item(Optional.empty());
        }
        return find("{ 'userId': ?1, 'deletedAt': null }", new ObjectId(userId)).firstResult()
                .map(Optional::ofNullable);
    }

    public Uni<Optional<Card>> findCardByCardNumber(String cardNumber) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber).firstResult()
                .map(Optional::ofNullable);
    }

    public Uni<Card> findByCardNumber(String cardNumber) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber).firstResult();
    }
}
