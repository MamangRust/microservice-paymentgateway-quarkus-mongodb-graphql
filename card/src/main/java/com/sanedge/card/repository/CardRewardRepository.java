package com.sanedge.card.repository;

import java.math.BigDecimal;
import java.util.List;

import com.sanedge.card.entity.CardReward;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardRewardRepository implements ReactivePanacheMongoRepository<CardReward> {

    public Uni<List<CardReward>> findByCardNumber(String cardNumber, int page, int size) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", Sort.descending("createdAt"), cardNumber)
                .page(page > 0 ? page - 1 : 0, size > 0 ? size : 10)
                .list();
    }

    public Uni<Long> countByCardNumber(String cardNumber) {
        return count("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber);
    }

    public Uni<BigDecimal> sumPointsByCardNumber(String cardNumber) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null, 'redeemed': false }", cardNumber)
                .list()
                .map(list -> list.stream()
                        .map(r -> r.pointsEarned)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));
    }
}
