package com.sanedge.card.repository;

import java.time.LocalDate;
import java.util.List;

import com.sanedge.card.entity.BillingStatement;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BillingStatementRepository implements ReactivePanacheMongoRepository<BillingStatement> {

    public Uni<BillingStatement> findByCardNumberAndStatementDate(String cardNumber, LocalDate statementDate) {
        return find("{ 'cardNumber': ?1, 'statementDate': ?2, 'deletedAt': null }", cardNumber, statementDate)
                .firstResult();
    }

    public Uni<List<BillingStatement>> findByCardNumber(String cardNumber, int page, int size) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .page(page > 0 ? page - 1 : 0, size > 0 ? size : 10)
                .list();
    }

    public Uni<Long> countByCardNumber(String cardNumber) {
        return count("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber);
    }

    public Uni<List<BillingStatement>> findByBillingCycleDay(int billingCycleDay) {
        return find("{ 'billingCycleDay': ?1, 'deletedAt': null, 'status': 'OPEN' }", billingCycleDay).list();
    }
}
