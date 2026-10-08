package com.sanedge.card.repository;

import java.time.Instant;
import java.util.List;

import org.bson.types.ObjectId;

import com.sanedge.card.entity.CardPayment;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CardPaymentRepository implements ReactivePanacheMongoRepository<CardPayment> {

    public Uni<List<CardPayment>> findByCardNumber(String cardNumber, int page, int size) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .page(page > 0 ? page - 1 : 0, size > 0 ? size : 10)
                .list();
    }

    public Uni<Long> countByCardNumber(String cardNumber) {
        return count("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber);
    }

    public Uni<CardPayment> completePayment(String paymentId) {
        return find("{ '_id': ?1 }", new ObjectId(paymentId)).firstResult()
                .chain(payment -> {
                    if (payment == null) {
                        return Uni.createFrom().nullItem();
                    }
                    payment.status = "COMPLETED";
                    payment.paidAt = Instant.now();
                    payment.setUpdatedAt(Instant.now());
                    return payment.update().map(v -> payment);
                });
    }
}
