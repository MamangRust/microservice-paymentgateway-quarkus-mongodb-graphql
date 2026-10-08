package com.sanedge.card.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.sanedge.card.entity.BillingStatement;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@RunOnVertxContext
class BillingStatementRepositoryTest {

    @Inject
    BillingStatementRepository repository;

    private Uni<BillingStatement> persistStatement(String cardNumber, LocalDate statementDate, int billingCycleDay,
            String status) {
        BillingStatement stmt = new BillingStatement();
        stmt.setCardNumber(cardNumber);
        stmt.setStatementDate(statementDate);
        stmt.setBillingCycleDay(billingCycleDay);
        stmt.setStatus(status);
        return repository.persist(stmt).map(s -> s);
    }

    private Uni<Void> clean() {
        return repository.deleteAll().replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberAndStatementDate() {
        LocalDate today = LocalDate.now();
        return clean()
                .chain(() -> persistStatement("CARD-BILL", today, 15, "OPEN"))
                .chain(() -> repository.findByCardNumberAndStatementDate("CARD-BILL", today))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getBillingCycleDay()).isEqualTo(15);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumber_Paginated() {
        return clean()
                .chain(() -> persistStatement("CARD-BILL-2", LocalDate.now(), 10, "OPEN"))
                .chain(() -> persistStatement("CARD-BILL-2", LocalDate.now().plusDays(1), 10, "CLOSED"))
                .chain(() -> repository.findByCardNumber("CARD-BILL-2", 1, 10))
                .invoke(list -> assertThat(list).hasSize(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testCountByCardNumber() {
        return clean()
                .chain(() -> persistStatement("CARD-COUNT", LocalDate.now(), 5, "OPEN"))
                .chain(() -> repository.countByCardNumber("CARD-COUNT"))
                .invoke(count -> assertThat(count).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByBillingCycleDay() {
        return clean()
                .chain(() -> persistStatement("CARD-CYCLE", LocalDate.now(), 20, "OPEN"))
                .chain(() -> persistStatement("CARD-CYCLE", LocalDate.now(), 20, "CLOSED"))
                .chain(() -> persistStatement("CARD-CYCLE", LocalDate.now(), 25, "OPEN"))
                .chain(() -> repository.findByBillingCycleDay(20))
                .invoke(list -> {
                    // Only OPEN with cycle day 20
                    assertThat(list).hasSize(1);
                    assertThat(list.get(0).getBillingCycleDay()).isEqualTo(20);
                })
                .replaceWithVoid();
    }
}
