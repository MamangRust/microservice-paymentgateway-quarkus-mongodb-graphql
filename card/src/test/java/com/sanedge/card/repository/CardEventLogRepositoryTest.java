package com.sanedge.card.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;

import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

/**
 * Executable proof that the card event log sink is idempotent under
 * at-least-once Kafka redelivery:
 * <ul>
 * <li>the same (topic, reference_id) is appended only once;</li>
 * <li>events without a stable reference (card.statement.generated) are always
 * appended;</li>
 * <li>distinct references on the same topic are both kept.</li>
 * </ul>
 *
 * <p>These tests block (the {@code @BeforeEach} in {@link MongoTestBase} wipes
 * every collection synchronously), so the class is annotated {@code @Blocking}
 * to run on a worker thread where blocking is permitted.</p>
 *
 * <p>The idempotency of {@code appendIfAbsent} relies on the partial unique
 * index {@code (topic, referenceId)} where {@code referenceId} exists. That
 * index is normally created by the {@code db-migration} module, which is not on
 * the test classpath, so it is created here before each test.</p>
 */
@QuarkusTestResource(MongoDbResource.class)
@QuarkusTest
@Blocking
class CardEventLogRepositoryTest extends MongoTestBase {

    @Inject
    CardEventLogRepository eventLogRepo;

    @Inject
    ReactiveMongoClient reactiveMongoClient;

    @ConfigProperty(name = "quarkus.mongodb.database", defaultValue = "test")
    String databaseName;

    private Uni<Void> clean() {
        return ensureCardEventLogIndex()
                .onItem().transformToUni(ignored -> eventLogRepo.deleteAll().replaceWithVoid());
    }

    private Uni<String> ensureCardEventLogIndex() {
        return reactiveMongoClient.getDatabase(databaseName)
                .getCollection("card_event_logs")
                .createIndex(Indexes.ascending("topic", "referenceId"),
                        new IndexOptions().unique(true)
                                .partialFilterExpression(Filters.exists("referenceId", true)));
    }

    @Test
    void appendIfAbsent_deduplicatesSameTopicReference() {
        clean()
                .chain(() -> eventLogRepo.appendIfAbsent("card.payment.posted", "card.payment.posted",
                        "4111111111111111", "42", "{\"paymentId\":42}"))
                .chain(() -> eventLogRepo.appendIfAbsent("card.payment.posted", "card.payment.posted",
                        "4111111111111111", "42", "{\"paymentId\":42}"))
                .chain(() -> eventLogRepo.count("topic = ?1 AND referenceId = ?2", "card.payment.posted", "42"))
                .invoke(count -> assertThat(count).isEqualTo(1L))
                .await().indefinitely();
    }

    @Test
    void appendIfAbsent_alwaysAppendsWhenReferenceNull() {
        clean()
                .chain(() -> eventLogRepo.appendIfAbsent("card.statement.generated", "card.statement.generated",
                        null, null, "{\"billingCycleDay\":10,\"statementsProcessed\":3}"))
                .chain(() -> eventLogRepo.appendIfAbsent("card.statement.generated", "card.statement.generated",
                        null, null, "{\"billingCycleDay\":10,\"statementsProcessed\":3}"))
                .chain(() -> eventLogRepo.count("topic = ?1", "card.statement.generated"))
                .invoke(count -> assertThat(count).isEqualTo(2L))
                .await().indefinitely();
    }

    @Test
    void appendIfAbsent_keepsDistinctReferencesOnSameTopic() {
        clean()
                .chain(() -> eventLogRepo.appendIfAbsent("card.fraud.alert", "card.fraud.alert",
                        "4111111111111111", "100", "{\"authTxnId\":100}"))
                .chain(() -> eventLogRepo.appendIfAbsent("card.fraud.alert", "card.fraud.alert",
                        "4111111111111111", "101", "{\"authTxnId\":101}"))
                .chain(() -> eventLogRepo.count("topic = ?1", "card.fraud.alert"))
                .invoke(count -> assertThat(count).isEqualTo(2L))
                .await().indefinitely();
    }
}
