package com.sanedge.withdraw.seed;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import org.bson.Document;
import org.jboss.logging.Logger;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.sanedge.common.seed.SeedContext;
import com.sanedge.common.seed.Seeder;

import io.quarkus.mongodb.reactive.ReactiveMongoCollection;
import io.quarkus.mongodb.reactive.ReactiveMongoDatabase;
import io.smallrye.mutiny.Uni;

/**
 * Withdraw seeder: demo withdrawals for the seeded cards. Order = 40.
 *
 * <p>Idempotent: upsert on the natural key {@code (cardNumber, withdrawAmount)}.
 * Requires the card seeder first.</p>
 */
public class WithdrawSeeder implements Seeder {

    @Override
    public String domain() {
        return "withdraw";
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase financial = ctx.db("financial");
        ReactiveMongoCollection<Document> withdraws = financial.getCollection("withdraws", Document.class);

        final String cardNumber = "4111111111111111";
        final int amount = 100_000;

        Date now = Date.from(Instant.now());
        return withdraws.updateOne(
                Filters.and(Filters.eq("cardNumber", cardNumber), Filters.eq("withdrawAmount", amount)),
                Updates.combine(
                        Updates.setOnInsert("withdrawNo", UUID.randomUUID()),
                        Updates.setOnInsert("cardNumber", cardNumber),
                        Updates.setOnInsert("withdrawAmount", amount),
                        Updates.setOnInsert("withdrawTime", now),
                        Updates.setOnInsert("status", "SUCCESS"),
                        Updates.setOnInsert("createdAt", now),
                        Updates.setOnInsert("updatedAt", now),
                        Updates.setOnInsert("deletedAt", null)),
                new UpdateOptions().upsert(true))
                .replaceWithVoid()
                .invoke(() -> log.info("Withdraws seeded: 1 withdraw"));
    }
}
