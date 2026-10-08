package com.sanedge.topup.seed;

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
 * Topup seeder: demo topups for the seeded cards. Order = 40.
 *
 * <p>Idempotent: upsert on the natural key {@code (cardNumber, topupAmount)}.
 * Requires the card seeder first.</p>
 */
public class TopupSeeder implements Seeder {

    private static final Object[][] TOPUPS = {
            {"4111111111111111", 250_000},
            {"4111111111111112", 100_000}
    };

    @Override
    public String domain() {
        return "topup";
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase financial = ctx.db("financial");
        ReactiveMongoCollection<Document> topups = financial.getCollection("topups", Document.class);

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Object[] t : TOPUPS) {
            final String cardNumber = (String) t[0];
            final int amount = (Integer) t[1];
            chain = chain.chain(() -> {
                Date now = Date.from(Instant.now());
                return topups.updateOne(
                        Filters.and(Filters.eq("cardNumber", cardNumber), Filters.eq("topupAmount", amount)),
                        Updates.combine(
                                Updates.setOnInsert("topupNo", UUID.randomUUID()),
                                Updates.setOnInsert("cardNumber", cardNumber),
                                Updates.setOnInsert("topupAmount", amount),
                                Updates.setOnInsert("topupMethod", "BANK_TRANSFER"),
                                Updates.setOnInsert("topupTime", now),
                                Updates.setOnInsert("status", "SUCCESS"),
                                Updates.setOnInsert("createdAt", now),
                                Updates.setOnInsert("updatedAt", now),
                                Updates.setOnInsert("deletedAt", null)),
                        new UpdateOptions().upsert(true)).replaceWithVoid();
            });
        }
        return chain.invoke(() -> log.infof("Topups seeded: %d topups", TOPUPS.length));
    }
}
