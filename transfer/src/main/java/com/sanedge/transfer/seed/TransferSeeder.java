package com.sanedge.transfer.seed;

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
 * Transfer seeder: demo fund transfers between the seeded cards. Order = 40.
 *
 * <p>Idempotent: upsert on the natural key
 * {@code (transferFrom, transferTo, transferAmount)}. Requires the card seeder
 * first.</p>
 */
public class TransferSeeder implements Seeder {

    @Override
    public String domain() {
        return "transfer";
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase financial = ctx.db("financial");
        ReactiveMongoCollection<Document> transfers = financial.getCollection("transfers", Document.class);

        final String from = "4111111111111111";
        final String to = "4111111111111112";
        final int amount = 50_000;

        Date now = Date.from(Instant.now());
        return transfers.updateOne(
                Filters.and(Filters.eq("transferFrom", from),
                        Filters.eq("transferTo", to),
                        Filters.eq("transferAmount", amount)),
                Updates.combine(
                        Updates.setOnInsert("transferNo", UUID.randomUUID()),
                        Updates.setOnInsert("transferFrom", from),
                        Updates.setOnInsert("transferTo", to),
                        Updates.setOnInsert("transferAmount", amount),
                        Updates.setOnInsert("transferTime", now),
                        Updates.setOnInsert("status", "SUCCESS"),
                        Updates.setOnInsert("createdAt", now),
                        Updates.setOnInsert("updatedAt", now),
                        Updates.setOnInsert("deletedAt", null)),
                new UpdateOptions().upsert(true))
                .replaceWithVoid()
                .invoke(() -> log.info("Transfers seeded: 1 transfer"));
    }
}
