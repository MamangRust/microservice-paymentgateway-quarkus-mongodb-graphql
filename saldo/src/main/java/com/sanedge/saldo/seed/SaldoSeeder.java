package com.sanedge.saldo.seed;

import java.time.Instant;
import java.util.Date;

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
 * Saldo seeder: opening balances for the seeded cards. Order = 30.
 *
 * <p>Idempotent: upsert on {@code cardNumber}. Requires the card seeder first.</p>
 */
public class SaldoSeeder implements Seeder {

    private static final Object[][] SALDOS = {
            {"4111111111111111", 1_000_000},
            {"4111111111111112", 500_000}
    };

    @Override
    public String domain() {
        return "saldo";
    }

    @Override
    public int order() {
        return 30;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase payment = ctx.db("payment");
        ReactiveMongoCollection<Document> saldos = payment.getCollection("saldos", Document.class);

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Object[] s : SALDOS) {
            final String cardNumber = (String) s[0];
            final int totalBalance = (Integer) s[1];
            chain = chain.chain(() -> {
                Date now = Date.from(Instant.now());
                return saldos.updateOne(
                        Filters.eq("cardNumber", cardNumber),
                        Updates.combine(
                                Updates.setOnInsert("cardNumber", cardNumber),
                                Updates.setOnInsert("totalBalance", totalBalance),
                                Updates.setOnInsert("withdrawAmount", 0),
                                Updates.setOnInsert("createdAt", now),
                                Updates.setOnInsert("updatedAt", now),
                                Updates.setOnInsert("deletedAt", null)),
                        new UpdateOptions().upsert(true)).replaceWithVoid();
            });
        }
        return chain.invoke(() -> log.infof("Saldos seeded: %d balances", SALDOS.length));
    }
}
