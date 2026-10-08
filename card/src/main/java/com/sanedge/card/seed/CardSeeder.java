package com.sanedge.card.seed;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;
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
 * Card seeder: demo cards linked to the identity users. Order = 20.
 *
 * <p>Idempotent: upsert on {@code cardNumber}. Requires the identity seeder to
 * have run first (user_id is resolved by username).</p>
 */
public class CardSeeder implements Seeder {

    private static final String[][] CARDS = {
            {"admin", "4111111111111111"},
            {"user", "4111111111111112"}
    };

    @Override
    public String domain() {
        return "card";
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase payment = ctx.db("payment");
        ReactiveMongoDatabase identity = ctx.db("identity");
        ReactiveMongoCollection<Document> cards = payment.getCollection("cards", Document.class);
        ReactiveMongoCollection<Document> users = identity.getCollection("users", Document.class);

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (String[] c : CARDS) {
            final String username = c[0];
            final String cardNumber = c[1];
            chain = chain.chain(() -> users.find(Filters.eq("username", username)).collect().first()
                    .onItem().ifNull().continueWith(() -> new Document("_id", new ObjectId()))
                    .chain(user -> {
                        ObjectId userId = user.getObjectId("_id");
                        Date now = Date.from(Instant.now());
                        return cards.updateOne(
                                Filters.eq("cardNumber", cardNumber),
                                Updates.combine(
                                        Updates.setOnInsert("userId", userId),
                                        Updates.setOnInsert("cardNumber", cardNumber),
                                        Updates.setOnInsert("cardType", "VISA"),
                                        Updates.setOnInsert("expireDate", LocalDate.of(2030, 12, 31)),
                                        Updates.setOnInsert("cvv", "123"),
                                        Updates.setOnInsert("cardProvider", "VISA"),
                                        Updates.setOnInsert("creditLimit", new BigDecimal("5000000")),
                                        Updates.setOnInsert("points", BigDecimal.ZERO),
                                        Updates.setOnInsert("status", "ACTIVE"),
                                        Updates.setOnInsert("createdAt", now),
                                        Updates.setOnInsert("updatedAt", now),
                                        Updates.setOnInsert("deletedAt", null)),
                                new UpdateOptions().upsert(true)).replaceWithVoid();
                    }));
        }
        return chain.invoke(() -> log.infof("Cards seeded: %d cards", CARDS.length));
    }
}
