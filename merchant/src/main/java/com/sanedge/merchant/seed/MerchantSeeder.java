package com.sanedge.merchant.seed;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

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
 * Merchant seeder: demo merchants linked to the identity users. Order = 20.
 *
 * <p>Idempotent: upsert on {@code apiKey}. Requires the identity seeder first
 * (user_id is resolved by username).</p>
 */
public class MerchantSeeder implements Seeder {

    private static final String[][] MERCHANTS = {
            {"Admin Merchant", "seed-admin-merchant-key", "admin"},
            {"User Merchant", "seed-user-merchant-key", "user"}
    };

    @Override
    public String domain() {
        return "merchant";
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
        ReactiveMongoCollection<Document> merchants = payment.getCollection("merchants", Document.class);
        ReactiveMongoCollection<Document> users = identity.getCollection("users", Document.class);

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (String[] m : MERCHANTS) {
            final String name = m[0];
            final String apiKey = m[1];
            final String username = m[2];
            chain = chain.chain(() -> users.find(Filters.eq("username", username)).collect().first()
                    .onItem().ifNull().continueWith(() -> new Document("_id", new ObjectId()))
                    .chain(user -> {
                        ObjectId userId = user.getObjectId("_id");
                        Date now = Date.from(Instant.now());
                        return merchants.updateOne(
                                Filters.eq("apiKey", apiKey),
                                Updates.combine(
                                        Updates.setOnInsert("merchantNo", UUID.randomUUID()),
                                        Updates.setOnInsert("name", name),
                                        Updates.setOnInsert("apiKey", apiKey),
                                        Updates.setOnInsert("userId", userId),
                                        Updates.setOnInsert("status", "ACTIVE"),
                                        Updates.setOnInsert("createdAt", now),
                                        Updates.setOnInsert("updatedAt", now),
                                        Updates.setOnInsert("deletedAt", null)),
                                new UpdateOptions().upsert(true)).replaceWithVoid();
                    }));
        }
        return chain.invoke(() -> log.infof("Merchants seeded: %d merchants", MERCHANTS.length));
    }
}
