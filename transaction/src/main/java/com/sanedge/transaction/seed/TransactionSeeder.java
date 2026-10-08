package com.sanedge.transaction.seed;

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
 * Transaction seeder: demo purchase transactions for the seeded cards, linked
 * to the seeded merchants. Order = 40.
 *
 * <p>Idempotent: upsert on the natural key
 * {@code (cardNumber, amount, paymentMethod)}. Requires the card and merchant
 * seeders first (merchant_id is resolved by api_key in the payment context).</p>
 */
public class TransactionSeeder implements Seeder {

    private static final Object[][] TRANSACTIONS = {
            {"4111111111111111", 75_000, "QRIS", "seed-admin-merchant-key"},
            {"4111111111111112", 45_000, "CARD", "seed-user-merchant-key"}
    };

    @Override
    public String domain() {
        return "transaction";
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Uni<Void> seed(SeedContext ctx) {
        Logger log = ctx.log();
        ReactiveMongoDatabase financial = ctx.db("financial");
        ReactiveMongoDatabase payment = ctx.db("payment");
        ReactiveMongoCollection<Document> transactions = financial.getCollection("transactions", Document.class);
        ReactiveMongoCollection<Document> merchants = payment.getCollection("merchants", Document.class);

        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Object[] t : TRANSACTIONS) {
            final String cardNumber = (String) t[0];
            final int amount = (Integer) t[1];
            final String method = (String) t[2];
            final String apiKey = (String) t[3];
            chain = chain.chain(() -> merchants.find(Filters.eq("apiKey", apiKey)).collect().first()
                    .onItem().ifNull().continueWith(() -> new Document("_id", new ObjectId()))
                    .chain(merchant -> {
                        ObjectId merchantId = merchant.getObjectId("_id");
                        Date now = Date.from(Instant.now());
                        return transactions.updateOne(
                                Filters.and(Filters.eq("cardNumber", cardNumber),
                                        Filters.eq("amount", amount),
                                        Filters.eq("paymentMethod", method)),
                                Updates.combine(
                                        Updates.setOnInsert("transactionNo", UUID.randomUUID()),
                                        Updates.setOnInsert("cardNumber", cardNumber),
                                        Updates.setOnInsert("amount", amount),
                                        Updates.setOnInsert("paymentMethod", method),
                                        Updates.setOnInsert("merchantId", merchantId),
                                        Updates.setOnInsert("transactionTime", now),
                                        Updates.setOnInsert("status", "SUCCESS"),
                                        Updates.setOnInsert("createdAt", now),
                                        Updates.setOnInsert("updatedAt", now),
                                        Updates.setOnInsert("deletedAt", null)),
                                new UpdateOptions().upsert(true)).replaceWithVoid();
                    }));
        }
        return chain.invoke(() -> log.infof("Transactions seeded: %d transactions", TRANSACTIONS.length));
    }
}
