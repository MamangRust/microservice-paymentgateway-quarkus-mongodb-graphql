package com.sanedge.statsbackfill;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.sanedge.common.event.EventEnvelope;

import io.quarkus.mongodb.MongoClientName;
import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.quarkus.mongodb.reactive.ReactiveMongoCollection;
import io.quarkus.mongodb.reactive.ReactiveMongoDatabase;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * One-shot stats backfill.
 *
 * <p>After the MongoDB migration the OLTP data is split across two bounded
 * contexts: {@code financial} (transaction/topup/transfer/withdraw) and
 * {@code payment} (saldo/merchant/card). This runner reads each domain's
 * collection and enqueues one stats event per document into that context's
 * outbox collection (status PENDING) with a deterministic {@code eventId}
 * ({@code backfill:&lt;domain&gt;:&lt;id&gt;}). The per-service
 * {@code OutboxPublisher} then relays them to Kafka
 * ({@code stats.payment.*.event}) → stats-writer → ClickHouse. Writing through
 * the outbox gives idempotency for free: re-running hits the natural key and
 * inserts nothing.
 *
 * <p>Usage: {@code BACKFILL_DOMAINS=transaction,merchant BACKFILL_FROM=2024-01-01T00:00:00Z
 * java -jar stats-backfill/target/quarkus-app/quarkus-run.jar}
 */
@ApplicationScoped
public class BackfillLifecycle {

    private static final Logger LOGGER = Logger.getLogger(BackfillLifecycle.class);

    private static final String IDENTITY_DB = "pg_identity";
    private static final String PAYMENT_DB = "pg_payment";
    private static final String FINANCIAL_DB = "pg_financial";

    /** Domain → (context, OLTP collection, outbox collection, Kafka topic). */
    private static final List<DomainSpec> DOMAINS = List.of(
            new DomainSpec("transaction", FINANCIAL_DB, "transactions", "transaction_outbox",
                    "stats.payment.transaction.event"),
            new DomainSpec("topup", FINANCIAL_DB, "topups", "topup_outbox",
                    "stats.payment.topup.event"),
            new DomainSpec("transfer", FINANCIAL_DB, "transfers", "transfer_outbox",
                    "stats.payment.transfer.event"),
            new DomainSpec("withdraw", FINANCIAL_DB, "withdraws", "withdraw_outbox",
                    "stats.payment.withdraw.event"),
            new DomainSpec("saldo", PAYMENT_DB, "saldos", "saldo_outbox",
                    "stats.payment.saldo.event"),
            new DomainSpec("merchant", PAYMENT_DB, "merchants", "merchant_outbox",
                    "stats.payment.merchant.event"),
            new DomainSpec("card", PAYMENT_DB, "cards", "card_outbox",
                    "stats.payment.card.event"));

    @Inject
    @MongoClientName("payment")
    ReactiveMongoClient paymentClient;

    @Inject
    @MongoClientName("financial")
    ReactiveMongoClient financialClient;

    @ConfigProperty(name = "backfill.domains", defaultValue = "all")
    String domainsCfg;

    @ConfigProperty(name = "backfill.from", defaultValue = "none")
    String fromCfg;

    void onStart(@Observes StartupEvent ev) {
        List<String> filter = Arrays.stream(domainsCfg.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank() && !"all".equalsIgnoreCase(s))
                .map(s -> s.toLowerCase(Locale.ROOT))
                .toList();

        List<DomainSpec> targets = new ArrayList<>();
        for (DomainSpec spec : DOMAINS) {
            if (filter.isEmpty() || filter.contains(spec.domain)) {
                targets.add(spec);
            }
        }

        LOGGER.infof("Stats backfill started. Domains: %s, from: %s",
                targets.stream().map(DomainSpec::domain).toList(),
                "none".equalsIgnoreCase(fromCfg) ? "(all)" : fromCfg);

        runBackfill(targets)
                .subscribe().with(
                        v -> {
                            LOGGER.info("Stats backfill completed successfully.");
                            Quarkus.asyncExit(0);
                        },
                        err -> {
                            LOGGER.error("Stats backfill failed: " + err.getMessage(), err);
                            Quarkus.asyncExit(1);
                        });
    }

    private Uni<Void> runBackfill(List<DomainSpec> targets) {
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (DomainSpec spec : targets) {
            chain = chain.chain(() -> backfillDomain(spec));
        }
        return chain;
    }

    private Uni<Void> backfillDomain(DomainSpec spec) {
        ReactiveMongoDatabase db = client(spec.context).getDatabase(spec.context);
        ReactiveMongoCollection<Document> source = db.getCollection(spec.collection, Document.class);
        ReactiveMongoCollection<Document> outbox = db.getCollection(spec.outboxCollection, Document.class);

        Document filter = new Document("deletedAt", null);
        if (!fromCfg.isBlank() && !"none".equalsIgnoreCase(fromCfg)) {
            filter.append("createdAt", new Document("$gte", Instant.parse(fromCfg)));
        }

        return source.find(filter).collect().asList()
                .chain(docs -> enqueue(docs, spec, outbox));
    }

    /** Inserts one outbox document per OLTP document with a deterministic eventId. */
    private Uni<Void> enqueue(List<Document> docs, DomainSpec spec, ReactiveMongoCollection<Document> outbox) {
        List<Uni<Void>> inserts = new ArrayList<>();
        int skipped = 0;
        for (Document row : docs) {
            String idHex = hex(row.get("_id"));
            String eventId = "backfill:" + spec.domain + ":" + idHex;
            JsonObject payload = buildPayload(spec, row);
            if (payload == null) {
                skipped++;
                continue;
            }
            inserts.add(enqueueEvent(outbox, spec, eventId, idHex, payload));
        }
        if (inserts.isEmpty()) {
            LOGGER.infof("  %s: 0 rows (skipped %d)", spec.domain, skipped);
            return Uni.createFrom().voidItem();
        }
        return Uni.combine().all().unis(inserts).discardItems()
                .invoke(() -> LOGGER.infof("  %s: enqueued %d event(s)", spec.domain, inserts.size()));
    }

    private Uni<Void> enqueueEvent(ReactiveMongoCollection<Document> outbox, DomainSpec spec,
                                   String eventId, String eventKey, JsonObject payload) {
        Instant now = Instant.now();
        Document doc = new Document()
                .append("_id", new ObjectId())
                .append("domain", spec.domain)
                .append("eventId", eventId)
                .append("topic", spec.topic)
                .append("eventKey", eventKey)
                .append("payload", payload.encode())
                .append("status", "PENDING")
                .append("attempts", 0)
                .append("nextAttemptAt", null)
                .append("claimedAt", null)
                .append("claimToken", null)
                .append("lastError", null)
                .append("createdAt", now)
                .append("updatedAt", now)
                .append("deletedAt", null);

        // Idempotent: only insert when this deterministic eventId is not present.
        return outbox.updateOne(
                new Document("eventId", eventId),
                new Document("$setOnInsert", doc),
                new com.mongodb.client.model.UpdateOptions().upsert(true))
                .replaceWithVoid();
    }

    /**
     * Maps an OLTP document → the flattened EventEnvelope payload written by the
     * command services (field names = ClickHouse columns).
     */
    private JsonObject buildPayload(DomainSpec spec, Document row) {
        JsonObject p = new JsonObject();
        String occurred = row.get("createdAt") != null
                ? toIso(row.get("createdAt"))
                : Instant.now().toString();
        switch (spec.domain) {
            case "transaction" -> {
                p.put("transaction_id", hex(row.get("_id")));
                p.put("transaction_no", str(row.get("transactionNo")));
                p.put("card_number", str(row.get("cardNumber")));
                p.put("amount", intVal(row.get("amount")));
                p.put("payment_method", str(row.get("paymentMethod")));
                p.put("merchant_id", hex(row.get("merchantId")));
                p.put("status", lower(row.get("status")));
            }
            case "topup" -> {
                p.put("topup_id", hex(row.get("_id")));
                p.put("topup_no", str(row.get("topupNo")));
                p.put("card_number", str(row.get("cardNumber")));
                p.put("amount", intVal(row.get("topupAmount")));
                p.put("payment_method", str(row.get("topupMethod")));
                p.put("status", lower(row.get("status")));
            }
            case "transfer" -> {
                p.put("transfer_id", hex(row.get("_id")));
                p.put("transfer_no", str(row.get("transferNo")));
                p.put("source_card", str(row.get("transferFrom")));
                p.put("destination_card", str(row.get("transferTo")));
                p.put("amount", intVal(row.get("transferAmount")));
                p.put("status", lower(row.get("status")));
            }
            case "withdraw" -> {
                p.put("withdraw_id", hex(row.get("_id")));
                p.put("withdraw_no", str(row.get("withdrawNo")));
                p.put("card_number", str(row.get("cardNumber")));
                p.put("amount", intVal(row.get("withdrawAmount")));
                p.put("status", lower(row.get("status")));
            }
            case "saldo" -> {
                p.put("card_number", str(row.get("cardNumber")));
                p.put("total_balance", intVal(row.get("totalBalance")));
            }
            case "merchant" -> {
                p.put("merchant_id", hex(row.get("_id")));
                p.put("user_id", hex(row.get("userId")));
                p.put("name", str(row.get("name")));
                p.put("status", lower(row.get("status")));
            }
            case "card" -> {
                p.put("card_id", hex(row.get("_id")));
                p.put("user_id", hex(row.get("userId")));
                p.put("card_number", str(row.get("cardNumber")));
                p.put("card_type", str(row.get("cardType")));
                p.put("card_provider", str(row.get("cardProvider")));
                p.put("status", lower(row.get("status")));
            }
            default -> {
                return null;
            }
        }
        p.put("created_at", occurred);
        return EventEnvelope.withDefaults(p, spec.domain + ".created");
    }

    private ReactiveMongoClient client(String context) {
        return PAYMENT_DB.equals(context) ? paymentClient : financialClient;
    }

    private static String hex(Object value) {
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        return value == null ? "" : value.toString();
    }

    private static String str(Object value) {
        return value == null ? "" : value.toString();
    }

    private static int intVal(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String lower(Object value) {
        return value == null ? "" : value.toString().toLowerCase(Locale.ROOT);
    }

    private static String toIso(Object value) {
        if (value instanceof Instant instant) {
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        }
        if (value instanceof java.util.Date date) {
            return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
        }
        return String.valueOf(value);
    }

    private record DomainSpec(String domain, String context, String collection, String outboxCollection, String topic) {
    }
}
