package com.sanedge.seeder;

import com.sanedge.common.seed.SeedContext;
import com.sanedge.common.seed.Seeder;
import com.sanedge.common.utils.PasswordUtil;
import io.quarkus.mongodb.MongoClientName;
import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.quarkus.mongodb.reactive.ReactiveMongoDatabase;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Seeder orchestrator lifecycle.
 * <p>
 * On startup:
 * 1. Loads all {@link Seeder} implementations via {@link ServiceLoader}
 * 2. Filters by SEED_DOMAINS env var (comma-separated, or "all"/empty for all)
 * 3. Sorts by {@link Seeder#order()}
 * 4. Runs each seeder sequentially
 * 5. Exits with code 0 (success) or 1 (failure)
 *
 * <p>Because payment keeps one MongoDB instance per bounded context
 * (identity, payment, financial), the orchestrator exposes a
 * {@link ReactiveMongoDatabase} per context to every seeder through
 * {@link SeedContext#db(String)}. The database names are fixed by the
 * deployment topology (see PGTOMONGODB.md §1.1).
 */
@ApplicationScoped
public class SeederLifecycle {

    private static final Logger LOGGER = Logger.getLogger(SeederLifecycle.class);

    /** Context -> Mongo database name (fixed by the payment topology). */
    private static final Map<String, String> CONTEXT_DATABASES = Map.of(
            "identity", "pg_identity",
            "payment", "pg_payment",
            "financial", "pg_financial");

    @Inject
    @MongoClientName("identity")
    ReactiveMongoClient identityClient;

    @Inject
    @MongoClientName("payment")
    ReactiveMongoClient paymentClient;

    @Inject
    @MongoClientName("financial")
    ReactiveMongoClient financialClient;

    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("=== Seeder Orchestrator starting ===");

        List<Seeder> allSeeders = ServiceLoader.load(Seeder.class).stream()
                .map(ServiceLoader.Provider::get)
                .sorted(Comparator.comparingInt(Seeder::order))
                .toList();

        LOGGER.infof("Loaded %d seeders via ServiceLoader", allSeeders.size());

        String seedDomainsEnv = System.getenv("SEED_DOMAINS");
        Set<String> allowedDomains = parseSeedDomains(seedDomainsEnv);

        List<Seeder> seeders = allSeeders.stream()
                .filter(s -> allowedDomains.isEmpty() || allowedDomains.contains(s.domain()))
                .toList();

        if (seeders.isEmpty()) {
            LOGGER.warn("No seeders matched SEED_DOMAINS filter. Nothing to seed.");
            Quarkus.asyncExit(0);
            return;
        }

        LOGGER.infof("Will run %d seeders: %s",
                seeders.size(),
                seeders.stream().map(s -> s.domain() + " (" + s.order() + ")").collect(Collectors.joining(", ")));

        PasswordUtil passwordUtil = new PasswordUtil();
        SeedContext ctx = new SeedContext(
                buildDatabases(),
                LOGGER,
                passwordUtil::hashPassword
        );

        runSeeders(seeders, ctx, 0);
    }

    private Map<String, ReactiveMongoDatabase> buildDatabases() {
        Map<String, ReactiveMongoDatabase> databases = new HashMap<>();
        databases.put("identity", identityClient.getDatabase(CONTEXT_DATABASES.get("identity")));
        databases.put("payment", paymentClient.getDatabase(CONTEXT_DATABASES.get("payment")));
        databases.put("financial", financialClient.getDatabase(CONTEXT_DATABASES.get("financial")));
        return databases;
    }

    private void runSeeders(List<Seeder> seeders, SeedContext ctx, int index) {
        if (index >= seeders.size()) {
            LOGGER.info("=== All seeders completed successfully ===");
            Quarkus.asyncExit(0);
            return;
        }

        Seeder seeder = seeders.get(index);
        LOGGER.infof("Running seeder [%d/%d]: %s (order=%d)",
                index + 1, seeders.size(), seeder.domain(), seeder.order());

        seeder.seed(ctx)
                .onItem().invoke(() -> {
                    LOGGER.infof("Seeder [%d/%d] %s completed successfully", index + 1, seeders.size(), seeder.domain());
                    runSeeders(seeders, ctx, index + 1);
                })
                .onFailure().invoke(failure -> {
                    LOGGER.errorf(failure, "Seeder [%d/%d] %s FAILED", index + 1, seeders.size(), seeder.domain());
                    Quarkus.asyncExit(1);
                })
                .subscribe().with(
                        item -> { /* handled by onItem */ },
                        failure -> { /* handled by onFailure */ }
                );
    }

    /**
     * Parse SEED_DOMAINS env var.
     * Accepts: "identity,merchant" or "all" or empty (all).
     */
    private Set<String> parseSeedDomains(String envValue) {
        if (envValue == null || envValue.isBlank() || "all".equalsIgnoreCase(envValue.trim())) {
            return Set.of();
        }
        return Arrays.stream(envValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}
