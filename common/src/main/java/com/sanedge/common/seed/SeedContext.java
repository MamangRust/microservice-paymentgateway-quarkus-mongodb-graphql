package com.sanedge.common.seed;

import io.quarkus.mongodb.reactive.ReactiveMongoDatabase;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.function.Function;

/**
 * Context passed to each {@link Seeder#seed(SeedContext)} invocation.
 *
 * <p>Because payment keeps one MongoDB instance per bounded context
 * (identity, payment, financial), the seeder no longer receives a single SQL
 * pool. Instead it receives a map of context name to the
 * {@link ReactiveMongoDatabase} for that context, and resolves the database it
 * needs via {@link #db(String)}.
 *
 * @param databases      Mongo database per bounded context (key = context name)
 * @param log            Logger instance
 * @param passwordHasher Function to hash passwords (PBKDF2 via PasswordUtil)
 */
public record SeedContext(
        Map<String, ReactiveMongoDatabase> databases,
        Logger log,
        Function<String, String> passwordHasher
) {

    /**
     * Resolve the Mongo database for a bounded context.
     *
     * @param context one of {@code identity}, {@code payment}, {@code financial}
     * @return the reactive database for that context
     * @throws IllegalStateException when no database is configured for the context
     */
    public ReactiveMongoDatabase db(String context) {
        ReactiveMongoDatabase database = databases.get(context);
        if (database == null) {
            throw new IllegalStateException("No MongoDB database configured for context '" + context + "'");
        }
        return database;
    }
}
