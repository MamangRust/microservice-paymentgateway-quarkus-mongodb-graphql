package com.sanedge.common.test;

import org.bson.Document;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import jakarta.inject.Inject;

/**
 * Base class for {@code @QuarkusTest} repository tests.
 *
 * <p>MongoDB does not roll back test data when a test finishes, and Quarkus
 * reuses the application across test classes, so without an explicit reset data
 * leaks between tests. This wipes every collection before each test method
 * while leaving indexes intact.</p>
 *
 * <p>Uses the imperative client (rather than {@code ReactiveMongoClient}) so the
 * reset also works from {@code @RunOnVertxContext} lifecycle methods: blocking a
 * reactive operation on the Vert.x event loop deadlocks, while the imperative
 * driver completes its work on its own threads.</p>
 */
public abstract class MongoTestBase {

    @Inject
    MongoClient mongoClient;

    @ConfigProperty(name = "quarkus.mongodb.database", defaultValue = "test")
    String databaseName;

    @BeforeEach
    void resetMongoState() {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        for (String name : database.listCollectionNames()) {
            database.getCollection(name).deleteMany(new Document());
        }
    }
}
