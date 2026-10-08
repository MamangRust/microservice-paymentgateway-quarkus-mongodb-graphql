package com.sanedge.common.entity;

import java.time.Instant;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoEntity;
import lombok.Getter;
import lombok.Setter;

/**
 * Single shared Panache base for every Mongo entity: the audit timestamps
 * ({@code createdAt}, {@code updatedAt}, {@code deletedAt}) and the
 * {@code ObjectId id} supplied by {@link ReactivePanacheMongoEntity}.
 *
 * <p>Mongo Panache does <b>not</b> invoke JPA lifecycle callbacks, so
 * {@link #onCreate()} / {@link #onUpdate()} are plain helpers that services
 * call explicitly before persisting/updating.</p>
 */
@Getter
@Setter
public abstract class BaseModel extends ReactivePanacheMongoEntity {

    private Instant createdAt;

    private Instant updatedAt;

    private Instant deletedAt;

    protected void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
