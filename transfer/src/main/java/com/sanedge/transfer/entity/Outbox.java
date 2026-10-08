package com.sanedge.transfer.entity;

import java.time.Instant;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Transactional outbox document. Written next to the business entity, then
 * relayed to Kafka by {@code OutboxPublisher}. Each service owns its own
 * collection ({@code transfer_outbox}) so multiple services sharing a MongoDB
 * instance never collide on the former single {@code outbox} table.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@MongoEntity(collection = "transfer_outbox")
public class Outbox extends BaseModel {

    private String domain;

    private String eventId;

    private String topic;

    private String eventKey;

    private String payload;

    private String status = "PENDING";

    private int attempts;

    private Instant nextAttemptAt;

    private Instant claimedAt;

    private String claimToken;

    private String lastError;
}
