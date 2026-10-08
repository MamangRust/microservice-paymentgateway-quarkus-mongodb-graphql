package com.sanedge.card.entity;

import java.math.BigDecimal;
import java.time.Instant;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Durable event audit sink (Phase 4): one row per consumed card domain event
 * that was previously publish-only. Redelivery is deduplicated by the unique
 * index (topic, referenceId); the repository also performs a find-before-insert
 * as the first line of defense.
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "card_event_logs")
public class CardEventLog extends BaseModel {

    public String topic;

    public String eventType;

    public String cardNumber;

    public String referenceId;

    public String payload;

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getEventId() {
        return id != null ? id.toHexString() : null;
    }

    public void setEventId(String eventId) {
        this.id = eventId != null ? new org.bson.types.ObjectId(eventId) : null;
    }
}
