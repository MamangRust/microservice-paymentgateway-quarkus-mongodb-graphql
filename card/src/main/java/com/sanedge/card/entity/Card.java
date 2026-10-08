package com.sanedge.card.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.bson.types.ObjectId;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "cards")
public class Card extends BaseModel {

    public ObjectId userId;

    public String cardNumber;

    public String cardType;

    public LocalDate expireDate;

    public String cvv;

    public String cardProvider;

    public BigDecimal creditLimit = BigDecimal.ZERO;

    public BigDecimal points = BigDecimal.ZERO;

    public CardStatus status;

    public enum CardStatus {
        ACTIVE, SUSPENDED, BLOCKED, FROZEN
    }

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getCardId() {
        return id != null ? id.toHexString() : null;
    }

    public void setCardId(String cardId) {
        this.id = cardId != null ? new ObjectId(cardId) : null;
    }
}
