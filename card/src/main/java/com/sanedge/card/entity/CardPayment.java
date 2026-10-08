package com.sanedge.card.entity;

import java.math.BigDecimal;
import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "card_payments")
public class CardPayment extends BaseModel {

    public String cardNumber;

    public ObjectId statementId;

    public BigDecimal amount;

    public String paymentChannel;

    public String referenceId;

    public String status = "PENDING";

    public Instant paidAt;

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getPaymentId() {
        return id != null ? id.toHexString() : null;
    }

    public void setPaymentId(String paymentId) {
        this.id = paymentId != null ? new ObjectId(paymentId) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CardPayment))
            return false;
        return id != null && id.equals(((CardPayment) o).id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
