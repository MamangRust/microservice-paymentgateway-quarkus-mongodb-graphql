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
@MongoEntity(collection = "card_auth_transactions")
public class CardAuthTransaction extends BaseModel {

    public enum AuthTxnStatus {
        PENDING, APPROVED, DECLINED, REVERSED
    }

    public String cardNumber;

    public ObjectId merchantId;

    public BigDecimal amount;

    public String currency;

    public String posEntryMode;

    public String mcc;

    public String idempotencyKey;

    public Integer riskScore = 0;

    public AuthTxnStatus status = AuthTxnStatus.PENDING;

    public Instant authorizedAt;

    public Instant reversedAt;

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getAuthTxnId() {
        return id != null ? id.toHexString() : null;
    }

    public void setAuthTxnId(String authTxnId) {
        this.id = authTxnId != null ? new ObjectId(authTxnId) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CardAuthTransaction))
            return false;
        return id != null && id.equals(((CardAuthTransaction) o).id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
