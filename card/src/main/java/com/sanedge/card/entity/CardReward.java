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
@MongoEntity(collection = "card_rewards")
public class CardReward extends BaseModel {

    public String cardNumber;

    public ObjectId authTxnId;

    public BigDecimal amount;

    public String mcc;

    public BigDecimal pointsEarned = BigDecimal.ZERO;

    public Instant expiresAt;

    public Boolean redeemed = false;

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getRewardId() {
        return id != null ? id.toHexString() : null;
    }

    public void setRewardId(String rewardId) {
        this.id = rewardId != null ? new ObjectId(rewardId) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CardReward))
            return false;
        return id != null && id.equals(((CardReward) o).id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
