package com.sanedge.withdraw.entity;

import com.sanedge.common.entity.BaseModel;

import java.time.Instant;
import java.util.UUID;

import org.bson.types.ObjectId;

import com.sanedge.common.enums.Status;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "withdraws")
public class Withdraw extends BaseModel {

    public UUID withdrawNo;

    public String cardNumber;

    public Integer withdrawAmount;

    public Instant withdrawTime;

    public Status status = Status.PENDING;

    public Boolean compensationLegAApplied;

    public String compensationLegACard;

    public Integer compensationLegADelta;

    public String compensationWorkerId;

    public String compensationClaimToken;

    public Instant compensationLeaseUntil;

    public Integer compensationAttempts = 0;

    public String compensationReason;

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getWithdrawId() {
        return id != null ? id.toHexString() : null;
    }

    public void setWithdrawId(String withdrawId) {
        this.id = (withdrawId != null && !withdrawId.isBlank()) ? new ObjectId(withdrawId) : null;
    }

    public Boolean getCompensationLegAApplied() { return compensationLegAApplied; }

    public String getCompensationLegACard() { return compensationLegACard; }

    public Integer getCompensationLegADelta() { return compensationLegADelta; }

    public String getCompensationClaimedBy() { return compensationWorkerId; }

    public String getLastFailureReason() { return compensationReason; }
}
