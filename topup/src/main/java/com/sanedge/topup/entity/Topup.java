package com.sanedge.topup.entity;

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
@MongoEntity(collection = "topups")
public class Topup extends BaseModel {

    public UUID topupNo;

    public String cardNumber;

    public Integer topupAmount;

    public String topupMethod;

    public Instant topupTime;

    public Status status = Status.PENDING;

    public Boolean compensationLegAApplied;

    public String compensationLegACard;

    public Integer compensationLegADelta;

    public String compensationWorkerId;

    public String compensationClaimToken;

    public Instant compensationLeaseUntil;

    public Integer compensationAttempts = 0;

    public String compensationReason;

    /**
     * Bridge between the inherited {@link ObjectId} {@code id} and the proto/domain
     * layer, which now exchanges ids as 24-char hex strings.
     */
    public String getTopupId() {
        return id != null ? id.toHexString() : null;
    }

    public void setTopupId(String topupId) {
        this.id = (topupId != null && !topupId.isBlank()) ? new ObjectId(topupId) : null;
    }
}
