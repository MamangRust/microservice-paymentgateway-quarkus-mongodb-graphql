package com.sanedge.transfer.entity;

import java.time.Instant;
import java.util.UUID;

import org.bson.types.ObjectId;

import com.sanedge.common.entity.BaseModel;
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
@MongoEntity(collection = "transfers")
public class Transfer extends BaseModel {

    public UUID transferNo;

    public String transferFrom;

    public String transferTo;

    public Integer transferAmount;

    public Instant transferTime;

    public Status status = Status.PENDING;

    public Boolean compensationLegAApplied;

    public String compensationLegACard;

    public Integer compensationLegADelta;

    public Boolean compensationLegBApplied;

    public String compensationLegBCard;

    public Integer compensationLegBDelta;

    public String compensationWorkerId;

    public String compensationClaimToken;

    public Instant compensationLeaseUntil;

    public Integer compensationAttempts = 0;

    public String compensationReason;

    public String getTransferId() {
        return this.id != null ? this.id.toHexString() : null;
    }

    public void setTransferId(String transferId) {
        this.id = transferId != null ? new ObjectId(transferId) : null;
    }
}
