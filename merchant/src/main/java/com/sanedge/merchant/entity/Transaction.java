package com.sanedge.merchant.entity;

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
@MongoEntity(collection = "transactions")
public class Transaction extends BaseModel {

    public UUID transactionNo;

    public String cardNumber;

    public Integer amount;

    public String paymentMethod;

    public ObjectId merchantId;

    public Instant transactionTime;

    public Status status = Status.PENDING;

    public String getTransactionId() {
        return this.id != null ? this.id.toHexString() : null;
    }

    public void setTransactionId(String transactionId) {
        this.id = transactionId != null ? new ObjectId(transactionId) : null;
    }
}
