package com.sanedge.merchant.entity;

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
@MongoEntity(collection = "merchants")
public class Merchant extends BaseModel {

    public UUID merchantNo;

    public String name;

    public String apiKey;

    public ObjectId userId;

    public Status status = Status.PENDING;

    public String getMerchantId() {
        return this.id != null ? this.id.toHexString() : null;
    }

    public void setMerchantId(String merchantId) {
        this.id = merchantId != null ? new ObjectId(merchantId) : null;
    }
}
