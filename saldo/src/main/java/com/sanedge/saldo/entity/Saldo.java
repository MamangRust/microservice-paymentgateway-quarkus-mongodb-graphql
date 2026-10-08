package com.sanedge.saldo.entity;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "saldos")
public class Saldo extends BaseModel {

    public String cardNumber;

    public Integer totalBalance;

    public Integer withdrawAmount;

    public Instant withdrawTime;

    /**
     * Bridge between the inherited {@link ObjectId} {@code id} and the proto/domain
     * layer, which exchanges ids as 24-char hex strings.
     */
    public String getSaldoId() {
        return id != null ? id.toHexString() : null;
    }

    public void setSaldoId(String saldoId) {
        this.id = (saldoId != null && !saldoId.isBlank()) ? new ObjectId(saldoId) : null;
    }
}
