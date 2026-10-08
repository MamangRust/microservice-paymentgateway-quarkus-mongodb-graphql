package com.sanedge.saldo.entity;

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
@MongoEntity(collection = "saldo_mutation_operations")
public class SaldoMutationOperation extends BaseModel {

    private String operationKey;

    private String cardNumber;

    private Integer requestedDelta;

    private Integer minimumBalance = 0;

    private String resultStatus;

    private Integer resultBalance;

    private String failureReason;
}
