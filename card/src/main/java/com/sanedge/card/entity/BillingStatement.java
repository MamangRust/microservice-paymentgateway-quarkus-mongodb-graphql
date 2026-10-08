package com.sanedge.card.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

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
@MongoEntity(collection = "billing_statements")
public class BillingStatement extends BaseModel {

    public String cardNumber;

    public Integer billingCycleDay;

    public BigDecimal openingBalance = BigDecimal.ZERO;

    public BigDecimal closingBalance = BigDecimal.ZERO;

    public BigDecimal minimumPayment = BigDecimal.ZERO;

    public LocalDate dueDate;

    public BigDecimal fees = BigDecimal.ZERO;

    public BigDecimal interest = BigDecimal.ZERO;

    public LocalDate statementDate;

    public String status = "OPEN";

    /** Bridge to the inherited {@link BaseModel#id} exposed as a 24-hex string. */
    public String getStatementId() {
        return id != null ? id.toHexString() : null;
    }

    public void setStatementId(String statementId) {
        this.id = statementId != null ? new org.bson.types.ObjectId(statementId) : null;    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BillingStatement))
            return false;
        return id != null && id.equals(((BillingStatement) o).id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
