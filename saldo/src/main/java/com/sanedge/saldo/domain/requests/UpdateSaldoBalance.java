package com.sanedge.saldo.domain.requests;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "Request untuk update saldo (top up)")
public class UpdateSaldoBalance {

    @NotBlank(message = "Card number wajib diisi")
    private String cardNumber;

    @Min(value = 50000, message = "Minimal saldo 50.000")
    private Long totalBalance;

    /** When set, the balance is mutated atomically by this delta instead of being overwritten. */
    private Long deltaBalance;

    /** Floor the resulting balance must not drop below; only used with {@link #deltaBalance}. */
    private Long minimumBalance;

    /** Idempotency key for the atomic delta mutation; only used with {@link #deltaBalance}. */
    private String operationKey;
}