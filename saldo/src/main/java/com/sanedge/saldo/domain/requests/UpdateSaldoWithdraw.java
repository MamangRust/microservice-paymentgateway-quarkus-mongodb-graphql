package com.sanedge.saldo.domain.requests;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Schema(description = "Request untuk withdraw saldo")
public class UpdateSaldoWithdraw {

    @NotBlank(message = "Card number wajib diisi")
    private String cardNumber;

    @Min(value = 50000, message = "Minimal saldo 50.000")
    private Long totalBalance;

    @Min(value = 0, message = "Withdraw amount tidak boleh negatif")
    private Long withdrawAmount;

    private LocalDateTime withdrawTime;

    /** When set, the balance is debited atomically by this delta instead of being overwritten. */
    private Long deltaBalance;

    /** Floor the resulting balance must not drop below; only used with {@link #deltaBalance}. */
    private Long minimumBalance;

    /** Idempotency key for the atomic delta debit; only used with {@link #deltaBalance}. */
    private String operationKey;
}