package com.sanedge.transaction.domain.requests;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import java.time.Instant;

@Data
@Schema(description = "Request untuk update transaksi")
public class UpdateTransactionRequest {

    @NotBlank(message = "Transaction ID wajib diisi")
    private String transactionId;

    @NotBlank(message = "Card number wajib diisi")
    private String cardNumber;

    @Min(value = 50000, message = "Minimal transaksi 50.000")
    private Long amount;

    @NotBlank(message = "Payment method wajib diisi")
    private String paymentMethod;

    @NotBlank(message = "Merchant ID wajib diisi")
    private String merchantId;

    private Instant transactionTime;
}