package com.sanedge.transaction.domain.requests;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class UpdateTransactionStatus {

    @NotBlank(message = "Transaction ID wajib diisi")
    private String transactionId;

    @NotBlank(message = "Status wajib diisi")
    private String status;
}