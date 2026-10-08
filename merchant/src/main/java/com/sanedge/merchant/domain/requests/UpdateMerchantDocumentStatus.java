package com.sanedge.merchant.domain.requests;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class UpdateMerchantDocumentStatus {
    private String documentId;

    private String merchantId;

    private String note;

    @NotBlank(message = "Status wajib diisi")
    private String status;
}
