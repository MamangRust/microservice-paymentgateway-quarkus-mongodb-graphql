package com.sanedge.merchant.domain.requests;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class CreateMerchantDocumentRequest {
    private String merchantId;

    @NotBlank(message = "Document type wajib diisi")
    private String documentType;

    @NotBlank(message = "Document url wajib diisi")
    private String documentUrl;
}
