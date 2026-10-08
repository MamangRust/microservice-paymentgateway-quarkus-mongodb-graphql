package com.sanedge.topup.domain.requests;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "Request untuk update status topup")
public class UpdateTopupStatus {

    @NotBlank(message = "Topup ID wajib diisi")
    private String topupId;

    @NotBlank(message = "Status wajib diisi")
    private String status;
}