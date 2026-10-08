package com.sanedge.card.domain.requests;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.time.LocalDate;

@Data
@RegisterForReflection
public class CreateCardRequest {

    @NotBlank(message = "User ID wajib diisi")
    private String userId;

    @NotBlank(message = "Card type wajib diisi")
    private String cardType;

    private LocalDate expireDate;

    @NotBlank(message = "CVV wajib diisi")
    private String cvv;

    @NotBlank(message = "Card provider wajib diisi")
    private String cardProvider;
}
