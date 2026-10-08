package com.sanedge.transfer.domain.response;

import com.sanedge.transfer.entity.Transfer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferResponse {
    private String id;
    private String transferNo;
    private String transferFrom;
    private String transferTo;
    private Long transferAmount;
    private String transferTime;
    private String createdAt;
    private String updatedAt;

    public static TransferResponse from(Transfer t) {
        if (t == null) {
            return null;
        }
        return TransferResponse.builder()
                .id(t.getTransferId())
                .transferNo(t.getTransferNo() != null ? t.getTransferNo().toString() : null)
                .transferFrom(t.getTransferFrom())
                .transferTo(t.getTransferTo())
                .transferAmount(t.getTransferAmount() != null ? t.getTransferAmount().longValue() : null)
                .transferTime(t.getTransferTime() != null ? t.getTransferTime().toString() : null)
                .createdAt(t.getCreatedAt() != null ? t.getCreatedAt().toString() : null)
                .updatedAt(t.getUpdatedAt() != null ? t.getUpdatedAt().toString() : null)
                .build();
    }
}
