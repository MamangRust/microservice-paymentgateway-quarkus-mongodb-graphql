package com.sanedge.transfer.service;


import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.transfer.domain.requests.CreateTransferRequest;
import com.sanedge.transfer.domain.requests.UpdateTransferRequest;
import com.sanedge.transfer.domain.response.TransferResponse;
import com.sanedge.transfer.domain.response.TransferResponseDeleteAt;

import io.smallrye.mutiny.Uni;

public interface TransferCommandService {
    Uni<ApiResponse<TransferResponse>> create(CreateTransferRequest req);

    Uni<ApiResponse<TransferResponse>> update(UpdateTransferRequest req);

    Uni<ApiResponse<TransferResponseDeleteAt>> trashed(String transferId);

    Uni<ApiResponse<TransferResponseDeleteAt>> restore(String transferId);

    Uni<ApiResponse<Boolean>> deletePermanent(String transferId);

    Uni<ApiResponse<Boolean>> restoreAll();

    Uni<ApiResponse<Boolean>> deleteAll();
}
