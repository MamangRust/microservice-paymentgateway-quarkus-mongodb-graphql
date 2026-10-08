package com.sanedge.withdraw.service;

import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.withdraw.domain.requests.CreateWithdrawRequest;
import com.sanedge.withdraw.domain.requests.UpdateWithdrawRequest;
import com.sanedge.withdraw.domain.response.WithdrawResponse;
import com.sanedge.withdraw.domain.response.WithdrawResponseDeleteAt;

import io.smallrye.mutiny.Uni;

public interface WithdrawCommandService {
    Uni<ApiResponse<WithdrawResponse>> create(CreateWithdrawRequest req);

    Uni<ApiResponse<WithdrawResponse>> update(UpdateWithdrawRequest req);

    Uni<ApiResponse<WithdrawResponseDeleteAt>> trashed(String withdrawId);

    Uni<ApiResponse<WithdrawResponseDeleteAt>> restore(String withdrawId);

    Uni<ApiResponse<Boolean>> deletePermanent(String withdrawId);

    Uni<ApiResponse<Boolean>> restoreAll();

    Uni<ApiResponse<Boolean>> deleteAll();
}
