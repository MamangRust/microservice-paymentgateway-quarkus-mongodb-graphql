package com.sanedge.topup.service;

import com.sanedge.topup.domain.requests.CreateTopupRequest;
import com.sanedge.topup.domain.requests.UpdateTopupRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.topup.domain.response.TopupResponse;
import com.sanedge.topup.domain.response.TopupResponseDeleteAt;

import io.smallrye.mutiny.Uni;

public interface TopupCommandService {
    Uni<ApiResponse<TopupResponse>> create(CreateTopupRequest req);

    Uni<ApiResponse<TopupResponse>> update(UpdateTopupRequest req);

    Uni<ApiResponse<TopupResponseDeleteAt>> trashed(String topupId);

    Uni<ApiResponse<TopupResponseDeleteAt>> restore(String topupId);

    Uni<ApiResponse<Boolean>> deletePermanent(String topupId);

    Uni<ApiResponse<Boolean>> restoreAll();

    Uni<ApiResponse<Boolean>> deleteAll();
}
