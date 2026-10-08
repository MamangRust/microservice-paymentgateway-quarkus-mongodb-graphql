package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.WithdrawDto.*;
import io.smallrye.mutiny.Uni;

public interface WithdrawService {
    Uni<FindAllWithdrawResponse> listWithdraws(int page, int size, String search);
    Uni<FindAllWithdrawResponse> findByCard(String cardNumber);
    Uni<FindAllWithdrawResponse> findActiveWithdraws(int page, int size, String search);
    Uni<FindAllWithdrawResponse> findTrashedWithdraws(int page, int size, String search);
    Uni<FindByIdWithdrawResponse> getWithdraw(String id);
    Uni<CreateWithdrawResponse> createWithdraw(CreateWithdrawBody body);
    Uni<UpdateWithdrawResponse> updateWithdraw(String id, CreateWithdrawBody body);
    Uni<TrashedWithdrawResponse> deleteWithdraw(String id);
    Uni<SimpleStatusMessageResponse> deleteWithdrawPermanent(String id);
    Uni<TrashedWithdrawResponse> trashWithdraw(String id);
    Uni<TrashedWithdrawResponse> restoreWithdraw(String id);
    Uni<SimpleStatusMessageResponse> restoreAllWithdraws();
    Uni<SimpleStatusMessageResponse> deleteAllWithdraws();

    Uni<ApiResponseWithdrawMonthAmount> findMonthlyAmounts(int year);
    Uni<ApiResponseWithdrawYearAmount> findYearlyAmounts(int year);
    Uni<ApiResponseWithdrawMonthAmount> findMonthlyByCard(int year, String cardNumber);
    Uni<ApiResponseWithdrawYearAmount> findYearlyByCard(int year, String cardNumber);

    Uni<ApiResponseWithdrawMonthStatusSuccess> findMonthlyStatusSuccess(int year, int month);
    Uni<ApiResponseWithdrawYearStatusSuccess> findYearlyStatusSuccess(int year);
    Uni<ApiResponseWithdrawMonthStatusFailed> findMonthlyStatusFailed(int year, int month);
    Uni<ApiResponseWithdrawYearStatusFailed> findYearlyStatusFailed(int year);

    Uni<ApiResponseWithdrawMonthStatusSuccess> findMonthlyStatusSuccessByCard(int year, int month, String cardNumber);
    Uni<ApiResponseWithdrawYearStatusSuccess> findYearlyStatusSuccessByCard(int year, String cardNumber);
    Uni<ApiResponseWithdrawMonthStatusFailed> findMonthlyStatusFailedByCard(int year, int month, String cardNumber);
    Uni<ApiResponseWithdrawYearStatusFailed> findYearlyStatusFailedByCard(int year, String cardNumber);
}
