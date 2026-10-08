package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.MerchantDto.*;
import io.smallrye.mutiny.Uni;

public interface MerchantService {
    Uni<FindAllMerchantResponse> listMerchants(int page, int size, String search);
    Uni<FindByIdMerchantResponse> getMerchant(String id);
    Uni<FindByIdMerchantResponse> getMerchantByApiKey(String apiKey);
    Uni<FindAllMerchantResponse> getMerchantsByUserId(String userId);
    Uni<FindAllMerchantResponse> findActiveMerchants(int page, int size, String search);
    Uni<FindAllMerchantResponse> findTrashedMerchants(int page, int size, String search);
    Uni<CreateMerchantResponse> createMerchant(CreateMerchantRequest body);
    Uni<UpdateMerchantResponse> updateMerchant(String id, UpdateMerchantRequest body);
    Uni<UpdateMerchantResponse> updateMerchantStatus(String id, String status);
    Uni<SimpleStatusMessageResponse> deleteMerchantPermanent(String id);
    Uni<TrashedMerchantResponse> deleteMerchant(String id);
    Uni<TrashedMerchantResponse> restoreMerchant(String id);
    Uni<SimpleStatusMessageResponse> restoreAllMerchants();
    Uni<SimpleStatusMessageResponse> deleteAllMerchants();

    Uni<ApiResponsePaginationMerchantTransaction> findAllTransactions(int page, int size, String search, String merchantId);
    Uni<ApiResponsePaginationMerchantTransaction> findTransactionsById(int page, int size, String search, String id);
    Uni<ApiResponsePaginationMerchantTransaction> findTransactionsByApiKey(int page, int size, String search, String apiKey);

    Uni<ApiResponseMerchantMonthlyAmount> getMonthlyAmount(int year);
    Uni<ApiResponseMerchantYearlyAmount> getYearlyAmount(int year);
    Uni<ApiResponseMerchantMonthlyAmount> getMonthlyAmountById(int year, String merchantId);
    Uni<ApiResponseMerchantYearlyAmount> getYearlyAmountById(int year, String merchantId);
    Uni<ApiResponseMerchantMonthlyAmount> getMonthlyAmountByApiKey(int year, String apiKey);
    Uni<ApiResponseMerchantYearlyAmount> getYearlyAmountByApiKey(int year, String apiKey);

    Uni<ApiResponseMerchantMonthlyPaymentMethod> getMonthlyMethod(int year);
    Uni<ApiResponseMerchantYearlyPaymentMethod> getYearlyMethod(int year);
    Uni<ApiResponseMerchantMonthlyPaymentMethod> getMonthlyMethodById(int year, String merchantId);
    Uni<ApiResponseMerchantYearlyPaymentMethod> getYearlyMethodById(int year, String merchantId);
    Uni<ApiResponseMerchantMonthlyPaymentMethod> getMonthlyMethodByApiKey(int year, String apiKey);
    Uni<ApiResponseMerchantYearlyPaymentMethod> getYearlyMethodByApiKey(int year, String apiKey);

    Uni<ApiResponseMerchantMonthlyTotalAmount> getTotalMonthlyAmount(int year);
    Uni<ApiResponseMerchantYearlyTotalAmount> getTotalYearlyAmount(int year);
    Uni<ApiResponseMerchantMonthlyTotalAmount> getTotalMonthlyAmountById(int year, String merchantId);
    Uni<ApiResponseMerchantYearlyTotalAmount> getTotalYearlyAmountById(int year, String merchantId);
    Uni<ApiResponseMerchantMonthlyTotalAmount> getTotalMonthlyAmountByApiKey(int year, String apiKey);
    Uni<ApiResponseMerchantYearlyTotalAmount> getTotalYearlyAmountByApiKey(int year, String apiKey);
}
