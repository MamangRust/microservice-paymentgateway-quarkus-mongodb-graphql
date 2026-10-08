package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.TransactionDto.*;
import io.smallrye.mutiny.Uni;

public interface TransactionService {
    Uni<FindAllTransactionResponse> listTransactions(int page, int size, String search);
    Uni<FindAllTransactionResponse> findTransactionsByCard(String cardNumber);
    Uni<FindAllTransactionResponse> findTransactionsByMerchant(String merchantId);
    Uni<FindAllTransactionResponse> findTransactionsByCardAndYear(String cardNumber, int year);
    Uni<FindAllTransactionResponse> findActiveTransactions(int page, int size, String search);
    Uni<FindAllTransactionResponse> findTrashedTransactions(int page, int size, String search);
    Uni<FindByIdTransactionResponse> getTransaction(String id);
    Uni<CreateTransactionResponse> createTransaction(CreateTransactionBody body);
    Uni<UpdateTransactionResponse> updateTransaction(String id, UpdateTransactionBody body);
    Uni<SimpleStatusMessageResponse> deleteTransactionPermanent(String id);
    Uni<TrashedTransactionResponse> trashTransaction(String id);
    Uni<TrashedTransactionResponse> restoreTransaction(String id);
    Uni<SimpleStatusMessageResponse> restoreAllTransactions();
    Uni<SimpleStatusMessageResponse> deleteAllTransactions();

    Uni<ApiResponseTransactionMonthAmount> findMonthlyAmounts(int year);
    Uni<ApiResponseTransactionYearAmount> findYearlyAmounts(int year);
    Uni<ApiResponseTransactionMonthAmount> findMonthlyAmountsByCard(int year, String cardNumber);
    Uni<ApiResponseTransactionYearAmount> findYearlyAmountsByCard(int year, String cardNumber);

    Uni<ApiResponseTransactionMonthMethod> findMonthlyMethods(int year, int month);
    Uni<ApiResponseTransactionYearMethod> findYearlyMethods(int year);
    Uni<ApiResponseTransactionMonthMethod> findMonthlyMethodsByCard(int year, int month, String cardNumber);
    Uni<ApiResponseTransactionYearMethod> findYearlyMethodsByCard(int year, String cardNumber);

    Uni<ApiResponseTransactionMonthStatusSuccess> findMonthlyStatusSuccess(int year, int month);
    Uni<ApiResponseTransactionYearStatusSuccess> findYearlyStatusSuccess(int year);
    Uni<ApiResponseTransactionMonthStatusFailed> findMonthlyStatusFailed(int year, int month);
    Uni<ApiResponseTransactionYearStatusFailed> findYearlyStatusFailed(int year);

    Uni<ApiResponseTransactionMonthStatusSuccess> findMonthlyStatusSuccessByCard(int year, int month, String cardNumber);
    Uni<ApiResponseTransactionYearStatusSuccess> findYearlyStatusSuccessByCard(int year, String cardNumber);
    Uni<ApiResponseTransactionMonthStatusFailed> findMonthlyStatusFailedByCard(int year, int month, String cardNumber);
    Uni<ApiResponseTransactionYearStatusFailed> findYearlyStatusFailedByCard(int year, String cardNumber);
}
