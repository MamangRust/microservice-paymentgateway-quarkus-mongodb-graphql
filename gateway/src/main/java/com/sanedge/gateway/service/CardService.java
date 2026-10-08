package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.CardDto.*;
import io.smallrye.mutiny.Uni;

public interface CardService {
    Uni<FindAllCardResponse> listCards(int page, int size, String search);
    Uni<FindAllCardResponse> findActiveCards(int page, int size, String search);
    Uni<FindAllCardResponse> findTrashedCards(int page, int size, String search);
    Uni<FindByIdCardResponse> getCard(String id);
    Uni<FindByIdCardResponse> findCardByUser(String userId);
    Uni<FindByIdCardResponse> findCardByNumber(String cardNumber);
    Uni<CardWithEmailResponse> findUserCardByCardNumber(String cardNumber);
    Uni<CreateCardResponse> createCard(CreateCardRequest body);
    Uni<UpdateCardResponse> updateCard(String id, UpdateCardRequest body);
    Uni<SimpleStatusMessageResponse> deleteCardPermanent(String id);
    Uni<TrashedCardResponse> deleteCard(String id);
    Uni<TrashedCardResponse> trashCard(String id);
    Uni<TrashedCardResponse> restoreCard(String id);
    Uni<SimpleStatusMessageResponse> restoreAllCards();
    Uni<SimpleStatusMessageResponse> deleteAllCards();

    Uni<ApiResponseMonthlyBalance> findMonthlyBalance(int year);
    Uni<ApiResponseYearlyBalance> findYearlyBalance(int year);
    Uni<ApiResponseMonthlyBalance> getMonthlyBalanceByCard(int year, String cardNumber);
    Uni<ApiResponseYearlyBalance> getYearlyBalanceByCard(int year, String cardNumber);

    Uni<ApiResponseMonthlyAmount> findMonthlyTopupAmount(int year);
    Uni<ApiResponseYearlyAmount> findYearlyTopupAmount(int year);
    Uni<ApiResponseMonthlyAmount> getMonthlyTopupAmountByCard(int year, String cardNumber);
    Uni<ApiResponseYearlyAmount> getYearlyTopupAmountByCard(int year, String cardNumber);

    Uni<ApiResponseMonthlyAmount> findMonthlyTransactionAmount(int year);
    Uni<ApiResponseYearlyAmount> findYearlyTransactionAmount(int year);
    Uni<ApiResponseMonthlyAmount> getMonthlyTransactionAmountByCard(int year, String cardNumber);
    Uni<ApiResponseYearlyAmount> getYearlyTransactionAmountByCard(int year, String cardNumber);

    Uni<ApiResponseMonthlyAmount> findMonthlyTransferAmountSender(int year);
    Uni<ApiResponseMonthlyAmount> findMonthlyTransferAmountReceiver(int year);
    Uni<ApiResponseYearlyAmount> findYearlyTransferAmountSender(int year);
    Uni<ApiResponseYearlyAmount> findYearlyTransferAmountReceiver(int year);
    Uni<ApiResponseMonthlyAmount> getMonthlyTransferAmountByCardSender(int year, String cardNumber);
    Uni<ApiResponseMonthlyAmount> getMonthlyTransferAmountByCardReceiver(int year, String cardNumber);
    Uni<ApiResponseYearlyAmount> getYearlyTransferAmountByCardSender(int year, String cardNumber);
    Uni<ApiResponseYearlyAmount> getYearlyTransferAmountByCardReceiver(int year, String cardNumber);

    Uni<ApiResponseMonthlyAmount> findMonthlyWithdrawAmount(int year);
    Uni<ApiResponseYearlyAmount> findYearlyWithdrawAmount(int year);
    Uni<ApiResponseMonthlyAmount> getMonthlyWithdrawAmountByCard(int year, String cardNumber);
    Uni<ApiResponseYearlyAmount> getYearlyWithdrawAmountByCard(int year, String cardNumber);

    Uni<ApiResponseDashboardCard> findCardDashboard();
    Uni<ApiResponseDashboardCardNumber> findCardDashboardByCardNumber(String cardNumber);
}
