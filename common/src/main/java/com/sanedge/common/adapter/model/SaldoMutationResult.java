package com.sanedge.common.adapter.model;

/**
 * Slim saldo view returned by balance mutations (debit/credit/update).
 */
public record SaldoMutationResult(String saldoId, String cardNumber, int totalBalance) {
}
