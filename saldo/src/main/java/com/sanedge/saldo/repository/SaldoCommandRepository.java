package com.sanedge.saldo.repository;

import java.time.Instant;

import org.bson.types.ObjectId;

import com.sanedge.saldo.entity.Saldo;
import com.sanedge.saldo.entity.SaldoMutationOperation;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SaldoCommandRepository implements ReactivePanacheMongoRepository<Saldo> {

    @Inject
    SaldoMutationOperationRepository saldoMutationOperationRepository;

    public Uni<Integer> updateBalanceByDelta(String cardNumber, long delta, long minimumBalance, String operationKey) {
        Instant now = Instant.now();

        return saldoMutationOperationRepository.findByOperationKey(operationKey)
                .chain(existing -> {
                    // Exactly-once: an already APPLIED operation is never re-applied.
                    if (existing != null && "APPLIED".equals(existing.getResultStatus())) {
                        return Uni.createFrom().item(1);
                    }

                    return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber).firstResult()
                            .chain(saldo -> {
                                if (saldo == null) {
                                    return Uni.createFrom().item(0);
                                }

                                long currentBalance = saldo.getTotalBalance() != null ? saldo.getTotalBalance() : 0L;
                                long newBalance = currentBalance + delta;

                                if (newBalance < minimumBalance) {
                                    SaldoMutationOperation op = existing != null ? existing : new SaldoMutationOperation();
                                    op.setOperationKey(operationKey);
                                    op.setCardNumber(cardNumber);
                                    op.setRequestedDelta((int) delta);
                                    op.setMinimumBalance((int) minimumBalance);
                                    op.setResultStatus("REJECTED");
                                    op.setResultBalance((int) currentBalance);
                                    op.setFailureReason("Insufficient balance");
                                    op.setCreatedAt(now);
                                    op.setUpdatedAt(now);
                                    return saldoMutationOperationRepository.persistOrUpdate(op).map(v -> 0);
                                }

                                saldo.setTotalBalance((int) newBalance);
                                saldo.setUpdatedAt(now);

                                return saldo.update()
                                        .chain(s -> {
                                            SaldoMutationOperation op = existing != null ? existing : new SaldoMutationOperation();
                                            op.setOperationKey(operationKey);
                                            op.setCardNumber(cardNumber);
                                            op.setRequestedDelta((int) delta);
                                            op.setMinimumBalance((int) minimumBalance);
                                            op.setResultStatus("APPLIED");
                                            op.setResultBalance((int) newBalance);
                                            op.setFailureReason(null);
                                            op.setCreatedAt(now);
                                            op.setUpdatedAt(now);
                                            return saldoMutationOperationRepository.persistOrUpdate(op).map(v -> 1);
                                        });
                            });
                });
    }

    public Uni<Saldo> trashed(String saldoId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        saldo.setDeletedAt(Instant.now());
                        return saldo.update().map(v -> saldo);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Saldo> restore(String saldoId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        saldo.setDeletedAt(null);
                        return saldo.update().map(v -> saldo);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Integer> updateBalanceByCardNumber(String cardNumber, Long newBalance) {
        return update("{ '$set': { 'totalBalance': ?1, 'updatedAt': ?2 } }",
                newBalance != null ? newBalance.intValue() : 0, Instant.now())
                .where("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .map(count -> count.intValue());
    }

    public Uni<Integer> updateWithdrawByCardNumber(String cardNumber, Long withdrawAmount) {
        Instant now = Instant.now();
        return update("{ '$set': { 'withdrawAmount': ?1, 'withdrawTime': ?2, 'updatedAt': ?3 } }",
                withdrawAmount != null ? withdrawAmount.intValue() : 0, now, now)
                .where("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .map(count -> count.intValue());
    }

    public Uni<Boolean> deletePermanent(String saldoId) {
        return find("{ '_id': ?1 }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        return saldo.delete().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> restoreAllDeleted() {
        return update("{ '$set': { 'deletedAt': null } }")
                .where("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }

    public Uni<Boolean> deleteAllDeleted() {
        return delete("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }
}
