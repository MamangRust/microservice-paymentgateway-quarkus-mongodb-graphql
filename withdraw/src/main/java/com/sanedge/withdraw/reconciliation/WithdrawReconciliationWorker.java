package com.sanedge.withdraw.reconciliation;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.withdraw.entity.Withdraw;
import com.sanedge.withdraw.repository.WithdrawCommandRepository;
import com.sanedge.withdraw.repository.WithdrawQueryRepository;

import com.sanedge.common.adapter.saldo.SaldoPort;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Durable reconciliation worker for withdraws. A withdraw only ever debits the
 * card balance (leg A); compensation credits the same amount back through the
 * idempotent saldo mutation ledger. The operation key is deterministic per
 * ledger record so lease steals never double-credit.
 */
@Unremovable
@ApplicationScoped
public class WithdrawReconciliationWorker {
    private static final Logger log = LoggerFactory.getLogger(WithdrawReconciliationWorker.class);
    private static final String WORKER_ID = "withdraw-reconciliation-worker";

    @Inject
    Vertx vertx;

    @Inject
    WithdrawQueryRepository withdrawQueryRepository;

    @Inject
    WithdrawCommandRepository withdrawCommandRepository;

    @Inject
    SaldoPort saldoPort;

    @ConfigProperty(name = "withdraw.reconciliation.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "withdraw.reconciliation.interval-ms", defaultValue = "30000")
    long intervalMs;

    @ConfigProperty(name = "withdraw.reconciliation.max-attempts", defaultValue = "5")
    int maxAttempts;

    @ConfigProperty(name = "withdraw.reconciliation.lease-minutes", defaultValue = "2")
    long leaseMinutes;

    private Long timerId;

    @PostConstruct
    void init() {
        if (!enabled) {
            log.info("WithdrawReconciliationWorker disabled by configuration");
            return;
        }
        timerId = vertx.setPeriodic(intervalMs, id -> runCycle().subscribe().with(
                count -> log.debug("WithdrawReconciliationWorker cycle completed, processed={}", count),
                err -> log.warn("WithdrawReconciliationWorker cycle failed: {}", err.getMessage())));
        log.info("WithdrawReconciliationWorker started (every {}ms, maxAttempts={}, leaseMinutes={})",
                intervalMs, maxAttempts, leaseMinutes);
    }

    @PreDestroy
    void destroy() {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
            log.info("WithdrawReconciliationWorker stopped");
        }
    }

    public Uni<Integer> runCycle() {
        return withdrawQueryRepository.findPendingCompensation(maxAttempts)
                .chain(list -> processSequentially(list, 0));
    }

    private Uni<Integer> processSequentially(java.util.List<Withdraw> list, int index) {
        if (index >= list.size()) {
            return Uni.createFrom().item(index);
        }
        Withdraw withdraw = list.get(index);
        return processRecord(withdraw)
                .onItem().transform(v -> null)
                .onFailure().recoverWithItem(() -> {
                    log.warn("Withdraw {} reconciliation failed", withdraw.getWithdrawId());
                    return null;
                })
                .chain(v -> processSequentially(list, index + 1));
    }

    private Uni<Void> processRecord(Withdraw withdraw) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(Duration.ofMinutes(leaseMinutes));
        String claimToken = UUID.randomUUID().toString();
        String id = withdraw.getWithdrawId();

        return withdrawCommandRepository.claimCompensation(id, WORKER_ID, claimToken, now, leaseUntil, maxAttempts)
                .chain(claimed -> {
                    if (!claimed) {
                        return Uni.createFrom().voidItem();
                    }
                    boolean applied = Boolean.TRUE.equals(withdraw.getCompensationLegAApplied());
                    String card = withdraw.getCompensationLegACard();
                    Integer delta = withdraw.getCompensationLegADelta();

                    if (!applied || card == null || delta == null) {
                        return withdrawCommandRepository.completeCompensation(id, WORKER_ID, claimToken)
                                .map(v -> (Void) null);
                    }
                    int reverseDelta = -delta;
                    return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                    card, reverseDelta, null, null, null, null, null))
                            .chain(resp -> withdrawCommandRepository.completeCompensation(id, WORKER_ID, claimToken)
                                    .map(v -> (Void) null))
                            .onFailure().recoverWithUni(err -> failAndRelease(id, claimToken,
                                    "compensation adapter failed: " + err.getMessage()));
                });
    }

    private Uni<Void> failAndRelease(String id, String claimToken, String reason) {
        return withdrawCommandRepository.releaseCompensation(id, WORKER_ID, claimToken,
                Instant.now().plus(Duration.ofSeconds(60)), reason)
                .chain(released -> withdrawCommandRepository.exhaustCompensation(id, maxAttempts, reason)
                        .map(v -> (Void) null));
    }
}
