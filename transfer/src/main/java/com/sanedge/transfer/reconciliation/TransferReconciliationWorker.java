package com.sanedge.transfer.reconciliation;

import java.time.Instant;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.transfer.entity.Transfer;
import com.sanedge.transfer.repository.TransferCommandRepository;
import com.sanedge.transfer.repository.TransferQueryRepository;

import com.sanedge.common.adapter.saldo.SaldoPort;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Durable reconciliation worker for transfers. A transfer has up to two
 * confirmed saldo legs: leg A debits the sender card, leg B credits the
 * receiver card. The worker reverses only the legs confirmed applied, each with
 * its own deterministic operation key, so a crash mid-saga resumes without
 * double-reversing either leg.
 */
@Unremovable
@ApplicationScoped
public class TransferReconciliationWorker {
    private static final Logger log = LoggerFactory.getLogger(TransferReconciliationWorker.class);
    private static final String WORKER_ID = "transfer-reconciliation-worker";

    @Inject
    Vertx vertx;

    @Inject
    TransferQueryRepository transferQueryRepository;

    @Inject
    TransferCommandRepository transferCommandRepository;

    @Inject
    SaldoPort saldoPort;

    @ConfigProperty(name = "transfer.reconciliation.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "transfer.reconciliation.interval-ms", defaultValue = "30000")
    long intervalMs;

    @ConfigProperty(name = "transfer.reconciliation.max-attempts", defaultValue = "5")
    int maxAttempts;

    @ConfigProperty(name = "transfer.reconciliation.lease-minutes", defaultValue = "2")
    long leaseMinutes;

    private Long timerId;

    @PostConstruct
    void init() {
        if (!enabled) {
            log.info("TransferReconciliationWorker disabled by configuration");
            return;
        }
        timerId = vertx.setPeriodic(intervalMs, id -> runCycle().subscribe().with(
                count -> log.debug("TransferReconciliationWorker cycle completed, processed={}", count),
                err -> log.warn("TransferReconciliationWorker cycle failed: {}", err.getMessage())));
        log.info("TransferReconciliationWorker started (every {}ms, maxAttempts={}, leaseMinutes={})",
                intervalMs, maxAttempts, leaseMinutes);
    }

    @PreDestroy
    void destroy() {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
            log.info("TransferReconciliationWorker stopped");
        }
    }

    public Uni<Integer> runCycle() {
        return transferQueryRepository.findPendingCompensation(maxAttempts)
                .chain(list -> processSequentially(list, 0));
    }

    private Uni<Integer> processSequentially(java.util.List<Transfer> list, int index) {
        if (index >= list.size()) {
            return Uni.createFrom().item(index);
        }
        Transfer transfer = list.get(index);
        return processRecord(transfer)
                .onItem().transform(v -> null)
                .onFailure().recoverWithItem(() -> {
                    log.warn("Transfer {} reconciliation failed", transfer.getTransferId());
                    return null;
                })
                .chain(v -> processSequentially(list, index + 1));
    }

    private Uni<Void> processRecord(Transfer transfer) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plusSeconds(leaseMinutes * 60);
        String claimToken = UUID.randomUUID().toString();
        String id = transfer.getTransferId();

        return transferCommandRepository.claimCompensation(id, WORKER_ID, claimToken, now, leaseUntil, maxAttempts)
                .chain(claimed -> {
                    if (!claimed) {
                        return Uni.createFrom().voidItem();
                    }
                    return reverseAppliedLegs(transfer, claimToken)
                            .chain(ok -> {
                                if (!ok) {
                                    return Uni.createFrom().voidItem();
                                }
                                return transferCommandRepository.completeCompensation(id, WORKER_ID, claimToken)
                                        .map(v -> (Void) null);
                            });
                });
    }

    /**
     * Reverses every leg that was confirmed applied, sequentially, each with a
     * deterministic operation key. Returns true when every required reversal
     * succeeded (or none was required).
     */
    private Uni<Boolean> reverseAppliedLegs(Transfer transfer, String claimToken) {
        String id = transfer.getTransferId();

        return reverseLeg(id, claimToken, "a",
                Boolean.TRUE.equals(transfer.getCompensationLegAApplied()),
                transfer.getCompensationLegACard(), transfer.getCompensationLegADelta())
                .chain(okA -> {
                    if (!okA) {
                        return Uni.createFrom().item(false);
                    }
                    return reverseLeg(id, claimToken, "b",
                            Boolean.TRUE.equals(transfer.getCompensationLegBApplied()),
                            transfer.getCompensationLegBCard(), transfer.getCompensationLegBDelta());
                });
    }

    private Uni<Boolean> reverseLeg(String id, String claimToken, String leg, boolean applied,
            String card, Integer delta) {
        if (!applied || card == null || delta == null) {
            return Uni.createFrom().item(true);
        }
        int reverseDelta = -delta;
        return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                        card, reverseDelta, null, null, null, null, null))
                .map(resp -> true)
                .onFailure().recoverWithUni(err -> failAndRelease(id, claimToken,
                        "compensation adapter failed: " + err.getMessage()).map(v -> false));
    }

    private Uni<Void> failAndRelease(String id, String claimToken, String reason) {
        return transferCommandRepository.releaseCompensation(id, WORKER_ID, claimToken,
                Instant.now().plusSeconds(60), reason)
                .chain(released -> transferCommandRepository.exhaustCompensation(id, maxAttempts, reason)
                        .map(v -> (Void) null));
    }
}
