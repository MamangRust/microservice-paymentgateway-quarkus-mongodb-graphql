package com.sanedge.topup.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.model.SaldoMutationResult;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.common.enums.Status;
import com.sanedge.topup.entity.Topup;
import com.sanedge.topup.repository.TopupCommandRepository;
import com.sanedge.topup.repository.TopupQueryRepository;

import io.smallrye.mutiny.Uni;

@ExtendWith(MockitoExtension.class)
class TopupReconciliationWorkerTest {

    private static final String TOPUP_ID = "507f1f77bcf86cd799439011";

    @Mock
    TopupQueryRepository topupQueryRepository;

    @Mock
    TopupCommandRepository topupCommandRepository;

    @Mock
    SaldoPort saldoPort;

    private TopupReconciliationWorker worker;

    @BeforeEach
    void setUp() throws Exception {
        worker = new TopupReconciliationWorker();
        setField("topupQueryRepository", topupQueryRepository);
        setField("topupCommandRepository", topupCommandRepository);
        setField("saldoPort", saldoPort);
        // config fields are only read by init()/claims; set sensible values
        setField("enabled", false);
        setField("intervalMs", 30000L);
        setField("maxAttempts", 5);
        setField("leaseMinutes", 2L);
    }

    private void setField(String name, Object value) throws Exception {
        Field f = TopupReconciliationWorker.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(worker, value);
    }

    private Topup pendingTopup(boolean applied, int delta) {
        Topup t = new Topup();
        t.setTopupId(TOPUP_ID);
        t.setTopupNo(UUID.randomUUID());
        t.setCardNumber("1234-5678-9012-3456");
        t.setTopupAmount(delta);
        t.setStatus(Status.COMPENSATION_REQUIRED);
        t.setCompensationAttempts(0);
        t.setCompensationLegACard("1234-5678-9012-3456");
        t.setCompensationLegADelta(delta);
        t.setCompensationLegAApplied(applied);
        t.setCreatedAt(Instant.now());
        t.setUpdatedAt(Instant.now());
        return t;
    }

    @Test
    void appliesReverseDeltaWithDeterministicKeyAndCompletes() {
        when(topupQueryRepository.findPendingCompensation(5))
                .thenReturn(Uni.createFrom().item(Collections.singletonList(pendingTopup(true, 50000))));
        when(topupCommandRepository.claimCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(Uni.createFrom().item(true));
        when(saldoPort.updateSaldoBalance(any()))
                .thenReturn(Uni.createFrom().item(new SaldoMutationResult("99", "1234-5678-9012-3456", 50000)));
        when(topupCommandRepository.completeCompensation(anyString(), anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(true));

        int processed = worker.runCycle().await().indefinitely();

        assertThat(processed).isEqualTo(1);
        ArgumentCaptor<SaldoPort.BalanceUpdate> captor = ArgumentCaptor.forClass(SaldoPort.BalanceUpdate.class);
        verify(saldoPort).updateSaldoBalance(captor.capture());
        assertThat(captor.getValue().cardNumber()).isEqualTo("1234-5678-9012-3456");
        assertThat(captor.getValue().totalBalance()).isEqualTo(-50000);
        verify(topupCommandRepository).completeCompensation(eq(TOPUP_ID), eq("topup-reconciliation-worker"), anyString());
    }

    @Test
    void releasesAndExhaustsWhenAdapterFails() {
        when(topupQueryRepository.findPendingCompensation(5))
                .thenReturn(Uni.createFrom().item(Collections.singletonList(pendingTopup(true, 50000))));
        when(topupCommandRepository.claimCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(Uni.createFrom().item(true));
        when(saldoPort.updateSaldoBalance(any()))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("saldo down")));
        when(topupCommandRepository.releaseCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), anyString()))
                .thenReturn(Uni.createFrom().item(true));
        when(topupCommandRepository.exhaustCompensation(anyString(), anyInt(), anyString()))
                .thenReturn(Uni.createFrom().item(true));

        int processed = worker.runCycle().await().indefinitely();

        assertThat(processed).isEqualTo(1);
        verify(topupCommandRepository).releaseCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), anyString());
        verify(topupCommandRepository).exhaustCompensation(anyString(), anyInt(), anyString());
        verify(topupCommandRepository, never()).completeCompensation(anyString(), anyString(), anyString());
    }

    @Test
    void completesWithoutReversalWhenNoLegWasApplied() {
        when(topupQueryRepository.findPendingCompensation(5))
                .thenReturn(Uni.createFrom().item(Collections.singletonList(pendingTopup(false, 50000))));
        when(topupCommandRepository.claimCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(Uni.createFrom().item(true));
        when(topupCommandRepository.completeCompensation(anyString(), anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(true));

        int processed = worker.runCycle().await().indefinitely();

        assertThat(processed).isEqualTo(1);
        verify(saldoPort, never()).updateSaldoBalance(any());
        verify(topupCommandRepository).completeCompensation(eq(TOPUP_ID), eq("topup-reconciliation-worker"), anyString());
    }

    @Test
    void skipsRecordsClaimedByAnotherWorker() {
        when(topupQueryRepository.findPendingCompensation(5))
                .thenReturn(Uni.createFrom().item(Collections.singletonList(pendingTopup(true, 50000))));
        when(topupCommandRepository.claimCompensation(anyString(), anyString(), anyString(),
                any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(Uni.createFrom().item(false));

        int processed = worker.runCycle().await().indefinitely();

        assertThat(processed).isEqualTo(1);
        verify(saldoPort, never()).updateSaldoBalance(any());
        verify(topupCommandRepository, never()).completeCompensation(anyString(), anyString(), anyString());
    }
}
