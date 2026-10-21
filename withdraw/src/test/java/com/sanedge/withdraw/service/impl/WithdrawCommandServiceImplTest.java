package com.sanedge.withdraw.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.adapter.fraud.FraudPort;
import com.sanedge.common.adapter.model.Card;
import com.sanedge.common.adapter.model.Saldo;
import com.sanedge.common.adapter.model.SaldoMutationResult;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.withdraw.domain.requests.CreateWithdrawRequest;
import com.sanedge.withdraw.domain.requests.UpdateWithdrawRequest;
import com.sanedge.withdraw.domain.response.WithdrawResponse;
import com.sanedge.withdraw.domain.response.WithdrawResponseDeleteAt;
import com.sanedge.withdraw.entity.Withdraw;
import com.sanedge.withdraw.repository.WithdrawCommandRepository;
import com.sanedge.withdraw.repository.WithdrawQueryRepository;
import com.sanedge.withdraw.service.KafkaService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Validator;

@ExtendWith(MockitoExtension.class)
class WithdrawCommandServiceImplTest {

        private static final String WITHDRAW_ID = "507f1f77bcf86cd799439011";
        private static final String MISSING_ID = "000000000000000000000000";
        private static final String CARD_NUMBER = "1234567890123456";

        @Mock
        private WithdrawQueryRepository withdrawQueryRepository;

        @Mock
        private WithdrawCommandRepository withdrawCommandRepo;

        @Mock
        private CardPort cardPort;

        @Mock
        private SaldoPort saldoPort;

        @Mock
        private FraudPort fraudPort;

        @Mock
        private Validator validator;

        @Mock
        private RedisService redisService;

        @Mock
        private KafkaService kafkaService;

        @Mock
        private TracingMetrics tracingMetrics;

        private WithdrawCommandServiceImpl service;

        @BeforeEach
        void setUp() {
                service = new WithdrawCommandServiceImpl(
                                withdrawQueryRepository,
                                withdrawCommandRepo,
                                cardPort,
                                saldoPort,
                                fraudPort,
                                validator,
                                redisService,
                                kafkaService,
                                tracingMetrics);

                // Default: security verification allows the withdraw.
                lenient().when(fraudPort.verifySecurity(any(), any(), anyDouble()))
                                .thenReturn(Uni.createFrom().item(
                                                new FraudPort.SecurityResult(true, 0.1, "ok", "ALLOW")));

                lenient().doAnswer(invocation -> {
                        Supplier<Uni<?>> supplier = invocation.getArgument(3);
                        return supplier.get();
                }).when(tracingMetrics)
                                .traceAndMeasure(
                                                anyString(),
                                                anyString(),
                                                any(Attributes.class),
                                                any());

                lenient().when(redisService.deleteReactive(anyString()))
                                .thenReturn(Uni.createFrom().voidItem());

                lenient().doAnswer(invocation -> {
                        Supplier<Uni<?>> supplier = invocation.getArgument(2);
                        return supplier.get();
                }).when(tracingMetrics)
                                .traceAndMeasure(anyString(), anyString(), any());

                lenient().when(withdrawCommandRepo.deleteAllDeleted())
                                .thenReturn(Uni.createFrom().item(true));
                lenient().when(withdrawCommandRepo.restoreAllDeleted())
                                .thenReturn(Uni.createFrom().item(true));
        }

        private static Card cardWithEmail() {
                return new Card("card-1", "user-1", CARD_NUMBER, "VISA", "2030-01-01", "123", "BANK",
                                "test@example.com", null, null);
        }

        private static Saldo saldo(int balance) {
                return new Saldo("99", CARD_NUMBER, balance, null, null, null, null);
        }

        private Withdraw createTestWithdraw(String id, String cardNumber, Long amount, Status status) {
                Withdraw w = new Withdraw();
                w.setWithdrawId(id);
                w.setWithdrawNo(UUID.randomUUID());
                w.setCardNumber(cardNumber);
                w.setWithdrawAmount(amount.intValue());
                w.setStatus(status);
                w.setWithdrawTime(Instant.parse("2024-06-15T10:30:00Z"));
                w.setCreatedAt(Instant.now());
                w.setUpdatedAt(Instant.now());
                return w;
        }

        private CreateWithdrawRequest createValidCreateRequest() {
                CreateWithdrawRequest req = new CreateWithdrawRequest();
                req.setCardNumber(CARD_NUMBER);
                req.setWithdrawAmount(150000L);
                req.setWithdrawTime(LocalDateTime.of(2024, 6, 15, 10, 30, 0));
                return req;
        }

        private UpdateWithdrawRequest createValidUpdateRequest() {
                UpdateWithdrawRequest req = new UpdateWithdrawRequest();
                req.setWithdrawId(WITHDRAW_ID);
                req.setCardNumber(CARD_NUMBER);
                req.setWithdrawAmount(200000L);
                req.setWithdrawTime(LocalDateTime.of(2024, 6, 16, 12, 0, 0));
                return req;
        }

        @Nested
        @DisplayName("create withdraw tests")
        class CreateWithdrawTests {

                @Test
                @DisplayName("should successfully create withdraw")
                void create_Success() {
                        CreateWithdrawRequest req = createValidCreateRequest();

                        when(cardPort.findUserCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(saldoPort.findByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(saldo(500000)));
                        when(saldoPort.updateSaldoWithdraw(any()))
                                        .thenReturn(Uni.createFrom()
                                                        .item(new SaldoMutationResult("99", CARD_NUMBER, 350000)));

                        when(withdrawCommandRepo.persist(any(Withdraw.class)))
                                        .thenAnswer(inv -> {
                                                Withdraw w = inv.getArgument(0);
                                                if (w.getWithdrawId() == null) {
                                                        w.setWithdrawId(WITHDRAW_ID);
                                                }
                                                return Uni.createFrom().item(w);
                                        });
                        when(withdrawCommandRepo.updateStatus(any(), anyString()))
                                        .thenAnswer(inv -> {
                                                Withdraw w = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L,
                                                                Status.SUCCESS);
                                                return Uni.createFrom().item(w);
                                        });

                        lenient().when(kafkaService.sendMessage(anyString(), anyString(), any()))
                                        .thenReturn(Uni.createFrom().voidItem());

                        ApiResponse<WithdrawResponse> response = service.create(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Created withdraw successfully");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(WITHDRAW_ID);
                        assertThat(response.data().getCardNumber()).isEqualTo(CARD_NUMBER);
                        assertThat(response.data().getWithdrawAmount()).isEqualTo(150000L);

                        verify(redisService, atLeastOnce()).deleteReactive(anyString());
                }

                @Test
                @DisplayName("should reject withdraw when ai-security denies and not debit")
                void create_DeniedBySecurity() {
                        CreateWithdrawRequest req = createValidCreateRequest();

                        when(cardPort.findUserCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(fraudPort.verifySecurity(any(), any(), anyDouble()))
                                        .thenReturn(Uni.createFrom().item(new FraudPort.SecurityResult(
                                                        false, 0.9, "Critical risk detected", "DENY")));

                        ApiResponse<WithdrawResponse> response = service.create(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.data()).isNull();
                        verify(saldoPort, org.mockito.Mockito.never()).updateSaldoWithdraw(any());
                }

                @Test
                @DisplayName("should fail open and succeed when ai-security is unreachable")
                void create_SecurityUnreachable_FailsOpen() {
                        CreateWithdrawRequest req = createValidCreateRequest();

                        when(cardPort.findUserCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(fraudPort.verifySecurity(any(), any(), anyDouble()))
                                        .thenReturn(Uni.createFrom()
                                                        .failure(io.grpc.Status.UNAVAILABLE.asRuntimeException()));
                        when(saldoPort.findByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(saldo(500000)));
                        when(saldoPort.updateSaldoWithdraw(any()))
                                        .thenReturn(Uni.createFrom()
                                                        .item(new SaldoMutationResult("99", CARD_NUMBER, 350000)));

                        when(withdrawCommandRepo.persist(any(Withdraw.class)))
                                        .thenAnswer(inv -> {
                                                Withdraw w = inv.getArgument(0);
                                                if (w.getWithdrawId() == null) {
                                                        w.setWithdrawId(WITHDRAW_ID);
                                                }
                                                return Uni.createFrom().item(w);
                                        });
                        when(withdrawCommandRepo.updateStatus(any(), anyString()))
                                        .thenAnswer(inv -> Uni.createFrom().item(
                                                        createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L,
                                                                        Status.SUCCESS)));
                        lenient().when(kafkaService.sendMessage(anyString(), anyString(), any()))
                                        .thenReturn(Uni.createFrom().voidItem());

                        ApiResponse<WithdrawResponse> response = service.create(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("success");
                        verify(saldoPort, atLeastOnce()).updateSaldoWithdraw(any());
                }
        }

        @Nested
        @DisplayName("update withdraw tests")
        class UpdateWithdrawTests {

                @Test
                @DisplayName("should fail when withdraw not found")
                void update_NotFound() {
                        UpdateWithdrawRequest req = createValidUpdateRequest();

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(withdrawQueryRepository.findById(WITHDRAW_ID))
                                        .thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<WithdrawResponse> response = service.update(req).await().indefinitely();
                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to update withdraw");
                }

                @Test
                @DisplayName("should fail when withdraw is SUCCESS and cannot be modified")
                void update_AlreadySuccess() {
                        UpdateWithdrawRequest req = createValidUpdateRequest();

                        Withdraw existingWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.SUCCESS);

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(withdrawQueryRepository.findById(WITHDRAW_ID))
                                        .thenReturn(Uni.createFrom().item(existingWithdraw));

                        ApiResponse<WithdrawResponse> response = service.update(req).await().indefinitely();
                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to update withdraw");
                }

                @Test
                @DisplayName("should fail when withdraw is FAILED and cannot be modified")
                void update_AlreadyFailed() {
                        UpdateWithdrawRequest req = createValidUpdateRequest();

                        Withdraw existingWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.FAILED);

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(withdrawQueryRepository.findById(WITHDRAW_ID))
                                        .thenReturn(Uni.createFrom().item(existingWithdraw));

                        ApiResponse<WithdrawResponse> response = service.update(req).await().indefinitely();
                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to update withdraw");
                }

                @Test
                @DisplayName("should successfully update withdraw on happy path")
                void update_Success() {
                        UpdateWithdrawRequest req = createValidUpdateRequest();

                        Withdraw existingWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.PENDING);

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));
                        when(withdrawQueryRepository.findById(WITHDRAW_ID))
                                        .thenReturn(Uni.createFrom().item(existingWithdraw));
                        when(saldoPort.findByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(saldo(500000)));
                        when(saldoPort.updateSaldoWithdraw(any()))
                                        .thenReturn(Uni.createFrom()
                                                        .item(new SaldoMutationResult("99", CARD_NUMBER, 450000)));

                        when(withdrawCommandRepo.persist(any(Withdraw.class)))
                                        .thenAnswer(inv -> Uni.createFrom().item((Withdraw) inv.getArgument(0)));
                        when(withdrawCommandRepo.updateStatus(any(), anyString()))
                                        .thenAnswer(inv -> {
                                                Withdraw w = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 200000L,
                                                                Status.SUCCESS);
                                                return Uni.createFrom().item(w);
                                        });

                        ApiResponse<WithdrawResponse> response = service.update(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Updated withdraw successfully");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(WITHDRAW_ID);
                        assertThat(response.data().getWithdrawAmount()).isEqualTo(200000L);

                        verify(redisService, atLeastOnce()).deleteReactive(anyString());
                }
        }

        @Nested
        @DisplayName("trash withdraw tests")
        class TrashWithdrawTests {

                @Test
                @DisplayName("should successfully trash existing withdraw")
                void trash_Success() {
                        Withdraw trashedWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.PENDING);
                        trashedWithdraw.setDeletedAt(Instant.now());

                        when(withdrawCommandRepo.trashed(WITHDRAW_ID)).thenReturn(Uni.createFrom().item(trashedWithdraw));

                        ApiResponse<WithdrawResponseDeleteAt> response = service.trashed(WITHDRAW_ID).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Withdraw trashed successfully!");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(WITHDRAW_ID);
                        assertThat(response.data().getDeletedAt()).isNotNull();
                }

                @Test
                @DisplayName("should fail when withdraw not found for trash")
                void trash_NotFound() {
                        when(withdrawCommandRepo.trashed(MISSING_ID)).thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<WithdrawResponseDeleteAt> response = service.trashed(MISSING_ID).await().indefinitely();
                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to trash withdraw");
                }
        }

        @Nested
        @DisplayName("restore withdraw tests")
        class RestoreWithdrawTests {

                @Test
                @DisplayName("should successfully restore trashed withdraw")
                void restore_Success() {
                        Withdraw restoredWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.PENDING);

                        when(withdrawCommandRepo.restore(WITHDRAW_ID)).thenReturn(Uni.createFrom().item(restoredWithdraw));

                        ApiResponse<WithdrawResponseDeleteAt> response = service.restore(WITHDRAW_ID).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Withdraw restored successfully!");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(WITHDRAW_ID);
                        assertThat(response.data().getDeletedAt()).isNull();
                }

                @Test
                @DisplayName("should fail when withdraw is not trashed")
                void restore_NotTrashed() {
                        when(withdrawCommandRepo.restore(WITHDRAW_ID)).thenReturn(Uni.createFrom().nullItem());

                        assertThatThrownBy(() -> service.restore(WITHDRAW_ID).await().indefinitely())
                                        .isInstanceOf(InvalidRequestException.class)
                                        .hasMessageContaining("must be trashed first");
                }
        }

        @Nested
        @DisplayName("delete by id tests")
        class DeleteByIdTests {

                @Test
                @DisplayName("should successfully permanently delete trashed withdraw")
                void deletePermanent_Success() {
                        Withdraw trashedWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.PENDING);
                        trashedWithdraw.setDeletedAt(Instant.now());
                        when(withdrawQueryRepository.findById(WITHDRAW_ID)).thenReturn(Uni.createFrom().item(trashedWithdraw));

                        when(withdrawCommandRepo.deletePermanent(WITHDRAW_ID)).thenReturn(Uni.createFrom().item(true));

                        ApiResponse<Boolean> response = service.deletePermanent(WITHDRAW_ID).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Withdraw permanently deleted!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when withdraw not found for permanent delete")
                void deletePermanent_NotFound() {
                        when(withdrawQueryRepository.findById(MISSING_ID)).thenReturn(Uni.createFrom().nullItem());

                        assertThatThrownBy(() -> service.deletePermanent(MISSING_ID).await().indefinitely())
                                        .isInstanceOf(InvalidRequestException.class)
                                        .hasMessageContaining(
                                                        "Withdraw not found or must be trashed before permanent deletion");
                }

                @Test
                @DisplayName("should fail when trying to permanently delete active withdraw")
                void deletePermanent_NotTrashed() {
                        Withdraw activeWithdraw = createTestWithdraw(WITHDRAW_ID, CARD_NUMBER, 150000L, Status.PENDING);
                        when(withdrawQueryRepository.findById(WITHDRAW_ID)).thenReturn(Uni.createFrom().item(activeWithdraw));

                        assertThatThrownBy(() -> service.deletePermanent(WITHDRAW_ID).await().indefinitely())
                                        .isInstanceOf(InvalidRequestException.class)
                                        .hasMessageContaining(
                                                        "Withdraw not found or must be trashed before permanent deletion");
                }
        }

        @Nested
        @DisplayName("delete all trashed tests")
        class DeleteAllTests {

                @Test
                @DisplayName("should successfully delete all trashed withdraws")
                void deleteAll_Success() {
                        ApiResponse<Boolean> response = service.deleteAll().await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("All withdraws permanently deleted!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when no trashed withdraws to delete")
                void deleteAll_NoTrashed() {
                        when(withdrawCommandRepo.deleteAllDeleted())
                                        .thenReturn(Uni.createFrom().item(false));

                        assertThatThrownBy(() -> service.deleteAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class)
                                        .hasMessageContaining("No trashed withdraws found");
                }
        }

        @Nested
        @DisplayName("restore all trashed tests")
        class RestoreAllTests {

                @Test
                @DisplayName("should successfully restore all trashed withdraws")
                void restoreAll_Success() {
                        ApiResponse<Boolean> response = service.restoreAll().await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("All withdraws restored successfully!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when no trashed withdraws to restore")
                void restoreAll_NoTrashed() {
                        when(withdrawCommandRepo.restoreAllDeleted())
                                        .thenReturn(Uni.createFrom().item(false));

                        assertThatThrownBy(() -> service.restoreAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class)
                                        .hasMessageContaining("No trashed withdraws found");
                }
        }
}
