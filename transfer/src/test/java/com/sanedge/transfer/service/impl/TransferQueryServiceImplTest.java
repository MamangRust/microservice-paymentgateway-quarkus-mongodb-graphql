package com.sanedge.transfer.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
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
import com.sanedge.common.adapter.model.Card;
import com.sanedge.common.adapter.model.Saldo;
import com.sanedge.common.adapter.model.SaldoMutationResult;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.transfer.domain.requests.CreateTransferRequest;
import com.sanedge.transfer.domain.requests.UpdateTransferRequest;
import com.sanedge.transfer.domain.response.TransferResponse;
import com.sanedge.transfer.domain.response.TransferResponseDeleteAt;
import com.sanedge.transfer.entity.Transfer;
import com.sanedge.transfer.repository.TransferCommandRepository;
import com.sanedge.transfer.repository.TransferQueryRepository;
import com.sanedge.transfer.service.KafkaService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Validator;

@ExtendWith(MockitoExtension.class)
class TransferCommandServiceImplTest {

        private static final String ID = "507f1f77bcf86cd799439011";

        @Mock
        private TransferQueryRepository transferQueryRepo;

        @Mock
        private TransferCommandRepository transferCommandRepo;

        @Mock
        private CardPort cardPort;

        @Mock
        private SaldoPort saldoPort;

        @Mock
        private Validator validator;

        @Mock
        private RedisService redisService;

        @Mock
        private KafkaService kafkaService;

        @Mock
        private TracingMetrics tracingMetrics;

        private TransferCommandServiceImpl service;

        @BeforeEach
        void setUp() {
                service = new TransferCommandServiceImpl(
                                transferCommandRepo,
                                cardPort,
                                saldoPort,
                                transferQueryRepo,
                                validator,
                                redisService,
                                kafkaService,
                                tracingMetrics);

                lenient().doAnswer(invocation -> {
                        Supplier<Uni<?>> supplier = invocation.getArgument(3);
                        return supplier.get();
                }).when(tracingMetrics)
                                .traceAndMeasure(anyString(), anyString(), any(Attributes.class), any());

                lenient().doAnswer(invocation -> {
                        Supplier<Uni<?>> supplier = invocation.getArgument(2);
                        return supplier.get();
                }).when(tracingMetrics)
                                .traceAndMeasure(anyString(), anyString(), any());

                lenient().when(redisService.deleteReactive(anyString()))
                                .thenReturn(Uni.createFrom().voidItem());

                lenient().when(transferCommandRepo.deleteAllDeleted())
                                .thenReturn(Uni.createFrom().item(true));
                lenient().when(transferCommandRepo.restoreAllDeleted())
                                .thenReturn(Uni.createFrom().item(true));
        }

        private Transfer createTestTransfer(String id, String from, String to, int amount, Status status) {
                Transfer t = new Transfer();
                t.setTransferId(id);
                t.setTransferNo(UUID.randomUUID());
                t.setTransferFrom(from);
                t.setTransferTo(to);
                t.setTransferAmount(amount);
                t.setStatus(status);
                Instant now = Instant.now();
                t.setTransferTime(now);
                t.setCreatedAt(now);
                t.setUpdatedAt(now);
                return t;
        }

        private CreateTransferRequest createValidCreateRequest() {
                CreateTransferRequest req = new CreateTransferRequest();
                req.setTransferFrom("111122223333");
                req.setTransferTo("444455556666");
                req.setTransferAmount(150000L);
                return req;
        }

        private UpdateTransferRequest createValidUpdateRequest() {
                UpdateTransferRequest req = new UpdateTransferRequest();
                req.setTransferId(ID);
                req.setTransferFrom("111122223333");
                req.setTransferTo("444455556666");
                req.setTransferAmount(200000L);
                return req;
        }

        private void mockCardAndSaldoServices() {
                Card senderCard = new Card("card-1", "user-1", "111122223333", "VISA", "2030-01-01", "123",
                                "BANK", "test@test.com", null, null);
                lenient().when(cardPort.findUserCardByCardNumber(any()))
                                .thenReturn(Uni.createFrom().item(senderCard));

                Card receiverCard = new Card("card-2", "user-2", "444455556666", "VISA", "2030-01-01", "123",
                                "BANK", null, null, null);
                lenient().when(cardPort.findCardByCardNumber(any()))
                                .thenReturn(Uni.createFrom().item(receiverCard));

                Saldo senderSaldo = new Saldo("saldo-1", "111122223333", 500000, null, null, null, null);
                lenient().when(saldoPort.findByCardNumber(any()))
                                .thenReturn(Uni.createFrom().item(senderSaldo));

                lenient().when(saldoPort.updateSaldoBalance(any()))
                                .thenReturn(Uni.createFrom()
                                                .item(new SaldoMutationResult("saldo-1", "111122223333", 350000)));

                lenient().when(kafkaService.sendMessage(anyString(), anyString(), any()))
                                .thenReturn(Uni.createFrom().voidItem());
        }

        @Nested
        @DisplayName("create transfer tests")
        class CreateTransferTests {

                @Test
                @DisplayName("should successfully create transfer on happy path")
                void create_Success() {
                        CreateTransferRequest req = createValidCreateRequest();
                        mockCardAndSaldoServices();

                        when(transferCommandRepo.persist(any(Transfer.class)))
                                        .thenAnswer(inv -> {
                                                Transfer t = inv.getArgument(0);
                                                if (t.getTransferId() == null)
                                                        t.setTransferId(ID);
                                                return Uni.createFrom().item(t);
                                        });
                        when(transferCommandRepo.updateTransferStatus(anyString(), anyString()))
                                        .thenAnswer(inv -> {
                                                String id = inv.getArgument(0);
                                                Transfer t = createTestTransfer(id, "111122223333", "444455556666",
                                                                150000, Status.SUCCESS);
                                                return Uni.createFrom().item(t);
                                        });

                        ApiResponse<TransferResponse> response = service.create(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Transfer created successfully");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(ID);

                        verify(redisService, atLeastOnce()).deleteReactive(anyString());
                }

                @SuppressWarnings({"unchecked", "rawtypes"})
                @Test
                @DisplayName("should fail when validation fails")
                void create_ValidationFails() {
                        CreateTransferRequest req = new CreateTransferRequest();
                        jakarta.validation.ConstraintViolation<?> violation = org.mockito.Mockito.mock(
                                        jakarta.validation.ConstraintViolation.class);
                        when(violation.getPropertyPath()).thenReturn(
                                        org.mockito.Mockito.mock(jakarta.validation.Path.class));
                        when(violation.getMessage()).thenReturn("must not be null");
                        java.util.Set violations = new java.util.HashSet();
                        violations.add(violation);
                        when(validator.validate(any())).thenReturn(violations);

                        ApiResponse<TransferResponse> response = service.create(req).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Validation failed");
                }
        }

        @Nested
        @DisplayName("update transfer tests")
        class UpdateTransferTests {

                @Test
                @DisplayName("should fail when transfer not found")
                void update_NotFound() {
                        UpdateTransferRequest req = createValidUpdateRequest();

                        when(transferQueryRepo.findTransferById(ID))
                                        .thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<TransferResponse> response = service.update(req).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to update transfer");
                }

                @Test
                @DisplayName("should fail when transfer is SUCCESS and cannot be modified")
                void update_AlreadySuccess() {
                        UpdateTransferRequest req = createValidUpdateRequest();

                        Transfer existingTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.SUCCESS);
                        when(transferQueryRepo.findTransferById(ID))
                                        .thenReturn(Uni.createFrom().item(existingTransfer));

                        ApiResponse<TransferResponse> response = service.update(req).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to update transfer");
                }

                @Test
                @DisplayName("should successfully update transfer on happy path")
                void update_Success() {
                        UpdateTransferRequest req = createValidUpdateRequest();

                        Transfer existingTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.PENDING);
                        when(transferQueryRepo.findTransferById(ID))
                                        .thenReturn(Uni.createFrom().item(existingTransfer));

                        // Mock saldo query service for update flow
                        Saldo saldoResp = new Saldo("saldo-1", "111122223333", 500000, null, null, null, null);
                        when(saldoPort.findByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(saldoResp));

                        // Mock saldo command service
                        when(saldoPort.updateSaldoBalance(any()))
                                        .thenReturn(Uni.createFrom()
                                                        .item(new SaldoMutationResult("saldo-1", "111122223333", 450000)));

                        when(transferCommandRepo.updateTransferStatus(anyString(), anyString()))
                                        .thenAnswer(inv -> {
                                                Transfer t = createTestTransfer(ID, "111122223333", "444455556666",
                                                                200000, Status.SUCCESS);
                                                return Uni.createFrom().item(t);
                                        });

                        when(transferCommandRepo.updateTransfer(any(Transfer.class)))
                                        .thenAnswer(inv -> {
                                                Transfer t = inv.getArgument(0);
                                                return Uni.createFrom().item(t);
                                        });

                        ApiResponse<TransferResponse> response = service.update(req).await().indefinitely();

                        assertThat(response).isNotNull();
                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Transfer updated successfully");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(ID);
                        assertThat(response.data().getTransferAmount()).isEqualTo(200000L);

                        verify(redisService, atLeastOnce()).deleteReactive(anyString());
                }
        }

        @Nested
        @DisplayName("trash transfer tests")
        class TrashTransferTests {

                @Test
                @DisplayName("should successfully trash existing transfer")
                void trash_Success() {
                        String id = ID;
                        Transfer trashedTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.PENDING);
                        trashedTransfer.setDeletedAt(Instant.now());

                        when(transferCommandRepo.trashed(ID)).thenReturn(Uni.createFrom().item(trashedTransfer));

                        ApiResponse<TransferResponseDeleteAt> response = service.trashed(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Transfer trashed successfully!");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(ID);
                        assertThat(response.data().getDeletedAt()).isNotNull();
                }

                @Test
                @DisplayName("should fail when transfer not found for trash")
                void trash_NotFound() {
                        String id = "999";
                        when(transferCommandRepo.trashed("999")).thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<TransferResponseDeleteAt> response = service.trashed(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Failed to trash transfer");
                }
        }

        @Nested
        @DisplayName("restore transfer tests")
        class RestoreTransferTests {

                @Test
                @DisplayName("should successfully restore trashed transfer")
                void restore_Success() {
                        String id = ID;
                        Transfer restoredTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.PENDING);

                        when(transferCommandRepo.restore(ID)).thenReturn(Uni.createFrom().item(restoredTransfer));

                        ApiResponse<TransferResponseDeleteAt> response = service.restore(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Transfer restored successfully!");
                        assertThat(response.data()).isNotNull();
                        assertThat(response.data().getId()).isEqualTo(ID);
                        assertThat(response.data().getDeletedAt()).isNull();
                }

                @Test
                @DisplayName("should fail when transfer is not trashed")
                void restore_NotTrashed() {
                        String id = ID;
                        when(transferCommandRepo.restore(ID)).thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<TransferResponseDeleteAt> response = service.restore(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Transfer not found");
                }
        }

        @Nested
        @DisplayName("delete by id tests")
        class DeleteByIdTests {

                @Test
                @DisplayName("should successfully permanently delete trashed transfer")
                void deletePermanent_Success() {
                        String id = ID;
                        Transfer trashedTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.PENDING);
                        trashedTransfer.setDeletedAt(Instant.now());

                        when(transferQueryRepo.findTransferById(ID)).thenReturn(Uni.createFrom().item(trashedTransfer));
                        when(transferCommandRepo.deletePermanent(ID)).thenReturn(Uni.createFrom().item(true));

                        ApiResponse<Boolean> response = service.deletePermanent(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("Transfer permanently deleted!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when transfer not found for permanent delete")
                void deletePermanent_NotFound() {
                        String id = "999";
                        when(transferQueryRepo.findTransferById("999")).thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<Boolean> response = service.deletePermanent(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Transfer not found");
                }

                @Test
                @DisplayName("should fail when trying to permanently delete active transfer")
                void deletePermanent_NotTrashed() {
                        String id = ID;
                        Transfer activeTransfer = createTestTransfer(ID, "111122223333", "444455556666", 150000,
                                        Status.PENDING);
                        when(transferQueryRepo.findTransferById(ID)).thenReturn(Uni.createFrom().item(activeTransfer));

                        ApiResponse<Boolean> response = service.deletePermanent(id).await().indefinitely();

                        assertThat(response.status()).isEqualTo("error");
                        assertThat(response.message()).contains("Transfer not found");
                }
        }

        @Nested
        @DisplayName("delete all trashed tests")
        class DeleteAllTests {

                @Test
                @DisplayName("should successfully delete all trashed transfers")
                void deleteAll_Success() {
                        ApiResponse<Boolean> response = service.deleteAll().await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("All transfers permanently deleted!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when no trashed transfers to delete")
                void deleteAll_NoTrashed() {
                        when(transferCommandRepo.deleteAllDeleted())
                                        .thenReturn(Uni.createFrom().item(false));

                        assertThatThrownBy(() -> service.deleteAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class)
                                        .hasMessageContaining("No trashed transfers found");
                }
        }

        @Nested
        @DisplayName("restore all trashed tests")
        class RestoreAllTests {

                @Test
                @DisplayName("should successfully restore all trashed transfers")
                void restoreAll_Success() {
                        ApiResponse<Boolean> response = service.restoreAll().await().indefinitely();

                        assertThat(response.status()).isEqualTo("success");
                        assertThat(response.message()).isEqualTo("All transfers restored successfully!");
                        assertThat(response.data()).isTrue();
                }

                @Test
                @DisplayName("should fail when no trashed transfers to restore")
                void restoreAll_NoTrashed() {
                        when(transferCommandRepo.restoreAllDeleted())
                                        .thenReturn(Uni.createFrom().item(false));

                        assertThatThrownBy(() -> service.restoreAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class)
                                        .hasMessageContaining("No trashed transfers found");
                }
        }
}