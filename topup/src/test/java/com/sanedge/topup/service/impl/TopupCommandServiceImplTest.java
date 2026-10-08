package com.sanedge.topup.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
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
import com.sanedge.topup.domain.requests.CreateTopupRequest;
import com.sanedge.topup.domain.requests.UpdateTopupRequest;
import com.sanedge.topup.domain.response.TopupResponse;
import com.sanedge.topup.domain.response.TopupResponseDeleteAt;
import com.sanedge.topup.entity.Topup;
import com.sanedge.topup.repository.TopupCommandRepository;
import com.sanedge.topup.repository.TopupQueryRepository;
import com.sanedge.topup.service.KafkaService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Path;
import jakarta.validation.Validator;

@ExtendWith(MockitoExtension.class)
class TopupCommandServiceImplTest {

        private static final String TOPUP_ID = "507f1f77bcf86cd799439011";
        private static final String CARD_NUMBER = "1234-5678-9012-3456";

        @Mock
        private CardPort cardPort;
        @Mock
        private SaldoPort saldoPort;
        @Mock
        private TopupQueryRepository topupQueryRepo;
        @Mock
        private TopupCommandRepository topupCommandRepo;
        @Mock
        private Validator validator;
        @Mock
        private RedisService redisService;
        @Mock
        private KafkaService kafkaService;

        private TracingMetrics tracingMetrics;
        private TopupCommandServiceImpl service;

        @BeforeEach
        void setUp() {
                tracingMetrics = org.mockito.Mockito.mock(TracingMetrics.class,
                        org.mockito.Mockito.withSettings().lenient());

                // Stub both traceAndMeasure overloads:
                // 4-param: (String, String, Attributes, Supplier)
                lenient().doAnswer(inv -> {
                        Supplier<Uni<?>> s = inv.getArgument(3);
                        return s.get();
                }).when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any(Attributes.class), any());
                // 3-param: (String, String, Supplier)
                lenient().doAnswer(inv -> {
                        Supplier<Uni<?>> s = inv.getArgument(2);
                        return s.get();
                }).when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any());

                service = new TopupCommandServiceImpl(cardPort, saldoPort,
                                topupQueryRepo, topupCommandRepo, validator, redisService, kafkaService,
                                tracingMetrics);
                lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());
                lenient().when(topupCommandRepo.deleteAllDeleted()).thenReturn(Uni.createFrom().item(true));
                lenient().when(topupCommandRepo.restoreAllDeleted()).thenReturn(Uni.createFrom().item(true));
        }

        private static Card cardWithEmail() {
                return new Card("card-1", "user-1", CARD_NUMBER, "VISA", "2030-01-01", "123", "BANK",
                                "test@test.com", null, null);
        }

        private static Saldo saldo(int balance) {
                return new Saldo("99", CARD_NUMBER, balance, null, null, null, null);
        }

        private Topup createTopup(String id, int amount) {
                Topup t = new Topup();
                t.setTopupId(id);
                t.setTopupNo(UUID.randomUUID());
                t.setCardNumber(CARD_NUMBER);
                t.setTopupAmount(amount);
                t.setTopupMethod("BANK_TRANSFER");
                t.setStatus(Status.PENDING);
                t.setTopupTime(Instant.now());
                t.setCreatedAt(Instant.now());
                t.setUpdatedAt(Instant.now());
                return t;
        }

        private CreateTopupRequest createValidCreateRequest() {
                CreateTopupRequest r = new CreateTopupRequest();
                r.setCardNumber(CARD_NUMBER);
                r.setTopupAmount(50000L);
                r.setTopupMethod("BANK_TRANSFER");
                return r;
        }

        private UpdateTopupRequest createValidUpdateRequest() {
                UpdateTopupRequest r = new UpdateTopupRequest();
                r.setTopupId(TOPUP_ID);
                r.setCardNumber(CARD_NUMBER);
                r.setTopupAmount(75000L);
                r.setTopupMethod("BANK_TRANSFER");
                return r;
        }

        private void mockCardAndSaldoForCreate() {
                lenient().when(cardPort.findUserCardByCardNumber(any()))
                                .thenReturn(Uni.createFrom().item(cardWithEmail()));
                lenient().when(saldoPort.findByCardNumber(any()))
                                .thenReturn(Uni.createFrom().item(saldo(100000)));
                lenient().when(saldoPort.updateSaldoBalance(any()))
                                .thenReturn(Uni.createFrom().item(new SaldoMutationResult("99", CARD_NUMBER, 150000)));
                lenient().when(kafkaService.sendMessage(anyString(), anyString(), any()))
                                .thenReturn(Uni.createFrom().voidItem());
        }

        @Nested
        @DisplayName("create tests")
        class CreateTests {
                @Test
                void success() {
                        CreateTopupRequest req = createValidCreateRequest();
                        mockCardAndSaldoForCreate();
                        when(topupCommandRepo.persist(any(Topup.class))).thenAnswer(inv -> {
                                Topup t = inv.getArgument(0);
                                t.setTopupId(TOPUP_ID);
                                return Uni.createFrom().item(t);
                        });
                        ApiResponse<TopupResponse> resp = service.create(req).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                        assertThat(resp.data().getId()).isEqualTo(TOPUP_ID);
                        verify(redisService, atLeastOnce()).deleteReactive(anyString());
                }

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Test
                void validationFails() {
                        CreateTopupRequest req = new CreateTopupRequest();
                        ConstraintViolation<?> v = org.mockito.Mockito.mock(ConstraintViolation.class);
                        when(v.getPropertyPath()).thenReturn(org.mockito.Mockito.mock(Path.class));
                        when(v.getMessage()).thenReturn("must not be null");
                        Set violations = new HashSet();
                        violations.add(v);
                        when(validator.validate(any())).thenReturn(violations);
                        ApiResponse<TopupResponse> resp = service.create(req).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("error");
                        assertThat(resp.message()).contains("Validation failed");
                }
        }

        @Nested
        @DisplayName("update tests")
        class UpdateTests {
                @Test
                void success() {
                        UpdateTopupRequest req = createValidUpdateRequest();
                        Topup existing = createTopup(TOPUP_ID, 2000000);
                        when(topupQueryRepo.findTopupById(TOPUP_ID)).thenReturn(Uni.createFrom().item(existing));

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));

                        when(saldoPort.findByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(saldo(100000)));

                        when(saldoPort.updateSaldoBalance(any()))
                                        .thenReturn(Uni.createFrom()
                                                        .item(new SaldoMutationResult("99", CARD_NUMBER, 100000)));

                        Topup persisted = new Topup();
                        persisted.setTopupId(TOPUP_ID);
                        persisted.setTopupNo(UUID.randomUUID());
                        persisted.setCardNumber(CARD_NUMBER);
                        persisted.setTopupAmount(req.getTopupAmount().intValue());
                        persisted.setTopupMethod("BANK_TRANSFER");
                        persisted.setStatus(Status.SUCCESS);
                        when(topupCommandRepo.persist(any(Topup.class)))
                                        .thenReturn(Uni.createFrom().item(persisted));

                        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

                        ApiResponse<TopupResponse> resp = service.update(req).await().indefinitely();

                        assertThat(resp.status()).isEqualTo("success");
                        assertThat(resp.data()).isNotNull();
                        assertThat(resp.data().getId()).isEqualTo(TOPUP_ID);
                        assertThat(resp.data().getTopupAmount()).isEqualTo(75000L);
                }

                @Test
                void notFound() {
                        UpdateTopupRequest req = createValidUpdateRequest();

                        when(cardPort.findCardByCardNumber(any()))
                                        .thenReturn(Uni.createFrom().item(cardWithEmail()));

                        when(topupQueryRepo.findTopupById(TOPUP_ID)).thenReturn(Uni.createFrom().nullItem());
                        when(topupCommandRepo.updateTopupStatus(anyString(), anyString()))
                                        .thenReturn(Uni.createFrom().nullItem());

                        ApiResponse<TopupResponse> resp = service.update(req).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("error");
                        assertThat(resp.message()).contains("Topup not found");
                }
        }

        @Nested
        @DisplayName("trash tests")
        class TrashTests {
                @Test
                void success() {
                        Topup t = createTopup(TOPUP_ID, 50000);
                        t.setDeletedAt(Instant.now());
                        when(topupCommandRepo.trashed(TOPUP_ID)).thenReturn(Uni.createFrom().item(t));
                        ApiResponse<TopupResponseDeleteAt> resp = service.trashed(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                }

                @Test
                void notFound() {
                        when(topupCommandRepo.trashed(TOPUP_ID)).thenReturn(Uni.createFrom().nullItem());
                        ApiResponse<TopupResponseDeleteAt> resp = service.trashed(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("error");
                        assertThat(resp.message()).contains("Topup not found");
                }
        }

        @Nested
        @DisplayName("restore tests")
        class RestoreTests {
                @Test
                void success() {
                        when(topupCommandRepo.restore(TOPUP_ID)).thenReturn(Uni.createFrom().item(createTopup(TOPUP_ID, 50000)));
                        ApiResponse<TopupResponseDeleteAt> resp = service.restore(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                }

                @Test
                void notFoundOrNotTrashed() {
                        when(topupCommandRepo.restore(TOPUP_ID)).thenReturn(Uni.createFrom().nullItem());
                        ApiResponse<TopupResponseDeleteAt> resp = service.restore(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("error");
                        assertThat(resp.message()).contains("Topup not found");
                }
        }

        @Nested
        @DisplayName("deletePermanent tests")
        class DeletePermanentTests {
                @Test
                void success() {
                        Topup t = createTopup(TOPUP_ID, 50000);
                        t.setDeletedAt(Instant.now());
                        when(topupQueryRepo.findTopupById(TOPUP_ID)).thenReturn(Uni.createFrom().item(t));
                        when(topupCommandRepo.deletePermanent(TOPUP_ID)).thenReturn(Uni.createFrom().item(true));
                        ApiResponse<Boolean> resp = service.deletePermanent(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                        assertThat(resp.data()).isTrue();
                }

                @Test
                void notTrashed() {
                        when(topupQueryRepo.findTopupById(TOPUP_ID))
                                        .thenReturn(Uni.createFrom().item(createTopup(TOPUP_ID, 50000)));
                        ApiResponse<Boolean> resp = service.deletePermanent(TOPUP_ID).await().indefinitely();
                        assertThat(resp.status()).isEqualTo("error");
                        assertThat(resp.message()).contains("must be trashed first");
                }
        }

        @Nested
        @DisplayName("restoreAll tests")
        class RestoreAllTests {
                @Test
                void success() {
                        ApiResponse<Boolean> resp = service.restoreAll().await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                }

                @Test
                void noTrashed() {
                        when(topupCommandRepo.restoreAllDeleted()).thenReturn(Uni.createFrom().item(false));
                        assertThatThrownBy(() -> service.restoreAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class);
                }
        }

        @Nested
        @DisplayName("deleteAll tests")
        class DeleteAllTests {
                @Test
                void success() {
                        ApiResponse<Boolean> resp = service.deleteAll().await().indefinitely();
                        assertThat(resp.status()).isEqualTo("success");
                }

                @Test
                void noTrashed() {
                        when(topupCommandRepo.deleteAllDeleted()).thenReturn(Uni.createFrom().item(false));
                        assertThatThrownBy(() -> service.deleteAll().await().indefinitely())
                                        .isInstanceOf(ResourceNotFoundException.class);
                }
        }
}
