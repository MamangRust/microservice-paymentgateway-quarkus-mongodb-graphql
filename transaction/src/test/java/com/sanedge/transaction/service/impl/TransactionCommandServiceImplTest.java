package com.sanedge.transaction.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.adapter.merchant.MerchantPort;
import com.sanedge.common.adapter.model.Card;
import com.sanedge.common.adapter.model.Merchant;
import com.sanedge.common.adapter.model.Saldo;
import com.sanedge.common.adapter.model.SaldoMutationResult;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.enums.Status;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.transaction.domain.requests.CreateTransactionRequest;
import com.sanedge.transaction.domain.requests.UpdateTransactionRequest;
import com.sanedge.transaction.domain.response.TransactionResponse;
import com.sanedge.transaction.domain.response.TransactionResponseDeleteAt;
import com.sanedge.transaction.entity.Transaction;
import com.sanedge.transaction.repository.TransactionCommandRepository;
import com.sanedge.transaction.repository.TransactionQueryRepository;
import com.sanedge.transaction.service.KafkaService;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.validation.Validator;

/**
 * Unit tests for {@link TransactionCommandServiceImpl}.
 * <p>
 * Covers all CRUD operations: create, update, trashed, restore,
 * deletePermanent, restoreAll, deleteAll.
 */
@ExtendWith(MockitoExtension.class)
class TransactionCommandServiceImplTest {

    private static final String ID = "507f1f77bcf86cd799439011";
    private static final String MERCHANT_ID = "507f1f77bcf86cd799439022";
    private static final String USER_ID = "507f1f77bcf86cd799439033";
    private static final String SALDO_ID = "507f1f77bcf86cd799439044";

    @Mock
    CardPort cardPort;

    @Mock
    MerchantPort merchantPort;

    @Mock
    SaldoPort saldoPort;

    @Mock
    TransactionQueryRepository transactionQueryRepository;

    @Mock
    TransactionCommandRepository transactionCommandRepository;

    @Mock
    Validator validator;

    @Mock
    RedisService redisService;

    @Mock
    KafkaService kafkaService;

    @Mock
    TracingMetrics tracingMetrics;

    private TransactionCommandServiceImpl transactionCommandService;

    private Transaction transaction;

    @BeforeEach
    void setUp() {
        // The service builds Tracer/Meter from OpenTelemetry in its constructor, so it
        // cannot be created by @InjectMocks with a null/mock OpenTelemetry; construct it
        // manually with the no-op instance.
        transactionCommandService = new TransactionCommandServiceImpl(
                transactionQueryRepository,
                transactionCommandRepository,
                merchantPort,
                saldoPort,
                cardPort,
                validator,
                redisService,
                kafkaService,
                OpenTelemetry.noop());

        transaction = new Transaction();
        transaction.setTransactionId(ID);
        transaction.transactionNo = UUID.randomUUID();
        transaction.cardNumber = "4111111111111111";
        transaction.amount = 150000;
        transaction.paymentMethod = "CREDIT_CARD";
        transaction.merchantId = new ObjectId(MERCHANT_ID);
        transaction.transactionTime = Instant.now();
        transaction.status = Status.PENDING;

        // TracingMetrics no-op mock
        lenient().when(tracingMetrics.traceAndMeasure(anyString(), anyString(), any(), any()))
                .thenAnswer(inv -> {
                    java.util.function.Supplier<Uni<?>> supplier = inv.getArgument(3);
                    return supplier.get();
                });
        lenient().when(tracingMetrics.traceAndMeasure(anyString(), anyString(), any()))
                .thenAnswer(inv -> {
                    java.util.function.Supplier<Uni<?>> supplier = inv.getArgument(2);
                    return supplier.get();
                });

        // Validator passes by default
        lenient().when(validator.validate(any())).thenReturn(Set.of());

        // Redis cache-miss by default
        lenient().when(redisService.getReactive(anyString())).thenReturn(Uni.createFrom().nullItem());
        lenient().when(redisService.setWithExpirationReactive(anyString(), anyString(), any(Long.class)))
                .thenReturn(Uni.createFrom().voidItem());
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());
    }

    private static Merchant merchant() {
        return new Merchant(MERCHANT_ID, "Test Merchant", "test-api-key", "ACTIVE", USER_ID, null, null);
    }

    private static Card card(String number, String email) {
        return new Card("card-1", USER_ID, number, "VISA", "2030-01-01", "123", "BANK", email, null, null);
    }

    private static Saldo saldo(int balance) {
        return new Saldo(SALDO_ID, "4111111111111111", balance, null, null, null, null);
    }

    // ──────────────────────────────────────────────
    // Create Transaction
    // ──────────────────────────────────────────────

    @Test
    void createTransaction_success() {
        when(merchantPort.findByApiKey(any())).thenReturn(Uni.createFrom().item(merchant()));

        when(cardPort.findUserCardByCardNumber(any()))
                .thenReturn(Uni.createFrom().item(card("4111111111111111", "test@example.com")));

        when(saldoPort.findByCardNumber(any()))
                .thenReturn(Uni.createFrom().item(saldo(500000)));

        when(saldoPort.updateSaldoBalance(any()))
                .thenReturn(Uni.createFrom().item(new SaldoMutationResult(SALDO_ID, "4111111111111111", 350000)));

        when(transactionCommandRepository.persist(any(com.sanedge.transaction.entity.Transaction.class)))
                .thenAnswer(inv -> {
                    Transaction t = inv.getArgument(0);
                    t.setTransactionId(ID);
                    return Uni.createFrom().item(t);
                });

        when(transactionCommandRepository.updateTransactionStatus(anyString(), anyString()))
                .thenAnswer(inv -> {
                    transaction.setTransactionId(ID);
                    transaction.status = Status.SUCCESS;
                    return Uni.createFrom().item(transaction);
                });

        when(cardPort.findCardByUserId(any()))
                .thenReturn(Uni.createFrom().item(card("2222222222222222", null)));

        when(kafkaService.sendMessage(anyString(), anyString(), any(JsonObject.class)))
                .thenReturn(Uni.createFrom().voidItem());

        CreateTransactionRequest req = new CreateTransactionRequest();
        req.setCardNumber("4111111111111111");
        req.setAmount(150000L);
        req.setPaymentMethod("CREDIT_CARD");
        req.setMerchantId(MERCHANT_ID);

        ApiResponse<TransactionResponse> result = transactionCommandService.create("test-api-key", req)
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        assertThat(result.data()).isNotNull();
        assertThat(result.data().getCardNumber()).isEqualTo("4111111111111111");
        verify(transactionCommandRepository, times(1)).persist(any(com.sanedge.transaction.entity.Transaction.class));
    }

    // ──────────────────────────────────────────────
    // Update Transaction
    // ──────────────────────────────────────────────

    @Test
    void updateTransaction_success() {
        Transaction existing = new Transaction();
        existing.setTransactionId(ID);
        existing.cardNumber = "4111111111111111";
        existing.amount = 50000;
        existing.paymentMethod = "CREDIT_CARD";
        existing.merchantId = new ObjectId(MERCHANT_ID);
        existing.status = Status.SUCCESS;
        when(transactionQueryRepository.findTransactionById(ID))
                .thenReturn(Uni.createFrom().item(existing));

        when(merchantPort.findByApiKey(any())).thenReturn(Uni.createFrom().item(merchant()));

        when(cardPort.findCardByCardNumber(any()))
                .thenReturn(Uni.createFrom().item(card("4111111111111111", null)));

        when(saldoPort.findByCardNumber(any()))
                .thenReturn(Uni.createFrom().item(saldo(500000)));

        when(saldoPort.updateSaldoBalance(any()))
                .thenReturn(Uni.createFrom().item(new SaldoMutationResult(SALDO_ID, "4111111111111111", 400000)));

        when(transactionCommandRepository.update(any(Transaction.class)))
                .thenAnswer(inv -> Uni.createFrom().item((Transaction) inv.getArgument(0)));

        when(transactionCommandRepository.updateTransactionStatus(anyString(), anyString()))
                .thenAnswer(inv -> {
                    transaction.setTransactionId(ID);
                    transaction.status = Status.SUCCESS;
                    return Uni.createFrom().item(transaction);
                });

        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        UpdateTransactionRequest req = new UpdateTransactionRequest();
        req.setTransactionId(ID);
        req.setCardNumber("4111111111111111");
        req.setAmount(150000L);
        req.setPaymentMethod("CREDIT_CARD");
        req.setMerchantId(MERCHANT_ID);

        ApiResponse<TransactionResponse> result = transactionCommandService.update("test-api-key", req)
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        verify(transactionQueryRepository).findTransactionById(ID);
    }

    // ──────────────────────────────────────────────
    // Trash / Restore / Delete
    // ──────────────────────────────────────────────

    @Test
    void trashed_success() {
        when(transactionCommandRepository.trashed(ID)).thenReturn(Uni.createFrom().item(transaction));

        ApiResponse<TransactionResponseDeleteAt> result = transactionCommandService.trashed(ID)
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        verify(transactionCommandRepository).trashed(ID);
    }

    @Test
    void restore_success() {
        when(transactionCommandRepository.restore(ID)).thenReturn(Uni.createFrom().item(transaction));

        ApiResponse<TransactionResponseDeleteAt> result = transactionCommandService.restore(ID)
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        verify(transactionCommandRepository).restore(ID);
    }

    @Test
    void deletePermanent_success() {
        Transaction trashedTx = new Transaction();
        trashedTx.setTransactionId(ID);
        trashedTx.cardNumber = "4111111111111111";
        trashedTx.merchantId = new ObjectId(MERCHANT_ID);
        trashedTx.setDeletedAt(Instant.now());
        when(transactionCommandRepository.findById(new ObjectId(ID))).thenReturn(Uni.createFrom().item(trashedTx));
        when(transactionCommandRepository.deletePermanent(ID)).thenReturn(Uni.createFrom().item(true));

        ApiResponse<Boolean> result = transactionCommandService.deletePermanent(ID)
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        assertThat(result.data()).isTrue();
        verify(transactionCommandRepository).deletePermanent(ID);
    }

    @Test
    void restoreAll_success() {
        when(transactionCommandRepository.restoreAllDeleted()).thenReturn(Uni.createFrom().item(true));

        ApiResponse<Boolean> result = transactionCommandService.restoreAll()
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        assertThat(result.data()).isTrue();
    }

    @Test
    void deleteAll_success() {
        when(transactionCommandRepository.deleteAllDeleted()).thenReturn(Uni.createFrom().item(true));

        ApiResponse<Boolean> result = transactionCommandService.deleteAll()
                .await().indefinitely();

        assertThat(result.status()).isEqualTo("success");
        assertThat(result.data()).isTrue();
    }
}
