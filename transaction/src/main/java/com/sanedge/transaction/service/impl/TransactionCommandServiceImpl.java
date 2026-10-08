package com.sanedge.transaction.service.impl;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.config.RedisService;
import com.sanedge.transaction.domain.requests.CreateTransactionRequest;
import com.sanedge.transaction.domain.requests.UpdateTransactionRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.transaction.domain.response.TransactionResponse;
import com.sanedge.transaction.domain.response.TransactionResponseDeleteAt;
import com.sanedge.transaction.entity.Transaction;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.adapter.merchant.MerchantPort;
import com.sanedge.common.adapter.model.Card;
import com.sanedge.common.adapter.model.Merchant;
import com.sanedge.common.adapter.model.Saldo;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.transaction.service.KafkaService;
import io.vertx.core.json.JsonObject;
import com.sanedge.transaction.repository.TransactionCommandRepository;
import com.sanedge.transaction.repository.TransactionQueryRepository;
import com.sanedge.transaction.service.TransactionCommandService;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

@ApplicationScoped
public class TransactionCommandServiceImpl implements TransactionCommandService {
        private static final Logger logger = LoggerFactory.getLogger(TransactionCommandServiceImpl.class);

        private final TransactionQueryRepository transactionQueryRepository;
        private final TransactionCommandRepository transactionCommandRepository;
        private final MerchantPort merchantPort;
        private final SaldoPort saldoPort;
        private final CardPort cardPort;
        private final Validator validator;
        private final RedisService redisService;
        private final KafkaService kafkaService;

        private final Tracer tracer;
        private final LongCounter requestsTotal;
        private final DoubleHistogram requestDurationSeconds;

        @Inject
        public TransactionCommandServiceImpl(TransactionQueryRepository transactionQueryRepository,
                        TransactionCommandRepository transactionCommandRepository,
                        MerchantPort merchantPort,
                        SaldoPort saldoPort,
                        CardPort cardPort,
                        Validator validator,
                        RedisService redisService,
                        KafkaService kafkaService,
                        OpenTelemetry openTelemetry) {
                this.transactionQueryRepository = transactionQueryRepository;
                this.transactionCommandRepository = transactionCommandRepository;
                this.merchantPort = merchantPort;
                this.saldoPort = saldoPort;
                this.cardPort = cardPort;
                this.validator = validator;
                this.redisService = redisService;
                this.kafkaService = kafkaService;

                this.tracer = openTelemetry.getTracer("transaction-command-service", "1.0.0");
                Meter meter = openTelemetry.getMeter("transaction-command-service");

                this.requestsTotal = meter.counterBuilder("requests_total")
                                .setDescription("Total number of requests")
                                .build();
                this.requestDurationSeconds = meter.histogramBuilder("request_duration_seconds")
                                .setDescription("Request duration in seconds")
                                .setUnit("s")
                                .build();
        }

        private Uni<Void> evictCaches(String cardNum, String merchantCardNum, String merchantId, String transactionId) {
                String key1 = "saldo:card:" + cardNum;
                String key2 = merchantCardNum != null ? "saldo:card:" + merchantCardNum : null;
                String key3 = "transactions:id:" + transactionId;
                String key4 = merchantId != null ? "transactions:merchant:" + merchantId : null;

                if (key2 != null) {
                        return Uni.combine().all().unis(
                                        redisService.deleteReactive(key1),
                                        redisService.deleteReactive(key2),
                                        redisService.deleteReactive(key3),
                                        key4 != null ? redisService.deleteReactive(key4) : Uni.createFrom().voidItem())
                                        .discardItems();
                } else {
                        return Uni.combine().all().unis(
                                        redisService.deleteReactive(key1),
                                        redisService.deleteReactive(key3),
                                        key4 != null ? redisService.deleteReactive(key4) : Uni.createFrom().voidItem())
                                        .discardItems();
                }
        }

        private <T> void validateRequest(T req) {
                Set<ConstraintViolation<T>> violations = validator.validate(req);
                if (!violations.isEmpty()) {
                        StringBuilder sb = new StringBuilder();
                        for (ConstraintViolation<T> violation : violations) {
                                sb.append(violation.getPropertyPath()).append(": ").append(violation.getMessage())
                                                .append("; ");
                        }
                        logger.error("Validation failed: {}", sb.toString());
                        throw new ConstraintViolationException("Validation failed: " + sb, violations);
                }
        }

        @Override
        public Uni<ApiResponse<TransactionResponse>> create(String apiKey, CreateTransactionRequest req) {
                logger.info("▶️ Starting CreateTransaction process, apiKey={}, req={}", apiKey, req);

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("createTransaction")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "create")
                                .setAttribute("cardNumber", req.getCardNumber())
                                .startSpan();

                try {
                        validateRequest(req);
                } catch (Exception e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        span.end();
                        return Uni.createFrom().item(new ApiResponse<>("error", e.getMessage(), null));
                }

                return merchantPort.findByApiKey(apiKey)
                                .chain(merchant -> {
                                        return cardPort.findUserCardByCardNumber(req.getCardNumber())
                                                         .chain(card -> {
                                                                 return saldoPort.findByCardNumber(req.getCardNumber())
                                                                                .chain(saldo -> {
                                                                                        if (saldo.totalBalance() < req
                                                                                                        .getAmount()) {
                                                                                                logger.error("❌ Insufficient balance, requested: {}, available: {}",
                                                                                                                req.getAmount(),
                                                                                                                saldo.totalBalance());
                                                                                                return Uni.createFrom()
                                                                                                                .failure(new ResourceNotFoundException(
                                                                                                                                "Insufficient balance"));
                                                                                        }

                                                                                        Long updatedSaldo = (long) saldo.totalBalance()
                                                                                                        - req.getAmount();

                                                                                        Transaction transactionEntity = new Transaction();
                                                                                        UUID transactionNo = UUID
                                                                                                        .randomUUID();

                                                                                        transactionEntity.setCardNumber(
                                                                                                        req.getCardNumber());
                                                                                        transactionEntity.setMerchantId(
                                                                                                        new ObjectId(req
                                                                                                                        .getMerchantId()));
                                                                                        transactionEntity.setAmount(req
                                                                                                        .getAmount()
                                                                                                        .intValue());
                                                                                        transactionEntity
                                                                                                        .setPaymentMethod(
                                                                                                                        req.getPaymentMethod());
                                                                                        Instant now = Instant.now();
                                                                                        transactionEntity
                                                                                                        .setTransactionTime(
                                                                                                                        req.getTransactionTime() != null
                                                                                                                                        ? req.getTransactionTime()
                                                                                                                                        : now);
                                                                                        transactionEntity
                                                                                                        .setTransactionNo(
                                                                                                                        transactionNo);
                                                                                        transactionEntity.setStatus(
                                                                                                        Status.PENDING);
                                                                                        transactionEntity
                                                                                                        .setCreatedAt(now);
                                                                                        transactionEntity
                                                                                                        .setUpdatedAt(now);

                                                                                        return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                                                                        card.cardNumber(), updatedSaldo.intValue(),
                                                                                                        null, null, null, null, null))
                                                                                                        .chain(v -> transactionCommandRepository
                                                                                                                        .persist(transactionEntity))
                                                                                                        .chain(persistedTx -> {
                                                                                                                return transactionCommandRepository
                                                                                                                                .updateTransactionStatus(
                                                                                                                                                persistedTx.getTransactionId(),
                                                                                                                                                Status.SUCCESS.toString())
                                                                                                                                .chain(updatedTx -> {
                                                                                                                                        return cardPort.findCardByUserId(merchant.userId())
                                                                                                                                                        .chain(merchantCard -> {
                                                                                                                                                                return saldoPort.findByCardNumber(merchantCard.cardNumber())
                                                                                                                                                                                .chain(merchantSaldo -> {
                                                                                                                                                                                        Long updatedMerchantSaldo = (long) merchantSaldo.totalBalance()
                                                                                                                                                                                                        + req.getAmount();
                                                                                                                                                                                        return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                                                                                                                                                                        merchantCard.cardNumber(),
                                                                                                                                                                                                        updatedMerchantSaldo.intValue(),
                                                                                                                                                                                                        null, null, null, null, null))
                                                                                                                                                                                                        .chain(v2 -> evictCaches(
                                                                                                                                                                                                                        req.getCardNumber(),
                                                                                                                                                                                                                        merchantCard.cardNumber(),
                                                                                                                                                                                                                        merchant.id(),
                                                                                                                                                                                                                        updatedTx
                                                                                                                                                                                                                                        .getTransactionId()))
                                                                                                             .chain(v3 -> {
                                                                                                                     Uni<Void> emailUni = Uni.createFrom().voidItem();
                                                                                                                     if (card.email() != null && !card.email().isEmpty()) {
                                                                                                                             String emailSubject = "Transaction Successful - SanEdge";
                                                                                                                             String emailBody = String.format(
                                                                                                                                             "Hello,\n\nYour transaction of %d has been processed successfully.\n\nRegards,\nSupport Team",
                                                                                                                                             req.getAmount().intValue()
                                                                                                                             );

                                                                                                                             JsonObject emailPayload = new JsonObject()
                                                                                                                                             .put("email", card.email())
                                                                                                                                             .put("subject", emailSubject)
                                                                                                                                             .put("body", emailBody);

                                                                                                                             emailUni = kafkaService.sendMessage("email-service-topic-transaction-create", String.valueOf(updatedTx.getTransactionId()), emailPayload)
                                                                                                                                             .onFailure().recoverWithNull();
                                                                                                                     }

                                                                                                                     return emailUni.map(v4 -> {
                                                                                                                             TransactionResponse response = TransactionResponse
                                                                                                                                             .from(updatedTx);
                                                                                                                             logger.info(
                                                                                                                                             "✅ CreateTransaction completed, transaction_id={}",
                                                                                                                                             response.getId());
                                                                                                                             span.setStatus(
                                                                                                                                             StatusCode.OK);

                                                                                                                             requestsTotal
                                                                                                                                             .add(1, Attributes
                                                                                                                                                             .of(
                                                                                                                                                                             AttributeKey
                                                                                                                                                                                             .stringKey(
                                                                                                                                                                                                             "operation"),
                                                                                                                                                                             "create",
                                                                                                                                                                             AttributeKey
                                                                                                                                                                                             .stringKey(
                                                                                                                                                                                                             "status"),
                                                                                                                                                                             "success"));

                                                                                                                             return ApiResponse
                                                                                                                                             .success(
                                                                                                                                                             "Transaction created successfully",
                                                                                                                                                             response);
                                                                                                                     });
                                                                                                             });
                                                                                                                                                                                });
                                                                                                                                                        });
                                                                                                                                });
                                                                                                        });
                                                                                });
                                                        });
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("🔥 Error in create transaction: {}", e.getMessage(), e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "create",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "Error in create transaction: " + e.getMessage(), null);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "create"));
                                });
        }

        @Override
        public Uni<ApiResponse<TransactionResponse>> update(String apiKey, UpdateTransactionRequest req) {
                logger.info("▶️ Starting UpdateTransaction process: {}", req);

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("updateTransaction")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "update")
                                .setAttribute("transactionId", String.valueOf(req.getTransactionId()))
                                .startSpan();

                try {
                        validateRequest(req);
                } catch (Exception e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        span.end();
                        return Uni.createFrom().item(new ApiResponse<>("error", e.getMessage(), null));
                }

                String transactionId = req.getTransactionId();
                if (transactionId == null) {
                        span.setStatus(StatusCode.ERROR, "transaction_id is required");
                        span.end();
                        return Uni.createFrom().item(new ApiResponse<>("error", "transaction_id is required", null));
                }

                return transactionQueryRepository.findTransactionById(transactionId)
                                .chain(transaction -> {
                                        if (transaction == null) {
                                                return Uni.createFrom()
                                                                .failure(new ResourceNotFoundException("Transaction "
                                                                                + transactionId + " not found"));
                                        }
                                        return merchantPort.findByApiKey(apiKey)
                                                        .chain(merchant -> {
                                                                if (!transaction.getMerchantId()
                                                                                .toHexString()
                                                                                .equals(merchant.id())) {
                                                                        logger.error("🚫 Unauthorized access to transaction {}",
                                                                                        transactionId);
                                                                        return transactionCommandRepository
                                                                                        .updateTransactionStatus(
                                                                                                        transactionId,
                                                                                                        Status.FAILED.toString())
                                                                                        .chain(v -> Uni.createFrom()
                                                                                                        .<ApiResponse<TransactionResponse>>failure(
                                                                                                                        new ResourceNotFoundException(
                                                                                                                                        "unauthorized access")));
                                                                }

                                                                return cardPort.findCardByCardNumber(transaction.getCardNumber())
                                                                                .chain(card -> {
                                                                                        return saldoPort.findByCardNumber(card.cardNumber())
                                                                                                        .chain(saldo -> {

                                                                                                                Long restoredBalance = (long) saldo.totalBalance()
                                                                                                                                + transaction.getAmount()
                                                                                                                                                .longValue();

                                                                                                                return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                                                                                                card.cardNumber(), restoredBalance.intValue(),
                                                                                                                                null, null, null, null, null))
                                                                                                                                .chain(v1 -> {
                                                                                                                                        if (restoredBalance < req
                                                                                                                                                        .getAmount()) {
                                                                                                                                                logger.error(
                                                                                                                                                                "❌ Insufficient balance after restore, available={}, requested={}",
                                                                                                                                                                restoredBalance,
                                                                                                                                                                req.getAmount());
                                                                                                                                                return transactionCommandRepository
                                                                                                                                                                .updateTransactionStatus(
                                                                                                                                                                                transactionId,
                                                                                                                                                                                Status.FAILED.toString())
                                                                                                                                                                .chain(v2 -> Uni.createFrom()
                                                                                                                                                                                .<ApiResponse<TransactionResponse>>failure(
                                                                                                                                                                                                new ResourceNotFoundException(
                                                                                                                                                                                                                "Insufficient balance")));
                                                                                                                                        }

                                                                                                                                        Long updatedBalance = restoredBalance
                                                                                                                                                        - req.getAmount();

                                                                                                                                        transaction.setAmount(
                                                                                                                                                        req.getAmount().intValue());
                                                                                                                                        transaction
                                                                                                                                                        .setPaymentMethod(
                                                                                                                                                                        req.getPaymentMethod());
                                                                                                                                        transaction.setTransactionTime(
                                                                                                                                                        req.getTransactionTime() != null
                                                                                                                                                                        ? req.getTransactionTime()
                                                                                                                                                                        : Instant.now());
                                                                                                                                        transaction.setUpdatedAt(
                                                                                                                                                        Instant.now());

                                                                                                                                        return saldoPort.updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                                                                                                                        card.cardNumber(), updatedBalance.intValue(),
                                                                                                                                                        null, null, null, null, null))
                                                                                                                                                        .chain(v3 -> transactionCommandRepository
                                                                                                                                                                        .update(transaction))
                                                                                                                                                        .chain(v4 -> transactionCommandRepository
                                                                                                                                                                        .updateTransactionStatus(
                                                                                                                                                                                        transactionId,
                                                                                                                                                                                        Status.SUCCESS.toString()))
                                                                                                                                                        .chain(updatedTx -> evictCaches(
                                                                                                                                                                        card.cardNumber(),
                                                                                                                                                                        null,
                                                                                                                                                                        merchant.id(),
                                                                                                                                                                        transactionId)
                                                                                                                                                                        .map(v5 -> {
                                                                                                                                                                                TransactionResponse response = TransactionResponse
                                                                                                                                                                                                .from(updatedTx);
                                                                                                                                                                                logger.info(
                                                                                                                                                                                                "✅ Transaction {} updated successfully",
                                                                                                                                                                                                transactionId);
                                                                                                                                                                                span.setStatus(StatusCode.OK);

                                                                                                                                                                                requestsTotal.add(
                                                                                                                                                                                                1,
                                                                                                                                                                                                Attributes.of(
                                                                                                                                                                                                                AttributeKey
                                                                                                                                                                                                                                .stringKey(
                                                                                                                                                                                                                                                "operation"),
                                                                                                                                                                                                                "update",
                                                                                                                                                                                                                AttributeKey
                                                                                                                                                                                                                                .stringKey(
                                                                                                                                                                                                                                                "status"),
                                                                                                                                                                                                                "success"));

                                                                                                                                                                                return ApiResponse
                                                                                                                                                                                                .success(
                                                                                                                                                                                                                "Transaction updated successfully",
                                                                                                                                                                                                                response);
                                                                                                                                                                        }));
                                                                                                                                });
                                                                                                        });
                                                                                });
                                                        });
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("🔥 Error updating transaction: {}", e.getMessage(), e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "update",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "Failed to update transaction: " + e.getMessage(), null);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "update"));
                                });
        }

        @Override
        public Uni<ApiResponse<TransactionResponseDeleteAt>> trashed(String transactionId) {
                logger.info("🗑️ Trashing transaction id={}", transactionId);

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("trashTransaction")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "trashed")
                                .setAttribute("transactionId", String.valueOf(transactionId))
                                .startSpan();

                return transactionCommandRepository.trashed(transactionId)
                                .chain(tx -> {
                                        if (tx == null) {
                                                return Uni.createFrom().failure(
                                                                new ResourceNotFoundException("Transaction not found"));
                                        }
                                        return evictCaches(tx.getCardNumber(), null,
                                                        tx.getMerchantId() != null ? tx.getMerchantId().toHexString() : null,
                                                        transactionId)
                                                        .map(v -> {
                                                                span.setStatus(StatusCode.OK);
                                                                requestsTotal.add(1, Attributes.of(
                                                                                AttributeKey.stringKey("operation"),
                                                                                "trashed",
                                                                                AttributeKey.stringKey("status"),
                                                                                "success"));

                                                                return ApiResponse.success(
                                                                                "🗑️ Transaction trashed successfully!",
                                                                                TransactionResponseDeleteAt.from(tx));
                                                        });
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("💥 Failed to trash transaction id={}", transactionId, e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "trashed",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "💥 Failed to trash transaction: " + e.getMessage(), null);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "trashed"));
                                });
        }

        @Override
        public Uni<ApiResponse<TransactionResponseDeleteAt>> restore(String transactionId) {
                logger.info("♻️ Restoring transaction id={}", transactionId);

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("restoreTransaction")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "restore")
                                .setAttribute("transactionId", String.valueOf(transactionId))
                                .startSpan();

                return transactionCommandRepository.restore(transactionId)
                                .chain(tx -> {
                                        if (tx == null) {
                                                return Uni.createFrom().failure(
                                                                new ResourceNotFoundException("Transaction not found"));
                                        }
                                        return evictCaches(tx.getCardNumber(), null,
                                                        tx.getMerchantId() != null ? tx.getMerchantId().toHexString() : null,
                                                        transactionId)
                                                        .map(v -> {
                                                                span.setStatus(StatusCode.OK);
                                                                requestsTotal.add(1, Attributes.of(
                                                                                AttributeKey.stringKey("operation"),
                                                                                "restore",
                                                                                AttributeKey.stringKey("status"),
                                                                                "success"));

                                                                return ApiResponse.success(
                                                                                "♻️ Transaction restored successfully!",
                                                                                TransactionResponseDeleteAt.from(tx));
                                                        });
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("💥 Failed to restore transaction id={}", transactionId, e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "restore",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "💥 Failed to restore transaction: " + e.getMessage(), null);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "restore"));
                                });
        }

        @Override
        public Uni<ApiResponse<Boolean>> deletePermanent(String transactionId) {
                logger.info("🧨 Permanently deleting transaction id={}", transactionId);

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("deletePermanentTransaction")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "delete_permanent")
                                .setAttribute("transactionId", String.valueOf(transactionId))
                                .startSpan();

                return transactionCommandRepository.findById(new ObjectId(transactionId))
                                .chain(tx -> {
                                        if (tx == null) {
                                                return Uni.createFrom().item(ApiResponse
                                                                .success("💥 Transaction already deleted", false));
                                        }
                                        return transactionCommandRepository.deletePermanent(transactionId)
                                                        .chain(success -> evictCaches(tx.getCardNumber(), null,
                                                                        tx.getMerchantId() != null
                                                                                        ? tx.getMerchantId().toHexString()
                                                                                        : null,
                                                                        transactionId)
                                                                        .map(v -> {
                                                                                span.setStatus(StatusCode.OK);
                                                                                requestsTotal.add(1, Attributes.of(
                                                                                                AttributeKey.stringKey(
                                                                                                                "operation"),
                                                                                                "delete_permanent",
                                                                                                AttributeKey.stringKey(
                                                                                                                "status"),
                                                                                                "success"));

                                                                                return ApiResponse.success(
                                                                                                "🧨 Transaction permanently deleted!",
                                                                                                success);
                                                                        }));
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("💥 Failed to permanently delete transaction id={}", transactionId,
                                                        e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "delete_permanent",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "💥 Failed to permanently delete transaction: "
                                                                        + e.getMessage(),
                                                        false);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "delete_permanent"));
                                });
        }

        @Override
        public Uni<ApiResponse<Boolean>> restoreAll() {
                logger.info("🔄 Restoring ALL trashed transactions");

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("restoreAllTransactions")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "restore_all")
                                .startSpan();

                return transactionCommandRepository.restoreAllDeleted()
                                .map(success -> {
                                        span.setStatus(StatusCode.OK);
                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "restore_all",
                                                        AttributeKey.stringKey("status"), "success"));

                                        return ApiResponse.success("🔄 All transactions restored successfully!",
                                                        success);
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("💥 Failed to restore all transactions", e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "restore_all",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "💥 Failed to restore all transactions: " + e.getMessage(),
                                                        false);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "restore_all"));
                                });
        }

        @Override
        public Uni<ApiResponse<Boolean>> deleteAll() {
                logger.info("💣 Permanently deleting ALL trashed transactions");

                long startTime = System.currentTimeMillis();
                Span span = tracer.spanBuilder("deleteAllTransactions")
                                .setSpanKind(SpanKind.SERVER)
                                .setAttribute("service.name", "transaction-command-service")
                                .setAttribute("operation", "delete_all")
                                .startSpan();

                return transactionCommandRepository.deleteAllDeleted()
                                .map(success -> {
                                        span.setStatus(StatusCode.OK);
                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "delete_all",
                                                        AttributeKey.stringKey("status"), "success"));

                                        return ApiResponse.success("💣 All transactions permanently deleted!", success);
                                })
                                .onFailure().recoverWithItem(e -> {
                                        logger.error("💥 Failed to delete all transactions", e);
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, e.getMessage());

                                        requestsTotal.add(1, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "delete_all",
                                                        AttributeKey.stringKey("status"), "failed",
                                                        AttributeKey.stringKey("error_type"),
                                                        e.getClass().getSimpleName()));

                                        return new ApiResponse<>("error",
                                                        "💥 Failed to delete all transactions: " + e.getMessage(),
                                                        false);
                                })
                                .eventually(() -> {
                                        span.end();
                                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                                        requestDurationSeconds.record(duration, Attributes.of(
                                                        AttributeKey.stringKey("operation"), "delete_all"));
                                });
        }
}
