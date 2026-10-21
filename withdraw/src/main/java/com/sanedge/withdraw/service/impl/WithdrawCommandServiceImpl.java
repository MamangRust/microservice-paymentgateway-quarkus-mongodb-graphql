package com.sanedge.withdraw.service.impl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.config.RedisService;

import com.sanedge.withdraw.domain.requests.CreateWithdrawRequest;
import com.sanedge.withdraw.domain.requests.UpdateWithdrawRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.withdraw.domain.response.WithdrawResponse;
import com.sanedge.withdraw.domain.response.WithdrawResponseDeleteAt;
import com.sanedge.withdraw.entity.Withdraw;
import com.sanedge.withdraw.entity.Outbox;
import com.sanedge.withdraw.repository.OutboxRepository;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.adapter.fraud.FraudFallback;
import com.sanedge.common.adapter.fraud.FraudPort;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.withdraw.service.KafkaService;
import io.vertx.core.json.JsonObject;
import com.sanedge.withdraw.repository.WithdrawCommandRepository;
import com.sanedge.withdraw.repository.WithdrawQueryRepository;
import com.sanedge.withdraw.service.WithdrawCommandService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

@ApplicationScoped
public class WithdrawCommandServiceImpl implements WithdrawCommandService {
    private static final Logger logger = LoggerFactory.getLogger(WithdrawCommandServiceImpl.class);

    private final WithdrawQueryRepository withdrawQueryRepository;
    private final WithdrawCommandRepository withdrawCommandRepository;
    private final CardPort cardPort;
    private final SaldoPort saldoPort;
    private final FraudPort fraudPort;
    private final Validator validator;
    private final RedisService redisService;
    private final KafkaService kafkaService;
    private final TracingMetrics tracingMetrics;

    /** Transactional outbox for the stats pipeline (field-injected to keep test constructors stable). */
    @Inject
    OutboxRepository outboxRepository;

    @Inject
    public WithdrawCommandServiceImpl(WithdrawQueryRepository withdrawQueryRepository,
                                      WithdrawCommandRepository withdrawCommandRepository,
                                      CardPort cardPort,
                                      SaldoPort saldoPort,
                                      FraudPort fraudPort,
                                      Validator validator,
                                      RedisService redisService,
                                      KafkaService kafkaService,
                                      TracingMetrics tracingMetrics) {
        this.withdrawQueryRepository = withdrawQueryRepository;
        this.withdrawCommandRepository = withdrawCommandRepository;
        this.cardPort = cardPort;
        this.saldoPort = saldoPort;
        this.fraudPort = fraudPort;
        this.validator = validator;
        this.redisService = redisService;
        this.kafkaService = kafkaService;
        this.tracingMetrics = tracingMetrics;
    }

    private static Instant toInstant(LocalDateTime dateTime) {
        return dateTime != null ? dateTime.atZone(ZoneId.systemDefault()).toInstant() : null;
    }

    /**
     * Emits {@code stats.payment.withdraw.event} through the transactional outbox
     * so the ClickHouse stats pipeline has a producer. Failure-isolated: a stats
     * hiccup must never fail withdraw creation. Payload fields match the
     * {@code payment_stats.withdraw_events} columns; ids are ObjectId hex strings.
     */
    private Uni<Void> enqueueWithdrawStatsEvent(Withdraw withdraw) {
        if (outboxRepository == null) {
            return Uni.createFrom().voidItem();
        }
        JsonObject payload = new JsonObject()
                .put("withdraw_id", withdraw.getWithdrawId() != null ? withdraw.getWithdrawId() : "")
                .put("withdraw_no", withdraw.withdrawNo != null ? withdraw.withdrawNo.toString() : "")
                .put("card_number", withdraw.cardNumber != null ? withdraw.cardNumber : "")
                .put("amount", withdraw.withdrawAmount != null ? withdraw.withdrawAmount : 0)
                .put("status", withdraw.status != null ? withdraw.status.name().toLowerCase() : "");

        JsonObject envelope = com.sanedge.common.event.EventEnvelope
                .withDefaults(payload, "stats.payment.withdraw.event");

        Outbox outbox = new Outbox();
        outbox.setDomain("withdraw");
        outbox.setEventId(UUID.randomUUID().toString());
        outbox.setEventKey(withdraw.getWithdrawId());
        outbox.setTopic("stats.payment.withdraw.event");
        outbox.setPayload(envelope.encode());
        outbox.setStatus("PENDING");
        return outboxRepository.persist(outbox).replaceWithVoid()
                .onFailure().recoverWithItem((Void) null);
    }

    private <T> void validateRequest(T req) {
        Set<ConstraintViolation<T>> violations = validator.validate(req);
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (ConstraintViolation<T> violation : violations) {
                sb.append(violation.getPropertyPath()).append(": ").append(violation.getMessage()).append("; ");
            }
            logger.error("Validation failed: {}", sb);
            throw new ConstraintViolationException("Validation failed: " + sb, violations);
        }
    }

    private Uni<Void> evictCaches(String cardNumber, String withdrawId) {
        String key1 = "saldo:card:" + cardNumber;
        String key2 = "withdraws:id:" + withdrawId;
        String key3 = "withdraws:list:card:" + cardNumber;

        return Uni.combine().all().unis(
                redisService.deleteReactive(key1),
                redisService.deleteReactive(key2),
                redisService.deleteReactive(key3)
        ).discardItems();
    }

    @Override
    public Uni<ApiResponse<WithdrawResponse>> create(CreateWithdrawRequest req) {
        logger.info("🚀 Starting create withdraw request: {}", req);

        try {
            validateRequest(req);
        } catch (Exception e) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed: " + e.getMessage(), null));
        }

        if (req.getWithdrawAmount() == null || req.getWithdrawAmount() <= 0) {
            return Uni.createFrom().item(new ApiResponse<>("error", "withdraw_amount must be > 0", null));
        }

        Attributes attrs = Attributes.builder()
                .put("operation", "create")
                .put("cardNumber", req.getCardNumber())
                .build();

        final String[] senderEmailContainer = new String[1];
        return tracingMetrics.traceAndMeasure("createWithdraw", "create", attrs, () ->
                cardPort.findUserCardByCardNumber(req.getCardNumber())
                        .chain(card -> {
                            senderEmailContainer[0] = card.email();
                            // Security gate: verify the withdraw domain before money moves.
                            // Fail-open when ai-security is unreachable; a DENY action rejects.
                            return FraudFallback.verifySecurity(fraudPort, FraudPort.SecurityDomain.WITHDRAW,
                                            card.userId(), req.getWithdrawAmount().doubleValue())
                                    .chain(security -> {
                                        if ("DENY".equals(security.action())) {
                                            logger.warn("🚫 Withdraw for card={} denied by ai-security: {}",
                                                    req.getCardNumber(), security.reason());
                                            return Uni.createFrom().failure(
                                                    new ResourceNotFoundException("Withdraw denied by security policy"));
                                        }
                                        return saldoPort.findByCardNumber(req.getCardNumber());
                                    });
                        })
                        .chain(saldo -> {
                            if (saldo.totalBalance() < req.getWithdrawAmount()) {
                                logger.error("❌ Insufficient balance for card number={}. Balance={}, Requested={}",
                                        req.getCardNumber(), saldo.totalBalance(), req.getWithdrawAmount());
                                throw new IllegalStateException("Insufficient balance");
                            }

                            int newBalance = saldo.totalBalance() - req.getWithdrawAmount().intValue();

                            Withdraw withdraw = new Withdraw();
                            withdraw.setCardNumber(req.getCardNumber());
                            withdraw.setWithdrawNo(UUID.randomUUID());
                            withdraw.setStatus(Status.PENDING);
                            withdraw.setWithdrawTime(req.getWithdrawTime() != null ?
                                    toInstant(req.getWithdrawTime()) :
                                    Instant.now());
                            withdraw.setWithdrawAmount(req.getWithdrawAmount().intValue());
                            withdraw.setCreatedAt(Instant.now());
                            withdraw.setUpdatedAt(Instant.now());

                            return saldoPort.updateSaldoWithdraw(new SaldoPort.WithdrawUpdate(
                                            saldo.cardNumber(),
                                            newBalance,
                                            withdraw.getWithdrawTime().toString(),
                                            req.getWithdrawAmount().intValue(),
                                            null, null, null))
                                    .chain(v -> withdrawCommandRepository.persist(withdraw))
                                    .chain(savedWithdraw -> withdrawCommandRepository.updateStatus(savedWithdraw.getWithdrawId(), Status.SUCCESS.toString()))
                                    .chain(updatedWithdraw -> {
                                        logger.info("✅ Withdraw created successfully with ID={}", updatedWithdraw.getWithdrawId());
                                        return evictCaches(req.getCardNumber(), updatedWithdraw.getWithdrawId())
                                                .chain(v -> {
                                                    Uni<Void> emailUni = Uni.createFrom().voidItem();
                                                    String email = senderEmailContainer[0];
                                                    if (email != null && !email.isEmpty()) {
                                                        String emailSubject = "Withdraw Successful - SanEdge";
                                                        String emailBody = String.format(
                                                                "Hello,\n\nYour withdraw of %d has been processed successfully.\n\nRegards,\nSupport Team",
                                                                req.getWithdrawAmount().intValue()
                                                        );

                                                        JsonObject emailPayload = new JsonObject()
                                                                .put("email", email)
                                                                .put("subject", emailSubject)
                                                                .put("body", emailBody);

                                                        emailUni = kafkaService.sendMessage("email-service-topic-withdraw-create", String.valueOf(updatedWithdraw.getWithdrawId()), emailPayload)
                                                                .onFailure().recoverWithNull();
                                                    }

                                                    return emailUni.chain(v3 -> enqueueWithdrawStatsEvent(updatedWithdraw)).map(v3 -> ApiResponse.success("Created withdraw successfully", WithdrawResponse.from(updatedWithdraw)));
                                                });
                                    });
                        })
                        .onFailure().recoverWithItem(e -> {
                            logger.error("💥 Error creating withdraw", e);
                            return new ApiResponse<>("error", "Failed to create withdraw: " + e.getMessage(), null);
                        }));
    }

    @Override
    public Uni<ApiResponse<WithdrawResponse>> update(UpdateWithdrawRequest req) {
        logger.info("🚀 Starting update withdraw request: {}", req);

        try {
            validateRequest(req);
        } catch (Exception e) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed: " + e.getMessage(), null));
        }

        if (req.getWithdrawId() == null) {
            return Uni.createFrom().item(new ApiResponse<>("error", "withdraw_id is required", null));
        }

        if (req.getWithdrawAmount() == null || req.getWithdrawAmount() <= 0) {
            return Uni.createFrom().item(new ApiResponse<>("error", "withdraw_amount must be > 0", null));
        }

        Attributes attrs = Attributes.builder()
                .put("operation", "update")
                .put("withdrawId", String.valueOf(req.getWithdrawId()))
                .build();

        return tracingMetrics.traceAndMeasure("updateWithdraw", "update", attrs, () ->
                cardPort.findCardByCardNumber(req.getCardNumber())
                        .chain(card -> withdrawQueryRepository.findById(req.getWithdrawId()))
                        .chain(withdraw -> {
                            if (withdraw == null) {
                                logger.error("❌ Withdraw not found with ID={}", req.getWithdrawId());
                                throw new ResourceNotFoundException("Withdraw not found");
                            }
                            return saldoPort.findByCardNumber(req.getCardNumber())
                                    .chain(saldo -> {
                                        long amountDifference = req.getWithdrawAmount() - withdraw.getWithdrawAmount();
                                        if (saldo.totalBalance() < amountDifference) {
                                            logger.error("❌ Insufficient balance for update. Balance={}, Needed={}",
                                                    saldo.totalBalance(), amountDifference);
                                            throw new IllegalStateException("Insufficient balance");
                                        }

                                        int newBalance = saldo.totalBalance() - (int) amountDifference;

                                        withdraw.setCardNumber(req.getCardNumber());
                                        withdraw.setWithdrawAmount(req.getWithdrawAmount().intValue());
                                        withdraw.setWithdrawTime(req.getWithdrawTime() != null ?
                                                toInstant(req.getWithdrawTime()) :
                                                Instant.now());
                                        withdraw.setUpdatedAt(Instant.now());

                                        return saldoPort.updateSaldoWithdraw(new SaldoPort.WithdrawUpdate(
                                                        saldo.cardNumber(),
                                                        newBalance,
                                                        withdraw.getWithdrawTime().toString(),
                                                        req.getWithdrawAmount().intValue(),
                                                        null, null, null))
                                                .chain(v -> withdrawCommandRepository.persist(withdraw))
                                                .chain(savedWithdraw -> withdrawCommandRepository.updateStatus(savedWithdraw.getWithdrawId(), Status.SUCCESS.toString()))
                                                .chain(updatedWithdraw -> {
                                                    logger.info("✅ Withdraw updated successfully with ID={}", updatedWithdraw.getWithdrawId());
                                                    return evictCaches(req.getCardNumber(), updatedWithdraw.getWithdrawId())
                                                            .map(v -> ApiResponse.success("Updated withdraw successfully", WithdrawResponse.from(updatedWithdraw)));
                                                });
                                    });
                        })
                        .onFailure().recoverWithItem(e -> {
                            logger.error("💥 Error updating withdraw", e);
                            return new ApiResponse<>("error", "Failed to update withdraw: " + e.getMessage(), null);
                        }));
    }

    @Override
    public Uni<ApiResponse<WithdrawResponseDeleteAt>> trashed(String withdrawId) {
        logger.info("🗑️ Trashing withdraw id={}", withdrawId);

        return withdrawCommandRepository.trashed(withdrawId)
                .chain(withdraw -> {
                    if (withdraw == null) {
                        throw new ResourceNotFoundException("Withdraw not found with ID " + withdrawId);
                    }
                    return evictCaches(withdraw.getCardNumber(), withdraw.getWithdrawId())
                            .map(v -> ApiResponse.success("Withdraw trashed successfully!", WithdrawResponseDeleteAt.from(withdraw)));
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to trash withdraw id={}", withdrawId, e);
                    return new ApiResponse<>("error", "Failed to trash withdraw: " + e.getMessage(), null);
                });
    }

    @Override
    public Uni<ApiResponse<WithdrawResponseDeleteAt>> restore(String withdrawId) {
        logger.info("♻️ Restoring withdraw id={}", withdrawId);

        return withdrawCommandRepository.restore(withdrawId)
                .chain(withdraw -> {
                    if (withdraw == null) {
                        throw new InvalidRequestException("Withdraw with id=" + withdrawId + " must be trashed first");
                    }
                    return evictCaches(withdraw.getCardNumber(), withdraw.getWithdrawId())
                            .map(v -> ApiResponse.success("Withdraw restored successfully!", WithdrawResponseDeleteAt.from(withdraw)));
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deletePermanent(String withdrawId) {
        logger.info("🧨 Permanently deleting withdraw id={}", withdrawId);

        return withdrawQueryRepository.findById(withdrawId)
                .chain(withdraw -> {
                    if (withdraw == null || withdraw.getDeletedAt() == null) {
                        throw new InvalidRequestException(
                                "Withdraw not found or must be trashed before permanent deletion");
                    }
                    return withdrawCommandRepository.deletePermanent(withdrawId)
                            .chain(success -> evictCaches(withdraw.getCardNumber(), withdraw.getWithdrawId())
                                    .map(v -> ApiResponse.success("Withdraw permanently deleted!", success)));
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> restoreAll() {
        logger.info("🔄 Restoring ALL trashed withdraws");

        return withdrawCommandRepository.restoreAllDeleted()
                .chain(success -> {
                    if (!success) {
                        throw new ResourceNotFoundException("No trashed withdraws found");
                    }
                    return Uni.createFrom().item(ApiResponse.success("All withdraws restored successfully!", true));
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteAll() {
        logger.info("💣 Permanently deleting ALL trashed withdraws");

        return withdrawCommandRepository.deleteAllDeleted()
                .chain(success -> {
                    if (!success) {
                        throw new ResourceNotFoundException("No trashed withdraws found");
                    }
                    return Uni.createFrom().item(ApiResponse.success("All withdraws permanently deleted!", true));
                });
    }
}
