package com.sanedge.transfer.service.impl;

import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.transfer.domain.requests.CreateTransferRequest;
import com.sanedge.transfer.domain.requests.UpdateTransferRequest;
import com.sanedge.transfer.domain.response.TransferResponse;
import com.sanedge.transfer.domain.response.TransferResponseDeleteAt;
import com.sanedge.transfer.entity.Transfer;
import com.sanedge.transfer.repository.TransferCommandRepository;
import com.sanedge.transfer.repository.TransferQueryRepository;
import com.sanedge.transfer.service.TransferCommandService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.adapter.model.Card;
import com.sanedge.common.adapter.model.Saldo;
import com.sanedge.common.adapter.saldo.SaldoPort;
import com.sanedge.transfer.service.KafkaService;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class TransferCommandServiceImpl implements TransferCommandService {
    private static final Logger logger = LoggerFactory.getLogger(TransferCommandServiceImpl.class);

    private final TransferCommandRepository transferCommandRepository;
    private final CardPort cardPort;
    private final SaldoPort saldoPort;
    private final TransferQueryRepository transferQueryRepository;
    private final Validator validator;
    private final RedisService redisService;
    private final KafkaService kafkaService;
    private final TracingMetrics tracingMetrics;

    @Inject
    public TransferCommandServiceImpl(TransferCommandRepository transferCommandRepository,
            CardPort cardPort,
            SaldoPort saldoPort,
            TransferQueryRepository transferQueryRepository,
            Validator validator,
            RedisService redisService,
            KafkaService kafkaService,
            TracingMetrics tracingMetrics) {
        this.transferCommandRepository = transferCommandRepository;
        this.cardPort = cardPort;
        this.saldoPort = saldoPort;
        this.transferQueryRepository = transferQueryRepository;
        this.validator = validator;
        this.redisService = redisService;
        this.kafkaService = kafkaService;
        this.tracingMetrics = tracingMetrics;
    }

    private <T> boolean validateRequest(T req) {
        Set<ConstraintViolation<T>> violations = validator.validate(req);
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (ConstraintViolation<T> violation : violations) {
                sb.append(violation.getPropertyPath())
                        .append(": ")
                        .append(violation.getMessage())
                        .append("; ");
            }
            logger.error("Validation failed: {}", sb);
            return false;
        }
        return true;
    }

    private Uni<Void> evictCaches(String senderCard, String receiverCard, String transferId) {
        String key1 = "saldo:card:" + senderCard;
        String key2 = "saldo:card:" + receiverCard;
        String key3 = "transfers:from:" + senderCard;
        String key4 = "transfers:to:" + receiverCard;
        String key5 = "transfers:id:" + transferId;

        return Uni.combine().all().unis(
                redisService.deleteReactive(key1),
                redisService.deleteReactive(key2),
                redisService.deleteReactive(key3),
                redisService.deleteReactive(key4),
                redisService.deleteReactive(key5)).discardItems();
    }

    @Override
    public Uni<ApiResponse<TransferResponse>> create(CreateTransferRequest req) {
        logger.info("🚀 Starting create transfer: {}", req);

        if (!validateRequest(req)) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed", null));
        }

        Attributes attrs = Attributes.builder()
                .put("operation", "create_transfer")
                .put("transferFrom", req.getTransferFrom())
                .put("transferTo", req.getTransferTo())
                .build();

        return tracingMetrics.traceAndMeasure("createTransfer", "create_transfer", attrs, () -> {
            final String[] senderEmailContainer = new String[1];
            return cardPort
                    .findUserCardByCardNumber(req.getTransferFrom())
                    .chain(senderCard -> {
                        senderEmailContainer[0] = senderCard.email();
                        return cardPort.findCardByCardNumber(req.getTransferTo());
                    })
                    .chain(receiverCard -> {
                        return saldoPort.findByCardNumber(req.getTransferFrom());
                    })
                    .chain(senderSaldo -> {
                        return saldoPort
                                .findByCardNumber(req.getTransferTo())
                                .map(receiverSaldo -> {
                                    if (senderSaldo.totalBalance() < req.getTransferAmount()) {
                                        logger.error("❌ insufficient balance, requested={}, available={}",
                                                req.getTransferAmount(), senderSaldo.totalBalance());
                                        throw new IllegalStateException("Insufficient balance");
                                    }

                                    return new SaldoPair(senderSaldo, receiverSaldo);
                                });
                    })
                    .chain(pair -> {
                        Transfer transferEntity = new Transfer();
                        transferEntity.setTransferNo(UUID.randomUUID());
                        transferEntity.setTransferFrom(req.getTransferFrom());
                        transferEntity.setTransferAmount(req.getTransferAmount().intValue());
                        transferEntity.setTransferTo(req.getTransferTo());
                        java.time.Instant now = java.time.Instant.now();
                        transferEntity.setTransferTime(now);
                        transferEntity.setStatus(Status.PENDING);
                        transferEntity.setCreatedAt(now);
                        transferEntity.setUpdatedAt(now);

                        return transferCommandRepository.persist(transferEntity)
                                .chain(savedTransfer -> {
                                    int newSenderBalance = pair.sender.totalBalance()
                                            - req.getTransferAmount().intValue();
                                    int newReceiverBalance = pair.receiver.totalBalance()
                                            + req.getTransferAmount().intValue();

                                    return saldoPort
                                            .updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                    pair.sender.cardNumber(), newSenderBalance, null, null, null, null, null))
                                            .chain(v -> saldoPort.updateSaldoBalance(
                                                    new SaldoPort.BalanceUpdate(
                                                            pair.receiver.cardNumber(), newReceiverBalance, null, null, null, null, null)))
                                            .chain(v -> transferCommandRepository
                                                    .updateTransferStatus(savedTransfer.getTransferId(), "SUCCESS"))
                                            .chain(updatedTransfer -> {
                                                logger.info("✅ Transfer created successfully with ID={}",
                                                        updatedTransfer.getTransferId());
                                                return evictCaches(req.getTransferFrom(), req.getTransferTo(),
                                                        updatedTransfer.getTransferId())
                                                        .chain(x -> {
                                                            Uni<Void> emailUni = Uni.createFrom().voidItem();
                                                            String email = senderEmailContainer[0];
                                                            if (email != null && !email.isEmpty()) {
                                                                String emailSubject = "Transfer Successful - SanEdge";
                                                                String emailBody = String.format(
                                                                        "Hello,\n\nYour transfer of %d to %s has been processed successfully.\n\nRegards,\nSupport Team",
                                                                        req.getTransferAmount().intValue(),
                                                                        req.getTransferTo()
                                                                );

                                                                JsonObject emailPayload = new JsonObject()
                                                                        .put("email", email)
                                                                        .put("subject", emailSubject)
                                                                        .put("body", emailBody);

                                                                emailUni = kafkaService.sendMessage("email-service-topic-transfer-create", String.valueOf(updatedTransfer.getTransferId()), emailPayload)
                                                                        .onFailure().recoverWithNull();
                                                            }

                                                            return emailUni.map(v3 -> ApiResponse.success("Transfer created successfully",
                                                                    TransferResponse.from(updatedTransfer)));
                                                        });
                                            });
                                });
                    })
                    .onFailure().recoverWithItem(e -> {
                        logger.error("❌ Failed to create transfer", e);
                        return new ApiResponse<>("error", "Failed to create transfer: " + e.getMessage(), null);
                    });
        });
    }

    private static class SaldoPair {
        final Saldo sender;
        final Saldo receiver;

        SaldoPair(Saldo sender, Saldo receiver) {
            this.sender = sender;
            this.receiver = receiver;
        }
    }

    @Override
    public Uni<ApiResponse<TransferResponse>> update(UpdateTransferRequest req) {
        logger.info("🔄 Starting update transfer process: {}", req);

        if (!validateRequest(req)) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed", null));
        }

        if (req.getTransferId() == null || req.getTransferId().isBlank()) {
            return Uni.createFrom().item(new ApiResponse<>("error", "transferId is required", null));
        }

        Attributes attrs = Attributes.builder()
                .put("operation", "update_transfer")
                .put("transferId", String.valueOf(req.getTransferId()))
                .build();

        return tracingMetrics.traceAndMeasure("updateTransfer", "update_transfer", attrs, () -> {
            return transferQueryRepository.findTransferById(req.getTransferId())
                    .chain(transfer -> {
                        if (transfer == null) {
                            logger.error("❌ Transfer {} not found", req.getTransferId());
                            throw new ResourceNotFoundException("Transfer " + req.getTransferId() + " not found");
                        }

                        long amountDifference = req.getTransferAmount() - transfer.getTransferAmount();

                        return saldoPort
                                .findByCardNumber(transfer.getTransferFrom())
                                .onFailure(ResourceNotFoundException.class)
                                .recoverWithUni(e -> transferCommandRepository
                                        .updateTransferStatus(req.getTransferId(), "FAILED")
                                        .chain(v -> {
                                            throw new ResourceNotFoundException(
                                                    "Sender card " + transfer.getTransferFrom() + " not found");
                                        }))
                                .chain(senderSaldo -> {
                                    long newSenderBalance = senderSaldo.totalBalance() - amountDifference;
                                    if (newSenderBalance < 0) {
                                        logger.error("❌ Insufficient balance for sender {}", transfer.getTransferFrom());
                                        return transferCommandRepository.updateTransferStatus(req.getTransferId(), "FAILED")
                                                .chain(v -> {
                                                    throw new IllegalStateException("Insufficient balance");
                                                });
                                    }

                                    return saldoPort
                                            .findByCardNumber(transfer.getTransferTo())
                                            .onFailure(ResourceNotFoundException.class)
                                            .recoverWithUni(e -> transferCommandRepository
                                                    .updateTransferStatus(req.getTransferId(), "FAILED")
                                                    .chain(v -> {
                                                        throw new ResourceNotFoundException("Receiver card "
                                                                + transfer.getTransferTo() + " not found");
                                                    }))
                                            .chain(receiverSaldo -> {
                                                long newReceiverBalance = receiverSaldo.totalBalance()
                                                        + amountDifference;

                                                return saldoPort
                                                        .updateSaldoBalance(new SaldoPort.BalanceUpdate(
                                                                senderSaldo.cardNumber(), (int) newSenderBalance, null, null, null, null, null))
                                                        .chain(v -> saldoPort.updateSaldoBalance(
                                                                new SaldoPort.BalanceUpdate(
                                                                        receiverSaldo.cardNumber(), (int) newReceiverBalance, null, null, null, null, null)))
                                                        .chain(v -> {
                                                            transfer.setTransferAmount(req.getTransferAmount().intValue());
                                                            transfer.setTransferFrom(req.getTransferFrom());
                                                            transfer.setTransferTo(req.getTransferTo());
                                                            transfer.setUpdatedAt(java.time.Instant.now());
                                                            return transferCommandRepository.updateTransfer(transfer);
                                                        })
                                                        .chain(updatedTransfer -> transferCommandRepository
                                                                .updateTransferStatus(req.getTransferId(), "SUCCESS"))
                                                        .chain(finalTransfer -> {
                                                            logger.info("✅ Successfully updated transfer {}",
                                                                    req.getTransferId());
                                                            return evictCaches(transfer.getTransferFrom(),
                                                                    transfer.getTransferTo(), finalTransfer.getTransferId())
                                                                    .map(x -> ApiResponse.success(
                                                                            "Transfer updated successfully",
                                                                            TransferResponse.from(finalTransfer)));
                                                        });
                                            });
                                });
                    })
                    .onFailure().recoverWithItem(e -> {
                        logger.error("❌ Failed to update transfer {}", req.getTransferId(), e);
                        return new ApiResponse<>("error", "Failed to update transfer: " + e.getMessage(), null);
                    });
        });
    }

    @Override
    public Uni<ApiResponse<TransferResponseDeleteAt>> trashed(String transferId) {
        logger.info("🗑️ Trashing transfer id={}", transferId);

        return transferCommandRepository.trashed(transferId)
                .chain(transfer -> {
                    if (transfer == null) {
                        throw new ResourceNotFoundException("Transfer not found with ID " + transferId);
                    }
                    return evictCaches(transfer.getTransferFrom(), transfer.getTransferTo(), transfer.getTransferId())
                            .map(x -> ApiResponse.success("Transfer trashed successfully!",
                                    TransferResponseDeleteAt.from(transfer)));
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to trash transfer id={}", transferId, e);
                    return new ApiResponse<>("error", "Failed to trash transfer: " + e.getMessage(), null);
                });
    }

    @Override
    public Uni<ApiResponse<TransferResponseDeleteAt>> restore(String transferId) {
        logger.info("♻️ Restoring transfer id={}", transferId);

        return transferCommandRepository.restore(transferId)
                .chain(transfer -> {
                    if (transfer == null) {
                        throw new ResourceNotFoundException("Transfer not found with ID " + transferId);
                    }
                    return evictCaches(transfer.getTransferFrom(), transfer.getTransferTo(), transfer.getTransferId())
                            .map(x -> ApiResponse.success("Transfer restored successfully!",
                                    TransferResponseDeleteAt.from(transfer)));
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to restore transfer id={}", transferId, e);
                    return new ApiResponse<>("error", "Failed to restore transfer: " + e.getMessage(), null);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deletePermanent(String transferId) {
        logger.info("🧨 Permanently deleting transfer id={}", transferId);

        return transferQueryRepository.findTransferById(transferId)
                .chain(transfer -> {
                    if (transfer == null || transfer.getDeletedAt() == null) {
                        throw new InvalidRequestException(
                                "Transfer not found or must be trashed before permanent deletion");
                    }
                    return transferCommandRepository.deletePermanent(transferId)
                            .chain(success -> evictCaches(transfer.getTransferFrom(), transfer.getTransferTo(),
                                    transfer.getTransferId())
                                    .map(x -> ApiResponse.success("Transfer permanently deleted!", success)));
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to permanently delete transfer id={}", transferId, e);
                    return new ApiResponse<>("error", "Failed to permanently delete transfer: " + e.getMessage(),
                            false);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> restoreAll() {
        logger.info("🔄 Restoring ALL trashed transfers");

        return transferCommandRepository.restoreAllDeleted()
                .map(success -> {
                    if (!success) {
                        throw new ResourceNotFoundException("No trashed transfers found");
                    }
                    return ApiResponse.success("All transfers restored successfully!", success);
                })
                .onFailure().invoke(e -> {
                    logger.error("💥 Failed to restore all transfers", e);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteAll() {
        logger.info("💣 Permanently deleting ALL trashed transfers");

        return transferCommandRepository.deleteAllDeleted()
                .map(success -> {
                    if (!success) {
                        throw new ResourceNotFoundException("No trashed transfers found");
                    }
                    return ApiResponse.success("All transfers permanently deleted!", success);
                })
                .onFailure().invoke(e -> {
                    logger.error("💥 Failed to delete all transfers", e);
                });
    }
}