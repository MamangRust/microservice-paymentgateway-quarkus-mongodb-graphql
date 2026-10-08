package com.sanedge.card.service.impl;

import java.time.Instant;
import java.util.Set;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.adapter.user.UserPort;
import com.sanedge.card.domain.requests.CreateCardRequest;
import com.sanedge.card.domain.requests.UpdateCardRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.card.domain.response.CardResponse;
import com.sanedge.card.domain.response.CardResponseDeleteAt;
import com.sanedge.card.entity.Card;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.card.service.KafkaService;
import io.vertx.core.json.JsonObject;
import com.sanedge.card.repository.CardCommandRepository;
import com.sanedge.card.repository.CardQueryRepository;
import com.sanedge.card.service.CardCommandService;
import com.sanedge.common.utils.CardNumberGenerator;
import com.sanedge.common.observability.TracingMetrics;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

@ApplicationScoped
public class CardCommandImplService implements CardCommandService {
    private static final Logger logger = LoggerFactory.getLogger(CardCommandImplService.class);

    @Inject
    CardCommandRepository cardCommandRepository;

    @Inject
    CardQueryRepository cardQueryRepository;

    @Inject
    UserPort userPort;

    @Inject
    KafkaService kafkaService;

    @Inject
    Validator validator;

    @Inject
    TracingMetrics tracingMetrics;

    @Override
    public Uni<ApiResponse<CardResponse>> createCard(CreateCardRequest req) {
        logger.info("🆕 Creating card for user_id={}", req.getUserId());

        if (!validateRequest(req)) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed", null));
        }

        if (req.getUserId() == null || req.getUserId().isBlank()) {
            logger.error("user_id is required");
            return Uni.createFrom().item(new ApiResponse<>("error", "user_id is required", null));
        }

        return userPort.findById(req.getUserId())
                .chain(user -> {
                    Card card = new Card();
                    try {
                        String cardNumber = CardNumberGenerator.randomCardNumber();
                        card.setCardNumber(cardNumber);
                    } catch (Exception e) {
                        logger.error("❌ Failed to generate card number", e);
                        throw new RuntimeException("Failed to generate card number");
                    }

                    card.setUserId(new ObjectId(req.getUserId()));
                    card.setCardType(req.getCardType());
                    card.setExpireDate(req.getExpireDate());
                    card.setCvv(req.getCvv());
                    card.setCardProvider(req.getCardProvider());
                    Instant now = Instant.now();
                    card.setCreatedAt(now);
                    card.setUpdatedAt(now);

                    return cardCommandRepository.persist(card)
                            .chain(savedCard -> {
                                JsonObject saldoPayload = new JsonObject()
                                        .put("card_number", savedCard.getCardNumber())
                                        .put("total_balance", 0);
                                return kafkaService.sendMessage(
                                        "saldo-service-topic-create-saldo",
                                        savedCard.getCardNumber(),
                                        saldoPayload)
                                        .map(v -> savedCard);
                            })
                            .map(savedCard -> {
                                CardResponse cardResp = CardResponse.from(savedCard);
                                logger.info("✅ Card created successfully with card_id={}", cardResp.getId());
                                return new ApiResponse<>("success", "Card created successfully", cardResp);
                            });
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to create card for user_id={}", req.getUserId(), e);
                    String msg = "Failed to create card";
                    if (e instanceof IllegalArgumentException) {
                        msg = e.getMessage();
                    }
                    return new ApiResponse<>("error", msg, null);
                });
    }

    @Override
    public Uni<ApiResponse<CardResponse>> updateCard(UpdateCardRequest req) {
        logger.info("🔄 Updating card id={} for user_id={}", req.getCardId(), req.getUserId());

        if (!validateRequest(req)) {
            return Uni.createFrom().item(new ApiResponse<>("error", "Validation failed", null));
        }

        if (req.getCardId() == null || req.getCardId().isBlank()) {
            logger.error("card_id is required");
            return Uni.createFrom().item(new ApiResponse<>("error", "card_id is required", null));
        }

        if (req.getUserId() == null || req.getUserId().isBlank()) {
            logger.error("user_id is required");
            return Uni.createFrom().item(new ApiResponse<>("error", "user_id is required", null));
        }

        return userPort.findById(req.getUserId())
                .chain(user -> cardQueryRepository.findCardById(req.getCardId()))
                .chain(opt -> {
                    Card card = opt.orElse(null);
                    if (card == null) {
                        logger.error("💳 Card with id {} not found", req.getCardId());
                        throw new IllegalArgumentException("Card not found");
                    }

                    card.setCardType(req.getCardType());
                    card.setExpireDate(req.getExpireDate());
                    card.setCvv(req.getCvv());
                    card.setCardProvider(req.getCardProvider());
                    card.setUpdatedAt(Instant.now());

                    return cardCommandRepository.update(card)
                            .map(updatedCard -> {
                                CardResponse response = CardResponse.from(updatedCard);
                                logger.info("✅ Card updated successfully with card_id={}", response.getId());
                                return new ApiResponse<>("success", "Card updated successfully", response);
                            });
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to update card id={} for user_id={}", req.getCardId(), req.getUserId(), e);
                    String msg = "Failed to update card";
                    if (e instanceof IllegalArgumentException) {
                        msg = e.getMessage();
                    }
                    return new ApiResponse<>("error", msg, null);
                });
    }

    @Override
    public Uni<ApiResponse<CardResponseDeleteAt>> trashCard(String id) {
        logger.info("🗑️ Trashing card id={}", id);
        return cardCommandRepository.trashed(id)
                .map(card -> {
                    if (card == null) {
                        throw new IllegalArgumentException("Card not found");
                    }
                    return new ApiResponse<>("success", "Card trashed successfully", CardResponseDeleteAt.from(card));
                })
                .onFailure().recoverWithItem(e -> {
                    logger.error("💥 Failed to trash card id={}", id, e);
                    return new ApiResponse<>("error", "Failed to trash card", null);
                });
    }

    @Override
    public Uni<ApiResponse<CardResponseDeleteAt>> restoreCard(String id) {
        logger.info("♻️ Restoring card id={}", id);
        return cardCommandRepository.restore(id)
                .map(card -> {
                    if (card == null) {
                        throw new InvalidRequestException("Card must be trashed first");
                    }
                    return new ApiResponse<>("success", "Card restored successfully", CardResponseDeleteAt.from(card));
                })
                .onFailure(e -> !(e instanceof InvalidRequestException))
                .recoverWithItem(e -> {
                    logger.error("💥 Failed to restore card id={}", id, e);
                    return new ApiResponse<>("error", "Failed to restore card", null);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteCard(String id) {
        logger.info("🧨 Permanently deleting card id={}", id);
        return cardCommandRepository.deletePermanent(id)
                .map(deleted -> {
                    if (deleted == null) {
                        throw new InvalidRequestException("Card must be trashed before permanent deletion");
                    }
                    return new ApiResponse<>("success", "Card permanently deleted", true);
                })
                .onFailure(e -> !(e instanceof InvalidRequestException))
                .recoverWithItem(e -> {
                    logger.error("💥 Failed to permanently delete card id={}", id, e);
                    return new ApiResponse<>("error", "Failed to permanently delete card", false);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> restoreAll() {
        logger.info("🔄 Restoring ALL trashed cards");
        return cardCommandRepository.restoreAllDeleted()
                .map(restored -> {
                    if (Boolean.FALSE.equals(restored)) {
                        throw new ResourceNotFoundException("No trashed cards found");
                    }
                    return new ApiResponse<>("success", "All cards restored successfully", restored);
                })
                .onFailure(e -> !(e instanceof ResourceNotFoundException))
                .recoverWithItem(e -> {
                    logger.error("💥 Failed to restore all cards", e);
                    return new ApiResponse<>("error", "Failed to restore all cards", false);
                });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteAll() {
        logger.info("💣 Permanently deleting ALL trashed cards");
        return cardCommandRepository.deleteAllDeleted()
                .map(deleted -> {
                    if (Boolean.FALSE.equals(deleted)) {
                        throw new ResourceNotFoundException("No trashed cards found");
                    }
                    return new ApiResponse<>("success", "All cards permanently deleted", deleted);
                })
                .onFailure(e -> !(e instanceof ResourceNotFoundException))
                .recoverWithItem(e -> {
                    logger.error("💥 Failed to delete all cards", e);
                    return new ApiResponse<>("error", "Failed to delete all cards", false);
                });
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
}
