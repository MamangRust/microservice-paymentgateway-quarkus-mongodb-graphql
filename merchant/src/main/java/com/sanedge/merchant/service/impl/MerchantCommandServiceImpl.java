package com.sanedge.merchant.service.impl;

import java.time.Instant;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.adapter.user.UserPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.merchant.domain.requests.CreateMerchantRequest;
import com.sanedge.merchant.domain.requests.UpdateMerchantRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.merchant.domain.response.MerchantResponse;
import com.sanedge.merchant.domain.response.MerchantResponseDeleteAt;
import com.sanedge.merchant.entity.Merchant;
import com.sanedge.merchant.entity.Outbox;
import com.sanedge.merchant.repository.OutboxRepository;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.ResourceAlreadyExistsException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.merchant.repository.MerchantCommandRepository;
import com.sanedge.merchant.repository.MerchantQueryRepository;
import com.sanedge.merchant.service.MerchantCommandService;
import com.sanedge.common.utils.ApiKeyGenerator;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MerchantCommandServiceImpl implements MerchantCommandService {
    private static final Logger logger = LoggerFactory.getLogger(MerchantCommandServiceImpl.class);

    private final UserPort userPort;
    private final MerchantQueryRepository merchantQueryRepository;
    private final MerchantCommandRepository merchantCommandRepository;
    private final RedisService redisService;
    private final TracingMetrics tracingMetrics;

    /** Transactional outbox for the stats pipeline (field-injected to keep test constructors stable). */
    @Inject
    OutboxRepository outboxRepository;

    @Inject
    public MerchantCommandServiceImpl(
                    UserPort userPort,
                    MerchantQueryRepository merchantQueryRepository,
                    MerchantCommandRepository merchantCommandRepository,
                    RedisService redisService,
                    TracingMetrics tracingMetrics) {
        this.userPort = userPort;
        this.merchantQueryRepository = merchantQueryRepository;
        this.merchantCommandRepository = merchantCommandRepository;
        this.redisService = redisService;
        this.tracingMetrics = tracingMetrics;
    }

    /**
     * Emits {@code stats.payment.merchant.event} through the transactional outbox
     * so the ClickHouse stats pipeline has a producer. Failure-isolated: a stats
     * hiccup must never fail the merchant write. Payload fields match the
     * {@code payment_stats.merchant_events} columns; ids are ObjectId hex strings.
     */
    private Uni<Void> enqueueMerchantStatsEvent(Merchant merchant) {
        if (outboxRepository == null) {
            return Uni.createFrom().voidItem();
        }
        JsonObject payload = new JsonObject()
                .put("merchant_id", merchant.getMerchantId() != null ? merchant.getMerchantId() : "")
                .put("user_id", merchant.userId != null ? merchant.userId.toHexString() : "")
                .put("name", merchant.name != null ? merchant.name : "")
                .put("status", merchant.status != null ? merchant.status.name().toLowerCase() : "");

        JsonObject envelope = com.sanedge.common.event.EventEnvelope
                .withDefaults(payload, "stats.payment.merchant.event");

        Outbox outbox = new Outbox();
        outbox.setDomain("merchant");
        outbox.setEventId(java.util.UUID.randomUUID().toString());
        outbox.setEventKey(merchant.getMerchantId());
        outbox.setTopic("stats.payment.merchant.event");
        outbox.setPayload(envelope.encode());
        outbox.setStatus("PENDING");
        return outboxRepository.persist(outbox).replaceWithVoid()
                .onFailure().recoverWithItem((Void) null);
    }

    @Override
    public Uni<ApiResponse<MerchantResponse>> createMerchant(CreateMerchantRequest req) {
        Attributes attrs = Attributes.builder()
                        .put("operation", "create_merchant")
                        .put("service.name", "merchant-command-service")
                        .put("merchant.name", req.getName())
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponse>>traceAndMeasure(
                        "createMerchant", "create_merchant", attrs, () -> {
                                return userPort.findById(req.getUserId())
                                                .chain(user -> merchantQueryRepository.existsByName(req.getName()))
                                                .chain(nameExists -> {
                                                        if (nameExists) {
                                                                logger.error("❌ Merchant name already taken | Name: {}", req.getName());
                                                                throw new ResourceAlreadyExistsException("Merchant name already taken");
                                                        }

                                                        String apiKey = ApiKeyGenerator.generateApiKey();
                                                        UUID merchantNo = UUID.randomUUID();
                                                        Instant now = Instant.now();

                                                        Merchant merchant = new Merchant();
                                                        merchant.setName(req.getName());
                                                        merchant.setMerchantNo(merchantNo);
                                                        merchant.setUserId(new ObjectId(req.getUserId()));
                                                        merchant.setApiKey(apiKey);
                                                        merchant.setStatus(Status.PENDING);
                                                        merchant.setCreatedAt(now);
                                                        merchant.setUpdatedAt(now);

                                                        return merchantCommandRepository.persist(merchant)
                                                                        .chain(savedMerchant -> {
                                                                                logger.info("✅ Merchant created successfully | Id: {}, ApiKey: {}", merchant.getMerchantId(), apiKey);
                                                                                return enqueueMerchantStatsEvent(merchant)
                                                                                                .replaceWith(ApiResponse.success("Merchant created successfully", MerchantResponse.from(merchant)));
                                                                        });
                                                });
                        }).onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to create merchant | Name: {}, UserId: {}", req.getName(), req.getUserId(), e);
                                return new ApiResponse<>("error",
                                                "Failed to create merchant: " + e.getMessage(),
                                                (MerchantResponse) null);
                        });
    }

    @Override
    public Uni<ApiResponse<MerchantResponse>> updateMerchant(UpdateMerchantRequest req) {
        Attributes attrs = Attributes.builder()
                        .put("operation", "update_merchant")
                        .put("service.name", "merchant-command-service")
                        .put("merchant.id", req.getMerchantId())
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponse>>traceAndMeasure(
                        "updateMerchant", "update_merchant", attrs, () -> {
                                return merchantQueryRepository.findMerchantById(req.getMerchantId())
                                                .chain(merchant -> {
                                                        if (merchant == null) {
                                                                logger.error("❌ Merchant not found with id {}", req.getMerchantId());
                                                                throw new ResourceNotFoundException("Merchant not found");
                                                        }

                                                        Uni<Void> userCheckUni = Uni.createFrom().nullItem();
                                                        if (req.getUserId() != null) {
                                                                userCheckUni = userPort.findById(req.getUserId())
                                                                                .invoke(user -> merchant.setUserId(
                                                                                                new ObjectId(req.getUserId())))
                                                                                .replaceWithVoid();
                                                        }

                                                        return userCheckUni.chain(v -> {
                                                                merchant.setName(req.getName());
                                                                merchant.setStatus(Status.valueOf(req.getStatus().toUpperCase()));
                                                                merchant.setUpdatedAt(Instant.now());

                                                                return merchantCommandRepository.update(merchant)
                                                                                .chain(ignored -> {
                                                                                        String cacheIdKey = "merchant:id:" + req.getMerchantId();
                                                                                        String cacheApiKey = "merchant:apikey:" + merchant.getApiKey();
                                                                                        String cacheUserKey = "merchant:user:" + merchant.getUserId();

                                                                                        return Uni.combine().all().unis(
                                                                                                        redisService.deleteReactive(cacheIdKey),
                                                                                                        redisService.deleteReactive(cacheApiKey),
                                                                                                        redisService.deleteReactive(cacheUserKey))
                                                                                                        .asTuple()
                                                                                                        .chain(t -> enqueueMerchantStatsEvent(merchant))
                                                                                                        .map(v2 -> {
                                                                                                                logger.info("✅ Merchant updated successfully | Id: {}", req.getMerchantId());
                                                                                                                return ApiResponse.success("Merchant updated successfully",
                                                                                                                                MerchantResponse.from(merchant));
                                                                                                        });
                                                                                });
                                                        });
                                                });
                        }).onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to update merchant | Id: {}", req.getMerchantId(), e);
                                return new ApiResponse<>("error",
                                                "Failed to update merchant: " + e.getMessage(),
                                                (MerchantResponse) null);
                        });
    }

    @Override
    public Uni<ApiResponse<MerchantResponseDeleteAt>> trashMerchant(String id) {
        Attributes attrs = Attributes.builder()
                        .put("operation", "trash_merchant")
                        .put("service.name", "merchant-command-service")
                        .put("merchant.id", id)
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponseDeleteAt>>traceAndMeasure(
                        "trashMerchant", "trash_merchant", attrs, () -> {
                                logger.info("🗑️ Trashing merchant id={}", id);

                                return merchantCommandRepository.trashed(id)
                                                .chain(merchant -> {
                                                        if (merchant == null) {
                                                                logger.error("❌ Merchant not found with id {}", id);
                                                                throw new ResourceNotFoundException("Merchant not found");
                                                        }

                                                        String cacheIdKey = "merchant:id:" + id;
                                                        String cacheApiKey = "merchant:apikey:" + merchant.getApiKey();
                                                        String cacheUserKey = "merchant:user:" + merchant.getUserId();

                                                        return Uni.combine().all().unis(
                                                                        redisService.deleteReactive(cacheIdKey),
                                                                        redisService.deleteReactive(cacheApiKey),
                                                                        redisService.deleteReactive(cacheUserKey))
                                                                        .asTuple().map(v2 -> {
                                                                                logger.info("✅ Merchant trashed successfully | Id: {}", id);
                                                                                return ApiResponse.success("Merchant trashed successfully",
                                                                                                MerchantResponseDeleteAt.from(merchant));
                                                                        });
                                                });
                        }).onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to trash merchant id={}", id, e);
                                return new ApiResponse<>("error",
                                                "Failed to trash merchant: " + e.getMessage(),
                                                (MerchantResponseDeleteAt) null);
                        });
    }

    @Override
    public Uni<ApiResponse<MerchantResponseDeleteAt>> restoreMerchant(String id) {
        Attributes attrs = Attributes.builder()
                        .put("operation", "restore_merchant")
                        .put("service.name", "merchant-command-service")
                        .put("merchant.id", id)
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponseDeleteAt>>traceAndMeasure(
                        "restoreMerchant", "restore_merchant", attrs, () -> {
                                logger.info("♻️ Restoring merchant id={}", id);

                                return merchantCommandRepository.restore(id)
                                                .chain(merchant -> {
                                                        if (merchant == null) {
                                                                logger.error("❌ Merchant not found with id {}", id);
                                                                throw new ResourceNotFoundException("Merchant not found");
                                                        }

                                                        String cacheIdKey = "merchant:id:" + id;
                                                        String cacheApiKey = "merchant:apikey:" + merchant.getApiKey();
                                                        String cacheUserKey = "merchant:user:" + merchant.getUserId();

                                                        return Uni.combine().all().unis(
                                                                        redisService.deleteReactive(cacheIdKey),
                                                                        redisService.deleteReactive(cacheApiKey),
                                                                        redisService.deleteReactive(cacheUserKey))
                                                                        .asTuple().map(v2 -> {
                                                                                logger.info("✅ Merchant restored successfully | Id: {}", id);
                                                                                return ApiResponse.success("Merchant restored successfully",
                                                                                                MerchantResponseDeleteAt.from(merchant));
                                                                        });
                                                });
                        }).onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to restore merchant id={}", id, e);
                                return new ApiResponse<>("error",
                                                "Failed to restore merchant: " + e.getMessage(),
                                                (MerchantResponseDeleteAt) null);
                        });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteMerchant(String id) {
        Attributes attrs = Attributes.builder()
                        .put("operation", "delete_merchant")
                        .put("service.name", "merchant-command-service")
                        .put("merchant.id", id)
                        .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                        "deleteMerchant", "delete_merchant", attrs, () -> {
                                logger.info("🧨 Permanently deleting merchant id={}", id);

                                return merchantQueryRepository.findMerchantById(id)
                                                .chain(merchant -> {
                                                        if (merchant == null) {
                                                                logger.error("❌ Merchant not found with id {}", id);
                                                                throw new ResourceNotFoundException("Merchant not found");
                                                        }

                                                        String cacheIdKey = "merchant:id:" + id;
                                                        String cacheApiKey = "merchant:apikey:" + merchant.getApiKey();
                                                        String cacheUserKey = "merchant:user:" + merchant.getUserId();

                                                        return merchantCommandRepository.deletePermanent(id)
                                                                        .chain(deleted -> {
                                                                                return Uni.combine().all().unis(
                                                                                                redisService.deleteReactive(cacheIdKey),
                                                                                                redisService.deleteReactive(cacheApiKey),
                                                                                                redisService.deleteReactive(cacheUserKey))
                                                                                                .asTuple().map(v2 -> {
                                                                                                        logger.info("✅ Merchant permanently deleted | Id: {}", id);
                                                                                                        return ApiResponse.success("Merchant permanently deleted", true);
                                                                                                });
                                                                        });
                                                });
                        }).onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to permanently delete merchant id={}", id, e);
                                return new ApiResponse<>("error",
                                                "Failed to delete merchant: " + e.getMessage(),
                                                false);
                        });
    }

    @Override
    public Uni<ApiResponse<Boolean>> restoreAll() {
        Attributes attrs = Attributes.builder()
                        .put("operation", "restore_all_merchants")
                        .put("service.name", "merchant-command-service")
                        .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                        "restoreAllMerchants", "restore_all_merchants", attrs, () -> {
                                logger.info("🔄 Restoring ALL trashed merchants");

                                return merchantCommandRepository.restoreAllDeleted()
                                                .map(restored -> {
                                                        if (!restored) {
                                                                throw new ResourceNotFoundException("No trashed merchants");
                                                        }
                                                        logger.info("✅ Restored all trashed merchants");
                                                        return ApiResponse.success("Restored all trashed merchants", restored);
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("💥 Failed to restore all merchants", e);
                        });
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteAll() {
        Attributes attrs = Attributes.builder()
                        .put("operation", "delete_all_merchants")
                        .put("service.name", "merchant-command-service")
                        .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                        "deleteAllMerchants", "delete_all_merchants", attrs, () -> {
                                logger.info("💣 Permanently deleting ALL trashed merchants");

                                return merchantCommandRepository.deleteAllDeleted()
                                                .map(deleted -> {
                                                        if (!deleted) {
                                                                throw new ResourceNotFoundException("No trashed merchants");
                                                        }
                                                        logger.info("✅ Deleted all trashed merchants");
                                                        return ApiResponse.success("Deleted all trashed merchants", deleted);
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("💥 Failed to delete all merchants", e);
                        });
    }
}
