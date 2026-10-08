package com.sanedge.saldo.service.impl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.adapter.card.CardPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.saldo.domain.requests.CreateSaldoRequest;
import com.sanedge.saldo.domain.requests.UpdateSaldoRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.saldo.domain.response.SaldoResponse;
import com.sanedge.saldo.domain.response.SaldoResponseDeleteAt;
import com.sanedge.saldo.entity.Saldo;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.saldo.repository.SaldoCommandRepository;
import com.sanedge.saldo.repository.SaldoQueryRepository;
import com.sanedge.saldo.service.SaldoCommandService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SaldoCommandServiceImpl implements SaldoCommandService {
    private static final Logger logger = LoggerFactory.getLogger(SaldoCommandServiceImpl.class);

    private final CardPort cardPort;
    private final SaldoCommandRepository saldoCommandRepository;
    private final SaldoQueryRepository saldoQueryRepository;
    private final RedisService redisService;
    private final TracingMetrics tracingMetrics;

    @Inject
    public SaldoCommandServiceImpl(CardPort cardPort,
                                  SaldoCommandRepository saldoCommandRepository,
                                  SaldoQueryRepository saldoQueryRepository,
                                  RedisService redisService,
                                  TracingMetrics tracingMetrics) {
        this.cardPort = cardPort;
        this.saldoCommandRepository = saldoCommandRepository;
        this.saldoQueryRepository = saldoQueryRepository;
        this.redisService = redisService;
        this.tracingMetrics = tracingMetrics;
    }

    private static Instant toInstant(LocalDateTime dateTime) {
        return dateTime != null ? dateTime.atZone(ZoneId.systemDefault()).toInstant() : null;
    }

    @Override
    public Uni<ApiResponse<SaldoResponse>> create(CreateSaldoRequest request) {
        logger.info("Creating saldo for card_number={}", request.getCardNumber());

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "create_saldo")
                .put("cardNumber", request.getCardNumber())
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponse>>traceAndMeasure(
                "createSaldo", "create_saldo", attrs, () ->

                cardPort.findCardByCardNumber(request.getCardNumber())
                        .chain(card -> {
                            Saldo saldo = new Saldo();
                            saldo.setCardNumber(request.getCardNumber());
                            saldo.setTotalBalance(request.getTotalBalance().intValue());
                            saldo.setWithdrawAmount(0);
                            saldo.setWithdrawTime(null);
                            saldo.setCreatedAt(Instant.now());
                            saldo.setUpdatedAt(Instant.now());

                            return saldoCommandRepository.persist(saldo)
                                    .chain(savedSaldo -> {
                                        logger.info("Saldo created successfully with id={} for card={}", savedSaldo.getSaldoId(), request.getCardNumber());
                                        String cacheKey = "saldo:card:" + request.getCardNumber();

                                        return redisService.deleteReactive(cacheKey)
                                                .map(v -> ApiResponse.success("Create saldo success", SaldoResponse.from(savedSaldo)));
                                    });
                        })
                        .onFailure().recoverWithItem(e -> {
                            logger.error("💥 Failed to create saldo for card_number={}", request.getCardNumber(), e);
                            return new ApiResponse<>("error", e.getMessage(), null);
                        }));
    }

    @Override
    public Uni<ApiResponse<SaldoResponse>> update(UpdateSaldoRequest request) {
        logger.info("Updating saldo id={} for card={}", request.getSaldoId(), request.getCardNumber());

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "update_saldo")
                .put("saldo.id", String.valueOf(request.getSaldoId()))
                .put("cardNumber", request.getCardNumber())
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponse>>traceAndMeasure(
                "updateSaldo", "update_saldo", attrs, () -> {

                    if (request.getSaldoId() == null) {
                        logger.error("saldo_id is required");
                        return Uni.createFrom().item(new ApiResponse<>("error", "saldo_id is required", null));
                    }

                    return cardPort.findCardByCardNumber(request.getCardNumber())
                            .chain(card -> saldoQueryRepository.findById(request.getSaldoId()))
                            .chain(saldo -> {
                                if (saldo == null) {
                                    logger.error("Saldo not found with id {}", request.getSaldoId());
                                    throw new ResourceNotFoundException("Saldo not found");
                                }

                                saldo.setCardNumber(request.getCardNumber());
                                saldo.setTotalBalance(request.getTotalBalance().intValue());
                                saldo.setUpdatedAt(Instant.now());

                                return saldoCommandRepository.persist(saldo)
                                        .chain(updatedSaldo -> {
                                            logger.info("Saldo updated successfully with id={} for card={}", updatedSaldo.getSaldoId(), request.getCardNumber());
                                            String cacheId = "saldo:id:" + request.getSaldoId();
                                            String cacheCard = "saldo:card:" + request.getCardNumber();

                                            return Uni.combine().all().unis(
                                                    redisService.deleteReactive(cacheId),
                                                    redisService.deleteReactive(cacheCard)
                                            ).asTuple().map(t -> ApiResponse.success("Update saldo success", SaldoResponse.from(updatedSaldo)));
                                        });
                            })
                            .onFailure().recoverWithItem(e -> {
                                logger.error("💥 Failed to update saldo id={} for card={}", request.getSaldoId(), request.getCardNumber(), e);
                                return new ApiResponse<>("error", e.getMessage(), null);
                            });
                });
    }

    @Override
    public Uni<ApiResponse<SaldoResponseDeleteAt>> trash(String id) {
        logger.info("🗑️ Trashing saldo id={}", id);

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "trash_saldo")
                .put("saldo.id", String.valueOf(id))
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponseDeleteAt>>traceAndMeasure(
                "trashSaldo", "trash_saldo", attrs, () ->

                saldoCommandRepository.trashed(id)
                        .chain(saldo -> {
                            if (saldo == null) {
                                logger.error("Saldo not found with id {}", id);
                                throw new ResourceNotFoundException("Saldo not found");
                            }

                            String cacheId = "saldo:id:" + id;
                            String cacheCard = "saldo:card:" + saldo.getCardNumber();

                            return Uni.combine().all().unis(
                                    redisService.deleteReactive(cacheId),
                                    redisService.deleteReactive(cacheCard)
                            ).asTuple().map(t -> ApiResponse.success("Trash saldo success", SaldoResponseDeleteAt.from(saldo)));
                        })
                        .onFailure().recoverWithItem(e -> {
                            logger.error("💥 Failed to trash saldo id={}", id, e);
                            return new ApiResponse<>("error", e.getMessage(), null);
                        }));
    }

    @Override
    public Uni<ApiResponse<SaldoResponseDeleteAt>> restore(String id) {
        logger.info("♻️ Restoring saldo id={}", id);

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "restore_saldo")
                .put("saldo.id", String.valueOf(id))
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponseDeleteAt>>traceAndMeasure(
                "restoreSaldo", "restore_saldo", attrs, () ->

                saldoCommandRepository.restore(id)
                        .chain(saldo -> {
                            if (saldo == null) {
                                logger.error("Saldo with id {} must be trashed first", id);
                                throw new InvalidRequestException("Saldo must be trashed first");
                            }

                            String cacheId = "saldo:id:" + id;
                            String cacheCard = "saldo:card:" + saldo.getCardNumber();

                            return Uni.combine().all().unis(
                                    redisService.deleteReactive(cacheId),
                                    redisService.deleteReactive(cacheCard)
                            ).asTuple().map(t -> ApiResponse.success("Restore saldo success", SaldoResponseDeleteAt.from(saldo)));
                        }));
    }

    @Override
    public Uni<ApiResponse<Boolean>> delete(String id) {
        logger.info("🧨 Permanently deleting saldo id={}", id);

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "delete_saldo")
                .put("saldo.id", String.valueOf(id))
                .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                "deleteSaldo", "delete_saldo", attrs, () ->

                saldoQueryRepository.findById(id)
                        .chain(saldo -> {
                            if (saldo == null) {
                                logger.error("Saldo not found with id {}", id);
                                throw new ResourceNotFoundException("Saldo not found");
                            }

                            if (saldo.getDeletedAt() == null) {
                                logger.error("Saldo with id {} must be trashed before permanent deletion", id);
                                throw new InvalidRequestException("Saldo must be trashed before permanent deletion");
                            }

                            String cacheId = "saldo:id:" + id;
                            String cacheCard = "saldo:card:" + saldo.getCardNumber();

                            return saldoCommandRepository.deletePermanent(id)
                                    .chain(deleted -> Uni.combine().all().unis(
                                            redisService.deleteReactive(cacheId),
                                            redisService.deleteReactive(cacheCard)
                                    ).asTuple().map(t -> ApiResponse.success("Delete saldo success", deleted)));
                        }));
    }

    @Override
    public Uni<ApiResponse<Boolean>> restoreAll() {
        logger.info("🔄 Restoring ALL trashed saldos");

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "restore_all_saldos")
                .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                "restoreAllSaldos", "restore_all_saldos", attrs, () ->

                saldoCommandRepository.restoreAllDeleted()
                        .map(restored -> {
                            if (!restored) {
                                logger.error("No trashed saldos to restore");
                                throw new ResourceNotFoundException("No trashed saldos to restore");
                            }
                            return ApiResponse.success("Restore all saldo success", true);
                        }));
    }

    @Override
    public Uni<ApiResponse<Boolean>> deleteAll() {
        logger.info("💣 Permanently deleting ALL trashed saldos");

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "delete_all_saldos")
                .build();

        return tracingMetrics.<ApiResponse<Boolean>>traceAndMeasure(
                "deleteAllSaldos", "delete_all_saldos", attrs, () ->

                saldoCommandRepository.deleteAllDeleted()
                        .map(deleted -> {
                            if (!deleted) {
                                logger.error("No trashed saldos to delete");
                                throw new ResourceNotFoundException("No trashed saldos to delete");
                            }
                            return ApiResponse.success("Delete all saldo success", true);
                        }));
    }

    @Override
    public Uni<ApiResponse<SaldoResponse>> updateSaldoBalance(com.sanedge.saldo.domain.requests.UpdateSaldoBalance request) {
        logger.info("Updating saldo balance for card_number={}", request.getCardNumber());

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "update_saldo_balance")
                .put("cardNumber", request.getCardNumber())
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponse>>traceAndMeasure(
                "updateSaldoBalance", "update_saldo_balance", attrs, () -> {
                    Uni<ApiResponse<SaldoResponse>> action = request.getDeltaBalance() != null
                            ? applyBalanceDelta(request)
                            : overwriteBalance(request);

                    return action.onFailure().recoverWithItem(e -> {
                        logger.error("💥 Failed to update saldo balance for card_number={}", request.getCardNumber(), e);
                        return new ApiResponse<>("error", e.getMessage(), null);
                    });
                });
    }

    /**
     * Atomic path: the balance is mutated by {@code deltaBalance} through the
     * exactly-once mutation ledger, honouring {@code minimumBalance} and keyed by
     * {@code operationKey}. Replaying the same key does not re-apply the delta.
     */
    private Uni<ApiResponse<SaldoResponse>> applyBalanceDelta(com.sanedge.saldo.domain.requests.UpdateSaldoBalance request) {
        String operationKey = request.getOperationKey();
        if (operationKey == null || operationKey.isBlank()) {
            return Uni.createFrom().failure(
                    new InvalidRequestException("operation_key is required for delta balance updates"));
        }

        long minimumBalance = request.getMinimumBalance() != null ? request.getMinimumBalance() : 0L;

        return saldoCommandRepository
                .updateBalanceByDelta(request.getCardNumber(), request.getDeltaBalance(), minimumBalance, operationKey)
                .chain(applied -> saldoQueryRepository.findByCardNumber(request.getCardNumber())
                        .chain(saldo -> {
                            if (saldo == null) {
                                logger.error("Saldo not found for card {}", request.getCardNumber());
                                throw new ResourceNotFoundException("Saldo not found");
                            }
                            if (applied == null || applied == 0) {
                                throw new InvalidRequestException("Insufficient balance");
                            }

                            return evictSaldoCaches(request.getCardNumber(), saldo.getSaldoId())
                                    .map(v -> ApiResponse.success("Update saldo balance success", SaldoResponse.from(saldo)));
                        }));
    }

    /** Direct overwrite path: writes {@code totalBalance} verbatim. */
    private Uni<ApiResponse<SaldoResponse>> overwriteBalance(com.sanedge.saldo.domain.requests.UpdateSaldoBalance request) {
        return saldoQueryRepository.findByCardNumber(request.getCardNumber())
                .chain(saldo -> {
                    if (saldo == null) {
                        logger.error("Saldo not found for card {}", request.getCardNumber());
                        throw new ResourceNotFoundException("Saldo not found");
                    }

                    saldo.setTotalBalance(request.getTotalBalance().intValue());
                    saldo.setUpdatedAt(Instant.now());

                    return saldoCommandRepository.persist(saldo)
                            .chain(savedSaldo -> evictSaldoCaches(request.getCardNumber(), savedSaldo.getSaldoId())
                                    .map(v -> ApiResponse.success("Update saldo balance success", SaldoResponse.from(savedSaldo))));
                });
    }

    private Uni<Void> evictSaldoCaches(String cardNumber, String saldoId) {
        return Uni.combine().all().unis(
                redisService.deleteReactive("saldo:id:" + saldoId),
                redisService.deleteReactive("saldo:card:" + cardNumber)
        ).asTuple().replaceWithVoid();
    }

    @Override
    public Uni<ApiResponse<SaldoResponse>> updateSaldoWithdraw(com.sanedge.saldo.domain.requests.UpdateSaldoWithdraw request) {
        logger.info("Updating saldo withdraw for card_number={}", request.getCardNumber());

        Attributes attrs = Attributes.builder()
                .put("service.name", "saldo-command-service")
                .put("operation", "update_saldo_withdraw")
                .put("cardNumber", request.getCardNumber())
                .build();

        return tracingMetrics.<ApiResponse<SaldoResponse>>traceAndMeasure(
                "updateSaldoWithdraw", "update_saldo_withdraw", attrs, () -> {
                    Uni<ApiResponse<SaldoResponse>> action = request.getDeltaBalance() != null
                            ? applyWithdrawDelta(request)
                            : overwriteWithdraw(request);

                    return action.onFailure().recoverWithItem(e -> {
                        logger.error("💥 Failed to update saldo withdraw for card_number={}", request.getCardNumber(), e);
                        return new ApiResponse<>("error", e.getMessage(), null);
                    });
                });
    }

    /**
     * Atomic path: the balance is debited by {@code deltaBalance} through the
     * exactly-once mutation ledger, then the withdraw amount/time are recorded.
     * Replaying the same {@code operationKey} does not re-apply the debit.
     */
    private Uni<ApiResponse<SaldoResponse>> applyWithdrawDelta(com.sanedge.saldo.domain.requests.UpdateSaldoWithdraw request) {
        String operationKey = request.getOperationKey();
        if (operationKey == null || operationKey.isBlank()) {
            return Uni.createFrom().failure(
                    new InvalidRequestException("operation_key is required for delta withdraw updates"));
        }

        long minimumBalance = request.getMinimumBalance() != null ? request.getMinimumBalance() : 0L;

        return saldoCommandRepository
                .updateBalanceByDelta(request.getCardNumber(), request.getDeltaBalance(), minimumBalance, operationKey)
                .chain(applied -> saldoQueryRepository.findByCardNumber(request.getCardNumber())
                        .chain(saldo -> {
                            if (saldo == null) {
                                logger.error("Saldo not found for card {}", request.getCardNumber());
                                throw new ResourceNotFoundException("Saldo not found");
                            }
                            if (applied == null || applied == 0) {
                                throw new InvalidRequestException("Insufficient balance");
                            }

                            if (request.getWithdrawAmount() != null) {
                                saldo.setWithdrawAmount(request.getWithdrawAmount().intValue());
                            }
                            saldo.setWithdrawTime(request.getWithdrawTime() != null
                                    ? toInstant(request.getWithdrawTime())
                                    : Instant.now());
                            saldo.setUpdatedAt(Instant.now());

                            return saldoCommandRepository.persist(saldo)
                                    .chain(savedSaldo -> evictSaldoCaches(request.getCardNumber(), savedSaldo.getSaldoId())
                                            .map(v -> ApiResponse.success("Update saldo withdraw success", SaldoResponse.from(savedSaldo))));
                        }));
    }

    /** Direct overwrite path: writes {@code totalBalance} and withdraw fields verbatim. */
    private Uni<ApiResponse<SaldoResponse>> overwriteWithdraw(com.sanedge.saldo.domain.requests.UpdateSaldoWithdraw request) {
        return saldoQueryRepository.findByCardNumber(request.getCardNumber())
                .chain(saldo -> {
                    if (saldo == null) {
                        logger.error("Saldo not found for card {}", request.getCardNumber());
                        throw new ResourceNotFoundException("Saldo not found");
                    }

                    saldo.setTotalBalance(request.getTotalBalance().intValue());
                    saldo.setWithdrawAmount(request.getWithdrawAmount().intValue());
                    if (request.getWithdrawTime() != null) {
                        saldo.setWithdrawTime(toInstant(request.getWithdrawTime()));
                    } else {
                        saldo.setWithdrawTime(Instant.now());
                    }
                    saldo.setUpdatedAt(Instant.now());

                    return saldoCommandRepository.persist(saldo)
                            .chain(savedSaldo -> evictSaldoCaches(request.getCardNumber(), savedSaldo.getSaldoId())
                                    .map(v -> ApiResponse.success("Update saldo withdraw success", SaldoResponse.from(savedSaldo))));
                });
    }
}
