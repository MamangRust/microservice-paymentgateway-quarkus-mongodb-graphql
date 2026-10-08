package com.sanedge.merchant.service.impl;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.merchant.domain.requests.FindAllMerchants;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.domain.response.ApiResponsePagination;
import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.common.domain.response.PaginationMeta;
import com.sanedge.merchant.domain.response.MerchantResponse;
import com.sanedge.merchant.domain.response.MerchantResponseDeleteAt;
import com.sanedge.merchant.repository.MerchantQueryRepository;
import com.sanedge.merchant.service.MerchantQueryService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;

@ApplicationScoped
public class MerchantQueryServiceImpl implements MerchantQueryService {
    private static final Logger logger = LoggerFactory.getLogger(MerchantQueryServiceImpl.class);

    private final MerchantQueryRepository merchantQueryRepository;
    private final RedisService redisService;
    private final ObjectMapper objectMapper;
    private final TracingMetrics tracingMetrics;

    private static final long LIST_CACHE_TTL_SECONDS = 300;

    @Inject
    public MerchantQueryServiceImpl(MerchantQueryRepository merchantQueryRepository,
                    RedisService redisService,
                    ObjectMapper objectMapper,
                    TracingMetrics tracingMetrics) {
        this.merchantQueryRepository = merchantQueryRepository;
        this.redisService = redisService;
        this.objectMapper = objectMapper;
        this.tracingMetrics = tracingMetrics;
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing object to JSON", e);
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    private <T> T fromJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing JSON to object", e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }

    private <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing JSON to object with TypeReference", e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }

    @Override
    public Uni<ApiResponsePagination<List<MerchantResponse>>> findAll(FindAllMerchants req) {
        String cacheKey = String.format("merchants:all:%d:%d:%s", req.getPage(), req.getPageSize(),
                        req.getSearch());

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_all_merchants")
                        .put("service.name", "merchant-query-service")
                        .build();

        return tracingMetrics.<ApiResponsePagination<List<MerchantResponse>>>traceAndMeasure(
                        "findAllMerchants", "find_all_merchants", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                ApiResponsePagination<List<MerchantResponse>> response = fromJson(cachedJson,
                                                                                new TypeReference<ApiResponsePagination<List<MerchantResponse>>>() {
                                                                                });
                                                                return Uni.createFrom().item(response);
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findMerchants(req)
                                                                        .chain(pagedResult -> {
                                                                                ApiResponsePagination<List<MerchantResponse>> response = buildPaginatedResponse(
                                                                                                pagedResult, req, "Merchants retrieved successfully",
                                                                                                MerchantResponse::from);

                                                                                return redisService.setWithExpirationReactive(cacheKey, toJson(response),
                                                                                                LIST_CACHE_TTL_SECONDS)
                                                                                                .map(v -> {
                                                                                                        logger.info("Cached response for key: {}", cacheKey);
                                                                                                        logger.info("Successfully retrieved {} merchants",
                                                                                                                        pagedResult.getTotalRecords());
                                                                                                        return response;
                                                                                                });
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding all merchants", e);
                        });
    }

    @Override
    public Uni<ApiResponsePagination<List<MerchantResponseDeleteAt>>> findByActive(FindAllMerchants req) {
        String cacheKey = String.format("merchants:active:%d:%d:%s", req.getPage(), req.getPageSize(),
                        req.getSearch());

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_active_merchants")
                        .put("service.name", "merchant-query-service")
                        .build();

        return tracingMetrics.<ApiResponsePagination<List<MerchantResponseDeleteAt>>>traceAndMeasure(
                        "findActiveMerchants", "find_active_merchants", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                ApiResponsePagination<List<MerchantResponseDeleteAt>> response = fromJson(cachedJson,
                                                                                new TypeReference<ApiResponsePagination<List<MerchantResponseDeleteAt>>>() {
                                                                                });
                                                                return Uni.createFrom().item(response);
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findActiveMerchants(req)
                                                                        .chain(pagedResult -> {
                                                                                ApiResponsePagination<List<MerchantResponseDeleteAt>> response = buildPaginatedResponse(
                                                                                                pagedResult, req, "Active merchants retrieved successfully",
                                                                                                MerchantResponseDeleteAt::from);

                                                                                return redisService.setWithExpirationReactive(cacheKey, toJson(response),
                                                                                                LIST_CACHE_TTL_SECONDS)
                                                                                                .map(v -> {
                                                                                                        logger.info("Cached response for key: {}", cacheKey);
                                                                                                        logger.info("Successfully retrieved {} active merchants",
                                                                                                                        pagedResult.getTotalRecords());
                                                                                                        return response;
                                                                                                });
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding active merchants", e);
                        });
    }

    @Override
    public Uni<ApiResponsePagination<List<MerchantResponseDeleteAt>>> findByTrashed(FindAllMerchants req) {
        String cacheKey = String.format("merchants:trashed:%d:%d:%s", req.getPage(), req.getPageSize(),
                        req.getSearch());

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_trashed_merchants")
                        .put("service.name", "merchant-query-service")
                        .build();

        return tracingMetrics.<ApiResponsePagination<List<MerchantResponseDeleteAt>>>traceAndMeasure(
                        "findTrashedMerchants", "find_trashed_merchants", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                ApiResponsePagination<List<MerchantResponseDeleteAt>> response = fromJson(cachedJson,
                                                                                new TypeReference<ApiResponsePagination<List<MerchantResponseDeleteAt>>>() {
                                                                                });
                                                                return Uni.createFrom().item(response);
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findTrashedMerchants(req)
                                                                        .chain(pagedResult -> {
                                                                                ApiResponsePagination<List<MerchantResponseDeleteAt>> response = buildPaginatedResponse(
                                                                                                pagedResult, req, "Trashed merchants retrieved successfully",
                                                                                                MerchantResponseDeleteAt::from);

                                                                                return redisService.setWithExpirationReactive(cacheKey, toJson(response),
                                                                                                LIST_CACHE_TTL_SECONDS)
                                                                                                .map(v -> {
                                                                                                        logger.info("Cached response for key: {}", cacheKey);
                                                                                                        logger.info("Successfully retrieved {} trashed merchants",
                                                                                                                        pagedResult.getTotalRecords());
                                                                                                        return response;
                                                                                                });
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding trashed merchants", e);
                        });
    }

    @Override
    public Uni<ApiResponse<MerchantResponse>> findById(String merchantId) {
        String cacheKey = "merchant:id:" + merchantId;

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_merchant_by_id")
                        .put("service.name", "merchant-query-service")
                        .put("merchant.id", merchantId)
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponse>>traceAndMeasure(
                        "findMerchantById", "find_merchant_by_id", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                MerchantResponse cachedMerchant = fromJson(cachedJson, MerchantResponse.class);
                                                                return Uni.createFrom()
                                                                                .item(ApiResponse.success("Merchant retrieved successfully", cachedMerchant));
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findMerchantById(merchantId)
                                                                        .chain(merchant -> {
                                                                                if (merchant == null) {
                                                                                        logger.warn("Merchant not found with id: {}", merchantId);
                                                                                        throw new NotFoundException("Merchant not found with id: " + merchantId);
                                                                                }

                                                                                logger.info("Successfully found merchant with id: {} and name: {}", merchantId,
                                                                                                merchant.getName());
                                                                                MerchantResponse merchantResponse = MerchantResponse.from(merchant);

                                                                                return redisService.setReactive(cacheKey, toJson(merchantResponse))
                                                                                                .map(v -> ApiResponse.success("Merchant retrieved successfully",
                                                                                                                merchantResponse));
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding merchant by id: {}", merchantId, e);
                        });
    }

    @Override
    public Uni<ApiResponse<MerchantResponse>> findByApiKey(String apiKey) {
        String cacheKey = "merchant:apikey:" + apiKey;

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_merchant_by_api_key")
                        .put("service.name", "merchant-query-service")
                        .build();

        return tracingMetrics.<ApiResponse<MerchantResponse>>traceAndMeasure(
                        "findMerchantByApiKey", "find_merchant_by_api_key", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                MerchantResponse cachedMerchant = fromJson(cachedJson, MerchantResponse.class);
                                                                return Uni.createFrom()
                                                                                .item(ApiResponse.success("Merchant retrieved successfully", cachedMerchant));
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findByApiKey(apiKey)
                                                                        .chain(merchant -> {
                                                                                if (merchant == null) {
                                                                                        logger.warn("Merchant not found with api key: {}", apiKey);
                                                                                        throw new NotFoundException("Merchant not found with api key");
                                                                                }

                                                                                logger.info("Successfully found merchant with api key and name: {}",
                                                                                                merchant.getName());
                                                                                MerchantResponse merchantResponse = MerchantResponse.from(merchant);

                                                                                return redisService.setReactive(cacheKey, toJson(merchantResponse))
                                                                                                .map(v -> ApiResponse.success("Merchant retrieved successfully",
                                                                                                                merchantResponse));
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding merchant by api key", e);
                        });
    }

    @Override
    public Uni<ApiResponse<List<MerchantResponse>>> findByUserId(String userId) {
        String cacheKey = "merchant:user:" + userId;

        Attributes attrs = Attributes.builder()
                        .put("operation", "find_merchants_by_user_id")
                        .put("service.name", "merchant-query-service")
                        .put("user.id", userId)
                        .build();

        return tracingMetrics.<ApiResponse<List<MerchantResponse>>>traceAndMeasure(
                        "findMerchantsByUserId", "find_merchants_by_user_id", attrs, () -> {
                                return redisService.getReactive(cacheKey)
                                                .chain(cachedJson -> {
                                                        if (cachedJson != null) {
                                                                logger.info("Cache HIT for key: {}", cacheKey);
                                                                List<MerchantResponse> cachedMerchants = fromJson(cachedJson,
                                                                                new TypeReference<List<MerchantResponse>>() {
                                                                                });
                                                                return Uni.createFrom()
                                                                                .item(ApiResponse.success("Merchants retrieved successfully", cachedMerchants));
                                                        }

                                                        logger.info("Cache MISS for key: {}. Fetching from DB.", cacheKey);

                                                        return merchantQueryRepository.findByUserId(userId)
                                                                        .chain(merchants -> {
                                                                                List<MerchantResponse> responses = merchants.stream()
                                                                                                .map(MerchantResponse::from)
                                                                                                .collect(Collectors.toList());

                                                                                logger.info("Successfully found {} merchants for user id: {}", responses.size(),
                                                                                                userId);

                                                                                return redisService.setReactive(cacheKey, toJson(responses))
                                                                                                .map(v -> ApiResponse.success("Merchants retrieved successfully",
                                                                                                                responses));
                                                                        });
                                                });
                        }).onFailure().invoke(e -> {
                                logger.error("Error finding merchants by user id: {}", userId, e);
                        });
    }

    private <T, R> ApiResponsePagination<List<R>> buildPaginatedResponse(
                    PagedResult<T> pagedResult,
                    FindAllMerchants request,
                    String successMessage,
                    Function<T, R> mapper) {

        List<R> data = pagedResult.getData().stream()
                        .map(mapper)
                        .collect(Collectors.toList());

        int totalRecords = pagedResult.getTotalRecords();
        int size = request.getPageSize() > 0 ? request.getPageSize() : 1;
        int totalPages = (int) Math.ceil((double) totalRecords / size);

        PaginationMeta pagination = new PaginationMeta(request.getPage(), size, totalPages, totalRecords);

        return new ApiResponsePagination<>("success", successMessage, data, pagination);
    }
}
