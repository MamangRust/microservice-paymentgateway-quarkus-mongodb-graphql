package com.sanedge.merchant.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.model.User;
import com.sanedge.common.adapter.user.UserPort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.enums.Status;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.merchant.domain.requests.CreateMerchantRequest;
import com.sanedge.merchant.domain.requests.UpdateMerchantRequest;
import com.sanedge.merchant.domain.response.MerchantResponse;
import com.sanedge.merchant.domain.response.MerchantResponseDeleteAt;
import com.sanedge.merchant.entity.Merchant;
import com.sanedge.merchant.repository.MerchantCommandRepository;
import com.sanedge.merchant.repository.MerchantQueryRepository;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;

@ExtendWith(MockitoExtension.class)
class MerchantCommandServiceImplTest {

    private static final String ID_1 = "507f1f77bcf86cd799439011";
    private static final String ID_999 = "507f1f77bcf86cd799439099";
    private static final String USER_ID = "507f1f77bcf86cd799439010";

    @Mock
    private UserPort userPort;

    @Mock
    private MerchantQueryRepository merchantQueryRepo;

    @Mock
    private MerchantCommandRepository merchantCommandRepo;

    @Mock
    private RedisService redisService;

    private TracingMetrics tracingMetrics;

    private MerchantCommandServiceImpl service;

    @BeforeEach
    void setUp() {
        tracingMetrics = mock(TracingMetrics.class, withSettings().lenient());
        lenient().doAnswer(inv -> {
            Supplier<Uni<?>> s = inv.getArgument(3);
            return s.get();
        }).when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any(Attributes.class), any());

        lenient().doAnswer(inv -> {
            Supplier<Uni<?>> s = inv.getArgument(2);
            return s.get();
        }).when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any());

        service = new MerchantCommandServiceImpl(
                userPort,
                merchantQueryRepo,
                merchantCommandRepo,
                redisService,
                tracingMetrics);

        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());
        lenient().when(merchantCommandRepo.deleteAllDeleted()).thenReturn(Uni.createFrom().item(true));
        lenient().when(merchantCommandRepo.restoreAllDeleted()).thenReturn(Uni.createFrom().item(true));
    }

    private Merchant createMerchant(String id, String name, String apiKey) {
        Merchant m = new Merchant();
        m.setMerchantId(id);
        m.setMerchantNo(UUID.randomUUID());
        m.setName(name);
        m.setApiKey(apiKey);
        m.setUserId(new ObjectId(USER_ID));
        m.setStatus(Status.SUCCESS);
        Instant now = Instant.now();
        m.setCreatedAt(now);
        m.setUpdatedAt(now);
        return m;
    }

    private Merchant createPersistedMerchant(String id, String name, String apiKey) {
        return createMerchant(id, name, apiKey);
    }

    private CreateMerchantRequest createReq(String name, String userId) {
        CreateMerchantRequest r = new CreateMerchantRequest();
        r.setName(name);
        r.setUserId(userId);
        return r;
    }

    private UpdateMerchantRequest updateReq(String id, String name, String userId, String status) {
        UpdateMerchantRequest r = new UpdateMerchantRequest();
        r.setMerchantId(id);
        r.setName(name);
        r.setUserId(userId);
        r.setStatus(status);
        return r;
    }

    @Nested
    @DisplayName("createMerchant tests")
    class CreateMerchantTests {
        @Test
        void success() {
            CreateMerchantRequest req = createReq("New Merchant", USER_ID);
            when(userPort.findById(any())).thenReturn(Uni.createFrom().item(
                    new User(USER_ID, null, null, null, null, null)));
            when(merchantQueryRepo.existsByName(anyString())).thenReturn(Uni.createFrom().item(false));
            Merchant persisted = createPersistedMerchant(ID_1, "New Merchant", "dummy-key");
            when(merchantCommandRepo.persist(any(Merchant.class))).thenReturn(Uni.createFrom().item(persisted));

            ApiResponse<MerchantResponse> resp = service.createMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("success");
            assertThat(resp.data().getName()).isEqualTo("New Merchant");
            assertThat(resp.data().getApiKey()).isNotNull();
        }

        @Test
        void userNotFound_returnsError() {
            CreateMerchantRequest req = createReq("Merchant", ID_999);
            when(userPort.findById(any())).thenReturn(
                    Uni.createFrom().failure(new ResourceNotFoundException("User not found")));

            ApiResponse<MerchantResponse> resp = service.createMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("User not found");
        }

        @Test
        void nameAlreadyExists_returnsError() {
            CreateMerchantRequest req = createReq("Existing", USER_ID);
            when(userPort.findById(any())).thenReturn(Uni.createFrom().item(
                    new User(USER_ID, null, null, null, null, null)));
            when(merchantQueryRepo.existsByName("Existing")).thenReturn(Uni.createFrom().item(true));

            ApiResponse<MerchantResponse> resp = service.createMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("name already taken");
        }
    }

    @Nested
    @DisplayName("updateMerchant tests")
    class UpdateMerchantTests {
        @Test
        void success() {
            UpdateMerchantRequest req = updateReq(ID_1, "Updated", USER_ID, "SUCCESS");
            Merchant existing = createMerchant(ID_1, "Old", "old-key");
            when(merchantQueryRepo.findMerchantById(ID_1)).thenReturn(Uni.createFrom().item(existing));

            when(userPort.findById(any())).thenReturn(Uni.createFrom().item(
                    new User(USER_ID, null, null, null, null, null)));
            when(merchantCommandRepo.update(any(Merchant.class))).thenReturn(Uni.createFrom().item(existing));

            ApiResponse<MerchantResponse> resp = service.updateMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("success");
            assertThat(resp.data().getName()).isEqualTo("Updated");
        }

        @Test
        void merchantNotFound_returnsError() {
            UpdateMerchantRequest req = updateReq(ID_999, "X", null, "SUCCESS");
            when(merchantQueryRepo.findMerchantById(ID_999)).thenReturn(Uni.createFrom().nullItem());

            ApiResponse<MerchantResponse> resp = service.updateMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("Merchant not found");
        }

        @Test
        void userNotFoundInUpdate_returnsError() {
            UpdateMerchantRequest req = updateReq(ID_1, "Y", ID_999, "SUCCESS");
            Merchant existing = createMerchant(ID_1, "Old", "key");
            when(merchantQueryRepo.findMerchantById(ID_1)).thenReturn(Uni.createFrom().item(existing));
            when(userPort.findById(any())).thenReturn(
                    Uni.createFrom().failure(new ResourceNotFoundException("User not found")));

            ApiResponse<MerchantResponse> resp = service.updateMerchant(req).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("User not found");
        }
    }

    @Nested
    @DisplayName("trashMerchant tests")
    class TrashMerchantTests {
        @Test
        void success() {
            Merchant merchant = createMerchant(ID_1, "Trash", "api");
            merchant.setDeletedAt(Instant.now());
            when(merchantCommandRepo.trashed(ID_1)).thenReturn(Uni.createFrom().item(merchant));

            ApiResponse<MerchantResponseDeleteAt> resp = service.trashMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("success");
            assertThat(resp.data().getDeletedAt()).isNotNull();
        }

        @Test
        void notFound_returnsError() {
            when(merchantCommandRepo.trashed(ID_1)).thenReturn(Uni.createFrom().nullItem());

            ApiResponse<MerchantResponseDeleteAt> resp = service.trashMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("Merchant not found");
        }
    }

    @Nested
    @DisplayName("restoreMerchant tests")
    class RestoreMerchantTests {
        @Test
        void success() {
            when(merchantCommandRepo.restore(ID_1))
                    .thenReturn(Uni.createFrom().item(createMerchant(ID_1, "Restore", "api")));
            ApiResponse<MerchantResponseDeleteAt> resp = service.restoreMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("success");
        }

        @Test
        void notFound_returnsError() {
            when(merchantCommandRepo.restore(ID_1)).thenReturn(Uni.createFrom().nullItem());

            ApiResponse<MerchantResponseDeleteAt> resp = service.restoreMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("Merchant not found");
        }
    }

    @Nested
    @DisplayName("deleteMerchant tests")
    class DeleteMerchantTests {
        @Test
        void success() {
            Merchant existing = createMerchant(ID_1, "Del", "dk");
            when(merchantQueryRepo.findMerchantById(ID_1)).thenReturn(Uni.createFrom().item(existing));
            when(merchantCommandRepo.deletePermanent(ID_1)).thenReturn(Uni.createFrom().item(existing));

            ApiResponse<Boolean> resp = service.deleteMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("success");
            assertThat(resp.data()).isTrue();
        }

        @Test
        void notFound_returnsError() {
            when(merchantQueryRepo.findMerchantById(ID_1)).thenReturn(Uni.createFrom().nullItem());

            ApiResponse<Boolean> resp = service.deleteMerchant(ID_1).await().indefinitely();
            assertThat(resp.status()).isEqualTo("error");
            assertThat(resp.message()).contains("Merchant not found");
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
        void noTrashed_throwsException() {
            when(merchantCommandRepo.restoreAllDeleted()).thenReturn(Uni.createFrom().item(false));
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
        void noTrashed_throwsException() {
            when(merchantCommandRepo.deleteAllDeleted()).thenReturn(Uni.createFrom().item(false));
            assertThatThrownBy(() -> service.deleteAll().await().indefinitely())
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }
}
