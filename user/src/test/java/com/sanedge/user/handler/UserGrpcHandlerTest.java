package com.sanedge.user.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.domain.response.ApiResponsePagination;
import com.sanedge.common.domain.response.PaginationMeta;
import com.sanedge.user.domain.requests.FindAllUsers;
import com.sanedge.user.domain.response.UserResponse;
import com.sanedge.user.domain.response.UserResponseDeleteAt;
import com.sanedge.user.service.UserCommandService;
import com.sanedge.user.service.UserQueryService;

import io.smallrye.mutiny.Uni;
import pb.user.User.ApiResponseUser;
import pb.user.User.ApiResponseUserDeleteAt;
import pb.user.User.FindAllUserRequest;
import pb.user.User.FindByIdUserRequest;
import pb.user.UserCommand.ApiResponseUserAll;
import pb.user.UserCommand.ApiResponseUserDelete;
import pb.user.UserCommand.CreateUserRequest;
import pb.user.UserCommand.UpdateUserRequest;
import pb.user.UserCommand.VerifyPasswordRequest;
import pb.user.UserCommand.VerifyPasswordResponse;
import pb.user.UserQuery.ApiResponsePaginationUser;
import pb.user.UserQuery.ApiResponsePaginationUserDeleteAt;

@ExtendWith(MockitoExtension.class)
class UserHandlerGrpcTest {

    private static final String USER_ID = "507f1f77bcf86cd799439011";
    private static final String MISSING_ID = "507f1f77bcf86cd799439099";

    @Mock
    private UserCommandService userCommandService;

    @Mock
    private UserQueryService userQueryService;

    private void injectDependencies(Object handler) {
        try {
            for (java.lang.reflect.Field field : handler.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                if (field.getType() == UserCommandService.class) {
                    field.set(handler, userCommandService);
                } else if (field.getType() == UserQueryService.class) {
                    field.set(handler, userQueryService);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mocks via reflection", e);
        }
    }

    private UserResponse createDomainUserResponse() {
        return UserResponse.builder()
                .id(USER_ID)
                .username("testuser")
                .email("john@example.com")
                .firstname("John")
                .lastname("Doe")
                .createdAt(Instant.now().toString())
                .updatedAt(Instant.now().toString())
                .build();
    }

    private UserResponseDeleteAt createDomainUserResponseDeleteAt() {
        return UserResponseDeleteAt.builder()
                .id(USER_ID)
                .username("testuser")
                .email("john@example.com")
                .firstname("John")
                .lastname("Doe")
                .createdAt(Instant.now().toString())
                .updatedAt(Instant.now().toString())
                .deletedAt("2023-10-10T10:10:00Z")
                .build();
    }

    @Nested
    class UserCommandGrpcHandlerTest {

        private UserCommandGrpcHandler commandHandler;

        @BeforeEach
        void setUp() {
            commandHandler = new UserCommandGrpcHandler();
            injectDependencies(commandHandler);
        }

        @Test
        void create_success_mapsToProtoCorrectly() {
            CreateUserRequest request = CreateUserRequest.newBuilder()
                    .setFirstname("John").setLastname("Doe")
                    .setEmail("john@example.com").setPassword("pass").setConfirmPassword("pass")
                    .build();

            UserResponse domainRes = createDomainUserResponse();
            when(userCommandService.createUser(any()))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Success", domainRes)));

            ApiResponseUser response = commandHandler.create(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getData().getEmail()).isEqualTo("john@example.com");
            assertThat(response.getData().getFirstname()).isEqualTo("John");
        }

        @Test
        void create_withNullDates_handlesGracefully() {
            CreateUserRequest request = CreateUserRequest.newBuilder().setEmail("test@test.com")
                    .setPassword("p")
                    .setConfirmPassword("p").build();
            UserResponse domainRes = UserResponse.builder().id(USER_ID).email("test@test.com").createdAt(null)
                    .updatedAt(null)
                    .build();

            when(userCommandService.createUser(any()))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Success", domainRes)));

            ApiResponseUser response = commandHandler.create(request).await().indefinitely();

            assertThat(response.getData().getCreatedAt()).isEmpty();
            assertThat(response.getData().getUpdatedAt()).isEmpty();
        }

        @Test
        void update_success_mapsToProtoCorrectly() {
            UpdateUserRequest request = UpdateUserRequest.newBuilder().setId(USER_ID).setFirstname("Updated")
                    .build();
            UserResponse domainRes = createDomainUserResponse();

            when(userCommandService.updateUser(any()))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Updated", domainRes)));

            ApiResponseUser response = commandHandler.update(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getData().getFirstname()).isEqualTo("John");
        }

        @Test
        void trashedUser_success_mapsDeletedAtToStringValue() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(USER_ID).build();
            UserResponseDeleteAt domainRes = createDomainUserResponseDeleteAt();

            when(userCommandService.trashed(USER_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Trashed", domainRes)));

            ApiResponseUserDeleteAt response = commandHandler.trashedUser(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getData().hasDeletedAt()).isTrue();
            assertThat(response.getData().getDeletedAt().getValue()).contains("2023-10-10");
        }

        @Test
        void trashedUser_withNullDeletedAt_doesNotSetStringValue() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(USER_ID).build();
            UserResponseDeleteAt domainRes = UserResponseDeleteAt.builder().id(USER_ID).deletedAt(null).build();

            when(userCommandService.trashed(USER_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Trashed", domainRes)));

            ApiResponseUserDeleteAt response = commandHandler.trashedUser(request).await().indefinitely();

            assertThat(response.getData().hasDeletedAt()).isFalse();
        }

        @Test
        void restoreUser_success_mapsToProtoCorrectly() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(USER_ID).build();
            UserResponseDeleteAt domainRes = createDomainUserResponseDeleteAt();

            when(userCommandService.restore(USER_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Restored", domainRes)));

            ApiResponseUserDeleteAt response = commandHandler.restoreUser(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
        }

        @Test
        void deletePermanent_success_returnsVoidProto() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(USER_ID).build();

            when(userCommandService.deletePermanent(USER_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Deleted")));

            ApiResponseUserDelete response = commandHandler.deleteUserPermanent(request).await()
                    .indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getMessage()).isEqualTo("Deleted");
        }

        @Test
        void restoreAllUser_success_returnsVoidProto() {
            when(userCommandService.restoreAllTrashedUsers())
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("All Restored")));

            ApiResponseUserAll response = commandHandler
                    .restoreAllUser(com.google.protobuf.Empty.getDefaultInstance())
                    .await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
        }

        @Test
        void deleteAllUserPermanent_success_returnsVoidProto() {
            when(userCommandService.deleteAllTrashedUsers())
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("All Deleted")));

            ApiResponseUserAll response = commandHandler
                    .deleteAllUserPermanent(com.google.protobuf.Empty.getDefaultInstance()).await()
                    .indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
        }

        @Test
        void verifyPassword_success_returnsTrueAndUser() {
            VerifyPasswordRequest request = VerifyPasswordRequest.newBuilder().setEmail("e@e.com")
                    .setPassword("p")
                    .build();
            UserResponse domainRes = createDomainUserResponse();

            when(userCommandService.verifyPassword("e@e.com", "p"))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Valid", domainRes)));

            VerifyPasswordResponse response = commandHandler.verifyPassword(request).await().indefinitely();

            assertThat(response.getValid()).isTrue();
            assertThat(response.hasUser()).isTrue();
            assertThat(response.getUser().getEmail()).isEqualTo("john@example.com");
        }

        @Test
        void verifyPassword_failure_returnsFalseAndNoUser() {
            VerifyPasswordRequest request = VerifyPasswordRequest.newBuilder().setEmail("e@e.com")
                    .setPassword("wrong")
                    .build();

            when(userCommandService.verifyPassword(anyString(), anyString()))
                    .thenReturn(Uni.createFrom()
                            .failure(new RuntimeException("Invalid credentials")));

            VerifyPasswordResponse response = commandHandler.verifyPassword(request).await().indefinitely();

            assertThat(response.getValid()).isFalse();
            assertThat(response.hasUser()).isFalse();
        }
    }

    @Nested
    class UserQueryGrpcHandlerTest {

        private UserQueryGrpcHandler queryHandler;

        @BeforeEach
        void setUp() {
            queryHandler = new UserQueryGrpcHandler();
            try {
                java.lang.reflect.Field queryField = UserQueryGrpcHandler.class
                        .getDeclaredField("userQueryService");
                queryField.setAccessible(true);
                queryField.set(queryHandler, userQueryService);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        void findById_success_mapsToProtoCorrectly() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(USER_ID).build();
            UserResponse domainRes = createDomainUserResponse();

            when(userQueryService.findById(USER_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Found", domainRes)));

            ApiResponseUser response = queryHandler.findById(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getData().getId()).isEqualTo(USER_ID);
            assertThat(response.getData().getEmail()).isEqualTo("john@example.com");
        }

        @Test
        void findById_whenDataIsNull_doesNotSetData() {
            FindByIdUserRequest request = FindByIdUserRequest.newBuilder().setId(MISSING_ID).build();

            when(userQueryService.findById(MISSING_ID))
                    .thenReturn(Uni.createFrom().item(ApiResponse.success("Not found", null)));

            ApiResponseUser response = queryHandler.findById(request).await().indefinitely();

            assertThat(response.hasData()).isFalse();
        }

        @Test
        void findAll_success_mapsListAndPaginationCorrectly() {
            FindAllUserRequest request = FindAllUserRequest.newBuilder().setPage(1).setPageSize(10)
                    .setSearch("")
                    .build();

            List<UserResponse> mockList = List.of(createDomainUserResponse(), createDomainUserResponse());
            PaginationMeta meta = new PaginationMeta(1, 10, 1, 2);
            ApiResponsePagination<List<UserResponse>> serviceRes = new ApiResponsePagination<>("success",
                    "Found",
                    mockList, meta);

            when(userQueryService.findAllPaginated(any(FindAllUsers.class)))
                    .thenReturn(Uni.createFrom().item(serviceRes));

            ApiResponsePaginationUser response = queryHandler.findAll(request).await().indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getDataCount()).isEqualTo(2);
            assertThat(response.getPaginationMeta().getTotalRecords()).isEqualTo(2);
            assertThat(response.getPaginationMeta().getTotalPages()).isEqualTo(1);
            assertThat(response.getPaginationMeta().getCurrentPage()).isEqualTo(1);
        }

        @Test
        void findAll_withNullPagination_doesNotSetPagination() {
            FindAllUserRequest request = FindAllUserRequest.newBuilder().setPage(1).setPageSize(10).build();
            ApiResponsePagination<List<UserResponse>> serviceRes = new ApiResponsePagination<>("success",
                    "Found",
                    List.of(), null);

            when(userQueryService.findAllPaginated(any(FindAllUsers.class)))
                    .thenReturn(Uni.createFrom().item(serviceRes));

            ApiResponsePaginationUser response = queryHandler.findAll(request).await().indefinitely();

            assertThat(response.hasPaginationMeta()).isFalse();
        }

        @Test
        void findByActive_success_mapsDeletedAtToStringValue() {
            FindAllUserRequest request = FindAllUserRequest.newBuilder().setPage(1).setPageSize(10).build();

            List<UserResponseDeleteAt> mockList = List.of(createDomainUserResponseDeleteAt());
            PaginationMeta meta = new PaginationMeta(1, 10, 1, 1);
            ApiResponsePagination<List<UserResponseDeleteAt>> serviceRes = new ApiResponsePagination<>(
                    "success",
                    "Found", mockList, meta);

            when(userQueryService.findActivePaginated(any(FindAllUsers.class)))
                    .thenReturn(Uni.createFrom().item(serviceRes));

            ApiResponsePaginationUserDeleteAt response = queryHandler.findByActive(request).await()
                    .indefinitely();

            assertThat(response.getDataCount()).isEqualTo(1);
            assertThat(response.getData(0).hasDeletedAt()).isTrue();
        }

        @Test
        void findByTrashed_success_mapsCorrectly() {
            FindAllUserRequest request = FindAllUserRequest.newBuilder().setPage(1).setPageSize(10).build();

            List<UserResponseDeleteAt> mockList = List.of(createDomainUserResponseDeleteAt());
            PaginationMeta meta = new PaginationMeta(1, 10, 1, 1);
            ApiResponsePagination<List<UserResponseDeleteAt>> serviceRes = new ApiResponsePagination<>(
                    "success",
                    "Found", mockList, meta);

            when(userQueryService.findTrashedPaginated(any(FindAllUsers.class)))
                    .thenReturn(Uni.createFrom().item(serviceRes));

            ApiResponsePaginationUserDeleteAt response = queryHandler.findByTrashed(request).await()
                    .indefinitely();

            assertThat(response.getStatus()).isEqualTo("success");
            assertThat(response.getPaginationMeta().getTotalRecords()).isEqualTo(1);
        }
    }
}
