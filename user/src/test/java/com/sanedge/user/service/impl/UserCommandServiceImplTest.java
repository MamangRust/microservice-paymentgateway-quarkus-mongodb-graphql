package com.sanedge.user.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.function.Supplier;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.model.Role;
import com.sanedge.common.adapter.role.RolePort;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceAlreadyExistsException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.common.utils.PasswordUtil;
import com.sanedge.user.domain.requests.RegisterRequest;
import com.sanedge.user.domain.requests.UpdateUserRequest;
import com.sanedge.user.domain.response.UserResponse;
import com.sanedge.user.domain.response.UserResponseDeleteAt;
import com.sanedge.user.entity.User;
import com.sanedge.user.repository.UserRepository;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;

@ExtendWith(MockitoExtension.class)
class UserCommandServiceImplTest {

    private static final String USER_ID = "507f1f77bcf86cd799439011";
    private static final String MISSING_ID = "507f1f77bcf86cd799439099";
    private static final String ROLE_ID = "507f1f77bcf86cd799439022";

    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordUtil passwordUtil;

    @Mock
    private RedisService redisService;

    @Mock
    private TracingMetrics tracingMetrics;

    @Mock
    private RolePort rolePort;

    private UserCommandServiceImpl userCommandService;

    @BeforeEach
    void setUp() {
        userCommandService = new UserCommandServiceImpl(
                userRepository,
                passwordUtil,
                redisService,
                tracingMetrics);

        // Inject the role port via reflection
        try {
            java.lang.reflect.Field roleQueryField = UserCommandServiceImpl.class
                    .getDeclaredField("rolePort");
            roleQueryField.setAccessible(true);
            roleQueryField.set(userCommandService, rolePort);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject role port mock", e);
        }

        lenient().doAnswer(invocation -> {
            Supplier<Uni<?>> supplier = invocation.getArgument(3);
            return supplier.get();
        }).when(tracingMetrics)
                .traceAndMeasure(
                        anyString(),
                        anyString(),
                        any(Attributes.class),
                        any());
    }

    @Test
    void createUser_passwordMismatch_throwsInvalidRequestException() {
        RegisterRequest request = new RegisterRequest();
        request.setUsername("testuser");
        request.setPassword("password123");
        request.setConfirmPassword("differentpassword");
        request.setEmail("test@example.com");
        request.setFirstname("John");
        request.setLastname("Doe");

        try {
            userCommandService.createUser(request).await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected InvalidRequestException");
        } catch (InvalidRequestException e) {
            assertThat(e.getMessage()).isEqualTo("Passwords do not match");
        }
    }

    @Test
    void createUser_usernameAlreadyExists_throwsResourceAlreadyExistsException() {
        RegisterRequest request = new RegisterRequest();
        request.setUsername("existinguser");
        request.setPassword("password123");
        request.setConfirmPassword("password123");
        request.setEmail("test@example.com");
        request.setFirstname("John");
        request.setLastname("Doe");

        when(userRepository.existsByUsername("existinguser")).thenReturn(Uni.createFrom().item(true));

        try {
            userCommandService.createUser(request).await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected ResourceAlreadyExistsException");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getMessage()).isEqualTo("Username already exists");
        }
    }

    @Test
    void createUser_emailAlreadyExists_throwsResourceAlreadyExistsException() {
        RegisterRequest request = new RegisterRequest();
        request.setUsername("newuser");
        request.setPassword("password123");
        request.setConfirmPassword("password123");
        request.setEmail("existing@example.com");
        request.setFirstname("John");
        request.setLastname("Doe");

        when(userRepository.existsByUsername("newuser")).thenReturn(Uni.createFrom().item(false));
        when(userRepository.existsByEmail("existing@example.com")).thenReturn(Uni.createFrom().item(true));

        try {
            userCommandService.createUser(request).await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected ResourceAlreadyExistsException");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getMessage()).isEqualTo("Email already exists");
        }
    }

    @Test
    void createUser_success_createsDefaultUser() {
        RegisterRequest request = new RegisterRequest();
        request.setUsername("newuser");
        request.setPassword("password123");
        request.setConfirmPassword("password123");
        request.setEmail("newuser@example.com");
        request.setFirstname("John");
        request.setLastname("Doe");

        when(rolePort.findByName("ROLE_ADMIN"))
                .thenReturn(Uni.createFrom().item(new Role(ROLE_ID, "ROLE_ADMIN", null, null)));

        when(userRepository.persist(any(User.class))).thenAnswer(invocation -> {
            User userToPersist = invocation.getArgument(0);
            userToPersist.id = new ObjectId(USER_ID);
            return Uni.createFrom().item(userToPersist);
        });
        when(userRepository.existsByUsername("newuser")).thenReturn(Uni.createFrom().item(false));
        when(userRepository.existsByEmail("newuser@example.com")).thenReturn(Uni.createFrom().item(false));
        lenient().when(passwordUtil.hashPassword("password123")).thenReturn("hashedPassword");
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<UserResponse> response = userCommandService.createUser(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("User registered successfully");
        assertThat(response.data()).isNotNull();
        assertThat(response.data().getUsername()).isEqualTo("newuser");
    }

    @Test
    void updateUser_userNotFound_returnsErrorResponse() {
        UpdateUserRequest request = UpdateUserRequest.builder()
                .id(MISSING_ID)
                .firstname("Updated")
                .build();

        when(userRepository.findById(MISSING_ID)).thenReturn(Uni.createFrom().nullItem());

        ApiResponse<UserResponse> response = userCommandService.updateUser(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("error");
        assertThat(response.message()).contains("User not found");
    }

    @Test
    void updateUser_success_updatesUser() {
        User existingUser = new User();
        existingUser.id = new ObjectId(USER_ID);
        existingUser.setUsername("oldusername");
        existingUser.setEmail("old@example.com");
        existingUser.setFirstname("Old");
        existingUser.setLastname("Name");
        existingUser.setPassword("hashedPassword");

        UpdateUserRequest request = UpdateUserRequest.builder()
                .id(USER_ID)
                .firstname("Updated")
                .lastname("User")
                .build();

        when(userRepository.findById(USER_ID)).thenReturn(Uni.createFrom().item(existingUser));
        lenient().when(userRepository.persist(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            return Uni.createFrom().item(user);
        });
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<UserResponse> response = userCommandService.updateUser(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("User updated successfully");
    }

    @Test
    void updateUser_passwordMismatch_returnsErrorResponse() {
        User existingUser = new User();
        existingUser.id = new ObjectId(USER_ID);
        existingUser.setUsername("testuser");
        existingUser.setEmail("test@example.com");
        existingUser.setFirstname("John");
        existingUser.setLastname("Doe");
        existingUser.setPassword("hashedPassword");

        UpdateUserRequest request = UpdateUserRequest.builder()
                .id(USER_ID)
                .password("newpassword")
                .confirmPassword("differentpassword")
                .build();

        when(userRepository.findById(USER_ID)).thenReturn(Uni.createFrom().item(existingUser));

        ApiResponse<UserResponse> response = userCommandService.updateUser(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("error");
        assertThat(response.message()).contains("Passwords do not match");
    }

    @Test
    void trashUser_userNotFound_returnsErrorResponse() {
        when(userRepository.trash(MISSING_ID)).thenReturn(Uni.createFrom().nullItem());

        ApiResponse<UserResponseDeleteAt> response = userCommandService.trashed(MISSING_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("error");
        assertThat(response.message()).contains("Trashed user not found");
    }

    @Test
    void trashUser_success_trashesUser() {
        User trashedUser = new User();
        trashedUser.id = new ObjectId(USER_ID);
        trashedUser.setUsername("testuser");
        trashedUser.setEmail("test@example.com");
        trashedUser.setFirstname("John");
        trashedUser.setLastname("Doe");
        trashedUser.setCreatedAt(Instant.now());

        when(userRepository.trash(USER_ID)).thenReturn(Uni.createFrom().item(trashedUser));
        lenient().when(redisService.deleteReactive("user:" + USER_ID)).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<UserResponseDeleteAt> response = userCommandService.trashed(USER_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("User trashed successfully");
        assertThat(response.data()).isNotNull();
    }

    @Test
    void restoreUser_userNotFound_returnsErrorResponse() {
        when(userRepository.restore(MISSING_ID)).thenReturn(Uni.createFrom().nullItem());

        ApiResponse<UserResponseDeleteAt> response = userCommandService.restore(MISSING_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("error");
        assertThat(response.message()).contains("Restore user not found");
    }

    @Test
    void restoreUser_success_restoresUser() {
        User restoredUser = new User();
        restoredUser.id = new ObjectId(USER_ID);
        restoredUser.setUsername("testuser");
        restoredUser.setEmail("test@example.com");
        restoredUser.setFirstname("John");
        restoredUser.setLastname("Doe");

        when(userRepository.restore(USER_ID)).thenReturn(Uni.createFrom().item(restoredUser));
        when(redisService.deleteReactive("user:" + USER_ID)).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<UserResponseDeleteAt> response = userCommandService.restore(USER_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("User restored successfully");
        assertThat(response.data()).isNotNull();
    }

    @Test
    void deletePermanent_success_deletesUser() {
        User deletedUser = new User();
        deletedUser.id = new ObjectId(USER_ID);
        deletedUser.setUsername("testuser");

        when(userRepository.findById(USER_ID)).thenReturn(Uni.createFrom().item(deletedUser));
        when(userRepository.deletePermanent(USER_ID)).thenReturn(Uni.createFrom().item(deletedUser));
        when(redisService.deleteReactive("user:" + USER_ID)).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<Void> response = userCommandService.deletePermanent(USER_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("User deleted permanently");
    }
}
