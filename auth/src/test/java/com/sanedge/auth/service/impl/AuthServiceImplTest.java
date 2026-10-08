package com.sanedge.auth.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import com.sanedge.auth.domain.requests.RegisterRequest;
import com.sanedge.auth.domain.requests.LoginRequest;
import com.sanedge.auth.domain.requests.RefreshTokenRequest;
import com.sanedge.auth.domain.requests.ForgotPasswordRequest;
import com.sanedge.auth.domain.requests.ResetPasswordRequest;
import com.sanedge.auth.domain.requests.LogoutRequest;
import com.sanedge.auth.domain.requests.VerifyCodeRequest;
import com.sanedge.auth.domain.requests.GetMeRequest;
import com.sanedge.auth.entity.RefreshToken;
import com.sanedge.auth.entity.ResetToken;
import com.sanedge.auth.repository.RefreshTokenRepository;
import com.sanedge.auth.repository.ResetTokenRepository;
import com.sanedge.auth.service.AuthService;
import com.sanedge.auth.service.KafkaService;
import com.sanedge.common.adapter.model.Role;
import com.sanedge.common.adapter.model.User;
import com.sanedge.common.adapter.model.UserRole;
import com.sanedge.common.adapter.role.RolePort;
import com.sanedge.common.adapter.role.UserRolePort;
import com.sanedge.common.adapter.user.AuthUserPort;
import com.sanedge.common.adapter.user.AuthUserPort.UpdateData;
import com.sanedge.common.config.RedisService;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.common.utils.JwtUtil;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import pb.user.User.UserResponse;

@ExtendWith(MockitoExtension.class)
class AuthServiceImplTest {

        private static final String USER_ID = "507f1f77bcf86cd799439011";
        private static final String MISSING_ID = "507f1f77bcf86cd799439099";
        private static final String ROLE_ID = "507f1f77bcf86cd799439022";

        @Mock
        AuthUserPort authUserPort;

        @Mock
        RolePort rolePort;

        @Mock
        UserRolePort userRolePort;

        @Mock
        RefreshTokenRepository refreshTokenRepository;

        @Mock
        ResetTokenRepository resetTokenRepository;

        @Mock
        RedisService redisService;

        @Mock
        KafkaService kafkaService;

        @Mock
        JwtUtil jwtUtil;

        @Mock
        TracingMetrics tracingMetrics;

        @InjectMocks
        AuthService authServiceUnderTest;

        private RegisterRequest registerReq;
        private User user;

        @BeforeEach
        void setUp() throws Exception {
                // AuthService has @PostConstruct init() that builds Tracer/Meter from
                // OpenTelemetry; @InjectMocks doesn't call @PostConstruct and a Mockito
                // mock of OpenTelemetry would NPE on the builder chain, so inject the
                // no-op instance and invoke init() manually.
                java.lang.reflect.Field otField = AuthService.class.getDeclaredField("openTelemetry");
                otField.setAccessible(true);
                otField.set(authServiceUnderTest, OpenTelemetry.noop());
                Method initMethod = AuthService.class.getDeclaredMethod("init");
                initMethod.setAccessible(true);
                initMethod.invoke(authServiceUnderTest);

                registerReq = RegisterRequest.builder()
                                .firstName("John")
                                .lastName("Doe")
                                .email("john@example.com")
                                .password("SecurePass123!")
                                .build();

                user = new User(USER_ID, "John", "Doe", "john@example.com", null, null);

                // Stub both traceAndMeasure overloads so the Supplier is invoked
                // instead of Mockito returning a null Uni.
                lenient().doAnswer(invokeSupplier())
                                .when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any());
                lenient().doAnswer(invokeSupplier())
                                .when(tracingMetrics)
                                                .traceAndMeasure(anyString(), anyString(), any(Attributes.class), any());

                lenient().when(redisService.existsReactive(anyString())).thenReturn(Uni.createFrom().item(false));
                lenient().when(redisService.getReactive(anyString())).thenReturn(Uni.createFrom().nullItem());
                lenient().when(redisService.setWithExpirationReactive(anyString(), anyString(), any(Long.class)))
                                .thenReturn(Uni.createFrom().voidItem());
                lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

                lenient().when(kafkaService.sendMessage(anyString(), anyString(), any(JsonObject.class)))
                                .thenReturn(Uni.createFrom().voidItem());

                lenient().when(rolePort.findByName(anyString()))
                                .thenReturn(Uni.createFrom().item(new Role(ROLE_ID, "ROLE_USER", null, null)));
                lenient().when(userRolePort.assignRoleToUser(anyString(), anyString()))
                                .thenReturn(Uni.createFrom().item(new UserRole("ur-1", USER_ID, ROLE_ID)));

                lenient().when(jwtUtil.generateToken(anyString(), any(), anyString()))
                                .thenReturn("access-token-john");
                lenient().when(jwtUtil.generateRefreshToken(anyString(), anyString()))
                                .thenReturn("refresh-token-john");
                lenient().when(jwtUtil.validateToken(anyString())).thenReturn(true);
                lenient().when(jwtUtil.getRefreshExpirationMs()).thenReturn(3600000L);
        }

        @Test
        void registerUser_shouldSucceed() {
                when(authUserPort.findByEmail(anyString()))
                                .thenReturn(Uni.createFrom().item(Optional.empty()));
                when(authUserPort.create(any()))
                                .thenReturn(Uni.createFrom().item(user));

                UserResponse result = authServiceUnderTest.register(registerReq).await().indefinitely();

                assertThat(result).isNotNull();
                assertThat(result.getEmail()).isEqualTo("john@example.com");
                assertThat(result.getFirstname()).isEqualTo("John");
                verify(authUserPort).findByEmail(anyString());
                verify(authUserPort).create(any());
                verify(kafkaService).sendMessage(anyString(), anyString(), any(JsonObject.class));
        }

        @Test
        void registerUser_shouldFail_whenEmailAlreadyExists() {
                when(authUserPort.findByEmail(anyString()))
                                .thenReturn(Uni.createFrom().item(Optional.of(user)));

                try {
                        authServiceUnderTest.register(registerReq).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("already exists");
                }
        }

        @Test
        void login_shouldSucceed() {
                when(authUserPort.verifyPassword(anyString(), anyString()))
                                .thenReturn(Uni.createFrom().item(new AuthUserPort.VerifyResult(true, user)));

                lenient().when(refreshTokenRepository.deleteByUserId(USER_ID)).thenReturn(Uni.createFrom().item(1L));
                lenient().when(refreshTokenRepository.persist(any(RefreshToken.class)))
                                .thenAnswer(inv -> Uni.createFrom().item((RefreshToken) inv.getArgument(0)));

                String[] tokens = authServiceUnderTest.login(LoginRequest.builder()
                                .email("john@example.com")
                                .password("SecurePass123!")
                                .build()).
                                await()
                                .indefinitely();

                assertThat(tokens).hasSize(2);
                assertThat(tokens[0]).isEqualTo("access-token-john");
                assertThat(tokens[1]).isEqualTo("refresh-token-john");
        }

        @Test
        void login_shouldFail_whenAccountLocked() {
                when(redisService.existsReactive(anyString())).thenReturn(Uni.createFrom().item(true));

                try {
                        authServiceUnderTest.login(LoginRequest.builder()
                                        .email("john@example.com")
                                        .password("wrong")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("locked");
                }
        }

        @Test
        void login_shouldFail_withInvalidCredentials() {
                when(authUserPort.verifyPassword(anyString(), anyString()))
                                .thenReturn(Uni.createFrom().item(new AuthUserPort.VerifyResult(false, null)));

                try {
                        authServiceUnderTest.login(LoginRequest.builder()
                                        .email("john@example.com")
                                        .password("wrong")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("Invalid credentials");
                }
        }

        @Test
        void refresh_shouldSucceed() {
                RefreshToken storedToken = new RefreshToken();
                storedToken.setToken("old-refresh-token");
                storedToken.setUserId(USER_ID);
                storedToken.setExpiration(Instant.now().plusSeconds(3600));

                when(refreshTokenRepository.findByToken("old-refresh-token"))
                                .thenReturn(Uni.createFrom().item(storedToken));
                when(refreshTokenRepository.persist(any(RefreshToken.class)))
                                .thenAnswer(inv -> Uni.createFrom().item((RefreshToken) inv.getArgument(0)));

                when(authUserPort.findById(anyString()))
                                .thenReturn(Uni.createFrom().item(user));

                String[] tokens = authServiceUnderTest.refresh(RefreshTokenRequest.builder()
                                .refreshToken("old-refresh-token")
                                .build()).await().indefinitely();

                assertThat(tokens).hasSize(2);
                assertThat(tokens[0]).isEqualTo("access-token-john");
                verify(refreshTokenRepository).persist(any(RefreshToken.class));
        }

        @Test
        void refresh_shouldFail_whenTokenInvalid() {
                when(jwtUtil.validateToken("invalid-token")).thenReturn(false);

                try {
                        authServiceUnderTest.refresh(RefreshTokenRequest.builder()
                                        .refreshToken("invalid-token")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("Invalid or expired");
                }
        }

        @Test
        void refresh_shouldFail_whenTokenNotFound() {
                when(refreshTokenRepository.findByToken("unknown-token"))
                                .thenReturn(Uni.createFrom().nullItem());

                try {
                        authServiceUnderTest.refresh(RefreshTokenRequest.builder()
                                        .refreshToken("unknown-token")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("invalid or expired");
                }
        }

        @Test
        void forgotPassword_shouldSucceed() {
                when(authUserPort.findByEmail(anyString()))
                                .thenReturn(Uni.createFrom().item(Optional.of(user)));

                lenient().when(resetTokenRepository.deleteByUserId(USER_ID)).thenReturn(Uni.createFrom().item(1L));
                lenient().when(resetTokenRepository.persist(any(ResetToken.class)))
                                .thenAnswer(inv -> Uni.createFrom().item((ResetToken) inv.getArgument(0)));

                authServiceUnderTest.forgotPassword(ForgotPasswordRequest.builder()
                                .email("john@example.com")
                                .build()).await().indefinitely();

                verify(resetTokenRepository).persist(any(ResetToken.class));
                verify(kafkaService).sendMessage(anyString(), anyString(), any(JsonObject.class));
        }

        @Test
        void forgotPassword_shouldFail_whenUserNotFound() {
                when(authUserPort.findByEmail(anyString()))
                                .thenReturn(Uni.createFrom().item(Optional.empty()));

                try {
                        authServiceUnderTest.forgotPassword(ForgotPasswordRequest.builder()
                                        .email("unknown@example.com")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("User not found");
                }
        }

        @Test
        void resetPassword_shouldSucceed() {
                ResetToken resetToken = new ResetToken();
                resetToken.setToken("valid-reset-token");
                resetToken.setUserId(USER_ID);
                resetToken.setExpiration(Instant.now().plusSeconds(900));

                when(resetTokenRepository.findByToken("valid-reset-token"))
                                .thenReturn(Uni.createFrom().item(resetToken));

                when(authUserPort.findById(anyString()))
                                .thenReturn(Uni.createFrom().item(user));
                when(authUserPort.update(any(UpdateData.class)))
                                .thenReturn(Uni.createFrom().item(user));

                lenient().when(resetTokenRepository.delete(any(ResetToken.class)))
                                .thenReturn(Uni.createFrom().voidItem());

                ResetPasswordRequest req = ResetPasswordRequest.builder()
                                .token("valid-reset-token")
                                .password("NewPass123!")
                                .confirmPassword("NewPass123!")
                                .build();

                authServiceUnderTest.resetPassword(req).await().indefinitely();

                verify(authUserPort).update(any(UpdateData.class));
        }

        @Test
        void resetPassword_shouldFail_whenPasswordsMismatch() {
                ResetPasswordRequest req = ResetPasswordRequest.builder()
                                .token("some-token")
                                .password("Pass1!")
                                .confirmPassword("Pass2!")
                                .build();

                try {
                        authServiceUnderTest.resetPassword(req).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("do not match");
                }
        }

        @Test
        void resetPassword_shouldFail_whenTokenExpired() {
                ResetToken expiredToken = new ResetToken();
                expiredToken.setToken("expired-token");
                expiredToken.setUserId(USER_ID);
                expiredToken.setExpiration(Instant.now().minusSeconds(3600));

                when(resetTokenRepository.findByToken("expired-token"))
                                .thenReturn(Uni.createFrom().item(expiredToken));

                ResetPasswordRequest req = ResetPasswordRequest.builder()
                                .token("expired-token")
                                .password("NewPass123!")
                                .confirmPassword("NewPass123!")
                                .build();

                try {
                        authServiceUnderTest.resetPassword(req).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("expired");
                }
        }

        @Test
        void logout_shouldSucceed() {
                when(refreshTokenRepository.deleteByToken("refresh-token-to-revoke"))
                                .thenReturn(Uni.createFrom().item(1L));

                authServiceUnderTest.logout(LogoutRequest.builder()
                                .refreshToken("refresh-token-to-revoke")
                                .build()).await().indefinitely();

                verify(refreshTokenRepository).deleteByToken("refresh-token-to-revoke");
        }

        @Test
        void getMe_shouldReturnUser() {
                when(authUserPort.findById(anyString()))
                                .thenReturn(Uni.createFrom().item(user));

                UserResponse result = authServiceUnderTest.getMe(GetMeRequest.builder()
                                .userId(USER_ID)
                                .build()).await().indefinitely();

                assertThat(result).isNotNull();
                assertThat(result.getId()).isEqualTo(USER_ID);
                assertThat(result.getEmail()).isEqualTo("john@example.com");
        }

        @Test
        void getMe_shouldFail_whenUserNotFound() {
                when(authUserPort.findById(anyString()))
                                .thenReturn(Uni.createFrom().failure(new RuntimeException("User not found")));

                try {
                        authServiceUnderTest.getMe(GetMeRequest.builder()
                                        .userId(MISSING_ID)
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("User not found");
                }
        }

        @Test
        void verifyEmail_shouldSucceed() {
                when(redisService.getReactive("verification_code:ABC123"))
                                .thenReturn(Uni.createFrom().item("john@example.com"));

                authServiceUnderTest.verifyEmailByCode(VerifyCodeRequest.builder()
                                .code("ABC123")
                                .build()).await().indefinitely();

                verify(redisService).deleteReactive("verification_code:ABC123");
        }

        @Test
        void verifyEmail_shouldFail_whenInvalidCode() {
                when(redisService.getReactive("verification_code:INVALID"))
                                .thenReturn(Uni.createFrom().nullItem());

                try {
                        authServiceUnderTest.verifyEmailByCode(VerifyCodeRequest.builder()
                                        .code("INVALID")
                                        .build()).await().indefinitely();
                } catch (RuntimeException e) {
                        assertThat(e.getMessage()).contains("Invalid or expired");
                }
        }

        /**
         * Finds the Supplier argument in the invocation regardless of whether it was
         * passed positionally in the 3-arg overload (arg index 2) or 4-arg overload
         * (arg index 3), then invokes it and returns the resulting Uni. This lets
         * a single Answer<?> body serve both traceAndMeasure overloads.
         */
        private Answer<Uni<?>> invokeSupplier() {
                return invocation -> {
                        Supplier<?> supplier = null;
                        for (Object arg : invocation.getArguments()) {
                                if (arg instanceof Supplier<?>) {
                                        supplier = (Supplier<?>) arg;
                                        break;
                                }
                        }
                        return supplier != null ? (Uni<?>) supplier.get() : null;
                };
        }
}
