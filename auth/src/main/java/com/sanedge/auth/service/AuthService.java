package com.sanedge.auth.service;

import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import com.sanedge.common.config.RedisService;
import com.sanedge.common.utils.JwtUtil;
import com.sanedge.common.utils.PasswordUtil;
import com.sanedge.auth.entity.RefreshToken;
import com.sanedge.auth.entity.ResetToken;
import com.sanedge.auth.domain.requests.RegisterRequest;
import com.sanedge.auth.domain.requests.LoginRequest;
import com.sanedge.auth.domain.requests.RefreshTokenRequest;
import com.sanedge.auth.domain.requests.ForgotPasswordRequest;
import com.sanedge.auth.domain.requests.ResetPasswordRequest;
import com.sanedge.auth.domain.requests.LogoutRequest;
import com.sanedge.auth.domain.requests.VerifyCodeRequest;
import com.sanedge.auth.domain.requests.GetMeRequest;
import com.sanedge.auth.repository.RefreshTokenRepository;
import com.sanedge.auth.repository.ResetTokenRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import com.sanedge.common.adapter.model.Role;
import com.sanedge.common.adapter.model.User;
import com.sanedge.common.adapter.role.RolePort;
import com.sanedge.common.adapter.role.UserRolePort;
import com.sanedge.common.adapter.user.AuthUserPort;
import com.sanedge.common.adapter.user.AuthUserPort.RegisterData;
import com.sanedge.common.adapter.user.AuthUserPort.UpdateData;
import pb.user.User.UserResponse;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

@ApplicationScoped
public class AuthService {
    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    @Inject
    AuthUserPort authUserPort;

    @Inject
    RolePort rolePort;

    @Inject
    UserRolePort userRolePort;

    @Inject
    RefreshTokenRepository refreshTokenRepository;

    @Inject
    ResetTokenRepository resetTokenRepository;

    @Inject
    RedisService redisService;

    @Inject
    KafkaService kafkaService;

    @Inject
    JwtUtil jwtUtil;

    @Inject
    PasswordUtil passwordUtil;

    @Inject
    OpenTelemetry openTelemetry;

    private Tracer tracer;
    private LongCounter requestsTotal;
    private DoubleHistogram requestDurationSeconds;

    @PostConstruct
    void init() {
        this.tracer = openTelemetry.getTracer("auth-service", "1.0.0");
        io.opentelemetry.api.metrics.Meter meter = openTelemetry.getMeter("auth-service");

        this.requestsTotal = meter.counterBuilder("requests_total")
                .setDescription("Total number of requests")
                .build();
        this.requestDurationSeconds = meter.histogramBuilder("request_duration_seconds")
                .setDescription("Request duration in seconds")
                .setUnit("s")
                .build();
    }

    public Uni<UserResponse> register(RegisterRequest req) {
        String firstName = req.getFirstName();
        String lastName = req.getLastName();
        String email = req.getEmail();
        String password = req.getPassword();

        long startTime = System.currentTimeMillis();
        Span span = tracer.spanBuilder("registerUser")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("service.name", "auth-service")
                .setAttribute("operation", "register")
                .setAttribute("user.email", email)
                .startSpan();

        log.info("Attempting to register user with email: {}", email);
        return authUserPort.findByEmail(email)
                .chain(existing -> {
                    if (existing.isPresent()) {
                        return Uni.createFrom()
                                .failure(new RuntimeException("User with this email already exists"));
                    }

                    return authUserPort.create(new RegisterData(firstName, lastName, email, password, password));
                })
                .chain(user -> {
                    String verificationCode = UUID.randomUUID().toString().substring(0, 6).toUpperCase();

                    return redisService.setWithExpirationReactive("verification:" + email, verificationCode, 900)
                            .chain(() -> redisService.setWithExpirationReactive("verification_code:" + verificationCode,
                                    email, 900))
                            .chain(() -> sendWelcomeEmail(user, verificationCode))
                            .chain(() -> rolePort.findByName("ROLE_USER")
                                    .onFailure().recoverWithItem(err -> {
                                        log.warn("Failed to find ROLE_USER, skipping role assignment: {}",
                                                err.getMessage());
                                        return (Role) null;
                                    })
                                    .chain(role -> {
                                        String roleId = role == null ? "" : role.id();
                                        if (roleId == null || roleId.isEmpty()) {
                                            return Uni.createFrom().item(user);
                                        }
                                        return userRolePort.assignRoleToUser(user.id(), roleId)
                                                .replaceWith(user);
                                    }));
                })
                .map(this::toUserResponse)
                .invoke(user -> {
                    log.info("Successfully registered user: {}", user.getEmail());
                    span.setStatus(StatusCode.OK);
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "register",
                            AttributeKey.stringKey("status"), "success"));
                })
                .onFailure().invoke(err -> {
                    log.error("Registration failed for email: {}, error: {}", email, err.getMessage());
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, err.getMessage());
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "register",
                            AttributeKey.stringKey("status"), "failed",
                            AttributeKey.stringKey("error_type"), err.getClass().getSimpleName()));
                })
                .eventually(() -> {
                    span.end();
                    double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                    requestDurationSeconds.record(duration, Attributes.of(
                            AttributeKey.stringKey("operation"), "register"));
                });
    }

    public Uni<String[]> login(LoginRequest req) {
        String email = req.getEmail();
        String password = req.getPassword();

        long startTime = System.currentTimeMillis();
        Span span = tracer.spanBuilder("loginUser")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("service.name", "auth-service")
                .setAttribute("operation", "login")
                .setAttribute("user.email", email)
                .startSpan();

        log.info("Attempting login for email: {}", email);
        String failedAttemptsKey = "failed_login:" + email;
        String lockKey = "account_locked:" + email;

        return redisService.existsReactive(lockKey)
                .chain(locked -> {
                    if (locked) {
                        return Uni.createFrom()
                                .failure(new RuntimeException("Account is locked due to too many failed attempts"));
                    }
                    return authUserPort.verifyPassword(email, password);
                })
                .chain(verifyRes -> {
                    if (!verifyRes.valid()) {
                        return handleFailedLogin(email, failedAttemptsKey, lockKey);
                    }

                    User user = verifyRes.user();

                    String accessToken = jwtUtil.generateToken(user.email(), Collections.singletonList("ROLE_USER"),
                            user.id());
                    String refreshTokenStr = jwtUtil.generateRefreshToken(user.email(), user.id());

                    RefreshToken rt = new RefreshToken();
                    rt.setUserId(user.id());
                    rt.setToken(refreshTokenStr);
                    Instant now = Instant.now();
                    rt.setExpiration(now.plusMillis(jwtUtil.getRefreshExpirationMs()));
                    rt.setCreatedAt(now);
                    rt.setUpdatedAt(now);

                    return redisService.deleteReactive(failedAttemptsKey)
                            .chain(() -> refreshTokenRepository.deleteByUserId(user.id()))
                            .chain(() -> refreshTokenRepository.persist(rt))
                            .map(v -> new String[] { accessToken, refreshTokenStr });
                })
                .invoke(tokens -> {
                    log.info("Successfully logged in user: {}", email);
                    span.setStatus(StatusCode.OK);
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "login",
                            AttributeKey.stringKey("status"), "success"));
                })
                .onFailure().invoke(err -> {
                    log.error("Login failed for email: {}, error: {}", email, err.getMessage());
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, err.getMessage());
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "login",
                            AttributeKey.stringKey("status"), "failed",
                            AttributeKey.stringKey("error_type"), err.getClass().getSimpleName()));
                })
                .eventually(() -> {
                    span.end();
                    double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                    requestDurationSeconds.record(duration, Attributes.of(
                            AttributeKey.stringKey("operation"), "login"));
                });
    }

    public Uni<String[]> refresh(RefreshTokenRequest req) {
        String refreshTokenStr = req.getRefreshToken();
        long startTime = System.currentTimeMillis();
        Span span = tracer.spanBuilder("refreshToken")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("service.name", "auth-service")
                .setAttribute("operation", "refresh")
                .startSpan();

        log.info("Attempting to refresh access token");
        if (!jwtUtil.validateToken(refreshTokenStr)) {
            span.setStatus(StatusCode.ERROR, "Invalid or expired refresh token");
            requestsTotal.add(1, Attributes.of(
                    AttributeKey.stringKey("operation"), "refresh",
                    AttributeKey.stringKey("status"), "failed",
                    AttributeKey.stringKey("error_type"), "invalid_token"));
            span.end();
            return Uni.createFrom().failure(new RuntimeException("Invalid or expired refresh token"));
        }

        return refreshTokenRepository.findByToken(refreshTokenStr)
                .chain(rt -> {
                    if (rt == null || rt.getExpiration().isBefore(Instant.now())) {
                        return Uni.createFrom().failure(new RuntimeException("Refresh token is invalid or expired"));
                    }

                    return authUserPort
                            .findById(rt.getUserId())
                            .chain(user -> {
                                String newAccessToken = jwtUtil.generateToken(user.email(),
                                        Collections.singletonList("ROLE_USER"), user.id());
                                String newRefreshTokenStr = jwtUtil.generateRefreshToken(user.email(),
                                        user.id());

                                rt.setToken(newRefreshTokenStr);
                                rt.setExpiration(Instant.now().plusMillis(jwtUtil.getRefreshExpirationMs()));
                                rt.setUpdatedAt(Instant.now());

                                return refreshTokenRepository.persist(rt)
                                        .map(v -> new String[] { newAccessToken, newRefreshTokenStr });
                            });
                })
                .invoke(tokens -> {
                    log.info("Successfully refreshed access token");
                    span.setStatus(StatusCode.OK);
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "refresh",
                            AttributeKey.stringKey("status"), "success"));
                })
                .onFailure().invoke(err -> {
                    log.error("Token refresh failed, error: {}", err.getMessage());
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, err.getMessage());
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "refresh",
                            AttributeKey.stringKey("status"), "failed",
                            AttributeKey.stringKey("error_type"), err.getClass().getSimpleName()));
                })
                .eventually(() -> {
                    span.end();
                    double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                    requestDurationSeconds.record(duration, Attributes.of(
                            AttributeKey.stringKey("operation"), "refresh"));
                });
    }

    public Uni<Void> forgotPassword(ForgotPasswordRequest req) {
        String email = req.getEmail();
        long startTime = System.currentTimeMillis();
        Span span = tracer.spanBuilder("forgotPassword")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("service.name", "auth-service")
                .setAttribute("operation", "forgot_password")
                .setAttribute("user.email", email)
                .startSpan();

        log.info("Initiated forgot password process for email: {}", email);
        return authUserPort
                .findByEmail(email)
                .chain(found -> {
                    if (found.isEmpty()) {
                        return Uni.createFrom().failure(new RuntimeException("User not found"));
                    }

                    User user = found.get();
                    String token = UUID.randomUUID().toString();

                    ResetToken resetToken = new ResetToken();
                    resetToken.setUserId(user.id());
                    resetToken.setToken(token);
                    Instant now = Instant.now();
                    resetToken.setExpiration(now.plusMillis(900000)); // 15 mins
                    resetToken.setCreatedAt(now);
                    resetToken.setUpdatedAt(now);

                    return resetTokenRepository.deleteByUserId(user.id())
                            .chain(() -> resetTokenRepository.persist(resetToken))
                            .chain(() -> sendForgotPasswordEmail(user, token));
                })
                .invoke(() -> {
                    log.info("Successfully sent forgot password token to: {}", email);
                    span.setStatus(StatusCode.OK);
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "forgot_password",
                            AttributeKey.stringKey("status"), "success"));
                })
                .onFailure().invoke(err -> {
                    log.error("ForgotPassword failed for: {}, error: {}", email, err.getMessage());
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, err.getMessage());
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "forgot_password",
                            AttributeKey.stringKey("status"), "failed",
                            AttributeKey.stringKey("error_type"), err.getClass().getSimpleName()));
                })
                .eventually(() -> {
                    span.end();
                    double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                    requestDurationSeconds.record(duration, Attributes.of(
                            AttributeKey.stringKey("operation"), "forgot_password"));
                });
    }

    public Uni<Void> resetPassword(ResetPasswordRequest req) {
        String token = req.getToken();
        String password = req.getPassword();
        String confirmPassword = req.getConfirmPassword();
        long startTime = System.currentTimeMillis();
        Span span = tracer.spanBuilder("resetPassword")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("service.name", "auth-service")
                .setAttribute("operation", "reset_password")
                .startSpan();

        log.info("Attempting reset password using reset token");
        if (!password.equals(confirmPassword)) {
            span.setStatus(StatusCode.ERROR, "Passwords do not match");
            requestsTotal.add(1, Attributes.of(
                    AttributeKey.stringKey("operation"), "reset_password",
                    AttributeKey.stringKey("status"), "failed",
                    AttributeKey.stringKey("error_type"), "password_mismatch"));
            span.end();
            return Uni.createFrom().failure(new RuntimeException("Passwords do not match"));
        }

        return resetTokenRepository.findByToken(token)
                .chain(rt -> {
                    if (rt == null || rt.getExpiration().isBefore(Instant.now())) {
                        return Uni.createFrom().failure(new RuntimeException("Invalid or expired reset token"));
                    }

                    return authUserPort
                            .findById(rt.getUserId())
                            .chain(user -> authUserPort.update(new UpdateData(user.id(), user.firstname(),
                                    user.lastname(), user.email(), password, confirmPassword)))
                            .chain(updated -> resetTokenRepository.delete(rt))
                            .replaceWithVoid();
                })
                .invoke(() -> {
                    log.info("Successfully reset password");
                    span.setStatus(StatusCode.OK);
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "reset_password",
                            AttributeKey.stringKey("status"), "success"));
                })
                .onFailure().invoke(err -> {
                    log.error("Password reset failed, error: {}", err.getMessage());
                    span.recordException(err);
                    span.setStatus(StatusCode.ERROR, err.getMessage());
                    requestsTotal.add(1, Attributes.of(
                            AttributeKey.stringKey("operation"), "reset_password",
                            AttributeKey.stringKey("status"), "failed",
                            AttributeKey.stringKey("error_type"), err.getClass().getSimpleName()));
                })
                .eventually(() -> {
                    span.end();
                    double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                    requestDurationSeconds.record(duration, Attributes.of(
                            AttributeKey.stringKey("operation"), "reset_password"));
                });
    }

    public Uni<Void> logout(LogoutRequest req) {
        String refreshTokenStr = req.getRefreshToken();
        log.info("Attempting logout");
        return refreshTokenRepository.deleteByToken(refreshTokenStr)
                .replaceWithVoid()
                .invoke(() -> log.info("Successfully logged out"))
                .onFailure().invoke(err -> log.error("Logout failed, error: {}", err.getMessage()));
    }

    public Uni<Void> verifyEmailByCode(VerifyCodeRequest req) {
        String code = req.getCode();
        log.info("Attempting email verification with code: {}", code);
        String key = "verification_code:" + code;
        return redisService.getReactive(key)
                .chain(email -> {
                    if (email == null) {
                        return Uni.createFrom().failure(new RuntimeException("Invalid or expired verification code"));
                    }
                    return redisService.deleteReactive(key)
                            .chain(() -> redisService.deleteReactive("verification:" + email))
                            .replaceWithVoid();
                })
                .invoke(() -> log.info("Successfully verified email for code: {}", code))
                .onFailure()
                .invoke(err -> log.error("Email verification failed for code: {}, error: {}", code, err.getMessage()));
    }

    public Uni<UserResponse> getMe(GetMeRequest req) {
        String userId = req.getUserId();
        log.info("Fetching profile for user ID: {}", userId);
        return authUserPort.findById(userId)
                .map(this::toUserResponse)
                .invoke(user -> log.info("Successfully retrieved profile for user: {}", user.getEmail()))
                .onFailure().invoke(err -> log.error("Failed to retrieve profile for user ID: {}, error: {}", userId,
                        err.getMessage()));
    }

    private Uni<String[]> handleFailedLogin(String email, String failedAttemptsKey, String lockKey) {
        log.warn("Failed login attempt for email: {}", email);
        return redisService.getReactive(failedAttemptsKey)
                .chain(attemptsStr -> {
                    int currentAttempts = attemptsStr == null ? 0 : Integer.parseInt(attemptsStr);
                    int newAttempts = currentAttempts + 1;
                    if (newAttempts >= 5) {
                        log.error("Account locked due to 5 consecutive failed login attempts: {}", email);
                        return redisService.setWithExpirationReactive(lockKey, "true", 3600) // lock 1 hr
                                .chain(() -> redisService.deleteReactive(failedAttemptsKey))
                                .chain(() -> Uni.createFrom().failure(
                                        new RuntimeException("Account is locked due to too many failed attempts")));
                    } else {
                        log.warn("Incrementing failed login attempts for email: {}. Current failed attempts: {}", email,
                                newAttempts);
                        return redisService
                                .setWithExpirationReactive(failedAttemptsKey, String.valueOf(newAttempts), 600) // 10
                                                                                                                // mins
                                .chain(() -> Uni.createFrom().failure(
                                        new RuntimeException("Invalid credentials. Attempt " + newAttempts + " of 5")));
                    }
                });
    }

    private Uni<Void> sendWelcomeEmail(User user, String code) {
        String subject = "Welcome to Quarkus Modular Monolith";
        String body = String.format(
                "Hello %s %s,\n\nWelcome to our platform! Use the following code to verify your email address:\n\n%s\n\nRegards,\nSupport Team",
                user.firstname(), user.lastname(), code
        );

        JsonObject payload = new JsonObject()
                .put("email", user.email())
                .put("subject", subject)
                .put("body", body);

        return kafkaService.sendMessage("email-service-topic-auth-register", user.email(), payload);
    }

    private Uni<Void> sendForgotPasswordEmail(User user, String token) {
        String subject = "Reset Password Verification";
        String body = String.format(
                "Hello %s %s,\n\nYou have requested a password reset. Use the following token to reset your password:\n\n%s\n\nThis token will expire in 15 minutes.\n\nRegards,\nSupport Team",
                user.firstname(), user.lastname(), token
        );

        JsonObject payload = new JsonObject()
                .put("email", user.email())
                .put("subject", subject)
                .put("body", body);

        return kafkaService.sendMessage("email-service-topic-auth-forgot-password", user.email(), payload);
    }

    private UserResponse toUserResponse(User user) {
        if (user == null) {
            return null;
        }
        return UserResponse.newBuilder()
                .setId(user.id() == null ? "" : user.id())
                .setFirstname(user.firstname() == null ? "" : user.firstname())
                .setLastname(user.lastname() == null ? "" : user.lastname())
                .setEmail(user.email() == null ? "" : user.email())
                .setCreatedAt(user.createdAt() == null ? "" : user.createdAt().toString())
                .setUpdatedAt(user.updatedAt() == null ? "" : user.updatedAt().toString())
                .build();
    }
}
