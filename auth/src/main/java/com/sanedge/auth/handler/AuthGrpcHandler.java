package com.sanedge.auth.handler;

import com.sanedge.auth.service.AuthService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import pb.Auth.ApiResponseVerifyCode;
import pb.Auth.VerifyCodeRequest;
import pb.Auth.ApiResponseForgotPassword;
import pb.Auth.ForgotPasswordRequest;
import pb.Auth.ApiResponseResetPassword;
import pb.Auth.ResetPasswordRequest;
import pb.Auth.ApiResponseRegister;
import pb.Auth.RegisterRequest;
import pb.Auth.ApiResponseLogin;
import pb.Auth.LoginRequest;
import pb.Auth.TokenResponse;
import pb.Auth.ApiResponseRefreshToken;
import pb.Auth.RefreshTokenRequest;
import pb.Auth.ApiResponseGetMe;
import pb.Auth.GetMeRequest;

@GrpcService
public class AuthGrpcHandler extends pb.MutinyAuthServiceGrpc.AuthServiceImplBase {

    @Inject
    AuthService authService;

    @Override
    public Uni<ApiResponseVerifyCode> verifyCode(VerifyCodeRequest request) {
        return authService.verifyEmailByCode(com.sanedge.auth.domain.requests.VerifyCodeRequest.builder()
                        .code(request.getCode())
                        .build())
                .map(v -> ApiResponseVerifyCode.newBuilder()
                        .setStatus("success")
                        .setMessage("Email verified successfully")
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseVerifyCode.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseForgotPassword> forgotPassword(ForgotPasswordRequest request) {
        return authService.forgotPassword(com.sanedge.auth.domain.requests.ForgotPasswordRequest.builder()
                        .email(request.getEmail())
                        .build())
                .map(v -> ApiResponseForgotPassword.newBuilder()
                        .setStatus("success")
                        .setMessage("Password reset email sent successfully")
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseForgotPassword.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseResetPassword> resetPassword(ResetPasswordRequest request) {
        return authService.resetPassword(com.sanedge.auth.domain.requests.ResetPasswordRequest.builder()
                        .token(request.getResetToken())
                        .password(request.getPassword())
                        .confirmPassword(request.getConfirmPassword())
                        .build())
                .map(v -> ApiResponseResetPassword.newBuilder()
                        .setStatus("success")
                        .setMessage("Password reset successfully")
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseResetPassword.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseRegister> registerUser(RegisterRequest request) {
        return authService
                .register(com.sanedge.auth.domain.requests.RegisterRequest.builder()
                        .firstName(request.getFirstname())
                        .lastName(request.getLastname())
                        .email(request.getEmail())
                        .password(request.getPassword())
                        .build())
                .map(user -> ApiResponseRegister.newBuilder()
                        .setStatus("success")
                        .setMessage("User registered successfully. Verification email sent.")
                        .setData(user)
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseRegister.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseLogin> loginUser(LoginRequest request) {
        return authService.login(com.sanedge.auth.domain.requests.LoginRequest.builder()
                        .email(request.getEmail())
                        .password(request.getPassword())
                        .build())
                .map(tokens -> ApiResponseLogin.newBuilder()
                        .setStatus("success")
                        .setMessage("Logged in successfully")
                        .setData(TokenResponse.newBuilder()
                                .setAccessToken(tokens[0])
                                .setRefreshToken(tokens[1])
                                .build())
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseLogin.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseRefreshToken> refreshToken(RefreshTokenRequest request) {
        return authService.refresh(com.sanedge.auth.domain.requests.RefreshTokenRequest.builder()
                        .refreshToken(request.getRefreshToken())
                        .build())
                .map(tokens -> ApiResponseRefreshToken.newBuilder()
                        .setStatus("success")
                        .setMessage("Token refreshed successfully")
                        .setData(TokenResponse.newBuilder()
                                .setAccessToken(tokens[0])
                                .setRefreshToken(tokens[1])
                                .build())
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseRefreshToken.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }

    @Override
    public Uni<ApiResponseGetMe> getMe(GetMeRequest request) {
        return authService.getMe(com.sanedge.auth.domain.requests.GetMeRequest.builder()
                        .userId(request.getUserId())
                        .build())
                .map(user -> ApiResponseGetMe.newBuilder()
                        .setStatus("success")
                        .setMessage("Profile retrieved successfully")
                        .setData(user)
                        .build())
                .onFailure().recoverWithItem(err -> ApiResponseGetMe.newBuilder()
                        .setStatus("failed")
                        .setMessage(err.getMessage())
                        .build());
    }
}
