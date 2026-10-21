package com.sanedge.common.adapter.fraud;

import java.util.Map;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import pb.ai_security.AISecurityService;
import pb.ai_security.AiSecurity.FraudRequest;
import pb.ai_security.AiSecurity.SecurityRequest;

/**
 * CDI adapter over the ai-security gRPC client, mirroring the other
 * {@code *Adapter} beans (e.g. {@code SaldoAdapter}). The {@code ai-security}
 * client is wired through {@code quarkus.grpc.clients.ai-security.*}. Failure
 * handling lives in {@link FraudFallback} so the fail-open contract is uniform
 * across every consumer service.
 */
@ApplicationScoped
public class FraudAdapter implements FraudPort {

    @GrpcClient("ai-security")
    AISecurityService client;

    @Override
    public Uni<FraudResult> detectFraud(FraudCheck check) {
        FraudRequest request = FraudRequest.newBuilder()
                .setTransactionId(nullToEmpty(check.transactionId()))
                .setMerchantId(check.merchantId())
                .setUserId(check.userId())
                .setAmount(check.amount())
                .setCurrency(nullToEmpty(check.currency()))
                .setPaymentMethod(nullToEmpty(check.paymentMethod()))
                .setDeviceId(nullToEmpty(check.deviceId()))
                .setIpAddress(nullToEmpty(check.ipAddress()))
                .setTimestamp(check.timestamp())
                .build();

        return client.detectFraud(request).map(response -> new FraudResult(
                response.getTransactionId(),
                response.getRiskScore(),
                response.getIsFraudulent(),
                response.getReason()));
    }

    private static String nullToEmpty(String value) {
        return value != null ? value : "";
    }

    @Override
    public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
        SecurityRequest request = SecurityRequest.newBuilder()
                .setDomain(pb.ai_security.AiSecurity.SecurityDomain.valueOf(domain.name()))
                .setEntityId(nullToEmpty(entityId))
                .putAllMetadata(Map.of())
                .setAmount(amount)
                .build();

        return client.verifySecurity(request).map(response -> new SecurityResult(
                response.getIsSafe(),
                response.getRiskScore(),
                response.getReason(),
                response.getAction()));
    }
}
