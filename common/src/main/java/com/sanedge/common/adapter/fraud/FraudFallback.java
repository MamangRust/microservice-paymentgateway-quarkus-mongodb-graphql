package com.sanedge.common.adapter.fraud;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.adapter.fraud.FraudPort.FraudCheck;
import com.sanedge.common.adapter.fraud.FraudPort.FraudResult;
import com.sanedge.common.adapter.fraud.FraudPort.SecurityDomain;
import com.sanedge.common.adapter.fraud.FraudPort.SecurityResult;

import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;

/**
 * Fail-open helper for ai-security calls.
 *
 * <p>Policy: if ai-security is unreachable / times out, log a warning and let
 * the transaction proceed with a benign default ({@code risk 0.1}, not
 * fraudulent / {@code ALLOW}). If the service answers, its verdict is honoured —
 * a fraudulent verdict or a {@code DENY} action must still reject the operation.
 */
public final class FraudFallback {

    private static final Logger log = LoggerFactory.getLogger(FraudFallback.class);

    /** Benign fraud verdict used when ai-security cannot be reached. */
    public static final FraudResult FRAUD_ALLOW = new FraudResult(null, 0.1, false, "Fallback: ai-security unreachable");

    /** Benign security verdict used when ai-security cannot be reached. */
    public static final SecurityResult SECURITY_ALLOW = new SecurityResult(true, 0.1,
            "Fallback: ai-security unreachable", "ALLOW");

    private FraudFallback() {
    }

    /**
     * Runs {@code call} and, on any failure (synchronous throw or async
     * failure), logs a warning and returns {@link #FRAUD_ALLOW} instead of
     * propagating the error.
     */
    public static Uni<FraudResult> detectFraud(Supplier<Uni<FraudResult>> call) {
        Uni<FraudResult> result;
        try {
            result = call.get();
        } catch (Exception e) {
            logWarning("DetectFraud", e);
            return Uni.createFrom().item(FRAUD_ALLOW);
        }
        return result.onFailure().recoverWithItem(error -> {
            logWarning("DetectFraud", error);
            return FRAUD_ALLOW;
        });
    }

    /**
     * Runs {@code call} and, on any failure (synchronous throw or async
     * failure), logs a warning and returns {@link #SECURITY_ALLOW} instead of
     * propagating the error.
     */
    public static Uni<SecurityResult> verifySecurity(Supplier<Uni<SecurityResult>> call) {
        Uni<SecurityResult> result;
        try {
            result = call.get();
        } catch (Exception e) {
            logWarning("VerifySecurity", e);
            return Uni.createFrom().item(SECURITY_ALLOW);
        }
        return result.onFailure().recoverWithItem(error -> {
            logWarning("VerifySecurity", error);
            return SECURITY_ALLOW;
        });
    }

    private static void logWarning(String operation, Throwable error) {
        if (error instanceof StatusRuntimeException) {
            log.warn("ai-security {} unavailable (gRPC status {}), failing open", operation,
                    ((StatusRuntimeException) error).getStatus(), error);
        } else {
            log.warn("ai-security {} failed ({}), failing open", operation, error.getMessage(), error);
        }
    }

    /** Convenience overload delegating to a {@link FraudPort}. */
    public static Uni<FraudResult> detectFraud(FraudPort port, FraudCheck check) {
        return detectFraud(() -> port.detectFraud(check));
    }

    /** Convenience overload delegating to a {@link FraudPort}. */
    public static Uni<SecurityResult> verifySecurity(FraudPort port, SecurityDomain domain, String entityId,
            double amount) {
        return verifySecurity(() -> port.verifySecurity(domain, entityId, amount));
    }
}
