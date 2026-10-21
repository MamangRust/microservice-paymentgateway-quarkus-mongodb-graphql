package com.sanedge.common.adapter.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.sanedge.common.adapter.fraud.FraudPort.FraudResult;
import com.sanedge.common.adapter.fraud.FraudPort.SecurityResult;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;

class FraudFallbackTest {

    private static final FraudPort.SecurityResult SECURITY_ALLOW_PLACEHOLDER =
            new FraudPort.SecurityResult(true, 0.1, "ok", "ALLOW");

    @Test
    void detectFraud_returnsPortResultWhenServiceAnswers() {
        FraudPort port = new FraudPort() {
            @Override
            public Uni<FraudResult> detectFraud(FraudCheck check) {
                return Uni.createFrom().item(new FraudResult("tx-1", 0.9, true, "fraud"));
            }

            @Override
            public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
                return Uni.createFrom().item(SECURITY_ALLOW_PLACEHOLDER);
            }
        };

        FraudResult result = FraudFallback
                .detectFraud(port, new FraudPort.FraudCheck("tx-1", 0, 1, 100.0, "IDR", "CARD", null, null, 0L))
                .await().indefinitely();

        assertThat(result.fraudulent()).isTrue();
        assertThat(result.riskScore()).isEqualTo(0.9);
    }

    @Test
    void detectFraud_failsOpenOnAsyncFailure() {
        FraudPort port = new FraudPort() {
            @Override
            public Uni<FraudResult> detectFraud(FraudCheck check) {
                return Uni.createFrom().failure(Status.UNAVAILABLE.asRuntimeException());
            }

            @Override
            public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
                return Uni.createFrom().item(SECURITY_ALLOW_PLACEHOLDER);
            }
        };

        FraudResult result = FraudFallback
                .detectFraud(port, new FraudPort.FraudCheck("tx-1", 0, 1, 100.0, "IDR", "CARD", null, null, 0L))
                .await().indefinitely();

        assertThat(result.fraudulent()).isFalse();
        assertThat(FraudFallback.FRAUD_ALLOW).isEqualTo(result);
    }

    @Test
    void detectFraud_failsOpenOnSynchronousThrow() {
        FraudPort port = new FraudPort() {
            @Override
            public Uni<FraudResult> detectFraud(FraudCheck check) {
                throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
            }

            @Override
            public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
                return Uni.createFrom().item(SECURITY_ALLOW_PLACEHOLDER);
            }
        };

        FraudResult result = FraudFallback
                .detectFraud(port, new FraudPort.FraudCheck("tx-1", 0, 1, 100.0, "IDR", "CARD", null, null, 0L))
                .await().indefinitely();

        assertThat(result).isEqualTo(FraudFallback.FRAUD_ALLOW);
    }

    @Test
    void verifySecurity_honoursDenyWhenServiceAnswers() {
        FraudPort port = new FraudPort() {
            @Override
            public Uni<FraudResult> detectFraud(FraudCheck check) {
                return Uni.createFrom().item(FraudFallback.FRAUD_ALLOW);
            }

            @Override
            public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
                return Uni.createFrom().item(new SecurityResult(false, 0.95, "Critical risk", "DENY"));
            }
        };

        SecurityResult result = FraudFallback
                .verifySecurity(port, FraudPort.SecurityDomain.WITHDRAW, "u-1", 75000.0)
                .await().indefinitely();

        assertThat(result.action()).isEqualTo("DENY");
        assertThat(result.safe()).isFalse();
    }

    @Test
    void verifySecurity_failsOpenWhenUnreachable() {
        FraudPort port = new FraudPort() {
            @Override
            public Uni<FraudResult> detectFraud(FraudCheck check) {
                return Uni.createFrom().item(FraudFallback.FRAUD_ALLOW);
            }

            @Override
            public Uni<SecurityResult> verifySecurity(SecurityDomain domain, String entityId, double amount) {
                return Uni.createFrom().failure(new StatusRuntimeException(Status.UNAVAILABLE));
            }
        };

        SecurityResult result = FraudFallback
                .verifySecurity(port, FraudPort.SecurityDomain.TRANSFER, "u-1", 75000.0)
                .await().indefinitely();

        assertThat(result).isEqualTo(FraudFallback.SECURITY_ALLOW);
        assertThat(result.action()).isEqualTo("ALLOW");
        assertThat(result.safe()).isTrue();
    }
}
