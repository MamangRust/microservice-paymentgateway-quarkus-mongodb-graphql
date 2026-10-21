package com.sanedge.common.adapter.fraud;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sanedge.common.adapter.fraud.FraudPort.FraudCheck;

import io.smallrye.mutiny.Uni;
import pb.ai_security.AISecurityService;
import pb.ai_security.AiSecurity.FraudResponse;
import pb.ai_security.AiSecurity.SecurityResponse;

@ExtendWith(MockitoExtension.class)
class FraudAdapterTest {

    @Mock
    AISecurityService client;

    private FraudAdapter adapter() {
        FraudAdapter adapter = new FraudAdapter();
        adapter.client = client;
        return adapter;
    }

    @Test
    void detectFraud_mapsResponseToResult() {
        when(client.detectFraud(any())).thenReturn(Uni.createFrom().item(
                FraudResponse.newBuilder()
                        .setTransactionId("tx-1")
                        .setRiskScore(0.9)
                        .setIsFraudulent(true)
                        .setReason("Irregular pattern")
                        .build()));

        FraudPort.FraudResult result = adapter()
                .detectFraud(new FraudCheck("tx-1", 0, 42, 150000.0, "IDR", "CREDIT_CARD", null, null, 0L))
                .await().indefinitely();

        assertThat(result.transactionId()).isEqualTo("tx-1");
        assertThat(result.riskScore()).isEqualTo(0.9);
        assertThat(result.fraudulent()).isTrue();
        assertThat(result.reason()).isEqualTo("Irregular pattern");
    }

    @Test
    void verifySecurity_mapsResponseToResult() {
        when(client.verifySecurity(any())).thenReturn(Uni.createFrom().item(
                SecurityResponse.newBuilder()
                        .setIsSafe(false)
                        .setRiskScore(0.95)
                        .setReason("Critical risk")
                        .setAction("DENY")
                        .build()));

        FraudPort.SecurityResult result = adapter()
                .verifySecurity(FraudPort.SecurityDomain.WITHDRAW, "user-1", 75000.0)
                .await().indefinitely();

        assertThat(result.safe()).isFalse();
        assertThat(result.riskScore()).isEqualTo(0.95);
        assertThat(result.action()).isEqualTo("DENY");
        assertThat(result.reason()).isEqualTo("Critical risk");
    }
}
