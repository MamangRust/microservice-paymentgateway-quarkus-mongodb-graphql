package com.sanedge.card.domain.response;

import java.math.BigDecimal;

import com.sanedge.card.entity.CardReward;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@RegisterForReflection
public class CardRewardResponse {
    private String rewardId;
    private String cardNumber;
    private String authTxnId;
    private BigDecimal amount;
    private String mcc;
    private BigDecimal pointsEarned;
    private String expiresAt;
    private Boolean redeemed;
    private String createdAt;
    private String updatedAt;

    public static CardRewardResponse from(CardReward reward) {
        if (reward == null) {
            return null;
        }
        return CardRewardResponse.builder()
                .rewardId(reward.getRewardId())
                .cardNumber(reward.cardNumber)
                .authTxnId(reward.authTxnId != null ? reward.authTxnId.toHexString() : null)
                .amount(reward.amount)
                .mcc(reward.mcc)
                .pointsEarned(reward.pointsEarned)
                .expiresAt(reward.expiresAt != null ? reward.expiresAt.toString() : null)
                .redeemed(reward.redeemed)
                .createdAt(reward.getCreatedAt() != null ? reward.getCreatedAt().toString() : null)
                .updatedAt(reward.getUpdatedAt() != null ? reward.getUpdatedAt().toString() : null)
                .build();
    }
}
