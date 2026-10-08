package com.sanedge.auth.entity;

import java.time.Instant;

import com.sanedge.common.entity.BaseModel;

import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@MongoEntity(collection = "reset_tokens")
public class ResetToken extends BaseModel {

    private String userId;

    private String token;

    private Instant expiration;
}
