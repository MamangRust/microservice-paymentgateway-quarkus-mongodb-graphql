package com.sanedge.auth.repository;

import com.sanedge.auth.entity.ResetToken;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ResetTokenRepository implements ReactivePanacheMongoRepository<ResetToken> {

    public Uni<ResetToken> findByToken(String token) {
        return find("token", token).firstResult();
    }

    public Uni<Long> deleteByUserId(String userId) {
        return delete("userId", userId);
    }
}
