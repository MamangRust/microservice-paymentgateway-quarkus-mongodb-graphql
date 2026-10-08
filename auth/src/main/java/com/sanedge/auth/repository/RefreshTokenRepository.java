package com.sanedge.auth.repository;

import com.sanedge.auth.entity.RefreshToken;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RefreshTokenRepository implements ReactivePanacheMongoRepository<RefreshToken> {

    public Uni<RefreshToken> findByToken(String token) {
        return find("token", token).firstResult();
    }

    public Uni<RefreshToken> findByUserId(String userId) {
        return find("userId", userId).firstResult();
    }

    public Uni<Long> deleteByUserId(String userId) {
        return delete("userId", userId);
    }

    public Uni<Long> deleteByToken(String token) {
        return delete("token", token);
    }
}
