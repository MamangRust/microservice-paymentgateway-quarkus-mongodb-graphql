package com.sanedge.auth.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sanedge.auth.entity.RefreshToken;
import com.sanedge.auth.entity.ResetToken;
import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(MongoDbResource.class)
class AuthRepositoryTest extends MongoTestBase {

    @Inject
    RefreshTokenRepository refreshTokenRepo;

    @Inject
    ResetTokenRepository resetTokenRepo;

    @BeforeEach
    void cleanDb() {
        refreshTokenRepo.deleteAll().await().indefinitely();
        resetTokenRepo.deleteAll().await().indefinitely();
    }

    private Uni<RefreshToken> createRefreshToken(String token, String userId) {
        RefreshToken rt = new RefreshToken();
        rt.setToken(token);
        rt.setUserId(userId);
        rt.setExpiration(Instant.now().plusSeconds(3600));
        return refreshTokenRepo.persist(rt).replaceWith(rt);
    }

    private Uni<ResetToken> createResetToken(String token, String userId) {
        ResetToken rt = new ResetToken();
        rt.setToken(token);
        rt.setUserId(userId);
        rt.setExpiration(Instant.now().plusSeconds(900));
        return resetTokenRepo.persist(rt).replaceWith(rt);
    }

    @Test
    void persistRefreshToken() {
        RefreshToken token = new RefreshToken();
        token.setToken("test-refresh-token");
        token.setUserId("507f1f77bcf86cd799439011");
        token.setExpiration(Instant.now().plusSeconds(3600));
        RefreshToken saved = refreshTokenRepo.persist(token).await().indefinitely();
        assertThat(saved.id).isNotNull();
    }

    @Test
    void findRefreshTokenByToken() {
        createRefreshToken("find-me-token", "507f1f77bcf86cd799439011")
                .chain(ignored -> refreshTokenRepo.findByToken("find-me-token"))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getToken()).isEqualTo("find-me-token");
                    assertThat(found.getUserId()).isEqualTo("507f1f77bcf86cd799439011");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void findRefreshTokenByUserId() {
        createRefreshToken("user-token", "507f1f77bcf86cd799439012")
                .chain(ignored -> refreshTokenRepo.findByUserId("507f1f77bcf86cd799439012"))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getUserId()).isEqualTo("507f1f77bcf86cd799439012");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void deleteRefreshTokenByUserId() {
        createRefreshToken("delete-by-user", "507f1f77bcf86cd799439013")
                .chain(ignored -> refreshTokenRepo.deleteByUserId("507f1f77bcf86cd799439013"))
                .invoke(deleted -> assertThat(deleted).isPositive())
                .chain(ignored -> refreshTokenRepo.findByUserId("507f1f77bcf86cd799439013"))
                .invoke(found -> assertThat(found).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void deleteRefreshTokenByToken() {
        createRefreshToken("delete-by-token", "507f1f77bcf86cd799439014")
                .chain(ignored -> refreshTokenRepo.deleteByToken("delete-by-token"))
                .invoke(deleted -> assertThat(deleted).isPositive())
                .chain(ignored -> refreshTokenRepo.findByToken("delete-by-token"))
                .invoke(found -> assertThat(found).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void persistResetToken() {
        ResetToken rt = new ResetToken();
        rt.setToken("reset-token-001");
        rt.setUserId("507f1f77bcf86cd799439015");
        rt.setExpiration(Instant.now().plusSeconds(900));
        ResetToken saved = resetTokenRepo.persist(rt).await().indefinitely();
        assertThat(saved.id).isNotNull();
    }

    @Test
    void findResetTokenByToken() {
        createResetToken("find-reset-token", "507f1f77bcf86cd799439016")
                .chain(ignored -> resetTokenRepo.findByToken("find-reset-token"))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getToken()).isEqualTo("find-reset-token");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void deleteResetTokenByUserId() {
        createResetToken("reset-delete-user", "507f1f77bcf86cd799439017")
                .chain(ignored -> resetTokenRepo.deleteByUserId("507f1f77bcf86cd799439017"))
                .invoke(deleted -> assertThat(deleted).isPositive())
                .chain(ignored -> resetTokenRepo.findByToken("reset-delete-user"))
                .invoke(found -> assertThat(found).isNull())
                .replaceWithVoid().await().indefinitely();
    }
}
