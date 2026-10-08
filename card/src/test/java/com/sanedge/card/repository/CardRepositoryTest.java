package com.sanedge.card.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.sanedge.card.domain.requests.FindAllCards;
import com.sanedge.card.entity.Card;
import com.sanedge.card.entity.Card.CardStatus;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@RunOnVertxContext
class CardRepositoryTest {

    @Inject
    CardQueryRepository cardQueryRepo;

    @Inject
    CardCommandRepository cardCommandRepo;

    private static String missingId() {
        return new ObjectId().toHexString();
    }

    // ---------- helpers ----------
    private Uni<String> persistCard(String cardNumber, CardStatus status) {
        return persistCard(cardNumber, "VISA", "BankA", BigDecimal.valueOf(10000), BigDecimal.ZERO, status);
    }

    private Uni<String> persistCard(String cardNumber) {
        return persistCard(cardNumber, CardStatus.ACTIVE);
    }

    private Uni<String> persistCard(String cardNumber, String cardType, String cardProvider,
            BigDecimal creditLimit, BigDecimal points, CardStatus status) {
        Card card = new Card();
        card.setCardNumber(cardNumber);
        card.setCardType(cardType);
        card.setCardProvider(cardProvider);
        card.setCreditLimit(creditLimit);
        card.setPoints(points);
        card.setStatus(status);
        card.setUserId(new ObjectId());
        card.setCreatedAt(Instant.now());
        card.setUpdatedAt(Instant.now());
        return cardQueryRepo.persist(card).map(Card::getCardId);
    }

    private Uni<Void> clean() {
        return cardQueryRepo.deleteAll().replaceWithVoid();
    }

    private FindAllCards findAllReq(int page, int size, String search) {
        FindAllCards r = new FindAllCards();
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    // ==================== Basic CRUD Tests ====================

    @Test
    Uni<Void> testCreateAndFindById() {
        return clean()
                .chain(() -> persistCard("4111-1111-1111-1111"))
                .chain(id -> cardQueryRepo.findCardById(id))
                .invoke(opt -> {
                    assertThat(opt).isPresent();
                    assertThat(opt.get().getCardNumber()).isEqualTo("4111-1111-1111-1111");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByIdReturnsEmptyWhenNotFound() {
        return clean()
                .chain(() -> cardQueryRepo.findCardById(missingId()))
                .invoke(opt -> assertThat(opt).isEmpty())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByUserId() {
        ObjectId userId = new ObjectId();
        return clean()
                .chain(() -> {
                    Card c = new Card();
                    c.setCardNumber("user-card");
                    c.setUserId(userId);
                    c.setStatus(CardStatus.ACTIVE);
                    return cardQueryRepo.persist(c).map(Card::getCardId);
                })
                .chain(() -> cardQueryRepo.findCardByUserId(userId.toHexString()))
                .invoke(opt -> {
                    assertThat(opt).isPresent();
                    assertThat(opt.get().getCardNumber()).isEqualTo("user-card");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberOptional() {
        return clean()
                .chain(() -> persistCard("OPT-1234"))
                .chain(() -> cardQueryRepo.findCardByCardNumber("OPT-1234"))
                .invoke(opt -> {
                    assertThat(opt).isPresent();
                    assertThat(opt.get().getCardNumber()).isEqualTo("OPT-1234");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberNonOptional() {
        return clean()
                .chain(() -> persistCard("NON-OPT-5678"))
                .chain(() -> cardQueryRepo.findByCardNumber("NON-OPT-5678"))
                .invoke(card -> assertThat(card).isNotNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardQueryRepo.findByCardNumber("NO-CARD"))
                .invoke(card -> assertThat(card).isNull())
                .replaceWithVoid();
    }

    // ==================== Soft Delete (Trash) Tests ====================

    @Test
    Uni<Void> testTrashCard() {
        return clean()
                .chain(() -> persistCard("TRASH-1"))
                .chain(id -> cardCommandRepo.trashed(id))
                .invoke(trashed -> {
                    assertThat(trashed).isNotNull();
                    assertThat(trashed.getDeletedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashCardReturnsNullIfAlreadyTrashed() {
        return clean()
                .chain(() -> persistCard("TRASH-2"))
                .chain(id -> cardCommandRepo.trashed(id)
                        .chain(() -> cardCommandRepo.trashed(id)))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashCardReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.trashed(missingId()))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    // ==================== Restore Tests ====================

    @Test
    Uni<Void> testRestoreCard() {
        return clean()
                .chain(() -> persistCard("RESTORE-1"))
                .chain(id -> cardCommandRepo.trashed(id)
                        .chain(() -> cardCommandRepo.restore(id)))
                .invoke(restored -> {
                    assertThat(restored).isNotNull();
                    assertThat(restored.getDeletedAt()).isNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreCardReturnsNullIfNotTrashed() {
        return clean()
                .chain(() -> persistCard("RESTORE-2"))
                .chain(id -> cardCommandRepo.restore(id))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreCardReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.restore(missingId()))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    // ==================== Delete Permanent Tests ====================

    @Test
    Uni<Void> testDeletePermanent() {
        return clean()
                .chain(() -> persistCard("DEL-PERM-1"))
                .chain(id -> cardCommandRepo.trashed(id)
                        .chain(() -> cardCommandRepo.deletePermanent(id))
                        .map(deleted -> {
                            assertThat(deleted).isNotNull();
                            return id;
                        }))
                .chain(id -> cardQueryRepo.findCardById(id))
                .invoke(opt -> assertThat(opt).isEmpty())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentReturnsNullIfNotTrashed() {
        return clean()
                .chain(() -> persistCard("DEL-PERM-2"))
                .chain(id -> cardCommandRepo.deletePermanent(id))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.deletePermanent(missingId()))
                .invoke(result -> assertThat(result).isNull())
                .replaceWithVoid();
    }

    // ==================== Bulk Operations Tests ====================

    @Test
    Uni<Void> testRestoreAllDeleted() {
        return clean()
                .chain(() -> persistCard("BULK-REST-1").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persistCard("BULK-REST-2").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persistCard("ACTIVE-1"))
                .chain(() -> cardCommandRepo.restoreAllDeleted())
                .invoke(result -> assertThat(result).isTrue())
                .chain(() -> cardQueryRepo.findTrashedCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeleted() {
        return clean()
                .chain(() -> persistCard("BULK-DEL-1").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persistCard("BULK-DEL-2").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persistCard("ACTIVE-2"))
                .chain(() -> cardCommandRepo.deleteAllDeleted())
                .invoke(result -> assertThat(result).isTrue())
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persistCard("ONLY-ACTIVE"))
                .chain(() -> cardCommandRepo.restoreAllDeleted())
                .invoke(result -> assertThat(result).isFalse())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persistCard("ONLY-ACTIVE-2"))
                .chain(() -> cardCommandRepo.deleteAllDeleted())
                .invoke(result -> assertThat(result).isFalse())
                .replaceWithVoid();
    }

    // ==================== Query - Active/Trashed Tests ====================

    @Test
    Uni<Void> testFindActiveCardsExcludesTrashed() {
        return clean()
                .chain(() -> persistCard("ACT-1"))
                .chain(() -> persistCard("ACT-2").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> cardQueryRepo.findActiveCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTrashedCardsOnlyShowsTrashed() {
        return clean()
                .chain(() -> persistCard("TRASH-ONLY"))
                .chain(() -> persistCard("TRASH-2").chain(id -> cardCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> cardQueryRepo.findTrashedCards(findAllReq(1, 10, "")))
                .invoke(page -> {
                    assertThat(page.getTotalRecords()).isEqualTo(1);
                    assertThat(page.getData().get(0).getCardNumber()).isEqualTo("TRASH-2");
                })
                .replaceWithVoid();
    }

    // ==================== Query - Search Tests ====================

    @Test
    Uni<Void> testFindCardsSearchByCardNumber() {
        return clean()
                .chain(() -> persistCard("SEARCH-111", "MASTERCARD", "BRI", BigDecimal.TEN, BigDecimal.ZERO,
                        CardStatus.ACTIVE))
                .chain(() -> persistCard("SEARCH-222", "VISA", "BCA", BigDecimal.TEN, BigDecimal.ZERO,
                        CardStatus.ACTIVE))
                .chain(() -> persistCard("OTHER-333", "VISA", "BNI", BigDecimal.TEN, BigDecimal.ZERO,
                        CardStatus.ACTIVE))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "SEARCH")))
                .invoke(page -> {
                    assertThat(page.getTotalRecords()).isEqualTo(2);
                    assertThat(page.getData().stream().allMatch(c -> c.getCardNumber().contains("SEARCH"))).isTrue();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindCardsSearchByCardType() {
        return clean()
                .chain(() -> persistCard("C1", "GOLD", "X", BigDecimal.ZERO, BigDecimal.ZERO, CardStatus.ACTIVE))
                .chain(() -> persistCard("C2", "PLATINUM", "Y", BigDecimal.ZERO, BigDecimal.ZERO, CardStatus.ACTIVE))
                .chain(() -> persistCard("C3", "SILVER", "Z", BigDecimal.ZERO, BigDecimal.ZERO, CardStatus.ACTIVE))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "GOLD")))
                .invoke(page -> {
                    assertThat(page.getTotalRecords()).isEqualTo(1);
                    assertThat(page.getData().get(0).getCardType()).isEqualTo("GOLD");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindCardsSearchByCardProvider() {
        return clean()
                .chain(() -> persistCard("P1", "VISA", "MANDIRI", BigDecimal.ZERO, BigDecimal.ZERO, CardStatus.ACTIVE))
                .chain(() -> persistCard("P2", "VISA", "BCA", BigDecimal.ZERO, BigDecimal.ZERO, CardStatus.ACTIVE))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "mandiri")))
                .invoke(page -> {
                    assertThat(page.getTotalRecords()).isEqualTo(1);
                    assertThat(page.getData().get(0).getCardProvider()).isEqualTo("MANDIRI");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindCardsEmptySearchReturnsAll() {
        return clean()
                .chain(() -> persistCard("A1"))
                .chain(() -> persistCard("A2"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindCardsNullSearchReturnsAll() {
        return clean()
                .chain(() -> persistCard("NULL-1"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, null)))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    // ==================== Pagination Tests ====================

    @Test
    Uni<Void> testPagination() {
        return clean()
                .chain(() -> persistCard("PAGE-1"))
                .chain(() -> persistCard("PAGE-2"))
                .chain(() -> persistCard("PAGE-3"))
                .chain(() -> persistCard("PAGE-4"))
                .chain(() -> persistCard("PAGE-5"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 2, "")))
                .invoke(page1 -> {
                    assertThat(page1.getData()).hasSize(2);
                    assertThat(page1.getTotalRecords()).isEqualTo(5);
                })
                .chain(() -> cardQueryRepo.findCards(findAllReq(3, 2, "")))
                .invoke(page3 -> assertThat(page3.getData()).hasSize(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testPageZeroDefaultsToFirstPage() {
        return clean()
                .chain(() -> persistCard("P0"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(0, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testNegativePageDefaultsToFirstPage() {
        return clean()
                .chain(() -> persistCard("NEG"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(-1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testZeroSizeDefaultsToTen() {
        return clean()
                .chain(() -> persistCard("SZ"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 0, "")))
                .invoke(page -> assertThat(page.getData()).hasSizeLessThanOrEqualTo(10))
                .replaceWithVoid();
    }

    // ==================== Toggle Status Tests ====================

    @Test
    Uni<Void> testToggleStatusActiveToSuspended() {
        return clean()
                .chain(() -> persistCard("TOG-1", CardStatus.ACTIVE))
                .chain(id -> cardCommandRepo.toggleStatus(id))
                .invoke(card -> {
                    assertThat(card).isNotNull();
                    assertThat(card.getStatus()).isEqualTo(CardStatus.SUSPENDED);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testToggleStatusSuspendedToActive() {
        return clean()
                .chain(() -> persistCard("TOG-2", CardStatus.SUSPENDED))
                .chain(id -> cardCommandRepo.toggleStatus(id))
                .invoke(card -> {
                    assertThat(card).isNotNull();
                    assertThat(card.getStatus()).isEqualTo(CardStatus.ACTIVE);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testToggleStatusReturnsNullForOtherStatus() {
        return clean()
                .chain(() -> persistCard("TOG-3", CardStatus.ACTIVE))
                .chain(id -> cardCommandRepo.toggleStatus(id))
                .invoke(card -> assertThat(card).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testToggleStatusReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.toggleStatus(missingId()))
                .invoke(card -> assertThat(card).isNull())
                .replaceWithVoid();
    }

    // ==================== Update Credit Limit Tests ====================

    @Test
    Uni<Void> testUpdateCreditLimit() {
        return clean()
                .chain(() -> persistCard("LIMIT-1"))
                .chain(id -> cardCommandRepo.updateCreditLimit(id, new BigDecimal("50000")))
                .chain(() -> cardQueryRepo.findByCardNumber("LIMIT-1"))
                .invoke(card -> {
                    assertThat(card).isNotNull();
                    assertThat(card.getCreditLimit()).isEqualByComparingTo("50000");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateCreditLimitReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.updateCreditLimit(missingId(), BigDecimal.ONE))
                .invoke(card -> assertThat(card).isNull())
                .replaceWithVoid();
    }

    // ==================== Redeem Points Tests ====================

    @Test
    Uni<Void> testRedeemPointsSuccess() {
        return clean()
                .chain(() -> persistCard("POINTS-1", "VISA", "BANK", BigDecimal.ZERO, new BigDecimal("1000"),
                        CardStatus.ACTIVE))
                .chain(id -> cardCommandRepo.redeemPoints(id, new BigDecimal("400")))
                .chain(() -> cardQueryRepo.findByCardNumber("POINTS-1"))
                .invoke(card -> {
                    assertThat(card).isNotNull();
                    assertThat(card.getPoints()).isEqualByComparingTo("600");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRedeemPointsInsufficientBalance() {
        return clean()
                .chain(() -> persistCard("POINTS-2", "VISA", "BANK", BigDecimal.ZERO, new BigDecimal("200"),
                        CardStatus.ACTIVE))
                .chain(id -> cardCommandRepo.redeemPoints(id, new BigDecimal("500")))
                .invoke(card -> assertThat(card).isNull()) // should return null
                .chain(() -> cardQueryRepo.findByCardNumber("POINTS-2"))
                .invoke(card -> assertThat(card.getPoints()).isEqualByComparingTo("200")) // unchanged
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRedeemPointsReturnsNullIfNotFound() {
        return clean()
                .chain(() -> cardCommandRepo.redeemPoints(missingId(), BigDecimal.ONE))
                .invoke(card -> assertThat(card).isNull())
                .replaceWithVoid();
    }

    // ==================== Edge Cases ====================

    @Test
    Uni<Void> testEmptyDatabaseReturnsZeroRecords() {
        return clean()
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isZero())
                .chain(() -> cardQueryRepo.findActiveCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isZero())
                .chain(() -> cardQueryRepo.findTrashedCards(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testSearchWithNoMatchesReturnsZeroRecords() {
        return clean()
                .chain(() -> persistCard("UNIQUE-CARD"))
                .chain(() -> cardQueryRepo.findCards(findAllReq(1, 10, "NONEXISTENT")))
                .invoke(page -> assertThat(page.getTotalRecords()).isZero())
                .replaceWithVoid();
    }
}
