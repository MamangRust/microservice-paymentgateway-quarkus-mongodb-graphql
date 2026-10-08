package com.sanedge.topup.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.sanedge.common.enums.Status;
import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;
import com.sanedge.topup.domain.requests.FindAllTopups;
import com.sanedge.topup.domain.requests.FindAllTopupsByCardNumber;
import com.sanedge.topup.entity.Topup;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(MongoDbResource.class)
class TopupRepositoryTest extends MongoTestBase {

    private static final String MISSING_ID = "000000000000000000000000";

    @Inject
    TopupQueryRepository topupQueryRepo;

    @Inject
    TopupCommandRepository topupCommandRepo;

    private Uni<String> persist(String cardNumber, Long amount, String method, Status status, Instant topupTime) {
        Topup topup = new Topup();
        topup.topupNo = UUID.randomUUID();
        topup.cardNumber = cardNumber;
        topup.topupAmount = amount != null ? amount.intValue() : 0;
        topup.topupMethod = method;
        topup.status = status;
        if (topupTime != null) {
            topup.topupTime = topupTime;
        }
        topup.setCreatedAt(Instant.now());
        topup.setUpdatedAt(Instant.now());
        return topupQueryRepo.persist(topup).map(Topup::getTopupId);
    }

    private Uni<String> persist(String cardNumber, Long amount, String method) {
        return persist(cardNumber, amount, method, Status.PENDING, null);
    }

    private Uni<String> persist(String cardNumber, Long amount) {
        return persist(cardNumber, amount, "CREDIT_CARD");
    }

    private Uni<Void> clean() {
        return topupQueryRepo.deleteAll().replaceWithVoid();
    }

    private FindAllTopups findAllReq(int page, int size, String search) {
        FindAllTopups r = new FindAllTopups();
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    private FindAllTopupsByCardNumber findCardNumberReq(String cardNumber, int page, int size, String search) {
        FindAllTopupsByCardNumber r = new FindAllTopupsByCardNumber();
        r.setCardNumber(cardNumber);
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    // ==================== Create & Find By ID ====================

    @Test
    Uni<Void> testCreateAndFindById() {
        return clean()
                .chain(() -> persist("4111111111111111", 150000L))
                .chain(id -> topupQueryRepo.findTopupById(id))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.cardNumber).isEqualTo("4111111111111111");
                    assertThat((long) found.topupAmount).isEqualTo(150000L);
                    assertThat(found.topupMethod).isEqualTo("CREDIT_CARD");
                    assertThat(found.status).isEqualTo(Status.PENDING);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByIdReturnsNullWhenNotFound() {
        return clean()
                .chain(() -> topupQueryRepo.findTopupById(MISSING_ID))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid();
    }

    // ==================== Soft Delete (Trash) Tests ====================

    @Test
    Uni<Void> testTrashTopup() {
        return clean()
                .chain(() -> persist("4111111111111111", 200000L))
                .chain(id -> topupCommandRepo.trashed(id))
                .invoke(t -> {
                    assertThat(t).isNotNull();
                    assertThat(t.getDeletedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashTopupReturnsNullIfAlreadyTrashed() {
        return clean()
                .chain(() -> persist("4111111111111122", 200000L))
                .chain(id -> topupCommandRepo.trashed(id)
                        .chain(() -> topupCommandRepo.trashed(id)))
                .invoke(t -> assertThat(t).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashTopupReturnsNullIfNotFound() {
        return clean()
                .chain(() -> topupCommandRepo.trashed(MISSING_ID))
                .invoke(t -> assertThat(t).isNull())
                .replaceWithVoid();
    }

    // ==================== Restore Tests ====================

    @Test
    Uni<Void> testRestoreTopup() {
        return clean()
                .chain(() -> persist("4222222222222222", 300000L))
                .chain(id -> topupCommandRepo.trashed(id)
                        .chain(() -> topupCommandRepo.restore(id)))
                .invoke(r -> {
                    assertThat(r).isNotNull();
                    assertThat(r.getDeletedAt()).isNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreTopupReturnsNullIfNotTrashed() {
        return clean()
                .chain(() -> persist("4222222222222233", 300000L))
                .chain(id -> topupCommandRepo.restore(id))
                .invoke(r -> assertThat(r).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreTopupReturnsNullIfNotFound() {
        return clean()
                .chain(() -> topupCommandRepo.restore(MISSING_ID))
                .invoke(r -> assertThat(r).isNull())
                .replaceWithVoid();
    }

    // ==================== Delete Permanent Tests ====================

    @Test
    Uni<Void> testDeletePermanent() {
        return clean()
                .chain(() -> persist("4333333333333333", 400000L))
                .chain(id -> topupCommandRepo.trashed(id)
                        .chain(() -> topupCommandRepo.deletePermanent(id))
                        .chain(deleted -> topupQueryRepo.findTopupById(id)))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentFailsIfNotTrashed() {
        return clean()
                .chain(() -> persist("4333333333333344", 400000L))
                .chain(id -> topupCommandRepo.deletePermanent(id))
                .invoke(d -> assertThat(d).isFalse())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentReturnsFalseIfNotFound() {
        return clean()
                .chain(() -> topupCommandRepo.deletePermanent(MISSING_ID))
                .invoke(d -> assertThat(d).isFalse())
                .replaceWithVoid();
    }

    // ==================== Bulk Operations Tests ====================

    @Test
    Uni<Void> testRestoreAllDeleted() {
        return clean()
                .chain(() -> persist("4444444444444444", 500000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("4444444444444455", 600000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("4444444444444466", 700000L))
                .chain(() -> topupCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> topupQueryRepo.findTrashedTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeleted() {
        return clean()
                .chain(() -> persist("5555555555555555", 800000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("5555555555555566", 900000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("5555555555555577", 1000000L))
                .chain(() -> topupCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persist("5555555555555588", 100000L))
                .chain(() -> topupCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isFalse())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persist("5555555555555599", 100000L))
                .chain(() -> topupCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isFalse())
                .replaceWithVoid();
    }

    // ==================== Update Amount Tests ====================

    @Test
    Uni<Void> testUpdateTopupAmount() {
        return clean()
                .chain(() -> persist("6666666666666666", 100000L, "CREDIT_CARD", Status.PENDING, null))
                .chain(id -> topupCommandRepo.updateTopupAmount(id, 250000L))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat((long) updated.topupAmount).isEqualTo(250000L);
                    assertThat(updated.getUpdatedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateTopupAmountReturnsNullIfNotFound() {
        return clean()
                .chain(() -> topupCommandRepo.updateTopupAmount(MISSING_ID, 250000L))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    // ==================== Update Status Tests ====================

    @Test
    Uni<Void> testUpdateStatus() {
        return clean()
                .chain(() -> persist("6666666666666677", 100000L, "CREDIT_CARD", Status.PENDING, null))
                .chain(id -> topupCommandRepo.updateTopupStatus(id, "SUCCESS"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.status).isEqualTo(Status.SUCCESS);
                    assertThat(updated.getUpdatedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusToFailed() {
        return clean()
                .chain(() -> persist("6666666666666688", 100000L, "CREDIT_CARD", Status.PENDING, null))
                .chain(id -> topupCommandRepo.updateTopupStatus(id, "FAILED"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.status).isEqualTo(Status.FAILED);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusReturnsNullIfNotFound() {
        return clean()
                .chain(() -> topupCommandRepo.updateTopupStatus(MISSING_ID, "SUCCESS"))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusInvalidStatusDoesNotChange() {
        return clean()
                .chain(() -> persist("6666666666666699", 100000L, "CREDIT_CARD", Status.PENDING, null))
                .chain(id -> topupCommandRepo.updateTopupStatus(id, "INVALID_STATUS"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.status).isEqualTo(Status.PENDING);
                })
                .replaceWithVoid();
    }

    // ==================== Query - Active/Trashed Tests ====================

    @Test
    Uni<Void> testFindActiveTopups() {
        return clean()
                .chain(() -> persist("7777777777777777", 100000L))
                .chain(() -> persist("7777777777777788", 200000L))
                .chain(() -> topupQueryRepo.findActiveTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTrashedTopups() {
        return clean()
                .chain(() -> persist("8888888888888888", 100000L))
                .chain(() -> persist("8888888888888899", 200000L))
                .chain(() -> topupQueryRepo.findTrashedTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindActiveTopupsExcludesTrashed() {
        return clean()
                .chain(() -> persist("8888888888888800", 100000L))
                .chain(() -> persist("8888888888888811", 200000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> topupQueryRepo.findActiveTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTrashedTopupsOnlyShowsTrashed() {
        return clean()
                .chain(() -> persist("8888888888888822", 100000L))
                .chain(() -> persist("8888888888888833", 200000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> topupQueryRepo.findTrashedTopups(findAllReq(1, 10, "")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).cardNumber).isEqualTo("8888888888888833");
                })
                .replaceWithVoid();
    }

    // ==================== Query - Search Tests ====================

    @Test
    Uni<Void> testFindTopupsWithSearchByCardNumber() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L))
                .chain(() -> persist("9876543210987654", 200000L))
                .chain(() -> persist("1111222233334444", 300000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "1234")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).cardNumber).isEqualTo("1234567890123456");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsWithSearchByTopupMethod() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L, "E_WALLET"))
                .chain(() -> persist("9876543210987654", 200000L, "CREDIT_CARD"))
                .chain(() -> persist("1111222233334444", 300000L, "DEBIT_CARD"))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "E_WALLET")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).topupMethod).isEqualTo("E_WALLET");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsWithSearchByStatus() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L, "CREDIT_CARD", Status.SUCCESS, null))
                .chain(() -> persist("9876543210987654", 200000L, "CREDIT_CARD", Status.FAILED, null))
                .chain(() -> persist("1111222233334444", 300000L, "CREDIT_CARD", Status.PENDING, null))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "success")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).status).isEqualTo(Status.SUCCESS);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsWithEmptySearchReturnsAll() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L))
                .chain(() -> persist("9876543210987654", 200000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsWithNullSearchReturnsAll() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L))
                .chain(() -> persist("9876543210987654", 200000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, null)))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    // ==================== Query - Pagination Tests ====================

    @Test
    Uni<Void> testFindTopupsWithPagination() {
        return clean()
                .chain(() -> persist("11110001", 100000L))
                .chain(() -> persist("11110002", 200000L))
                .chain(() -> persist("11110003", 300000L))
                .chain(() -> persist("11110004", 400000L))
                .chain(() -> persist("11110005", 500000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 2, "")))
                .map(page1 -> {
                    assertThat(page1.getData()).hasSize(2);
                    assertThat(page1.getTotalRecords()).isEqualTo(5);
                    return page1;
                })
                .chain(page1 -> topupQueryRepo.findTopups(findAllReq(2, 2, "")))
                .invoke(page2 -> assertThat(page2.getData()).hasSize(2))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(3, 2, "")))
                .invoke(page3 -> assertThat(page3.getData()).hasSize(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsPageZeroDefaultsToFirstPage() {
        return clean()
                .chain(() -> persist("11110006", 100000L))
                .chain(() -> persist("11110007", 200000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(0, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsNegativePageDefaultsToFirstPage() {
        return clean()
                .chain(() -> persist("11110008", 100000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(-1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupsZeroSizeDefaultsToTen() {
        return clean()
                .chain(() -> persist("11110009", 100000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 0, "")))
                .invoke(r -> assertThat(r.getData()).hasSizeLessThanOrEqualTo(10))
                .replaceWithVoid();
    }

    // ==================== Query - Sort Order Tests ====================

    @Test
    Uni<Void> testFindTopupsSortedByTopupTimeDescending() {
        Instant time1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant time2 = Instant.parse("2024-01-02T10:00:00Z");
        Instant time3 = Instant.parse("2024-01-03T10:00:00Z");

        return clean()
                .chain(() -> persist("11110010", 100000L, "CREDIT_CARD", Status.PENDING, time1))
                .chain(() -> persist("11110011", 200000L, "CREDIT_CARD", Status.PENDING, time2))
                .chain(() -> persist("11110012", 300000L, "CREDIT_CARD", Status.PENDING, time3))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "")))
                .invoke(r -> {
                    assertThat(r.getData()).hasSize(3);
                    assertThat(r.getData().get(0).topupTime).isEqualTo(time3);
                    assertThat(r.getData().get(1).topupTime).isEqualTo(time2);
                    assertThat(r.getData().get(2).topupTime).isEqualTo(time1);
                })
                .replaceWithVoid();
    }

    // ==================== Query - By Card Number (List) Tests ====================

    @Test
    Uni<Void> testFindByCardNumber() {
        return clean()
                .chain(() -> persist("1234123412341234", 100000L))
                .chain(() -> persist("5678567856785678", 200000L))
                .chain(() -> persist("1234123412341234", 300000L))
                .chain(() -> topupQueryRepo.findByCardNumber("1234123412341234"))
                .invoke(list -> {
                    assertThat(list).hasSize(2);
                    assertThat(list.stream().allMatch(t -> t.cardNumber.equals("1234123412341234")))
                            .isTrue();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberExcludesTrashed() {
        return clean()
                .chain(() -> persist("1234123412341234", 100000L))
                .chain(() -> persist("1234123412341234", 200000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> topupQueryRepo.findByCardNumber("1234123412341234"))
                .invoke(list -> assertThat(list).hasSize(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberSortedByTopupTimeDescending() {
        Instant time1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant time2 = Instant.parse("2024-01-02T10:00:00Z");

        return clean()
                .chain(() -> persist("1234123412341235", 100000L, "CREDIT_CARD", Status.PENDING, time1))
                .chain(() -> persist("1234123412341235", 200000L, "CREDIT_CARD", Status.PENDING, time2))
                .chain(() -> topupQueryRepo.findByCardNumber("1234123412341235"))
                .invoke(list -> {
                    assertThat(list).hasSize(2);
                    assertThat(list.get(0).topupTime).isEqualTo(time2);
                    assertThat(list.get(1).topupTime).isEqualTo(time1);
                })
                .replaceWithVoid();
    }

    // ==================== Query - By Card Number (Paged) Tests ====================

    @Test
    Uni<Void> testFindTopupByCard() {
        return clean()
                .chain(() -> persist("1234123412341236", 100000L))
                .chain(() -> persist("5678567856785678", 200000L))
                .chain(() -> persist("1234123412341236", 300000L))
                .chain(() -> topupQueryRepo.findTopupByCard(findCardNumberReq("1234123412341236", 1, 10, "")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(2);
                    assertThat(r.getData().stream().allMatch(t -> t.cardNumber.equals("1234123412341236")))
                            .isTrue();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupByCardExcludesTrashed() {
        return clean()
                .chain(() -> persist("1234123412341237", 100000L))
                .chain(() -> persist("1234123412341237", 200000L)
                        .chain(id -> topupCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> topupQueryRepo.findTopupByCard(findCardNumberReq("1234123412341237", 1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupByCardWithSearchByMethod() {
        return clean()
                .chain(() -> persist("1234123412341238", 100000L, "CREDIT_CARD"))
                .chain(() -> persist("1234123412341238", 200000L, "E_WALLET"))
                .chain(() -> persist("1234123412341238", 300000L, "DEBIT_CARD"))
                .chain(() -> topupQueryRepo.findTopupByCard(findCardNumberReq("1234123412341238", 1, 10, "E_WALLET")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).topupMethod).isEqualTo("E_WALLET");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTopupByCardWithPagination() {
        return clean()
                .chain(() -> persist("1234123412341239", 100000L))
                .chain(() -> persist("1234123412341239", 200000L))
                .chain(() -> persist("1234123412341239", 300000L))
                .chain(() -> topupQueryRepo.findTopupByCard(findCardNumberReq("1234123412341239", 1, 2, "")))
                .invoke(r -> {
                    assertThat(r.getData()).hasSize(2);
                    assertThat(r.getTotalRecords()).isEqualTo(3);
                })
                .chain(() -> topupQueryRepo.findTopupByCard(findCardNumberReq("1234123412341239", 2, 2, "")))
                .invoke(r -> assertThat(r.getData()).hasSize(1))
                .replaceWithVoid();
    }

    // ==================== Edge Cases ====================

    @Test
    Uni<Void> testEmptyDatabaseReturnsZeroRecords() {
        return clean()
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .chain(() -> topupQueryRepo.findActiveTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .chain(() -> topupQueryRepo.findTrashedTopups(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testSearchWithNoMatchesReturnsZeroRecords() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "NONEXISTENT_SEARCH")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusOnTrashedTopupReturnsNull() {
        return clean()
                .chain(() -> persist("9999888877776666", 100000L))
                .chain(id -> topupCommandRepo.trashed(id)
                        .chain(() -> topupCommandRepo.updateTopupStatus(id, "SUCCESS")))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateAmountOnTrashedTopupReturnsNull() {
        return clean()
                .chain(() -> persist("9999888877776655", 100000L))
                .chain(id -> topupCommandRepo.trashed(id)
                        .chain(() -> topupCommandRepo.updateTopupAmount(id, 999999L)))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testCaseInsensitiveStatusSearch() {
        return clean()
                .chain(() -> persist("1111222233445566", 100000L, "CREDIT_CARD", Status.SUCCESS, null))
                .chain(() -> persist("1111222233445577", 200000L, "CREDIT_CARD", Status.FAILED, null))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "SUCCESS")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "success")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "SuCceSs")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testCaseInsensitiveTopupMethodSearch() {
        return clean()
                .chain(() -> persist("1111222233445588", 100000L, "CREDIT_CARD"))
                .chain(() -> persist("1111222233445599", 200000L, "E_WALLET"))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "credit_card")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "CREDIT_CARD")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> topupQueryRepo.findTopups(findAllReq(1, 10, "CrEdIt_CaRd")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByCardNumberReturnsEmptyWhenNoMatch() {
        return clean()
                .chain(() -> persist("1234567890123456", 100000L))
                .chain(() -> topupQueryRepo.findByCardNumber("9999999999999999"))
                .invoke(list -> assertThat(list).isEmpty())
                .replaceWithVoid();
    }

}
