package com.sanedge.transfer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.sanedge.common.enums.Status;
import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;
import com.sanedge.transfer.domain.requests.FindAllTransfers;
import com.sanedge.transfer.entity.Transfer;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(MongoDbResource.class)
@RunOnVertxContext
class TransferRepositoryTest extends MongoTestBase {

    @Inject
    TransferQueryRepository transferQueryRepo;

    @Inject
    TransferCommandRepository transferCommandRepo;

    private static final String MISSING_ID = "000000000000000000000000";

    private Uni<String> persist(String from, String to, Long amount, Status status, Instant transferTime) {
        Transfer transfer = new Transfer();
        transfer.setTransferNo(java.util.UUID.randomUUID());
        transfer.setTransferFrom(from);
        transfer.setTransferTo(to);
        transfer.setTransferAmount(amount != null ? amount.intValue() : 0);
        transfer.setStatus(status);
        Instant now = Instant.now();
        transfer.setCreatedAt(now);
        transfer.setUpdatedAt(now);
        transfer.setTransferTime(transferTime != null ? transferTime : now);
        return transferQueryRepo.persist(transfer).map(Transfer::getTransferId);
    }

    private Uni<String> persist(String from, String to, Long amount) {
        return persist(from, to, amount, Status.PENDING, null);
    }

    private Uni<Void> clean() {
        return transferQueryRepo.deleteAll().replaceWithVoid();
    }

    private FindAllTransfers findAllReq(int page, int size, String search) {
        FindAllTransfers r = new FindAllTransfers();
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    @Test
    Uni<Void> testCreateAndFindById() {
        return clean()
                .chain(() -> persist("111122223333", "444455556666", 150000L))
                .chain(id -> transferQueryRepo.findTransferById(id))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getTransferFrom()).isEqualTo("111122223333");
                    assertThat(found.getTransferTo()).isEqualTo("444455556666");
                    assertThat((long) found.getTransferAmount()).isEqualTo(150000L);
                    assertThat(found.getStatus()).isEqualTo(Status.PENDING);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindByIdReturnsNullWhenNotFound() {
        return clean()
                .chain(() -> transferQueryRepo.findTransferById(MISSING_ID))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid();
    }

    // ==================== Soft Delete (Trash) Tests ====================

    @Test
    Uni<Void> testTrashTransfer() {
        return clean()
                .chain(() -> persist("111122223333", "444455556666", 200000L))
                .chain(id -> transferCommandRepo.trashed(id))
                .invoke(t -> {
                    assertThat(t).isNotNull();
                    assertThat(t.getDeletedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashTransferReturnsNullIfAlreadyTrashed() {
        return clean()
                .chain(() -> persist("111122223344", "444455556677", 200000L))
                .chain(id -> transferCommandRepo.trashed(id)
                        .chain(() -> transferCommandRepo.trashed(id)))
                .invoke(t -> assertThat(t).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testTrashTransferReturnsNullIfNotFound() {
        return clean()
                .chain(() -> transferCommandRepo.trashed(MISSING_ID))
                .invoke(t -> assertThat(t).isNull())
                .replaceWithVoid();
    }

    // ==================== Restore Tests ====================

    @Test
    Uni<Void> testRestoreTransfer() {
        return clean()
                .chain(() -> persist("222233334444", "555566667777", 300000L))
                .chain(id -> transferCommandRepo.trashed(id)
                        .chain(() -> transferCommandRepo.restore(id)))
                .invoke(r -> {
                    assertThat(r).isNotNull();
                    assertThat(r.getDeletedAt()).isNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreTransferReturnsNullIfNotTrashed() {
        return clean()
                .chain(() -> persist("222233334455", "555566667788", 300000L))
                .chain(id -> transferCommandRepo.restore(id))
                .invoke(r -> assertThat(r).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreTransferReturnsNullIfNotFound() {
        return clean()
                .chain(() -> transferCommandRepo.restore(MISSING_ID))
                .invoke(r -> assertThat(r).isNull())
                .replaceWithVoid();
    }

    // ==================== Delete Permanent Tests ====================

    @Test
    Uni<Void> testDeletePermanent() {
        return clean()
                .chain(() -> persist("333344445555", "666677778888", 400000L))
                .chain(id -> transferCommandRepo.trashed(id)
                        .chain(() -> transferCommandRepo.deletePermanent(id))
                        .chain(deleted -> transferQueryRepo.findTransferById(id)))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentFailsIfNotTrashed() {
        return clean()
                .chain(() -> persist("333344445566", "666677778899", 400000L))
                .chain(id -> transferCommandRepo.deletePermanent(id))
                .invoke(d -> assertThat(d).isFalse())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeletePermanentReturnsFalseIfNotFound() {
        return clean()
                .chain(() -> transferCommandRepo.deletePermanent(MISSING_ID))
                .invoke(d -> assertThat(d).isFalse())
                .replaceWithVoid();
    }

    // ==================== Bulk Operations Tests ====================

    @Test
    Uni<Void> testRestoreAllDeleted() {
        return clean()
                .chain(() -> persist("444455556666", "777788889999", 500000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("444455556677", "777788889900", 600000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("444455556688", "777788889911", 700000L))
                .chain(() -> transferCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> transferQueryRepo.findTrashedTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeleted() {
        return clean()
                .chain(() -> persist("555566667777", "888899990000", 800000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("555566667788", "888899990011", 900000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> persist("555566667799", "888899990022", 1000000L))
                .chain(() -> transferCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testRestoreAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persist("555566667800", "888899990033", 100000L))
                .chain(() -> transferCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isFalse())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testDeleteAllDeletedReturnsFalseWhenNoTrashed() {
        return clean()
                .chain(() -> persist("555566667811", "888899990044", 100000L))
                .chain(() -> transferCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isFalse())
                .replaceWithVoid();
    }

    // ==================== Update Status Tests ====================

    @Test
    Uni<Void> testUpdateStatus() {
        return clean()
                .chain(() -> persist("666677778888", "999900001111", 100000L, Status.PENDING, null))
                .chain(id -> transferCommandRepo.updateTransferStatus(id, "SUCCESS"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.getStatus()).isEqualTo(Status.SUCCESS);
                    assertThat(updated.getUpdatedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusToFailed() {
        return clean()
                .chain(() -> persist("666677778899", "999900001122", 100000L, Status.PENDING, null))
                .chain(id -> transferCommandRepo.updateTransferStatus(id, "FAILED"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.getStatus()).isEqualTo(Status.FAILED);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusReturnsNullIfNotFound() {
        return clean()
                .chain(() -> transferCommandRepo.updateTransferStatus(MISSING_ID, "SUCCESS"))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusInvalidStatusDoesNotChange() {
        return clean()
                .chain(() -> persist("666677778900", "999900001133", 100000L, Status.PENDING, null))
                .chain(id -> transferCommandRepo.updateTransferStatus(id, "INVALID_STATUS"))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat(updated.getStatus()).isEqualTo(Status.PENDING);
                })
                .replaceWithVoid();
    }

    // ==================== Update Amount Tests ====================

    @Test
    Uni<Void> testUpdateTransferAmount() {
        return clean()
                .chain(() -> persist("666677779000", "999900001144", 100000L, Status.PENDING, null))
                .chain(id -> transferCommandRepo.updateTransferAmount(id, 250000L))
                .invoke(updated -> {
                    assertThat(updated).isNotNull();
                    assertThat((long) updated.getTransferAmount()).isEqualTo(250000L);
                    assertThat(updated.getUpdatedAt()).isNotNull();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateTransferAmountReturnsNullIfNotFound() {
        return clean()
                .chain(() -> transferCommandRepo.updateTransferAmount(MISSING_ID, 250000L))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    // ==================== Query - Active/Trashed Tests ====================

    @Test
    Uni<Void> testFindActiveTransfers() {
        return clean()
                .chain(() -> persist("777788889999", "111122223333", 100000L))
                .chain(() -> persist("777788889900", "111122223344", 200000L))
                .chain(() -> transferQueryRepo.findActiveTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTrashedTransfers() {
        return clean()
                .chain(() -> persist("888899990000", "222233334444", 100000L))
                .chain(() -> persist("888899990011", "222233334455", 200000L))
                .chain(() -> transferQueryRepo.findTrashedTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindActiveTransfersExcludesTrashed() {
        return clean()
                .chain(() -> persist("888899990022", "222233334466", 100000L))
                .chain(() -> persist("888899990033", "222233334477", 200000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> transferQueryRepo.findActiveTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTrashedTransfersOnlyShowsTrashed() {
        return clean()
                .chain(() -> persist("888899990044", "222233334488", 100000L))
                .chain(() -> persist("888899990055", "222233334499", 200000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> transferQueryRepo.findTrashedTransfers(findAllReq(1, 10, "")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getTransferFrom()).isEqualTo("888899990055");
                })
                .replaceWithVoid();
    }

    // ==================== Query - Search Tests ====================

    @Test
    Uni<Void> testFindTransfersWithSearchByTransferFrom() {
        return clean()
                .chain(() -> persist("1234567890123", "999999999999", 100000L))
                .chain(() -> persist("9876543210987", "888888888888", 200000L))
                .chain(() -> persist("1111222233333", "777777777777", 300000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "1234")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getTransferFrom()).isEqualTo("1234567890123");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersWithSearchByTransferTo() {
        return clean()
                .chain(() -> persist("1234567890123", "TARGET_CARD_1", 100000L))
                .chain(() -> persist("9876543210987", "TARGET_CARD_2", 200000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "TARGET_CARD_1")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getTransferTo()).isEqualTo("TARGET_CARD_1");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersWithSearchByStatus() {
        return clean()
                .chain(() -> persist("1234567890123", "999999999999", 100000L, Status.SUCCESS, null))
                .chain(() -> persist("9876543210987", "888888888888", 200000L, Status.FAILED, null))
                .chain(() -> persist("1111222233333", "777777777777", 300000L, Status.PENDING, null))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "success")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getStatus()).isEqualTo(Status.SUCCESS);
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersWithEmptySearchReturnsAll() {
        return clean()
                .chain(() -> persist("1234567890123", "999999999999", 100000L))
                .chain(() -> persist("9876543210987", "888888888888", 200000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersWithNullSearchReturnsAll() {
        return clean()
                .chain(() -> persist("1234567890123", "999999999999", 100000L))
                .chain(() -> persist("9876543210987", "888888888888", 200000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, null)))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    // ==================== Query - Pagination Tests ====================

    @Test
    Uni<Void> testFindTransfersWithPagination() {
        return clean()
                .chain(() -> persist("1111111111001", "2222222222001", 100000L))
                .chain(() -> persist("1111111111002", "2222222222002", 200000L))
                .chain(() -> persist("1111111111003", "2222222222003", 300000L))
                .chain(() -> persist("1111111111004", "2222222222004", 400000L))
                .chain(() -> persist("1111111111005", "2222222222005", 500000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 2, "")))
                .map(page1 -> {
                    assertThat(page1.getData()).hasSize(2);
                    assertThat(page1.getTotalRecords()).isEqualTo(5);
                    return page1;
                })
                .chain(page1 -> transferQueryRepo.findTransfers(findAllReq(2, 2, "")))
                .invoke(page2 -> assertThat(page2.getData()).hasSize(2))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(3, 2, "")))
                .invoke(page3 -> assertThat(page3.getData()).hasSize(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersPageZeroDefaultsToFirstPage() {
        return clean()
                .chain(() -> persist("1111111111006", "2222222222006", 100000L))
                .chain(() -> persist("1111111111007", "2222222222007", 200000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(0, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersNegativePageDefaultsToFirstPage() {
        return clean()
                .chain(() -> persist("1111111111008", "2222222222008", 100000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(-1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersZeroSizeDefaultsToTen() {
        return clean()
                .chain(() -> persist("1111111111009", "2222222222009", 100000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 0, "")))
                .invoke(r -> assertThat(r.getData()).hasSizeLessThanOrEqualTo(10))
                .replaceWithVoid();
    }

    // ==================== Query - Sort Order Tests ====================

    @Test
    Uni<Void> testFindTransfersSortedByTransferTimeDescending() {
        Instant time1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant time2 = Instant.parse("2024-01-02T10:00:00Z");
        Instant time3 = Instant.parse("2024-01-03T10:00:00Z");

        return clean()
                .chain(() -> persist("1111111111010", "2222222222010", 100000L, Status.PENDING, time1))
                .chain(() -> persist("1111111111011", "2222222222011", 200000L, Status.PENDING, time2))
                .chain(() -> persist("1111111111012", "2222222222012", 300000L, Status.PENDING, time3))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "")))
                .invoke(r -> {
                    assertThat(r.getData()).hasSize(3);
                    assertThat(r.getData().get(0).getTransferTime()).isEqualTo(time3);
                    assertThat(r.getData().get(1).getTransferTime()).isEqualTo(time2);
                    assertThat(r.getData().get(2).getTransferTime()).isEqualTo(time1);
                })
                .replaceWithVoid();
    }

    // ==================== Query - By Card Number Tests ====================

    @Test
    Uni<Void> testFindTransfersByCardNumber() {
        return clean()
                .chain(() -> persist("123412341234", "999999999999", 100000L))
                .chain(() -> persist("999999999999", "123412341234", 200000L))
                .chain(() -> persist("555555555555", "666666666666", 300000L))
                .chain(() -> transferQueryRepo.findTransfersByCardNumber("123412341234"))
                .invoke(list -> {
                    assertThat(list).hasSize(2);
                    assertThat(list.stream().allMatch(t -> t.getTransferFrom().equals("123412341234") ||
                            t.getTransferTo().equals("123412341234"))).isTrue();
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersBySourceCard() {
        return clean()
                .chain(() -> persist("123412341234", "999999999999", 100000L))
                .chain(() -> persist("999999999999", "123412341234", 200000L))
                .chain(() -> transferQueryRepo.findTransfersBySourceCard("123412341234"))
                .invoke(list -> {
                    assertThat(list).hasSize(1);
                    assertThat(list.get(0).getTransferFrom()).isEqualTo("123412341234");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersByDestinationCard() {
        return clean()
                .chain(() -> persist("123412341234", "999999999999", 100000L))
                .chain(() -> persist("999999999999", "123412341234", 200000L))
                .chain(() -> transferQueryRepo.findTransfersByDestinationCard("123412341234"))
                .invoke(list -> {
                    assertThat(list).hasSize(1);
                    assertThat(list.get(0).getTransferTo()).isEqualTo("123412341234");
                })
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersByCardNumberExcludesTrashed() {
        return clean()
                .chain(() -> persist("123412341234", "999999999999", 100000L))
                .chain(() -> persist("123412341234", "888888888888", 200000L)
                        .chain(id -> transferCommandRepo.trashed(id).replaceWithVoid()))
                .chain(() -> transferQueryRepo.findTransfersByCardNumber("123412341234"))
                .invoke(list -> assertThat(list).hasSize(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testFindTransfersByCardNumberSortedByTransferTimeDescending() {
        Instant time1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant time2 = Instant.parse("2024-01-02T10:00:00Z");

        return clean()
                .chain(() -> persist("123412341235", "999999999999", 100000L, Status.PENDING, time1))
                .chain(() -> persist("123412341235", "888888888888", 200000L, Status.PENDING, time2))
                .chain(() -> transferQueryRepo.findTransfersByCardNumber("123412341235"))
                .invoke(list -> {
                    assertThat(list).hasSize(2);
                    assertThat(list.get(0).getTransferTime()).isEqualTo(time2);
                    assertThat(list.get(1).getTransferTime()).isEqualTo(time1);
                })
                .replaceWithVoid();
    }

    // ==================== Edge Cases ====================

    @Test
    Uni<Void> testEmptyDatabaseReturnsZeroRecords() {
        return clean()
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .chain(() -> transferQueryRepo.findActiveTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .chain(() -> transferQueryRepo.findTrashedTransfers(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testSearchWithNoMatchesReturnsZeroRecords() {
        return clean()
                .chain(() -> persist("1234567890123", "999999999999", 100000L))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "NONEXISTENT_SEARCH")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testUpdateStatusOnTrashedTransferReturnsNull() {
        return clean()
                .chain(() -> persist("999988887777", "666655554444", 100000L))
                .chain(id -> transferCommandRepo.trashed(id)
                        .chain(() -> transferCommandRepo.updateTransferStatus(id, "SUCCESS")))
                .invoke(updated -> assertThat(updated).isNull())
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testCaseInsensitiveStatusSearch() {
        return clean()
                .chain(() -> persist("111122223344", "555566667788", 100000L, Status.SUCCESS, null))
                .chain(() -> persist("111122223355", "555566667799", 200000L, Status.FAILED, null))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "SUCCESS")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "success")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .chain(() -> transferQueryRepo.findTransfers(findAllReq(1, 10, "SuCceSs")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid();
    }

    @Test
    Uni<Void> testCompensationClaimIsExclusiveAndReleasable() {
        Instant now = Instant.now();
        Instant leaseUntil = now.plusSeconds(300);
        return clean()
                .chain(() -> persist("999900001111", "888800001111", 100000L, Status.PENDING, null))
                .chain(id -> transferCommandRepo.markCompensationRequired(id, "saldo update failed").replaceWith(id))
                .chain(id -> transferCommandRepo.claimCompensation(id, "worker-a", "token-a", now, leaseUntil, 3)
                        .invoke(claimed -> assertThat(claimed).isTrue())
                        .replaceWith(id))
                .chain(id -> transferCommandRepo.claimCompensation(id, "worker-b", "token-b", now, leaseUntil, 3)
                        .invoke(claimed -> assertThat(claimed).isFalse())
                        .replaceWith(id))
                .chain(id -> transferCommandRepo.releaseCompensation(id, "worker-a", "token-a",
                        Instant.now().minusSeconds(1), "retryable dependency failure"))
                .invoke(released -> assertThat(released).isTrue())
                .replaceWithVoid();
    }
}
