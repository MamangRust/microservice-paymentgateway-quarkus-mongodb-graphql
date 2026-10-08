package com.sanedge.merchant.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.sanedge.common.enums.Status;
import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;
import com.sanedge.merchant.domain.requests.FindAllMerchants;
import com.sanedge.merchant.entity.Merchant;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(MongoDbResource.class)
class MerchantRepositoryTest extends MongoTestBase {

    @Inject
    MerchantQueryRepository merchantQueryRepo;

    @Inject
    MerchantCommandRepository merchantCommandRepo;

    private static final String USER_ID = "507f1f77bcf86cd799439010";
    private static final String UNKNOWN_ID = "000000000000000000000000";

    private Uni<Merchant> persist(String name, String apiKey, Status status) {
        Merchant merchant = new Merchant();
        merchant.setName(name);
        merchant.setApiKey(apiKey);
        merchant.setUserId(new ObjectId(USER_ID));
        merchant.setStatus(status);
        merchant.setMerchantNo(java.util.UUID.randomUUID());
        Instant now = Instant.now();
        merchant.setCreatedAt(now);
        merchant.setUpdatedAt(now);
        return merchantQueryRepo.persist(merchant).map(v -> merchant);
    }

    private Uni<Merchant> persist(String name, String apiKey) {
        return persist(name, apiKey, Status.SUCCESS);
    }

    private Uni<Void> clean() {
        return merchantQueryRepo.deleteAll().replaceWithVoid();
    }

    private FindAllMerchants findAllReq(int page, int size, String search) {
        FindAllMerchants r = new FindAllMerchants();
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    @Test
    void testCreateAndFindById() {
        clean()
                .chain(() -> persist("Merchant One", "key-001"))
                .chain(m -> merchantQueryRepo.findMerchantById(m.getMerchantId()))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getName()).isEqualTo("Merchant One");
                    assertThat(found.getApiKey()).isEqualTo("key-001");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByIdReturnsNullWhenNotFound() {
        clean()
                .chain(() -> merchantQueryRepo.findMerchantById(UNKNOWN_ID))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByApiKey() {
        clean()
                .chain(() -> persist("Merchant Key", "unique-key"))
                .chain(() -> merchantQueryRepo.findByApiKey("unique-key"))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getName()).isEqualTo("Merchant Key");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testExistsByName() {
        clean()
                .chain(() -> persist("UniqueMart", "um-key"))
                .chain(() -> merchantQueryRepo.existsByName("UniqueMart"))
                .invoke(exists -> assertThat(exists).isTrue())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testExistsByNameReturnsFalse() {
        clean()
                .chain(() -> merchantQueryRepo.existsByName("GhostStore"))
                .invoke(exists -> assertThat(exists).isFalse())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testTrashMerchant() {
        clean()
                .chain(() -> persist("Trash Me", "trash-key"))
                .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()))
                .invoke(t -> {
                    assertThat(t).isNotNull();
                    assertThat(t.getDeletedAt()).isNotNull();
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testTrashMerchantReturnsNullIfAlreadyTrashed() {
        clean()
                .chain(() -> persist("Double Trash", "dt-key"))
                .chain(m -> merchantCommandRepo.trashed(m.getMerchantId())
                        .chain(() -> merchantCommandRepo.trashed(m.getMerchantId())))
                .invoke(t -> assertThat(t).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testRestoreMerchant() {
        clean()
                .chain(() -> persist("Restore Me", "restore-key"))
                .chain(m -> merchantCommandRepo.trashed(m.getMerchantId())
                        .chain(() -> merchantCommandRepo.restore(m.getMerchantId())))
                .invoke(r -> {
                    assertThat(r).isNotNull();
                    assertThat(r.getDeletedAt()).isNull();
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testRestoreMerchantReturnsNullIfNotTrashed() {
        clean()
                .chain(() -> persist("Active Only", "ao-key"))
                .chain(m -> merchantCommandRepo.restore(m.getMerchantId()))
                .invoke(r -> assertThat(r).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testDeletePermanent() {
        clean()
                .chain(() -> persist("Delete Perm", "del-key"))
                .chain(m -> merchantCommandRepo.deletePermanent(m.getMerchantId())
                        .chain(deleted -> merchantQueryRepo.findMerchantById(m.getMerchantId())))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testRestoreAllDeleted() {
        clean()
                .chain(() -> persist("Bulk Restore 1", "bulk1")
                        .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()).replaceWithVoid()))
                .chain(() -> persist("Bulk Restore 2", "bulk2")
                        .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()).replaceWithVoid()))
                .chain(() -> persist("Active One", "active1"))
                .chain(() -> merchantCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> merchantQueryRepo.findTrashedMerchants(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testDeleteAllDeleted() {
        clean()
                .chain(() -> persist("Delete All 1", "del1")
                        .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()).replaceWithVoid()))
                .chain(() -> persist("Delete All 2", "del2")
                        .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()).replaceWithVoid()))
                .chain(() -> persist("Keep", "keep"))
                .chain(() -> merchantCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> merchantQueryRepo.findActiveMerchants(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testUpdateStatus() {
        clean()
                .chain(() -> persist("Change Status", "cs-key"))
                .chain(m -> merchantCommandRepo.updateStatus(m.getMerchantId(), "PENDING")
                        .chain(() -> merchantQueryRepo.findMerchantById(m.getMerchantId())))
                .invoke(m -> assertThat(m.getStatus()).isEqualTo(Status.PENDING))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testUpdateStatusInvalidStatus() {
        clean()
                .chain(() -> persist("Invalid Status", "is-key"))
                .chain(m -> merchantCommandRepo.updateStatus(m.getMerchantId(), "INVALID"))
                .invoke(r -> assertThat(r).isFalse())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindActiveMerchants() {
        clean()
                .chain(() -> persist("Active1", "a1"))
                .chain(() -> persist("Active2", "a2"))
                .chain(() -> merchantQueryRepo.findActiveMerchants(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(2))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindTrashedMerchants() {
        clean()
                .chain(() -> persist("ToTrash", "tt")
                        .chain(m -> merchantCommandRepo.trashed(m.getMerchantId()).replaceWithVoid()))
                .chain(() -> persist("KeepActive", "ka"))
                .chain(() -> merchantQueryRepo.findTrashedMerchants(findAllReq(1, 10, "")))
                .invoke(page -> assertThat(page.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindMerchantsWithSearch() {
        clean()
                .chain(() -> persist("Alpha Store", "a-key"))
                .chain(() -> persist("Beta Shop", "b-key"))
                .chain(() -> persist("Gamma Market", "c-key"))
                .chain(() -> merchantQueryRepo.findMerchants(findAllReq(1, 10, "beta")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getName()).isEqualTo("Beta Shop");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testPagination() {
        clean()
                .chain(() -> persist("Page1", "p1"))
                .chain(() -> persist("Page2", "p2"))
                .chain(() -> persist("Page3", "p3"))
                .chain(() -> persist("Page4", "p4"))
                .chain(() -> persist("Page5", "p5"))
                .chain(() -> merchantQueryRepo.findMerchants(findAllReq(1, 2, "")))
                .invoke(page1 -> {
                    assertThat(page1.getData()).hasSize(2);
                    assertThat(page1.getTotalRecords()).isEqualTo(5);
                })
                .chain(() -> merchantQueryRepo.findMerchants(findAllReq(3, 2, "")))
                .invoke(page3 -> assertThat(page3.getData()).hasSize(1))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByName() {
        clean()
                .chain(() -> persist("Exact Match", "em-key"))
                .chain(() -> merchantQueryRepo.findByName("Exact Match"))
                .invoke(m -> {
                    assertThat(m).isNotNull();
                    assertThat(m.getName()).isEqualTo("Exact Match");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByUserId() {
        clean()
                .chain(() -> persist("User Merchant", "um2-key"))
                .chain(() -> merchantQueryRepo.findByUserId(USER_ID))
                .invoke(list -> {
                    assertThat(list).hasSize(1);
                    assertThat(list.get(0).getName()).isEqualTo("User Merchant");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByUserIdNull() {
        clean()
                .chain(() -> merchantQueryRepo.findByUserId(null))
                .invoke(list -> assertThat(list).isEmpty())
                .replaceWithVoid().await().indefinitely();
    }
}
