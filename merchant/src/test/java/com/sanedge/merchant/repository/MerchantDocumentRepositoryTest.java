package com.sanedge.merchant.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.sanedge.common.test.MongoDbResource;
import com.sanedge.common.test.MongoTestBase;
import com.sanedge.merchant.domain.requests.FindAllMerchantDocuments;
import com.sanedge.merchant.entity.MerchantDocument;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@QuarkusTest
@QuarkusTestResource(MongoDbResource.class)
class MerchantDocumentRepositoryTest extends MongoTestBase {

    @Inject
    MerchantDocumentQueryRepository documentQueryRepo;

    @Inject
    MerchantDocumentCommandRepository documentCommandRepo;

    private static final String MERCHANT_ID = "507f1f77bcf86cd799439011";

    private Uni<MerchantDocument> persist(String merchantId, String docType, String docUrl) {
        MerchantDocument doc = new MerchantDocument();
        doc.setMerchantId(new ObjectId(merchantId));
        doc.setDocumentType(docType);
        doc.setDocumentUrl(docUrl);
        Instant now = Instant.now();
        doc.setCreatedAt(now);
        doc.setUpdatedAt(now);
        return documentQueryRepo.persist(doc).map(v -> doc);
    }

    private Uni<MerchantDocument> persist() {
        return persist(MERCHANT_ID, "ID_CARD", "http://docs.com/id.jpg");
    }

    private Uni<Void> clean() {
        return documentQueryRepo.deleteAll().replaceWithVoid();
    }

    private FindAllMerchantDocuments findAllReq(int page, int size, String search) {
        FindAllMerchantDocuments r = new FindAllMerchantDocuments();
        r.setPage(page);
        r.setPageSize(size);
        r.setSearch(search == null ? "" : search);
        return r;
    }

    @Test
    void testCreateAndFindById() {
        clean()
                .chain(() -> persist(MERCHANT_ID, "PASSPORT", "http://passport.img"))
                .chain(d -> documentQueryRepo.findDocumentById(d.getDocumentId()))
                .invoke(found -> {
                    assertThat(found).isNotNull();
                    assertThat(found.getDocumentType()).isEqualTo("PASSPORT");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindByIdReturnsNullWhenNotFound() {
        clean()
                .chain(() -> documentQueryRepo.findDocumentById("000000000000000000000000"))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testTrashDocument() {
        clean()
                .chain(() -> persist())
                .chain(d -> documentCommandRepo.trashed(d.getDocumentId()))
                .invoke(t -> assertThat(t.getDeletedAt()).isNotNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testRestoreDocument() {
        clean()
                .chain(() -> persist())
                .chain(d -> documentCommandRepo.trashed(d.getDocumentId())
                        .chain(() -> documentCommandRepo.restore(d.getDocumentId())))
                .invoke(r -> assertThat(r.getDeletedAt()).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testDeletePermanent() {
        clean()
                .chain(() -> persist())
                .chain(d -> documentCommandRepo.trashed(d.getDocumentId())
                        .chain(() -> documentCommandRepo.deletePermanent(d.getDocumentId()))
                        .chain(deleted -> documentQueryRepo.findDocumentById(d.getDocumentId())))
                .invoke(n -> assertThat(n).isNull())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testRestoreAllDeleted() {
        clean()
                .chain(() -> persist().chain(d -> documentCommandRepo.trashed(d.getDocumentId()).replaceWithVoid()))
                .chain(() -> persist().chain(d -> documentCommandRepo.trashed(d.getDocumentId()).replaceWithVoid()))
                .chain(() -> persist())
                .chain(() -> documentCommandRepo.restoreAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> documentQueryRepo.findTrashedDocuments(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isZero())
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testDeleteAllDeleted() {
        clean()
                .chain(() -> persist().chain(d -> documentCommandRepo.trashed(d.getDocumentId()).replaceWithVoid()))
                .chain(() -> persist().chain(d -> documentCommandRepo.trashed(d.getDocumentId()).replaceWithVoid()))
                .chain(() -> persist())
                .chain(() -> documentCommandRepo.deleteAllDeleted())
                .invoke(r -> assertThat(r).isTrue())
                .chain(() -> documentQueryRepo.findActiveDocuments(findAllReq(1, 10, "")))
                .invoke(r -> assertThat(r.getTotalRecords()).isEqualTo(1))
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testFindDocumentsWithSearch() {
        clean()
                .chain(() -> persist(MERCHANT_ID, "ID_CARD", "id1.jpg"))
                .chain(() -> persist(MERCHANT_ID, "BUSINESS_LICENSE", "license.pdf"))
                .chain(() -> documentQueryRepo.findDocuments(findAllReq(1, 10, "BUSINESS")))
                .invoke(r -> {
                    assertThat(r.getTotalRecords()).isEqualTo(1);
                    assertThat(r.getData().get(0).getDocumentType()).isEqualTo("BUSINESS_LICENSE");
                })
                .replaceWithVoid().await().indefinitely();
    }

    @Test
    void testPagination() {
        clean()
                .chain(() -> persist())
                .chain(() -> persist())
                .chain(() -> persist())
                .chain(() -> persist())
                .chain(() -> documentQueryRepo.findDocuments(findAllReq(1, 2, "")))
                .invoke(page1 -> {
                    assertThat(page1.getData()).hasSize(2);
                    assertThat(page1.getTotalRecords()).isEqualTo(4);
                })
                .replaceWithVoid().await().indefinitely();
    }
}
