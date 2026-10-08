package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.MerchantDocumentDto.*;
import io.smallrye.mutiny.Uni;

public interface MerchantDocumentService {
    Uni<FindAllMerchantDocumentsResponse> listMerchantDocuments(int page, int size, String search);
    Uni<FindAllMerchantDocumentsResponse> listActiveMerchantDocuments(int page, int size, String search);
    Uni<FindAllMerchantDocumentsResponse> listTrashedMerchantDocuments(int page, int size, String search);
    Uni<FindByIdMerchantDocumentResponse> getMerchantDocument(String id);
    Uni<CreateMerchantDocumentResponse> createMerchantDocument(CreateMerchantDocumentBody body);
    Uni<UpdateMerchantDocumentResponse> updateMerchantDocument(String id, UpdateMerchantDocumentBody body);
    Uni<UpdateMerchantDocumentResponse> updateMerchantDocumentStatus(String id, UpdateMerchantDocumentStatusBody body);
    Uni<TrashedMerchantDocumentResponse> deleteMerchantDocument(String id);
    Uni<TrashedMerchantDocumentResponse> trashMerchantDocument(String id);
    Uni<TrashedMerchantDocumentResponse> restoreMerchantDocument(String id);
    Uni<SimpleStatusMessageResponse> deleteMerchantDocumentPermanent(String id);
    Uni<SimpleStatusMessageResponse> restoreAllMerchantDocuments();
    Uni<SimpleStatusMessageResponse> deleteAllMerchantDocuments();
}
