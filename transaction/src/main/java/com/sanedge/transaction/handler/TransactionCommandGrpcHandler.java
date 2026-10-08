package com.sanedge.transaction.handler;

import com.google.protobuf.Empty;
import com.sanedge.transaction.domain.response.TransactionResponse;
import com.sanedge.transaction.domain.response.TransactionResponseDeleteAt;
import com.sanedge.transaction.service.TransactionCommandService;
import com.sanedge.common.utils.IdValidator;

import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import pb.transaction.MutinyTransactionCommandServiceGrpc;
import pb.transaction.Transaction.ApiResponseTransaction;
import pb.transaction.Transaction.ApiResponseTransactionDeleteAt;
import pb.transaction.Transaction.FindByIdTransactionRequest;
import pb.transaction.TransactionCommand.ApiResponseTransactionAll;
import pb.transaction.TransactionCommand.ApiResponseTransactionDelete;
import pb.transaction.TransactionCommand.CreateTransactionRequest;
import pb.transaction.TransactionCommand.UpdateTransactionRequest;

@GrpcService
@Singleton
public class TransactionCommandGrpcHandler
        extends MutinyTransactionCommandServiceGrpc.TransactionCommandServiceImplBase {

    @Inject
    TransactionCommandService transactionCommandService;

    @Override
    public Uni<ApiResponseTransaction> createTransaction(CreateTransactionRequest request) {
        Uni<ApiResponseTransaction> invalidMerchantId = IdValidator.invalidObjectId("Merchant id",
                request.getMerchantId());
        if (invalidMerchantId != null) {
            return invalidMerchantId;
        }
        com.sanedge.transaction.domain.requests.CreateTransactionRequest domainReq = new com.sanedge.transaction.domain.requests.CreateTransactionRequest();
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setAmount((long) request.getAmount());
        domainReq.setPaymentMethod(request.getPaymentMethod());
        domainReq.setMerchantId(request.getMerchantId());

        if (request.hasTransactionTime()) {
            java.time.Instant instant = java.time.Instant.ofEpochSecond(request.getTransactionTime().getSeconds(),
                    request.getTransactionTime().getNanos());
            domainReq.setTransactionTime(instant);
        }

        return transactionCommandService.create(request.getApiKey(), domainReq)
                .map(apiResp -> {
                    ApiResponseTransaction.Builder builder = ApiResponseTransaction.newBuilder()
                            .setStatus(apiResp.status())
                            .setMessage(apiResp.message());
                    if (apiResp.data() != null) {
                        builder.setData(toProto(apiResp.data()));
                    }
                    return builder.build();
                })
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransaction> updateTransaction(UpdateTransactionRequest request) {
        Uni<ApiResponseTransaction> invalidTransactionId = IdValidator.invalidObjectId("Transaction id",
                request.getTransactionId());
        if (invalidTransactionId != null) {
            return invalidTransactionId;
        }
        Uni<ApiResponseTransaction> invalidMerchantId = IdValidator.invalidObjectId("Merchant id",
                request.getMerchantId());
        if (invalidMerchantId != null) {
            return invalidMerchantId;
        }
        com.sanedge.transaction.domain.requests.UpdateTransactionRequest domainReq = new com.sanedge.transaction.domain.requests.UpdateTransactionRequest();
        domainReq.setTransactionId(request.getTransactionId());
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setAmount((long) request.getAmount());
        domainReq.setPaymentMethod(request.getPaymentMethod());
        domainReq.setMerchantId(request.getMerchantId());

        if (request.hasTransactionTime()) {
            java.time.Instant instant = java.time.Instant.ofEpochSecond(request.getTransactionTime().getSeconds(),
                    request.getTransactionTime().getNanos());
            domainReq.setTransactionTime(instant);
        }

        return transactionCommandService.update(request.getApiKey(), domainReq)
                .map(apiResp -> {
                    ApiResponseTransaction.Builder builder = ApiResponseTransaction.newBuilder()
                            .setStatus(apiResp.status())
                            .setMessage(apiResp.message());
                    if (apiResp.data() != null) {
                        builder.setData(toProto(apiResp.data()));
                    }
                    return builder.build();
                })
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransactionDeleteAt> trashedTransaction(FindByIdTransactionRequest request) {
        Uni<ApiResponseTransactionDeleteAt> invalidId = IdValidator.invalidObjectId("Transaction id",
                request.getTransactionId());
        if (invalidId != null) {
            return invalidId;
        }
        return transactionCommandService.trashed(request.getTransactionId())
                .map(apiResp -> {
                    ApiResponseTransactionDeleteAt.Builder builder = ApiResponseTransactionDeleteAt.newBuilder()
                            .setStatus(apiResp.status())
                            .setMessage(apiResp.message());
                    if (apiResp.data() != null) {
                        builder.setData(toProto(apiResp.data()));
                    }
                    return builder.build();
                })
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransactionDeleteAt> restoreTransaction(FindByIdTransactionRequest request) {
        Uni<ApiResponseTransactionDeleteAt> invalidId = IdValidator.invalidObjectId("Transaction id",
                request.getTransactionId());
        if (invalidId != null) {
            return invalidId;
        }
        return transactionCommandService.restore(request.getTransactionId())
                .map(apiResp -> {
                    ApiResponseTransactionDeleteAt.Builder builder = ApiResponseTransactionDeleteAt.newBuilder()
                            .setStatus(apiResp.status())
                            .setMessage(apiResp.message());
                    if (apiResp.data() != null) {
                        builder.setData(toProto(apiResp.data()));
                    }
                    return builder.build();
                })
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransactionDelete> deleteTransactionPermanent(FindByIdTransactionRequest request) {
        Uni<ApiResponseTransactionDelete> invalidId = IdValidator.invalidObjectId("Transaction id",
                request.getTransactionId());
        if (invalidId != null) {
            return invalidId;
        }
        return transactionCommandService.deletePermanent(request.getTransactionId())
                .map(apiResp -> ApiResponseTransactionDelete.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransactionAll> restoreAllTransaction(Empty request) {
        return transactionCommandService.restoreAll()
                .map(apiResp -> ApiResponseTransactionAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransactionAll> deleteAllTransactionPermanent(Empty request) {
        return transactionCommandService.deleteAll()
                .map(apiResp -> ApiResponseTransactionAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    private pb.transaction.Transaction.TransactionResponse toProto(TransactionResponse r) {
        if (r == null) {
            return pb.transaction.Transaction.TransactionResponse.getDefaultInstance();
        }
        pb.transaction.Transaction.TransactionResponse.Builder builder = pb.transaction.Transaction.TransactionResponse
                .newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getTransactionNo() != null) {
            builder.setTransactionNo(r.getTransactionNo());
        }
        if (r.getAmount() != null) {
            builder.setAmount(r.getAmount().intValue());
        }
        if (r.getPaymentMethod() != null) {
            builder.setPaymentMethod(r.getPaymentMethod());
        }
        if (r.getMerchantId() != null) {
            builder.setMerchantId(r.getMerchantId());
        }
        if (r.getTransactionTime() != null) {
            builder.setTransactionTime(r.getTransactionTime());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt());
        }
        return builder.build();
    }

    private pb.transaction.Transaction.TransactionResponseDeleteAt toProto(TransactionResponseDeleteAt r) {
        if (r == null) {
            return pb.transaction.Transaction.TransactionResponseDeleteAt.getDefaultInstance();
        }
        pb.transaction.Transaction.TransactionResponseDeleteAt.Builder builder = pb.transaction.Transaction.TransactionResponseDeleteAt
                .newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getTransactionNo() != null) {
            builder.setTransactionNo(r.getTransactionNo());
        }
        if (r.getAmount() != null) {
            builder.setAmount(r.getAmount().intValue());
        }
        if (r.getPaymentMethod() != null) {
            builder.setPaymentMethod(r.getPaymentMethod());
        }
        if (r.getMerchantId() != null) {
            builder.setMerchantId(r.getMerchantId());
        }
        if (r.getTransactionTime() != null) {
            builder.setTransactionTime(r.getTransactionTime());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt());
        }
        if (r.getDeletedAt() != null) {
            builder.setDeletedAt(com.google.protobuf.StringValue.of(r.getDeletedAt()));
        }
        return builder.build();
    }
}
