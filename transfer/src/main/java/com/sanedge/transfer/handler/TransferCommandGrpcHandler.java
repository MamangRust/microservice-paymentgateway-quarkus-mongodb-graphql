package com.sanedge.transfer.handler;

import com.google.protobuf.Empty;
import com.sanedge.common.utils.IdValidator;
import com.sanedge.transfer.domain.response.TransferResponse;
import com.sanedge.transfer.domain.response.TransferResponseDeleteAt;
import com.sanedge.transfer.service.TransferCommandService;

import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import pb.transfer.MutinyTransferCommandServiceGrpc;
import pb.transfer.Transfer.ApiResponseTransfer;
import pb.transfer.Transfer.ApIResponseTransferDeleteAt;
import pb.transfer.Transfer.FindByIdTransferRequest;
import pb.transfer.TransferCommand.ApiResponseTransferAll;
import pb.transfer.TransferCommand.ApiResponseTransferDelete;
import pb.transfer.TransferCommand.CreateTransferRequest;
import pb.transfer.TransferCommand.UpdateTransferRequest;

@GrpcService
@Singleton
public class TransferCommandGrpcHandler extends MutinyTransferCommandServiceGrpc.TransferCommandServiceImplBase {

    @Inject
    TransferCommandService transferCommandService;

    @Override
    public Uni<ApiResponseTransfer> createTransfer(CreateTransferRequest request) {
        com.sanedge.transfer.domain.requests.CreateTransferRequest domainReq = new com.sanedge.transfer.domain.requests.CreateTransferRequest();
        domainReq.setTransferFrom(request.getTransferFrom());
        domainReq.setTransferTo(request.getTransferTo());
        domainReq.setTransferAmount((long) request.getTransferAmount());

        return transferCommandService.create(domainReq)
                .map(apiResp -> {
                    ApiResponseTransfer.Builder builder = ApiResponseTransfer.newBuilder()
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
    public Uni<ApiResponseTransfer> updateTransfer(UpdateTransferRequest request) {
        Uni<ApiResponseTransfer> invalidId = IdValidator.invalidObjectId("Transfer id",
                request.getTransferId());
        if (invalidId != null) {
            return invalidId;
        }
        com.sanedge.transfer.domain.requests.UpdateTransferRequest domainReq = new com.sanedge.transfer.domain.requests.UpdateTransferRequest();
        domainReq.setTransferId(request.getTransferId());
        domainReq.setTransferFrom(request.getTransferFrom());
        domainReq.setTransferTo(request.getTransferTo());
        domainReq.setTransferAmount((long) request.getTransferAmount());

        return transferCommandService.update(domainReq)
                .map(apiResp -> {
                    ApiResponseTransfer.Builder builder = ApiResponseTransfer.newBuilder()
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
    public Uni<ApIResponseTransferDeleteAt> trashedTransfer(FindByIdTransferRequest request) {
        Uni<ApIResponseTransferDeleteAt> invalidId = IdValidator.invalidObjectId("Transfer id",
                request.getTransferId());
        if (invalidId != null) {
            return invalidId;
        }
        return transferCommandService.trashed(request.getTransferId())
                .map(apiResp -> {
                    ApIResponseTransferDeleteAt.Builder builder = ApIResponseTransferDeleteAt.newBuilder()
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
    public Uni<ApIResponseTransferDeleteAt> restoreTransfer(FindByIdTransferRequest request) {
        Uni<ApIResponseTransferDeleteAt> invalidId = IdValidator.invalidObjectId("Transfer id",
                request.getTransferId());
        if (invalidId != null) {
            return invalidId;
        }
        return transferCommandService.restore(request.getTransferId())
                .map(apiResp -> {
                    ApIResponseTransferDeleteAt.Builder builder = ApIResponseTransferDeleteAt.newBuilder()
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
    public Uni<ApiResponseTransferDelete> deleteTransferPermanent(FindByIdTransferRequest request) {
        Uni<ApiResponseTransferDelete> invalidId = IdValidator.invalidObjectId("Transfer id",
                request.getTransferId());
        if (invalidId != null) {
            return invalidId;
        }
        return transferCommandService.deletePermanent(request.getTransferId())
                .map(apiResp -> ApiResponseTransferDelete.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransferAll> restoreAllTransfer(Empty request) {
        return transferCommandService.restoreAll()
                .map(apiResp -> ApiResponseTransferAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseTransferAll> deleteAllTransferPermanent(Empty request) {
        return transferCommandService.deleteAll()
                .map(apiResp -> ApiResponseTransferAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    private pb.transfer.Transfer.TransferResponse toProto(TransferResponse r) {
        if (r == null) {
            return pb.transfer.Transfer.TransferResponse.getDefaultInstance();
        }
        pb.transfer.Transfer.TransferResponse.Builder builder = pb.transfer.Transfer.TransferResponse.newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getTransferNo() != null) {
            builder.setTransferNo(r.getTransferNo());
        }
        if (r.getTransferFrom() != null) {
            builder.setTransferFrom(r.getTransferFrom());
        }
        if (r.getTransferTo() != null) {
            builder.setTransferTo(r.getTransferTo());
        }
        if (r.getTransferAmount() != null) {
            builder.setTransferAmount(r.getTransferAmount().intValue());
        }
        if (r.getTransferTime() != null) {
            builder.setTransferTime(r.getTransferTime());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt());
        }
        return builder.build();
    }

    private pb.transfer.Transfer.TransferResponseDeleteAt toProto(TransferResponseDeleteAt r) {
        if (r == null) {
            return pb.transfer.Transfer.TransferResponseDeleteAt.getDefaultInstance();
        }
        pb.transfer.Transfer.TransferResponseDeleteAt.Builder builder = pb.transfer.Transfer.TransferResponseDeleteAt.newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getTransferNo() != null) {
            builder.setTransferNo(r.getTransferNo());
        }
        if (r.getTransferFrom() != null) {
            builder.setTransferFrom(r.getTransferFrom());
        }
        if (r.getTransferTo() != null) {
            builder.setTransferTo(r.getTransferTo());
        }
        if (r.getTransferAmount() != null) {
            builder.setTransferAmount(r.getTransferAmount().intValue());
        }
        if (r.getTransferTime() != null) {
            builder.setTransferTime(r.getTransferTime());
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
