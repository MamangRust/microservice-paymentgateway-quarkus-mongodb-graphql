package com.sanedge.saldo.handler;

import com.google.protobuf.Empty;
import com.sanedge.saldo.domain.response.SaldoResponse;
import com.sanedge.saldo.domain.response.SaldoResponseDeleteAt;
import com.sanedge.saldo.service.SaldoCommandService;

import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import pb.saldo.MutinySaldoCommandServiceGrpc;
import pb.saldo.Saldo.ApiResponseSaldo;
import pb.saldo.Saldo.ApiResponseSaldoDeleteAt;
import pb.saldo.Saldo.FindByIdSaldoRequest;
import pb.saldo.SaldoCommand.ApiResponseSaldoAll;
import pb.saldo.SaldoCommand.ApiResponseSaldoDelete;
import pb.saldo.SaldoCommand.CreateSaldoRequest;
import pb.saldo.SaldoCommand.UpdateSaldoBalanceRequest;
import pb.saldo.SaldoCommand.UpdateSaldoRequest;
import pb.saldo.SaldoCommand.UpdateSaldoWithdrawRequest;

@GrpcService
@Singleton
public class SaldoCommandGrpcHandler extends MutinySaldoCommandServiceGrpc.SaldoCommandServiceImplBase {

    @Inject
    SaldoCommandService saldoCommandService;

    @Override
    public Uni<ApiResponseSaldo> createSaldo(CreateSaldoRequest request) {
        com.sanedge.saldo.domain.requests.CreateSaldoRequest domainReq = new com.sanedge.saldo.domain.requests.CreateSaldoRequest();
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setTotalBalance((long) request.getTotalBalance());

        return saldoCommandService.create(domainReq)
                .map(apiResp -> {
                    ApiResponseSaldo.Builder builder = ApiResponseSaldo.newBuilder()
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
    public Uni<ApiResponseSaldo> updateSaldo(UpdateSaldoRequest request) {
        com.sanedge.saldo.domain.requests.UpdateSaldoRequest domainReq = new com.sanedge.saldo.domain.requests.UpdateSaldoRequest();
        domainReq.setSaldoId(request.getSaldoId());
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setTotalBalance((long) request.getTotalBalance());

        return saldoCommandService.update(domainReq)
                .map(apiResp -> {
                    ApiResponseSaldo.Builder builder = ApiResponseSaldo.newBuilder()
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
    public Uni<ApiResponseSaldo> updateSaldoBalance(UpdateSaldoBalanceRequest request) {
        com.sanedge.saldo.domain.requests.UpdateSaldoBalance domainReq = new com.sanedge.saldo.domain.requests.UpdateSaldoBalance();
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setTotalBalance((long) request.getTotalBalance());
        if (request.getDeltaBalance() != 0) {
            domainReq.setDeltaBalance((long) request.getDeltaBalance());
            domainReq.setMinimumBalance((long) request.getMinimumBalance());
            if (!request.getOperationKey().isEmpty()) {
                domainReq.setOperationKey(request.getOperationKey());
            }
        }

        return saldoCommandService.updateSaldoBalance(domainReq)
                .map(apiResp -> {
                    ApiResponseSaldo.Builder builder = ApiResponseSaldo.newBuilder()
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
    public Uni<ApiResponseSaldo> updateSaldoWithdraw(UpdateSaldoWithdrawRequest request) {
        com.sanedge.saldo.domain.requests.UpdateSaldoWithdraw domainReq = new com.sanedge.saldo.domain.requests.UpdateSaldoWithdraw();
        domainReq.setCardNumber(request.getCardNumber());
        domainReq.setTotalBalance((long) request.getTotalBalance());
        domainReq.setWithdrawAmount((long) request.getWithdrawAmount());
        if (request.getWithdrawTime() != null && !request.getWithdrawTime().isEmpty()) {
            domainReq.setWithdrawTime(java.time.LocalDateTime.parse(request.getWithdrawTime()));
        }
        if (request.getDeltaBalance() != 0) {
            domainReq.setDeltaBalance((long) request.getDeltaBalance());
            domainReq.setMinimumBalance((long) request.getMinimumBalance());
            if (!request.getOperationKey().isEmpty()) {
                domainReq.setOperationKey(request.getOperationKey());
            }
        }

        return saldoCommandService.updateSaldoWithdraw(domainReq)
                .map(apiResp -> {
                    ApiResponseSaldo.Builder builder = ApiResponseSaldo.newBuilder()
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
    public Uni<ApiResponseSaldoDeleteAt> trashedSaldo(FindByIdSaldoRequest request) {
        return saldoCommandService.trash(request.getSaldoId())
                .map(apiResp -> {
                    ApiResponseSaldoDeleteAt.Builder builder = ApiResponseSaldoDeleteAt.newBuilder()
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
    public Uni<ApiResponseSaldoDeleteAt> restoreSaldo(FindByIdSaldoRequest request) {
        return saldoCommandService.restore(request.getSaldoId())
                .map(apiResp -> {
                    ApiResponseSaldoDeleteAt.Builder builder = ApiResponseSaldoDeleteAt.newBuilder()
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
    public Uni<ApiResponseSaldoDelete> deleteSaldoPermanent(FindByIdSaldoRequest request) {
        return saldoCommandService.delete(request.getSaldoId())
                .map(apiResp -> ApiResponseSaldoDelete.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseSaldoAll> restoreAllSaldo(Empty request) {
        return saldoCommandService.restoreAll()
                .map(apiResp -> ApiResponseSaldoAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseSaldoAll> deleteAllSaldoPermanent(Empty request) {
        return saldoCommandService.deleteAll()
                .map(apiResp -> ApiResponseSaldoAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    private pb.saldo.Saldo.SaldoResponse toProto(SaldoResponse r) {
        if (r == null) {
            return pb.saldo.Saldo.SaldoResponse.getDefaultInstance();
        }
        pb.saldo.Saldo.SaldoResponse.Builder builder = pb.saldo.Saldo.SaldoResponse.newBuilder();
        if (r.getId() != null) {
            builder.setSaldoId(r.getId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getTotalBalance() != null) {
            builder.setTotalBalance(r.getTotalBalance().intValue());
        }
        if (r.getWithdrawTime() != null) {
            builder.setWithdrawTime(r.getWithdrawTime().toString());
        }
        if (r.getWithdrawAmount() != null) {
            builder.setWithdrawAmount(r.getWithdrawAmount().intValue());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt().toString());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt().toString());
        }
        return builder.build();
    }

    private pb.saldo.Saldo.SaldoResponseDeleteAt toProto(SaldoResponseDeleteAt r) {
        if (r == null) {
            return pb.saldo.Saldo.SaldoResponseDeleteAt.getDefaultInstance();
        }
        pb.saldo.Saldo.SaldoResponseDeleteAt.Builder builder = pb.saldo.Saldo.SaldoResponseDeleteAt.newBuilder();
        if (r.getId() != null) {
            builder.setSaldoId(r.getId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getTotalBalance() != null) {
            builder.setTotalBalance(r.getTotalBalance().intValue());
        }
        if (r.getWithdrawTime() != null) {
            builder.setWithdrawTime(r.getWithdrawTime().toString());
        }
        if (r.getWithdrawAmount() != null) {
            builder.setWithdrawAmount(r.getWithdrawAmount().intValue());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt().toString());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt().toString());
        }
        if (r.getDeletedAt() != null) {
            builder.setDeletedAt(com.google.protobuf.StringValue.of(r.getDeletedAt().toString()));
        }
        return builder.build();
    }
}
