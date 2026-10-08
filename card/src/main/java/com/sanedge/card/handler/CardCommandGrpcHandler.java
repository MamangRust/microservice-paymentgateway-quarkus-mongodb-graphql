package com.sanedge.card.handler;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import com.google.protobuf.Empty;
import com.sanedge.card.service.CardCommandService;
import com.sanedge.common.utils.IdValidator;

import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import pb.card.Card.FindByIdCardRequest;
import pb.card.CardCommand.ApiResponseCardAll;
import pb.card.CardCommand.ApiResponseCardDelete;
import pb.card.CardCommand.CreateCardRequest;
import pb.card.CardCommand.UpdateCardRequest;
import pb.card.MutinyCardCommandServiceGrpc;

@GrpcService
@Singleton
public class CardCommandGrpcHandler extends MutinyCardCommandServiceGrpc.CardCommandServiceImplBase {

    @Inject
    CardCommandService cardCommandService;

    @Override
    public Uni<pb.card.Card.ApiResponseCard> createCard(CreateCardRequest request) {
        Uni<pb.card.Card.ApiResponseCard> invalidUserId = IdValidator.invalidObjectId("User id",
                request.getUserId());
        if (invalidUserId != null) {
            return invalidUserId;
        }
        com.sanedge.card.domain.requests.CreateCardRequest domainReq = new com.sanedge.card.domain.requests.CreateCardRequest();
        domainReq.setUserId(request.getUserId());
        domainReq.setCardType(request.getCardType());
        if (request.hasExpireDate()) {
            domainReq.setExpireDate(toLocalDate(request.getExpireDate()));
        }
        domainReq.setCvv(request.getCvv());
        domainReq.setCardProvider(request.getCardProvider());

        return cardCommandService.createCard(domainReq)
                .map(apiResp -> {
                    pb.card.Card.ApiResponseCard.Builder builder = pb.card.Card.ApiResponseCard.newBuilder()
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
    public Uni<pb.card.Card.ApiResponseCard> updateCard(UpdateCardRequest request) {
        Uni<pb.card.Card.ApiResponseCard> invalidCardId = IdValidator.invalidObjectId("Card id",
                request.getCardId());
        if (invalidCardId != null) {
            return invalidCardId;
        }
        Uni<pb.card.Card.ApiResponseCard> invalidUserId = IdValidator.invalidObjectId("User id",
                request.getUserId());
        if (invalidUserId != null) {
            return invalidUserId;
        }
        com.sanedge.card.domain.requests.UpdateCardRequest domainReq = new com.sanedge.card.domain.requests.UpdateCardRequest();
        domainReq.setCardId(request.getCardId());
        domainReq.setUserId(request.getUserId());
        domainReq.setCardType(request.getCardType());
        if (request.hasExpireDate()) {
            domainReq.setExpireDate(toLocalDate(request.getExpireDate()));
        }
        domainReq.setCvv(request.getCvv());
        domainReq.setCardProvider(request.getCardProvider());

        return cardCommandService.updateCard(domainReq)
                .map(apiResp -> {
                    pb.card.Card.ApiResponseCard.Builder builder = pb.card.Card.ApiResponseCard.newBuilder()
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
    public Uni<pb.card.Card.ApiResponseCardDeleteAt> trashedCard(FindByIdCardRequest request) {
        Uni<pb.card.Card.ApiResponseCardDeleteAt> invalidId = IdValidator.invalidObjectId("Card id",
                request.getCardId());
        if (invalidId != null) {
            return invalidId;
        }
        return cardCommandService.trashCard(request.getCardId())
                .map(apiResp -> {
                    pb.card.Card.ApiResponseCardDeleteAt.Builder builder = pb.card.Card.ApiResponseCardDeleteAt
                            .newBuilder()
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
    public Uni<pb.card.Card.ApiResponseCardDeleteAt> restoreCard(FindByIdCardRequest request) {
        Uni<pb.card.Card.ApiResponseCardDeleteAt> invalidId = IdValidator.invalidObjectId("Card id",
                request.getCardId());
        if (invalidId != null) {
            return invalidId;
        }
        return cardCommandService.restoreCard(request.getCardId())
                .map(apiResp -> {
                    pb.card.Card.ApiResponseCardDeleteAt.Builder builder = pb.card.Card.ApiResponseCardDeleteAt
                            .newBuilder()
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
    public Uni<ApiResponseCardDelete> deleteCardPermanent(FindByIdCardRequest request) {
        Uni<ApiResponseCardDelete> invalidId = IdValidator.invalidObjectId("Card id", request.getCardId());
        if (invalidId != null) {
            return invalidId;
        }
        return cardCommandService.deleteCard(request.getCardId())
                .map(apiResp -> ApiResponseCardDelete.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseCardAll> restoreAllCard(Empty request) {
        return cardCommandService.restoreAll()
                .map(apiResp -> ApiResponseCardAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    @Override
    public Uni<ApiResponseCardAll> deleteAllCardPermanent(Empty request) {
        return cardCommandService.deleteAll()
                .map(apiResp -> ApiResponseCardAll.newBuilder()
                        .setStatus(apiResp.status())
                        .setMessage(apiResp.message())
                        .build())
                .onFailure().transform(e -> Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }

    private LocalDate toLocalDate(com.google.protobuf.Timestamp timestamp) {
        if (timestamp == null || (timestamp.getSeconds() == 0 && timestamp.getNanos() == 0)) {
            return null;
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos())
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

    private pb.card.Card.CardResponse toProto(com.sanedge.card.domain.response.CardResponse r) {
        if (r == null) {
            return pb.card.Card.CardResponse.getDefaultInstance();
        }
        pb.card.Card.CardResponse.Builder builder = pb.card.Card.CardResponse.newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getUserId() != null) {
            builder.setUserId(r.getUserId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getCardType() != null) {
            builder.setCardType(r.getCardType());
        }
        if (r.getExpireDate() != null) {
            builder.setExpireDate(r.getExpireDate());
        }
        if (r.getCvv() != null) {
            builder.setCvv(r.getCvv());
        }
        if (r.getCardProvider() != null) {
            builder.setCardProvider(r.getCardProvider());
        }
        if (r.getCreatedAt() != null) {
            builder.setCreatedAt(r.getCreatedAt());
        }
        if (r.getUpdatedAt() != null) {
            builder.setUpdatedAt(r.getUpdatedAt());
        }
        return builder.build();
    }

    private pb.card.Card.CardResponseDeleteAt toProto(com.sanedge.card.domain.response.CardResponseDeleteAt r) {
        if (r == null) {
            return pb.card.Card.CardResponseDeleteAt.getDefaultInstance();
        }
        pb.card.Card.CardResponseDeleteAt.Builder builder = pb.card.Card.CardResponseDeleteAt.newBuilder();
        if (r.getId() != null) {
            builder.setId(r.getId());
        }
        if (r.getUserId() != null) {
            builder.setUserId(r.getUserId());
        }
        if (r.getCardNumber() != null) {
            builder.setCardNumber(r.getCardNumber());
        }
        if (r.getCardType() != null) {
            builder.setCardType(r.getCardType());
        }
        if (r.getExpireDate() != null) {
            builder.setExpireDate(r.getExpireDate());
        }
        if (r.getCvv() != null) {
            builder.setCvv(r.getCvv());
        }
        if (r.getCardProvider() != null) {
            builder.setCardProvider(r.getCardProvider());
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
