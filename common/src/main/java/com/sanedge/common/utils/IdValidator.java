package com.sanedge.common.utils;

import java.util.regex.Pattern;

import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.grpc.GrpcStatusMapper;

import io.smallrye.mutiny.Uni;

/**
 * Uniform guard for request IDs at the gRPC handler boundary.
 *
 * <p>Returns a failed {@code Uni} that is <em>already mapped</em> to a
 * {@code StatusRuntimeException(INVALID_ARGUMENT)} so handlers can short-circuit
 * before calling the service.</p>
 *
 * <pre>{@code
 * Uni<X> bad = IdValidator.invalidObjectId("Card id", request.getCardId());
 * if (bad != null) {
 *     return bad;
 * }
 * }</pre>
 */
public final class IdValidator {

    private static final Pattern OBJECT_ID_PATTERN = Pattern.compile("[0-9a-fA-F]{24}");

    private IdValidator() {
    }

    /**
     * Returns a failed {@code Uni} (already mapped to {@code INVALID_ARGUMENT})
     * when {@code value} is {@code null} or not a valid 24-char hex ObjectId, or
     * {@code null} when the value is valid so the caller can continue.
     */
    @SuppressWarnings("unchecked")
    public static <T> Uni<T> invalidObjectId(String field, String value) {
        if (value == null || !OBJECT_ID_PATTERN.matcher(value).matches()) {
            return (Uni<T>) (Uni<?>) Uni.createFrom()
                    .failure(new InvalidRequestException(field + " must be a valid id"))
                    .onFailure().transform(GrpcStatusMapper::toStatusRuntimeException);
        }
        return null;
    }
}
