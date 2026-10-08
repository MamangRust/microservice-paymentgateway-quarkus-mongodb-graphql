package com.sanedge.saldo.repository;

import java.time.Instant;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.sanedge.common.domain.response.PagedResult;
import com.sanedge.saldo.entity.Saldo;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.quarkus.mongodb.panache.reactive.ReactivePanacheQuery;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SaldoRepository implements ReactivePanacheMongoRepository<Saldo> {

    private Uni<PagedResult<Saldo>> paged(ReactivePanacheQuery<Saldo> query, int page, int size) {
        return Uni.combine().all().unis(
                query.page(page, size).list(),
                query.count())
                .asTuple()
                .map(tuple -> new PagedResult<>(tuple.getItem1(), tuple.getItem2().intValue()));
    }

    private ReactivePanacheQuery<Saldo> search(boolean trashed, String keyword) {
        String deletedFilter = trashed ? "'deletedAt': { '$ne': null }" : "'deletedAt': null";
        if (keyword == null) {
            return find("{ " + deletedFilter + " }", Sort.ascending("_id"));
        }
        return find("{ " + deletedFilter + ", 'cardNumber': { '$regex': ?1, '$options': 'i' } }",
                Sort.ascending("_id"), Pattern.quote(keyword));
    }

    public Uni<PagedResult<Saldo>> findSaldos(String keyword, int page, int size) {
        return paged(search(false, keyword), page, size);
    }

    public Uni<Saldo> findByCardNumber(String cardNumber) {
        return find("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber).firstResult();
    }

    public Uni<PagedResult<Saldo>> findActiveSaldos(String keyword, int page, int size) {
        return findSaldos(keyword, page, size);
    }

    public Uni<PagedResult<Saldo>> findTrashedSaldos(String keyword, int page, int size) {
        return paged(search(true, keyword), page, size);
    }

    public Uni<Saldo> trashed(String saldoId) {
        return find("{ '_id': ?1, 'deletedAt': null }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        saldo.setDeletedAt(Instant.now());
                        return saldo.update().map(v -> saldo);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Saldo> restore(String saldoId) {
        return find("{ '_id': ?1, 'deletedAt': { '$ne': null } }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        saldo.setDeletedAt(null);
                        return saldo.update().map(v -> saldo);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Integer> updateBalanceByCardNumber(String cardNumber, Long newBalance) {
        return update("{ '$set': { 'totalBalance': ?1, 'updatedAt': ?2 } }",
                newBalance != null ? newBalance.intValue() : 0, Instant.now())
                .where("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .map(count -> count.intValue());
    }

    public Uni<Integer> updateWithdrawByCardNumber(String cardNumber, Long withdrawAmount) {
        Instant now = Instant.now();
        return update("{ '$set': { 'withdrawAmount': ?1, 'withdrawTime': ?2, 'updatedAt': ?3 } }",
                withdrawAmount != null ? withdrawAmount.intValue() : 0, now, now)
                .where("{ 'cardNumber': ?1, 'deletedAt': null }", cardNumber)
                .map(count -> count.intValue());
    }

    public Uni<Boolean> deletePermanent(String saldoId) {
        return find("{ '_id': ?1 }", new ObjectId(saldoId)).firstResult()
                .chain(saldo -> {
                    if (saldo != null) {
                        return saldo.delete().map(v -> true);
                    }
                    return Uni.createFrom().item(false);
                });
    }

    public Uni<Boolean> restoreAllDeleted() {
        return update("{ '$set': { 'deletedAt': null } }")
                .where("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }

    public Uni<Boolean> deleteAllDeleted() {
        return delete("{ 'deletedAt': { '$ne': null } }")
                .map(count -> count > 0);
    }
}
