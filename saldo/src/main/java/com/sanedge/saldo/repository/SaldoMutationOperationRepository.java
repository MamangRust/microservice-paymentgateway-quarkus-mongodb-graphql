package com.sanedge.saldo.repository;

import com.sanedge.saldo.entity.SaldoMutationOperation;

import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SaldoMutationOperationRepository implements ReactivePanacheMongoRepository<SaldoMutationOperation> {
    public Uni<SaldoMutationOperation> findByOperationKey(String operationKey) {
        return find("{ 'operationKey': ?1 }", operationKey).firstResult();
    }
}
