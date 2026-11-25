package com.shaktidb.bench.rrb.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Read-only transactions go to the replica pool, everything else to the primary. */
public class RoutingDataSource extends AbstractRoutingDataSource {

    public enum Route { PRIMARY, REPLICA }

    @Override
    protected Object determineCurrentLookupKey() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly()
                ? Route.REPLICA : Route.PRIMARY;
    }
}
