package com.shaktidb.bench.rrb.config;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

/**
 * Two Hikari pools (primary, replica) behind an {@link RoutingDataSource}.
 *
 * <p>{@code @Transactional} (read-write) -> primary pool; {@code @Transactional(readOnly = true)}
 * -> replica pool. The routing source is wrapped in a {@link LazyConnectionDataSourceProxy} so
 * the physical connection is taken only at the first statement, after Spring has marked the
 * transaction read-only. Without the proxy every transaction would be routed to the primary.
 *
 * <p>Spring Boot binds Hikari metrics (hikaricp_connections_*) for both pool beans automatically.
 */
@Configuration(proxyBeanMethods = false)
public class DataSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

    @Bean
    @ConfigurationProperties("rrb.datasource.primary")
    public HikariDataSource primaryDataSource() {
        return new HikariDataSource();
    }

    @Bean
    @ConditionalOnProperty(name = "rrb.datasource.replica-enabled", havingValue = "true")
    @ConfigurationProperties("rrb.datasource.replica")
    public HikariDataSource replicaDataSource() {
        return new HikariDataSource();
    }

    @Bean
    @Primary
    public DataSource dataSource(@Qualifier("primaryDataSource") HikariDataSource primary,
                                 @Qualifier("replicaDataSource")
                                 org.springframework.beans.factory.ObjectProvider<HikariDataSource> replica) {
        DataSource readTarget = replica.getIfAvailable(() -> primary);

        Map<Object, Object> targets = new HashMap<>();
        targets.put(RoutingDataSource.Route.PRIMARY, primary);
        targets.put(RoutingDataSource.Route.REPLICA, readTarget);

        RoutingDataSource routing = new RoutingDataSource();
        routing.setTargetDataSources(targets);
        routing.setDefaultTargetDataSource(primary);
        routing.afterPropertiesSet();

        LazyConnectionDataSourceProxy lazy = new LazyConnectionDataSourceProxy();
        lazy.setTargetDataSource(routing);
        lazy.setDefaultAutoCommit(false);   // matches auto-commit: false on both pools
        lazy.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        lazy.afterPropertiesSet();

        log.info("Routing: writes -> {}, reads -> {}", primary.getPoolName(),
                readTarget == primary ? primary.getPoolName() + " (replica disabled)"
                                      : ((HikariDataSource) readTarget).getPoolName());
        return lazy;
    }

    /** Opens both pools at startup: fails fast on bad config and avoids a cold first spike. */
    @Bean
    public ApplicationRunner warmPools(@Qualifier("primaryDataSource") HikariDataSource primary,
                                       @Qualifier("replicaDataSource")
                                       org.springframework.beans.factory.ObjectProvider<HikariDataSource> replica,
                                       @Value("${rrb.datasource.replica-enabled:false}") boolean replicaEnabled) {
        return args -> {
            warm(primary);
            if (replicaEnabled) {
                warm(replica.getObject());
            }
        };
    }

    private static void warm(HikariDataSource ds) throws Exception {
        try (Connection c = ds.getConnection()) {
            log.info("Pool {} ready: {} (max {})", ds.getPoolName(), c.getMetaData().getURL(),
                    ds.getMaximumPoolSize());
        }
    }
}
