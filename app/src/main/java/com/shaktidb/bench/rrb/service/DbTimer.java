package com.shaktidb.bench.rrb.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * DB-side latency for the acceptance criteria (proposal 11.2): AC-02 "read latency at DB"
 * (p99 <= 50 ms, p99.9 <= 200 ms) and AC-03 "write / login transaction latency at DB" (p99 <= 100 ms).
 *
 * <p>Metric {@code rrb_db_tx_seconds{op}} is a Prometheus histogram, so percentiles can be computed
 * for any time window and merged across app servers. One sample = one call of a transactional
 * service method, measured around the transaction proxy: it starts before the pooled connection is
 * taken and ends after COMMIT, so it covers pool wait (app pool and sdbPool), every statement and
 * the commit, and excludes HTTP, JSON and token work. Pool wait alone is visible in
 * {@code hikaricp_connections_pending}; when that is zero, this is the database time.
 *
 * <ul>
 *   <li>{@code op=login}  login transaction on the primary: candidate read + session insert + audit insert</li>
 *   <li>{@code op=result} result read on the replica (one PK read)</li>
 *   <li>{@code op=cutoff} cut-off read on the replica (cache misses only)</li>
 * </ul>
 */
@Component
public class DbTimer {

    private final Map<String, Timer> timers;

    public DbTimer(MeterRegistry registry) {
        this.timers = Map.of(
                "login", timer(registry, "login", "login transaction on the primary"),
                "result", timer(registry, "result", "result read on the replica"),
                "cutoff", timer(registry, "cutoff", "cut-off read on the replica (cache miss)"));
    }

    private static Timer timer(MeterRegistry registry, String op, String what) {
        return Timer.builder("rrb.db.tx")
                .description("DB-side time of one transaction incl. pool wait and commit: " + what)
                .tag("op", op)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(100_000))     // 0.1 ms
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(registry);
    }

    public <T> T time(String op, Supplier<T> call) {
        long t0 = System.nanoTime();
        try {
            return call.get();
        } finally {
            timers.get(op).record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
        }
    }
}
