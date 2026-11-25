package com.shaktidb.bench.rrb.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.shaktidb.bench.rrb.config.RrbProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * Cut-offs for a CEN (21 RRBs x 5 categories = 105 rows), cached in memory for
 * rrb.cutoff.cache-ttl (default 60 s). Caffeine coalesces concurrent misses, so at most one
 * DB read per CEN per app server per TTL. cache-ttl=0 sends every request to the replica.
 */
@Service
public class CutoffService {

    public record CutoffView(int rrbCode, String category, BigDecimal cutoffScore, int vacancies) {}

    private final CutoffReader reader;
    private final LoadingCache<Integer, List<CutoffView>> cache;   // null when disabled

    private final DbTimer dbTimer;

    public CutoffService(CutoffReader reader, RrbProperties props, MeterRegistry registry, DbTimer dbTimer) {
        this.reader = reader;
        this.dbTimer = dbTimer;
        Duration ttl = props.cutoff().cacheTtl();
        if (ttl == null || ttl.isZero()) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(100).recordStats()
                    .build(this::load);
            CaffeineCacheMetrics.monitor(registry, cache, "cutoff");
        }
    }

    /** One replica read (through the CutoffReader transaction proxy), timed as op=cutoff. */
    private List<CutoffView> load(int cenId) {
        return dbTimer.time("cutoff", () -> reader.findByCen(cenId));
    }

    public List<CutoffView> find(int cenId, Integer rrbCode, String category) {
        List<CutoffView> all = cache != null ? cache.get(cenId) : load(cenId);
        if (rrbCode == null && category == null) {
            return all;
        }
        return all.stream()
                .filter(c -> rrbCode == null || c.rrbCode() == rrbCode)
                .filter(c -> category == null || c.category().equalsIgnoreCase(category))
                .toList();
    }
}
