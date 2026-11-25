package com.shaktidb.bench.rrb.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("rrb")
public record RrbProperties(int defaultCenId, Token token, Session session, Cutoff cutoff) {

    public record Token(String secret, Duration ttl) {}

    /** validateInDb=true: every authenticated request also reads user_session on the primary. */
    public record Session(boolean validateInDb) {}

    /** cacheTtl=0 disables the in-memory cut-off cache. */
    public record Cutoff(Duration cacheTtl) {}
}
