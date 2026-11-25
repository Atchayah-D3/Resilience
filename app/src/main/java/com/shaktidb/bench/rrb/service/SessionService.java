package com.shaktidb.bench.rrb.service;

import com.shaktidb.bench.rrb.auth.TokenService;
import com.shaktidb.bench.rrb.auth.TokenService.SessionToken;
import com.shaktidb.bench.rrb.config.RrbProperties;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Validates a bearer token. Signature and expiry are always checked in memory. With
 * rrb.session.validate-in-db=true the user_session row is also read on the PRIMARY
 * (read-write transaction), because a replica may not have the just-written row yet.
 *
 * <p>When validate-in-db is false no statement runs, so the lazy connection proxy never
 * takes a pooled connection for this transaction.
 */
@Service
public class SessionService {

    private static final String SESSION_ACTIVE =
            "SELECT 1 FROM rrb.user_session WHERE created_at = ? AND session_id = ? AND expires_at > now()";

    private final TokenService tokens;
    private final JdbcTemplate jdbc;
    private final boolean validateInDb;

    public SessionService(TokenService tokens, JdbcTemplate jdbc, RrbProperties props) {
        this.tokens = tokens;
        this.jdbc = jdbc;
        this.validateInDb = props.session().validateInDb();
    }

    @Transactional
    public Optional<SessionToken> authenticate(String bearerToken) {
        Optional<SessionToken> t = tokens.verify(bearerToken, Instant.now());
        if (t.isEmpty() || !validateInDb) {
            return t;
        }
        SessionToken s = t.get();
        boolean active = !jdbc.queryForList(SESSION_ACTIVE,
                s.createdAt().atOffset(ZoneOffset.UTC), s.sessionId()).isEmpty();
        return active ? t : Optional.empty();
    }
}
