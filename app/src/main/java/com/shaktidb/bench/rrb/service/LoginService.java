package com.shaktidb.bench.rrb.service;

import com.shaktidb.bench.rrb.auth.TokenService;
import com.shaktidb.bench.rrb.auth.TokenService.SessionToken;
import com.shaktidb.bench.rrb.config.RrbProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Login = ONE transaction on the PRIMARY:
 *   1. index-only read of candidate by registration_no (covering index)
 *   2. INSERT user_session
 *   3. INSERT audit_event
 * A failed login (unknown reg no / wrong DOB) writes only the audit row.
 */
@Service
public class LoginService {

    public record LoginResult(String token, long candidateId, int cenId, Instant expiresAt) {}

    private record CandidateAuth(long candidateId, LocalDate dob) {}

    private static final String FIND_CANDIDATE =
            "SELECT candidate_id, dob FROM rrb.candidate WHERE registration_no = ?";
    private static final String INSERT_SESSION =
            "INSERT INTO rrb.user_session (session_id, candidate_id, cen_id, created_at, expires_at, client_ip, user_agent) "
          + "VALUES (?, ?, ?, ?, ?, CAST(? AS inet), ?)";
    private static final String INSERT_AUDIT =
            "INSERT INTO rrb.audit_event (event_time, event_type, candidate_id, session_id, client_ip, outcome, detail) "
          + "VALUES (?, 'LOGIN', ?, ?, CAST(? AS inet), ?, CAST(? AS jsonb))";

    private final JdbcTemplate jdbc;
    private final TokenService tokens;
    private final Duration ttl;
    private final Counter ok;
    private final Counter failed;

    public LoginService(JdbcTemplate jdbc, TokenService tokens, RrbProperties props, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.tokens = tokens;
        this.ttl = props.token().ttl();
        this.ok = Counter.builder("rrb.login").tag("outcome", "success").register(registry);
        this.failed = Counter.builder("rrb.login").tag("outcome", "fail").register(registry);
    }

    @Transactional
    public Optional<LoginResult> login(String registrationNo, LocalDate dob, int cenId,
                                       String clientIp, String userAgent) {
        List<CandidateAuth> found = jdbc.query(FIND_CANDIDATE,
                (rs, i) -> new CandidateAuth(rs.getLong(1), rs.getObject(2, LocalDate.class)),
                registrationNo);

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);   // timestamptz precision
        String detail = "{\"cenId\":" + cenId + "}";

        if (found.isEmpty() || !found.get(0).dob().equals(dob)) {
            Long candidateId = found.isEmpty() ? null : found.get(0).candidateId();
            jdbc.update(INSERT_AUDIT, odt(now), new SqlParameterValue(Types.BIGINT, candidateId),
                    new SqlParameterValue(Types.OTHER, null), clientIp, "FAIL", detail);
            failed.increment();
            return Optional.empty();
        }

        long candidateId = found.get(0).candidateId();
        UUID sessionId = UUID.randomUUID();
        Instant expiresAt = now.plus(ttl);

        jdbc.update(INSERT_SESSION, sessionId, candidateId, cenId, odt(now), odt(expiresAt),
                clientIp, userAgent);
        jdbc.update(INSERT_AUDIT, odt(now), candidateId, sessionId, clientIp, "SUCCESS", detail);
        ok.increment();

        String token = tokens.issue(new SessionToken(sessionId, candidateId, cenId, now, expiresAt));
        return Optional.of(new LoginResult(token, candidateId, cenId, expiresAt));
    }

    private static OffsetDateTime odt(Instant i) {
        return i.atOffset(ZoneOffset.UTC);
    }
}
