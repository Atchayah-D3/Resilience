package com.shaktidb.bench.rrb.auth;

import com.shaktidb.bench.rrb.config.RrbProperties;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Service;

/**
 * Stateless signed bearer token (JWT-like, HMAC-SHA256) that also names the user_session row.
 *
 * <p>Format: base64url("sessionId|candidateId|cenId|createdAtMicros|expiresAtEpochSec") "." base64url(hmac)
 *
 * <p>createdAt is carried so the optional DB check can hit the single hourly user_session
 * partition by primary key (created_at, session_id).
 */
@Service
public class TokenService {

    public record SessionToken(UUID sessionId, long candidateId, int cenId,
                               Instant createdAt, Instant expiresAt) {}

    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private final Mac prototype;

    public TokenService(RrbProperties props) throws GeneralSecurityException {
        byte[] key = props.token().secret().getBytes(StandardCharsets.UTF_8);
        if (key.length < 32) {
            throw new IllegalStateException("rrb.token.secret must be at least 32 bytes");
        }
        prototype = Mac.getInstance("HmacSHA256");
        prototype.init(new SecretKeySpec(key, "HmacSHA256"));
    }

    public String issue(SessionToken t) {
        String payload = t.sessionId() + "|" + t.candidateId() + "|" + t.cenId() + "|"
                + ChronoUnit.MICROS.between(Instant.EPOCH, t.createdAt()) + "|"
                + t.expiresAt().getEpochSecond();
        byte[] p = payload.getBytes(StandardCharsets.UTF_8);
        return B64.encodeToString(p) + "." + B64.encodeToString(mac().doFinal(p));
    }

    /** Empty if malformed, badly signed or expired. */
    public Optional<SessionToken> verify(String token, Instant now) {
        if (token == null) {
            return Optional.empty();
        }
        int dot = token.indexOf('.');
        if (dot <= 0 || dot == token.length() - 1) {
            return Optional.empty();
        }
        try {
            byte[] p = B64D.decode(token.substring(0, dot));
            byte[] sig = B64D.decode(token.substring(dot + 1));
            if (!MessageDigest.isEqual(sig, mac().doFinal(p))) {
                return Optional.empty();
            }
            String[] f = new String(p, StandardCharsets.UTF_8).split("\\|");
            if (f.length != 5) {
                return Optional.empty();
            }
            Instant exp = Instant.ofEpochSecond(Long.parseLong(f[4]));
            if (!now.isBefore(exp)) {
                return Optional.empty();
            }
            return Optional.of(new SessionToken(UUID.fromString(f[0]), Long.parseLong(f[1]),
                    Integer.parseInt(f[2]), Instant.EPOCH.plus(Long.parseLong(f[3]), ChronoUnit.MICROS), exp));
        } catch (IllegalArgumentException e) {   // bad base64, number or UUID
            return Optional.empty();
        }
    }

    /** Mac is not thread-safe; cloning an initialised prototype is cheaper than getInstance+init. */
    private Mac mac() {
        try {
            return (Mac) prototype.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
