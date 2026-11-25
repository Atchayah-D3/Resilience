package com.shaktidb.bench.rrb.web;

import com.shaktidb.bench.rrb.auth.TokenService.SessionToken;
import com.shaktidb.bench.rrb.config.RrbProperties;
import com.shaktidb.bench.rrb.service.CutoffService;
import com.shaktidb.bench.rrb.service.CutoffService.CutoffView;
import com.shaktidb.bench.rrb.service.DbTimer;
import com.shaktidb.bench.rrb.service.LoginService;
import com.shaktidb.bench.rrb.service.LoginService.LoginResult;
import com.shaktidb.bench.rrb.service.ResultService;
import com.shaktidb.bench.rrb.service.ResultService.ResultView;
import com.shaktidb.bench.rrb.service.SessionService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The three quick-win endpoints:
 * <pre>
 *   POST /api/v1/auth/login          {registrationNo, dob, cenId?}  -> token      (primary: 1 read + 2 inserts)
 *   GET  /api/v1/results/{cenId}     Authorization: Bearer token     -> result     (replica: 1 PK read)
 *   GET  /api/v1/cutoffs/{cenId}     ?rrbCode=&category=             -> cut-offs   (app cache, 60 s)
 * </pre>
 */
@RestController
@RequestMapping("/api/v1")
public class ApiController {

    public record LoginRequest(
            @NotBlank @Pattern(regexp = "[A-Za-z0-9]{1,16}") String registrationNo,
            @NotNull LocalDate dob,
            Integer cenId) {}

    public record LoginResponse(String token, long candidateId, int cenId, Instant expiresAt) {}

    private static final String BEARER = "Bearer ";

    private final LoginService login;
    private final SessionService sessions;
    private final ResultService results;
    private final CutoffService cutoffs;
    private final DbTimer dbTimer;
    private final int defaultCenId;

    public ApiController(LoginService login, SessionService sessions, ResultService results,
                         CutoffService cutoffs, DbTimer dbTimer, RrbProperties props) {
        this.login = login;
        this.sessions = sessions;
        this.results = results;
        this.cutoffs = cutoffs;
        this.dbTimer = dbTimer;
        this.defaultCenId = props.defaultCenId();
    }

    @PostMapping("/auth/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest req, HttpServletRequest http) {
        int cenId = req.cenId() != null ? req.cenId() : defaultCenId;
        String ip = ClientInfo.ip(http);
        String ua = ClientInfo.userAgent(http);
        Optional<LoginResult> r = dbTimer.time("login",          // around the transaction proxy: incl. COMMIT
                () -> login.login(req.registrationNo(), req.dob(), cenId, ip, ua));
        return r.<ResponseEntity<?>>map(ok -> ResponseEntity.ok(
                        new LoginResponse(ok.token(), ok.candidateId(), ok.cenId(), ok.expiresAt())))
                .orElseGet(() -> error(HttpStatus.UNAUTHORIZED, "invalid registration number or date of birth"));
    }

    @GetMapping("/results/{cenId}")
    public ResponseEntity<?> result(@PathVariable int cenId,
                                    @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth) {
        String token = auth != null && auth.startsWith(BEARER) ? auth.substring(BEARER.length()) : null;
        Optional<SessionToken> session = sessions.authenticate(token);
        if (session.isEmpty()) {
            return error(HttpStatus.UNAUTHORIZED, "missing, invalid or expired token");
        }
        if (session.get().cenId() != cenId) {
            return error(HttpStatus.FORBIDDEN, "token was issued for another notification");
        }
        long candidateId = session.get().candidateId();
        Optional<ResultView> r = dbTimer.time("result", () -> results.find(cenId, candidateId));
        return r.<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> error(HttpStatus.NOT_FOUND, "no result for this candidate"));
    }

    @GetMapping("/cutoffs/{cenId}")
    public ResponseEntity<?> cutoffs(@PathVariable int cenId,
                                     @RequestParam(required = false) Integer rrbCode,
                                     @RequestParam(required = false) String category) {
        List<CutoffView> rows = cutoffs.find(cenId, rrbCode, category);
        if (rows.isEmpty()) {
            return error(HttpStatus.NOT_FOUND, "no cut-off published");
        }
        return ResponseEntity.ok(Map.of("cenId", cenId, "cutoffs", rows));
    }

    static ResponseEntity<Map<String, Object>> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("status", status.value(), "error", message));
    }
}
