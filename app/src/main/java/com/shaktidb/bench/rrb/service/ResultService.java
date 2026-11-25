package com.shaktidb.bench.rrb.service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Result read = one primary-key read on result, routed to the REPLICA. */
@Service
public class ResultService {

    public record ResultView(int cenId, long candidateId, String rollNo, int rrbCode, String category,
                             BigDecimal rawScore, BigDecimal normalizedScore, String status,
                             OffsetDateTime publishedAt) {}

    private static final String FIND_RESULT =
            "SELECT roll_no, rrb_code, category, raw_score, normalized_score, status, published_at "
          + "FROM rrb.result WHERE cen_id = ? AND candidate_id = ?";

    private final JdbcTemplate jdbc;

    public ResultService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(readOnly = true)
    public Optional<ResultView> find(int cenId, long candidateId) {
        List<ResultView> rows = jdbc.query(FIND_RESULT, (rs, i) -> new ResultView(
                cenId, candidateId,
                rs.getString(1), rs.getInt(2), rs.getString(3),
                rs.getBigDecimal(4), rs.getBigDecimal(5), rs.getString(6),
                rs.getObject(7, OffsetDateTime.class)), cenId, candidateId);
        return rows.stream().findFirst();
    }
}
