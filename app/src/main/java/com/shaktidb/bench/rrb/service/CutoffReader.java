package com.shaktidb.bench.rrb.service;

import com.shaktidb.bench.rrb.service.CutoffService.CutoffView;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Separate bean so the cache loader goes through the @Transactional proxy (-> replica). */
@Component
public class CutoffReader {

    private static final String FIND_CUTOFFS =
            "SELECT rrb_code, category, cutoff_score, vacancies FROM rrb.exam_cutoff "
          + "WHERE cen_id = ? ORDER BY rrb_code, category";

    private final JdbcTemplate jdbc;

    public CutoffReader(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(readOnly = true)
    public List<CutoffView> findByCen(Integer cenId) {
        return List.copyOf(jdbc.query(FIND_CUTOFFS, (rs, i) -> new CutoffView(
                rs.getInt(1), rs.getString(2), rs.getBigDecimal(3), rs.getInt(4)), cenId));
    }
}
