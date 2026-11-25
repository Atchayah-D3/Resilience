package com.shaktidb.bench.rrb.web;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Overload shows up as clean 503s (pool wait exceeded / DB timeout), not hangs, so the load
 * tool counts them as errors. rrb_db_errors_total{kind} separates the causes.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private final MeterRegistry registry;

    public ApiExceptionHandler(MeterRegistry registry) {
        this.registry = registry;
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
                       MethodArgumentTypeMismatchException.class})
    public ResponseEntity<Map<String, Object>> badRequest(Exception e) {
        return ApiController.error(HttpStatus.BAD_REQUEST, "invalid request");
    }

    @ExceptionHandler(CannotGetJdbcConnectionException.class)
    public ResponseEntity<Map<String, Object>> noConnection(CannotGetJdbcConnectionException e) {
        registry.counter("rrb.db.errors", "kind", "pool_timeout").increment();
        return ApiController.error(HttpStatus.SERVICE_UNAVAILABLE, "database busy");
    }

    @ExceptionHandler({QueryTimeoutException.class, TransientDataAccessException.class})
    public ResponseEntity<Map<String, Object>> transientDb(DataAccessException e) {
        registry.counter("rrb.db.errors", "kind", "transient").increment();
        return ApiController.error(HttpStatus.SERVICE_UNAVAILABLE, "database busy");
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> db(DataAccessException e) {
        registry.counter("rrb.db.errors", "kind", "other").increment();
        log.warn("DB error: {}", e.getMostSpecificCause().getMessage());
        return ApiController.error(HttpStatus.INTERNAL_SERVER_ERROR, "database error");
    }
}
