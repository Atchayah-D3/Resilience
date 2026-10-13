# Research: NL-I-02 Index Corruption

## R1 -- Kind of corruption

- **Decision**: one byte flipped in a btree *leaf* page, with the database cleanly stopped,
  proven by `pg_checksums` at exactly that block (Clarification 2026-10-12).
- **Rationale**: the Framework row says "Corrupt a btree file"; Arch §5 asks for precise
  targeted byte writes. It reuses NL-I-01's proven landing sequence (fast stop,
  `pg_controldata` "shut down", read-back, `pg_checksums --filenode`). With
  `data_checksums=on` (Framework §16.2) the engine refuses the page rather than returning wrong
  rows, which is exactly what "no wrong results returned first" asks to be shown.
- **Alternatives**: checksum-valid logical corruption (keys out of order, checksum recomputed) --
  exercises amcheck's own btree logic, but PostgreSQL does not verify order on read, so index
  scans can return wrong rows and the run would most likely fail by design; also needs a
  checksum routine on the driver host. Rejected for now, can be added later as another
  `relation_kind`.

## R2 -- Selecting the leaf page

- **Decision**: 20 000 sequential `int` keys, index built after the load (bulk build): block 0
  is the metapage, leaves follow in order; an internal page is allocated when a level above
  first needs one (the first early, at block 3), so a page is never assumed. The chosen block is
  `pages // 3`, then **proved** to be a leaf from its bytes while the database is down: the
  btree special space is the last 16 bytes of the page, `btpo_flags` sits at offset 12 of it,
  and its low byte (little-endian) has `BTP_LEAF` (0x01) set.
- **Rationale**: no `pageinspect` is installed on the target (the harness never installs
  anything); the on-disk check is cheap and fails closed. The adapter owns the layout knowledge
  and hands the driver a generic `page_check` (offset, mask, expected value, meaning).
- **Byte flipped** (revised 2026-10-13): located from the page, not a fixed offset. The
  driver reads the whole page with the database down; the adapter checks `pd_special ==
  block_size - 16`, header bounds, `BTP_LEAF` and level 0, then takes the middle data entry
  (line pointer 2 onward when the page has a right sibling -- 1 is its high key), checks it is
  a normal, plain entry (`t_info` size equals `lp_len`, no `INDEX_ALT_TID_MASK`, key one of the
  rows written) and flips the low byte of its heap row pointer's offset number. An engine that
  used the page unverified would fetch a different heap slot (another row, or none): a wrong
  result the read sees. The earlier fixed `block_size - 20` could land in alignment padding,
  where an unverified read still returns the right rows.

## R3 -- Forcing and proving an index read

- **Decision** (revised 2026-10-13): per attempt, a fresh connection with `enable_seqscan`,
  `enable_bitmapscan`, `enable_sort` and `enable_indexonlyscan` off; `EXPLAIN (FORMAT JSON)`
  must show a plain **Index Scan** on the target index; then
  `SELECT id, payload FROM t WHERE id >= 1 ORDER BY id`. Ids are compared with `1..N` exactly.
- **Why not an Index Only Scan**: the first lab run (NL-I-02-20261013T051043Z-ac5f31) chose
  one for `SELECT id`. It returns the key stored in the index and follows the row pointer only
  to check visibility, so the corrupted pointer -- redirected from (40,125) to (40,130), another
  visible row -- would still print the right id if the page were used unverified. A plain Index
  Scan returns the table row's id (6410 instead of 6405): a wrong result the read can see.
  `payload` is not in the index, so an Index Only Scan cannot answer the query at all.
- **Rationale**: a full-range ordered index scan walks every leaf, so it must reach the damaged
  page. A plan not using the index cannot speak for "no wrong results" (NOT_MEASURED).
  Planner: the table has no statistics (autovacuum off, never analyzed), so the planner does not
  probe the index ends (`get_actual_variable_range` needs a histogram).

## R4 -- REINDEX and proof of restoration

- **Decision**: `REINDEX INDEX resilience.corruption_target_idx` (non-concurrent: it builds new
  storage from the heap and never reads the damaged pages), then one forced index read and
  `pg_amcheck --relation=<table> --heapallindexed` (checks the heap and its index, including
  that every heap tuple is in the index).
- **Rationale**: REINDEX is the Framework's named recovery path; restoration is proven by the
  engine's own answers, not by REINDEX having returned. Since 2026-10-13 also by the index file:
  `pg_relation_filepath` after REINDEX must differ from the damaged file (unknown -> NOT_MEASURED),
  so a rebuild that kept serving the damaged storage cannot pass.

## R5 -- Whole-database check exclusions

- **Decision**: the run-wide `pg_amcheck --heapallindexed` excludes both the corruption table
  and its index; both are covered by the post-REINDEX check of R4 instead. Disclosed.
- **Rationale**: pg_amcheck checks a table's dependent indexes; whether an index exclusion
  overrides that was not verified on the lab build, and an unexpected hit would be a false
  `structural_integrity_errors`. Excluding both is unambiguous.

## R6 -- What is gated

Gated (Framework §10.4, §16.3, and the clean stop/start of the fault): `wrong_results_returned
== 0`, `amcheck_detects_target == true`, `reindex_restores == true`, `corruption_outside_target
== 0`, `structural_integrity_errors == 0`, `rpo_txn == 0`; and (Framework §16.2, NEEDS
SIGN-OFF, added 2026-10-13) `detection_identifies_block == true` -- otherwise a read that
walked the damaged leaf unverified and returned the right rows would pass. Reported only:
`corruption_detected_on_read`, `detection_repeatable`,
`checksum_failure_reported`, `reindex_duration_s`, `corruption_count`, `indeterminate_txn`.
Steady-state/workload values copied from NL-I-01: NEEDS SIGN-OFF.
