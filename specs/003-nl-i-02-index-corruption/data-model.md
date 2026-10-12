# Data Model: NL-I-02

## Corruption target (adapter -> driver, kept in preflight and inject detail)

| Field | Meaning |
|---|---|
| `relation` | the relation whose file is damaged (`resilience.corruption_target_idx`) |
| `table` | the owning table, dropped by revert (`resilience.corruption_target`) |
| `relation_kind` | `btree_index` (`heap` for NL-I-01) |
| `relation_path`, `filenode` | main fork relative to the data directory |
| `block`, `pages`, `block_size`, `byte_in_page` | where the byte is flipped |
| `page_check` | `{byte_in_page, mask, equals, meaning}` -- verified with the database down, before writing |
| `rows` | rows written (keys 1..rows) |
| `integrity_exclusions` | relations left out of the run-wide check: table and index |

## Index read attempt (adapter -> orchestrator, `facts.corruption_read.attempts[]`)

`used_index` (bool / None when EXPLAIN itself failed), `plan` (top node types), `error`,
`sqlstate`, `message`, `block`, `relation_path`, `rows` (count returned), `matches_written` (bool).

## Rebuild (`facts.index_rebuild`)

`reindex` `{ok, duration_s, error, relation_path_after}`, `read_after` (one attempt as above),
`amcheck_after` (`amcheck_relation(table, heapallindexed=True)` result).

## Measures

| Measure | Value | NOT_MEASURED when |
|---|---|---|
| `wrong_results_returned` | count of index reads whose rows != written | a read neither used the index nor failed with the corruption code |
| `amcheck_detects_target` | checker on the index reported it | checker inconclusive |
| `reindex_restores` | REINDEX ok AND read-after matches via index AND amcheck-after clean | any of the three inconclusive |
| `reindex_duration_s` | seconds (reported) | REINDEX failed |
| others | as NL-I-01 | as NL-I-01 |
