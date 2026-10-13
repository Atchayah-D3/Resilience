# Data Model: NL-I-02

## Corruption target (adapter -> driver, kept in preflight and inject detail)

| Field | Meaning |
|---|---|
| `relation` | the relation whose file is damaged (`resilience.corruption_target_idx`) |
| `table` | the owning table, dropped by revert (`resilience.corruption_target`) |
| `relation_kind` | `btree_index` (`heap` for NL-I-01) |
| `relation_path`, `filenode` | main fork relative to the data directory |
| `block`, `pages`, `block_size` | the page |
| `byte_in_page` / `locate` | a fixed byte (heap), or what to locate in the page (index: a data entry's heap row pointer offset number, low byte) |
| `page_check` | `{byte_in_page, mask, equals, meaning}` -- verified with the database down, before writing |
| `rows` | rows written (keys 1..rows) |

Inject detail adds `located` for an index: `{byte_in_page, field, item, items, entry_offset,
entry_length, key, heap_tid, heap_tid_after}`, and `byte_offset` in the file.
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
| `detection_identifies_block` (gated) | the first read failed with the corruption code naming the damaged block and file | no read attempt |
| `amcheck_detects_target` | checker on the index reported it | checker inconclusive |
| `reindex_restores` | REINDEX ok AND read-after matches via index AND amcheck-after clean | any of the three inconclusive |
| `reindex_duration_s` | seconds (reported) | REINDEX failed |
| others | as NL-I-01 | as NL-I-01 |
