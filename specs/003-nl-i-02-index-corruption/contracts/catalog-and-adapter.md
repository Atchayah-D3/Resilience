# Contracts: NL-I-02

## Catalog

```yaml
fault:
  type: data_corruption
  driver: os_ssh
  target: primary
  duration: permanent
  relation_kind: btree_index     # new; heap | btree_index; only for data_corruption
```

Schema: `Fault.relation_kind: Literal["heap", "btree_index"] | None = None`; a value with any
other fault type is a validation error. Absent means `heap` (NL-I-01 unchanged).

## Adapter (BaseDatabaseAdapter)

```python
async def prepare_corruption_target(self, kind: str = "heap") -> dict   # + table, relation_kind, page_check, integrity_exclusions
async def read_index_target(self, attempts: int = 2) -> dict            # {"attempts": [...]}, see data-model
async def rebuild_corruption_target(self) -> dict                       # {"ok", "duration_s", "error", "relation_path_after"}
async def amcheck_relation(self, relation: str, timeout_s: float, *, heapallindexed: bool = False) -> dict
```

Base implementations raise NotImplementedError (refuse). An unknown `kind` raises
DriverNotAvailable-equivalent ValueError in the adapter, which the driver's preflight surfaces.

## Injector

`FaultInjector.corruption_kind: str = "heap"`, set by the orchestrator from
`fault.relation_kind`. The corruption driver passes it to `prepare_corruption_target`, verifies
`page_check` after the clean stop and before the write, and reverts with
`DROP TABLE IF EXISTS <table or relation>` (harness schema only).
