# vendor/elle

The pinned Elle checker (Arch §10.3, §17). Elle reads a recorded operation history and reports
the isolation anomalies the declared consistency model forbids, with a minimal counterexample
cycle. The JVM is used here and nowhere else in the harness.

- `setup_elle.sh` installs elle-cli (github.com/ligurio/elle-cli, version pinned in the
  script, Java 21+) as `elle-cli.jar` on the **driver host**, and runs a smoke test.
- `run_elle.sh` re-checks a run's `history.edn` by hand.

Scenarios with `workload.history: list_append` (NL-C-03) record a real list-append history:
each marker transaction reads one list, appends to another and reads it back, at SERIALIZABLE,
and the history carries what the database returned. There is no fallback checker: without the
jar, `elle_anomalies_count` is NOT_MEASURED and any criterion on it fails.

`elle-cli.jar` is not committed (see `.gitignore`); install it with the script.
