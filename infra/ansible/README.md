# infra/ansible

Configuration management (Arch §3, §12). Roles: postgres, patroni, etcd, pgbouncer,
pgbackrest, storage (LVM plus the dm shim), monitoring.
Storage role note (Arch §3): PGDATA and pg_wal belong on a device-mapper target from day one,
and reset is snapshot-based rather than rebuild-based.
