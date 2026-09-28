# Environment setup — driver host and target

Two machines, as Arch §6.2 requires (the workload driver and its marker journals must not live
on the node under test):

| Role | Host | What runs there |
|---|---|---|
| **Driver host** | 10.11.21.111 (BDB-QA-U22-29) | the harness: workload driver, probes, marker journals, analysis, evidence |
| **Target** | 10.11.21.112 (BDB-QA-U22-30) | the disposable ShaktiDB cluster under test (port 5433) |

Both run Ubuntu 22.04. SSH listens on port **5015**, user `distdbsdb17user`, with passwordless sudo.
The target's pre-existing cluster on port 5432 is left untouched throughout.

---

## 1. Driver host (10.11.21.111)

### 1.1 Python 3.11+ (Arch §12)

Ubuntu 22.04 ships 3.10, so the deadsnakes PPA is added by hand (this host cannot reach
`api.launchpad.net`, which is what `add-apt-repository` calls):

```bash
sudo mkdir -p /etc/apt/keyrings
curl -fsSL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xF23C5A6CF475977595C89F51BA6932366A755776' \
  | sudo gpg --dearmor -o /etc/apt/keyrings/deadsnakes.gpg
gpg --show-keys --with-fingerprint /etc/apt/keyrings/deadsnakes.gpg
#   expect fingerprint F23C5A6CF475977595C89F51BA6932366A755776, uid "Launchpad PPA for deadsnakes"
echo "deb [signed-by=/etc/apt/keyrings/deadsnakes.gpg] https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main" \
  | sudo tee /etc/apt/sources.list.d/deadsnakes.list
sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
python3.11 --version
```

### 1.2 Key-based SSH to the target (Arch §12: asyncssh, key-based)

The harness opens SSH sessions to the target for the fault injection, the integrity check and
the log tail. It must never be prompted for a password.

```bash
ssh-keygen -t ed25519 -N "" -C "harness@BDB-QA-U22-29" -f ~/.ssh/id_ed25519
ssh-copy-id -i ~/.ssh/id_ed25519.pub -p 5015 distdbsdb17user@10.11.21.112
# verify: no password, and sudo works non-interactively
ssh -p 5015 -o BatchMode=yes distdbsdb17user@10.11.21.112 'hostname; sudo -n true && echo SUDO_OK'
```

### 1.3 The harness

Deployed from the development workstation with rsync; installed into its own virtualenv:

```bash
# from the workstation holding the repo
rsync -az --delete --exclude .venv --exclude __pycache__ --exclude '*.egg-info' \
  --exclude .pytest_cache -e 'ssh -p 5015' resilience/ distdbsdb17user@10.11.21.111:resilience/

# on 10.11.21.111
cd ~/resilience
python3.11 -m venv .venv
.venv/bin/pip install -e '.[test]'     # asyncpg, asyncssh, pydantic v2, PyYAML, pytest, pytest-xdist
.venv/bin/pytest -q -n 4               # harness unit tests
```

Evidence and the injection ledger are written to `~/resilience-runs/` on this host; the path is
declared in the env profile as `driver_host.run_dir`.

### 1.4 Database password for the harness role

Never stored in the repo: the harness connects with asyncpg, which reads `~/.pgpass`.

```bash
echo '10.11.21.112:5433:resilience:harness:<password>' >> ~/.pgpass
chmod 600 ~/.pgpass
```

### 1.5 Optional: Ansible

Environment provisioning belongs in Ansible (Arch §3, §12). Installed in its own venv:

```bash
python3.11 -m venv ~/resilience-venv
~/resilience-venv/bin/pip install ansible-core
~/resilience-venv/bin/ansible all -i '10.11.21.112,' -e ansible_port=5015 \
  -e ansible_user=distdbsdb17user -m ansible.builtin.ping -b
```

---

## 2. Target (10.11.21.112)

The harness needs a **disposable** cluster: it is crashed, killed and restarted by scenarios.
It is created separately from any existing cluster on the host, on its own port and data
directory, with its own service unit.

```bash
PGBIN=/usr/lib/postgresql/17.11.1.0/bin      # ShaktiDB binaries on this host
```

### 2.1 Data directory and cluster

Placed under `/var/lib/resilience`, with page checksums enabled from the start — the framework
refuses to certify a cluster without them (Framework §16.2, NL-I-09):

```bash
sudo mkdir -p /var/lib/resilience/pgdata
sudo chown -R postgres:postgres /var/lib/resilience
sudo chmod 700 /var/lib/resilience/pgdata

cd /tmp
sudo -u postgres $PGBIN/initdb -D /var/lib/resilience/pgdata \
     --data-checksums --auth-local=peer --auth-host=scram-sha-256 --no-ssl
```

`--no-ssl` is required by this build unless certificate paths are given.

### 2.2 Settings

Durability on, and a distinct port so the existing cluster is untouched:

```bash
sudo -u postgres tee -a /var/lib/resilience/pgdata/postgresql.conf >/dev/null <<'CONF'

# --- resilience harness target cluster ---
listen_addresses = '*'
port = 5433
fsync = on
synchronous_commit = on
full_page_writes = on
CONF
```

### 2.3 Access rules

Local connections by peer (so `pg_amcheck` runs as the `postgres` OS user without a password),
and the harness role only from the driver host:

```bash
sudo -u postgres sed -i -E \
  's/^local[[:space:]]+all[[:space:]]+all[[:space:]]+.*/local   all             all                                     peer/' \
  /var/lib/resilience/pgdata/pg_hba.conf
echo "host    resilience    harness    10.11.21.111/32    scram-sha-256" \
  | sudo -u postgres tee -a /var/lib/resilience/pgdata/pg_hba.conf
```

### 2.4 Service unit — must start unattended

After a crash or power loss the database has to come back with no human action
(Framework §10.1), so the unit is **enabled** and restarts on failure:

```bash
sudo tee /etc/systemd/system/shaktidb-resilience.service >/dev/null <<'EOF'
[Unit]
Description=ShaktiDB - resilience harness target cluster
After=network.target

[Service]
Type=forking
User=postgres
Group=postgres
Environment=PGDATA=/var/lib/resilience/pgdata
OOMScoreAdjust=-1000
Environment=PG_OOM_ADJUST_FILE=/proc/self/oom_score_adj
Environment=PG_OOM_ADJUST_VALUE=0
Environment=PGSTARTTIMEOUT=270
TimeoutSec=300
ExecStart=/usr/lib/postgresql/17.11.1.0/bin/pg_ctl -D ${PGDATA} -l ${PGDATA}/logfile -t ${PGSTARTTIMEOUT} start
ExecStop=/usr/lib/postgresql/17.11.1.0/bin/pg_ctl -D ${PGDATA} stop
ExecReload=/usr/lib/postgresql/17.11.1.0/bin/pg_ctl -D ${PGDATA} reload
StandardOutput=journal
StandardError=journal
SyslogIdentifier=shaktidb-resilience
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now shaktidb-resilience.service
systemctl is-enabled shaktidb-resilience.service    # enabled
systemctl is-active  shaktidb-resilience.service    # active
```

`Restart=on-failure` plus `enabled` is what makes crash and power-loss recovery unattended. The
process-kill driver refuses to inject if the unit cannot restart itself.

### 2.5 Harness role, database and integrity extension

```bash
cd /tmp
sudo -u postgres $PGBIN/psql -p 5433 \
  -c "CREATE ROLE harness LOGIN PASSWORD '<password>'" \
  -c "CREATE DATABASE resilience OWNER harness" \
  -c "GRANT pg_monitor TO harness"
sudo -u postgres $PGBIN/psql -p 5433 -d resilience -c "CREATE EXTENSION IF NOT EXISTS amcheck"
```

`pg_amcheck` itself runs on the node as the `postgres` OS user over SSH, so the harness role
needs no superuser rights.

---

## 3. The operator sentinel (Arch §15)

Created **by hand**, never by the harness. Without it the harness refuses to touch the target
(default deny). Run from the driver host:

```bash
/usr/lib/postgresql/17.11.1.0/bin/psql -w \
  "host=10.11.21.112 port=5433 dbname=resilience user=harness" <<'SQL'
CREATE SCHEMA IF NOT EXISTS resilience;
ALTER SCHEMA resilience OWNER TO harness;
CREATE TABLE IF NOT EXISTS resilience.harness_target (
    hostname      text PRIMARY KEY,
    inventory_tag text NOT NULL,
    disposable    boolean NOT NULL,
    created_by    text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO resilience.harness_target (hostname, inventory_tag, disposable, created_by)
VALUES ('BDB-QA-U22-30', 'resilience-lab-e2', true, 'dbre-operator')
ON CONFLICT (hostname) DO UPDATE SET disposable = true;
SELECT * FROM resilience.harness_target;
SQL
```

`hostname` must match the target's own `hostname`, and `inventory_tag` must equal
`safety.allowlist.inventory_tag` in the env profile, or the run is refused.

---

## 4. How the driver host reaches the target

| Path | Used for | Mechanism |
|---|---|---|
| SSH `10.11.21.112:5015` as `distdbsdb17user` | fault injection, `pg_amcheck`, log tail, hostname check | asyncssh with the ed25519 key from §1.2; privileged commands via `sudo -n` |
| PostgreSQL `10.11.21.112:5433` as `harness` | workload, write probes, marker verification, settings | asyncpg; password from `~/.pgpass`; allowed by the `pg_hba` line in §2.3 |

Both are declared in `envs/e2-dedicated-vm.yaml`; nothing in `catalog/` refers to either host.

---

## 5. Verification checklist

On the driver host:

```bash
python3.11 --version                                         # 3.11.x
ssh -p 5015 -o BatchMode=yes distdbsdb17user@10.11.21.112 'hostname; sudo -n true && echo SUDO_OK'
/usr/lib/postgresql/17.11.1.0/bin/psql -w \
  "host=10.11.21.112 port=5433 dbname=resilience user=harness" \
  -XAtc "select current_setting('data_checksums')||' '||current_setting('synchronous_commit')"   # on on
cd ~/resilience && .venv/bin/pytest -q -n 4                  # harness unit tests
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
  --scenario NL-C-01 --stop-before-fault -q -rA              # dry run: no fault, no verdict
```

Then the scenario itself:

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
  --scenario NL-C-01 -q -rA --junitxml=reports/junit.xml
d=$(ls -td ~/resilience-runs/NL-C-01-* | head -1); cat $d/summary.txt
```

Safety check at any time — reverts anything the ledger still records as outstanding:

```bash
.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm
```

---

## 6. Deliberately not installed

- **Nothing on the target but the cluster itself.** No agent: every fault is applied over SSH.
- **No harness code on the target.** It only ever runs on the driver host.
- **No credentials in the repo.** Database password in `~/.pgpass` on the driver host, SSH by key.
- **Reset driver**: `reset.driver: none` for now. Snapshot-based reset (Arch §3) requires PGDATA on
  a dedicated volume and is configured in the profile when that exists.
