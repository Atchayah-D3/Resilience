# Infrastructure requirements — resilience test harness

**Requested by:** Database QA
**Purpose:** execute the Tier‑1 (node‑local) resilience test suite — 54 scenarios that
deliberately crash, starve and power‑cycle a PostgreSQL/ShaktiDB instance and measure what the
failure cost.

---

## 1 · Summary

| # | Request | Scenarios requiring it | Priority |
|---|---|---|---|
| 1 | Separate data disk on the target, storage class stated | ~12 scenarios, plus per‑run reset for all 54 | **Critical** |
| 2 | Power control — force‑off and power‑on of the target | **6 scenarios, all P0** (NL‑D‑02, D‑04, D‑05, D‑06, NL‑W‑01, W‑02) | **Critical** |
| 3 | Operational exemptions: monitoring, auto‑patching, backups | prevents invalid results and false alerts | **Critical** |
| 4 | Permission to install five diagnostic packages | ~7 scenarios | High |
| 5 | Co‑tenant VM on the same physical host, plus host metrics | 3 scenarios | Medium |
| 6 | A standby node with streaming replication | 1 Tier‑1 scenario; prerequisite for all of Tier 2 | Low for now |

Requests 1–3 are required for meaningful Tier‑1 coverage. Requests 5 and 6 may be declined; the
affected scenarios would be recorded as documented coverage gaps.

**Machines requested: four.** A target (§8), a driver host running the harness (§8), a co‑tenant
(§6) and a standby (§7). The first two are required; the last two correspond to requests 5 and 6.
The standby is also a prerequisite for the entire Tier‑2 cluster suite, so it will be needed
regardless of whether Tier‑1 coverage is completed first.

**Duration:** required for Tier‑1 development and thereafter as a standing regression
environment — not a short‑lived allocation.

---

## 2 · Request 1 — separate data disk

**Attach one 250 GB raw volume to the target VM.** Unpartitioned, unformatted; we create the
volume group, logical volumes and filesystems.

**Required with it:**

- **the backing storage class** — local NVMe, local SSD, SAN, network block, or NFS
- **the disk cache mode**, which must be `none` or `directsync` for the primary volume
- **additionally: one small volume (20 GB) that can be set to `writeback`** — see below

**Preference:** local NVMe or local SSD. Any class is workable provided it is named.

### How it will be carved

Four scenarios fill a filesystem to capacity by design, so each needs its own mount — otherwise
one test takes the others down with it.

```
/dev/vdc → vgdata ─┬── pgdata     40 GB  → database files        (NL-R-03 fills this)
                   ├── pgwal      30 GB  → pg_wal                (NL-W-07 fills this)
                   ├── pgtemp     20 GB  → temp_tablespaces      (NL-R-08 fills this)
                   ├── pgarchive  30 GB  → WAL archive
                   ├── pgrestore  60 GB  → PITR restore target and pg_basebackup output
                   ├── ────────────────
                   │   180 GB allocated
                   └──  70 GB left UNALLOCATED  ← snapshot copy-on-write space
```

The 70 GB remainder is not spare capacity — it is **required** for LVM snapshot copy‑on‑write
space, and is sized to exceed `pgdata` so that a run which rewrites the whole database cannot
overflow it. A snapshot that overflows is invalidated by LVM, which would leave us with no reset
mechanism — the same position we are in today (`VFree = 0`). Snapshot rollback is the per‑run
reset mechanism for all 54 scenarios.

### Why the disk is needed

The database currently shares the root filesystem with the operating system, and the volume
group has no free extents.

- Filling a filesystem to 100 % would fill `/` and take down the host.
- Per‑run state reset requires a snapshot of the database volume. Today the only volume is the
  operating system; rolling it back would erase logs, packages and systemd state.
- Without a named storage class, no durability result can support a customer guarantee.

### Why cache mode is scoped rather than uniform

With `cache=writeback` the guest's flush stops in host memory, so a forced power‑off leaves it
intact and the durability tests would pass against a database that never flushed. The primary
volume therefore needs `cache=none`.

One scenario, however, tests **detection of a volatile write cache** — the failure mode of a
RAID controller whose battery has failed. That requires a volume deliberately configured
`writeback`. Hence the 20 GB secondary volume, or alternatively the ability to toggle the
primary volume's cache mode between runs.

Results for that scenario are labelled **simulated rather than hardware‑verified**: a physical
controller with a failed battery cannot be reproduced on virtual infrastructure. The `writeback`
volume does exercise the detection path genuinely — a forced power‑off discards its volatile
cache, so acknowledged transactions are lost and the harness must catch it — which is what the
scenario grades.

### Note on resizing

The existing root filesystem is **XFS**, which can grow but cannot shrink. Freeing extents in
the existing volume group is not possible without rebuilding the host, so a new disk is required
regardless.

---

## 3 · Request 2 — power control over the target

**API credentials (OpenStack, or libvirt access on the hypervisor) able to force‑off and
power‑on the target instance.** A hard force‑off, not a graceful shutdown.

### Why

A graceful shutdown causes the database to flush everything first, so nothing is at risk and the
test proves nothing. Only abrupt power loss exercises the storage stack.

This has been verified empirically. One scenario was run four times against a single target,
varying only the database's durability settings:

> **A cluster that would be unrecoverable after a real power cut passes a process‑kill test.**

| `fsync` | `synchronous_commit` | transactions lost | verdict |
|---|---|---|---|
| on | on | 0 | passed |
| **off** | on | **0** | **passed** |
| on | off | 19 | failed |
| off | off | 45 | failed |

Row two is the point. `fsync = off` disables the database's flush entirely, yet the test passed
with zero data loss and a clean integrity check. Process termination leaves the operating system
running and it completes the writes regardless. Only removing power from the machine exposes
this.

### Fidelity disclosure

Hypervisor force‑off is not the same as cutting physical power. The framework specification
records this explicitly (§19, open decision 2):

> *"If not, NL‑D‑02 and NL‑D‑06 rely on hypervisor force‑off — weaker evidence. Acceptable, but
> the limitation must be stated in any compliance claim."*

We therefore ask whether a lab node exists where **physical** power can be cut. If not,
hypervisor force‑off is acceptable and the limitation will be disclosed in every result.

### Risk

The target holds no production data and is marked disposable. With request 1 in place, snapshot
rollback restores it in roughly 15 seconds. The procedure has been run repeatedly on a local
test VM without incident.

---

## 4 · Request 3 — operational exemptions

These tests deliberately create the conditions that monitoring and automation exist to prevent.
Without exemptions, results will be invalid and on‑call staff will be paged.

Observed on the current lab hosts:

| Present today | Required change |
|---|---|
| `unattended-upgrades` **enabled**, with a reboot **already pending** | **Disable automatic updates and automatic reboots.** An unscheduled reboot during a run invalidates the measurement. Two scenarios run for four hours |
| **Checkmk** agent running | **Suppress alerting** on these hosts. Filled disks, saturated CPU and forced power‑offs are intentional |
| **Wazuh** agent running | **Exempt from security alerting.** The suite issues `SIGKILL`, edits configuration files and deliberately corrupts database files |
| Any automated remediation | **Disable auto‑restart, auto‑recovery and autoscaling.** Several scenarios measure whether the database restarts *unattended*; external automation invalidates the result |
| Backup or snapshot agents | **Disable on these hosts.** They add I/O noise to latency measurements |
| NTP managed centrally | **Permit us to stop `chronyd` and set the clock ±5 minutes** for one scenario, without it being re‑enabled or alerted |

---

## 5 · Request 4 — package installation

Approval to install the following on the target. We hold passwordless `sudo`; this is a policy
approval, not an access request.

| Package | Required by |
|---|---|
| `stress-ng` | CPU saturation (P0) |
| `dmsetup` / dm‑flakey | storage write‑loss injection (P0) |
| `fio` | I/O saturation |
| `pgBackRest` | backup contention |
| `postgresql-contrib` | `amcheck` (six P0 integrity scenarios) and `pg_visibility` (visibility‑map corruption) |
| `prometheus-node-exporter`, `postgres_exporter` | two P0 scenarios are graded partly on *attribution* — "steal correctly identified as steal, not misdiagnosed as database load" and "attributed to storage in telemetry; WAL stall alerted". Pass/fail values still come from the harness's own probes; these supply the telemetry layer those clauses refer to |

All are standard packaged diagnostic tools. dm‑flakey additionally requires the database on a
device‑mapper volume, which request 1 provides.

---

## 6 · Request 5 — co‑tenant VM (medium priority)

**One small VM (2 vCPU, 2 GB RAM, 20 GB disk) confirmed to be on the same physical hypervisor
host as the target**, plus read access to:

- host CPU steal (`%st`)
- cgroup throttling counters (`nr_throttled`)
- storage burst‑credit metrics, where the storage class has them
- hypervisor memory reclaim (ballooning) control, for one scenario

### Why

Three scenarios measure the effect of a misbehaving neighbour on shared hardware. Testing this
requires controlling a VM that genuinely contends for the same physical CPU, storage path and
network interface. Two VMs on different hosts share nothing and the tests would pass vacuously.
One of the three — network bandwidth contention — needs **both** this request and request 6,
since it is judged on replication lag and therefore needs a replica as well.

**Placement confirmation is the essential part** — "same cluster" or "same availability zone" is
not sufficient.

**One of the three additionally needs shared storage.** Shared‑storage IOPS exhaustion requires
the target's data volume, or one secondary volume, to sit on storage the co‑tenant can actually
contend for. If the data volume is dedicated local NVMe with no shared path, that scenario is a
coverage gap regardless of co‑tenant placement. Please tell us whether the storage the target is
given is shared with other instances on the host.

If declined, three scenarios become a documented coverage gap. Durability and integrity
guarantees are unaffected; the framework treats performance under contention as a conditional
commitment because the neighbour is not ours.

---

## 7 · Request 6 — standby node (low priority for Tier 1)

**A third VM of the same specification as the target**, to run as a streaming replica.

One Tier‑1 scenario (network bandwidth contention) is judged on replication lag and cannot be
evaluated without a replica. More importantly, **the entire Tier‑2 cluster suite — 55 further
scenarios — requires one**, so this will be needed regardless. Raised now so it can be planned
rather than requested twice.

---

## 8 · Target VM specification

Applies to a new VM, or as the target state for an existing one.

| | Requirement | Notes |
|---|---|---|
| OS | Ubuntu 22.04 LTS | matches the current lab hosts. **Please confirm what production uses** — package names and unit paths differ on RHEL derivatives |
| vCPU | 8 | one scenario saturates CPU deliberately |
| RAM | **16 GB fixed by default**; ballooning enabled only for the single scenario that tests memory reclaim | two scenarios test out‑of‑memory behaviour and need a known figure, so ballooning must not be on by default |
| OS disk | 50 GB | |
| **Data disk** | **250 GB, raw, separate** | request 1 — 180 GB is carved into filesystems, 70 GB must stay unallocated for snapshots |
| **Secondary volume** | **20 GB, settable to `writeback`** | request 1 |
| Storage class | **stated** | request 1 |
| Cache mode | **`none` or `directsync`** on the primary volume | request 1 |
| Power control | **force‑off / power‑on** | request 2 |
| Privileges | passwordless `sudo` | already held |
| Network | SSH and database port reachable from the driver host | already in place |

We will initialise the cluster with `data_checksums = on`. This cannot be enabled later without
taking the cluster offline, and eight corruption‑detection scenarios depend on it — noted so a
pre‑built cluster is not handed over without it.

### Driver host — separate machine, runs the harness

The harness must not run on the machine under test; its evidence journals have to survive the
target's failure.

| | Requirement | Notes |
|---|---|---|
| OS | Ubuntu 22.04 LTS | |
| vCPU | 4 | |
| RAM | 8 GB | |
| OS disk | 50 GB | |
| **Evidence disk** | **100 GB, low flush latency, separate** | see below |
| Network | must reach the **power‑control endpoint** — OpenStack API, or SSH to the hypervisor | this may be a new firewall path; request 2 is unusable without it |
| Metrics retention | Prometheus runs here, scraping the target's exporters | the 100 GB evidence disk is expected to cover this; if metric retention across many runs is wanted, allow a further 50 GB |

The harness writes and flushes a journal entry before every transaction it measures. On the
current driver host, whose root volume spans devices of differing speed, sustained flush rates
were observed varying between 3 and 1,400 per second. This caused runs to abort and made the
harness itself the bottleneck rather than the database. A dedicated low‑latency volume for the
evidence directory prevents it.
