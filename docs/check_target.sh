#!/usr/bin/env bash
# Preflight check — what the Tier-1 NL suite can run on this target.
# Read-only. Run as a user with passwordless sudo.  bash check_target.sh

ok(){ printf '  \033[32mOK\033[0m    %s\n' "$1"; }
no(){ printf '  \033[31mNO\033[0m    %s\n' "$1"; }
wr(){ printf '  \033[33mWARN\033[0m  %s\n' "$1"; }
hd(){ printf '\n\033[1m%s\033[0m\n' "$1"; }

PGDATA="${PGDATA:-$(sudo -u postgres psql -tAc 'SHOW data_directory' 2>/dev/null)}"

hd "1. Privileges"
sudo -n true 2>/dev/null && ok "passwordless sudo" || no "passwordless sudo — nothing below is testable"

hd "2. Database basics"
[ -n "$PGDATA" ] && ok "PGDATA = $PGDATA" || no "PGDATA not found — export PGDATA=... and re-run"
q(){ sudo -u postgres psql -tAc "$1" 2>/dev/null; }
[ -n "$(q 'SELECT 1')" ] && ok "can connect as postgres" || no "cannot connect as postgres"
echo "        version              : $(q 'SHOW server_version')"
echo "        fsync                : $(q 'SHOW fsync')"
echo "        synchronous_commit   : $(q 'SHOW synchronous_commit')"
echo "        full_page_writes     : $(q 'SHOW full_page_writes')"
echo "        max_wal_size         : $(q 'SHOW max_wal_size')"

hd "3. data_checksums  — gates 8 NL-I corruption scenarios"
CS=$(q 'SHOW data_checksums')
[ "$CS" = "on" ] && ok "data_checksums = on" \
  || no "data_checksums = $CS — NL-I-01/02/03/05/06 cannot detect corruption. Cannot be enabled without stopping the cluster (pg_checksums --enable)"

hd "4. Extensions  — gates 6 P0 integrity scenarios"
for e in amcheck pg_visibility; do
  [ -n "$(q "SELECT 1 FROM pg_available_extensions WHERE name='$e'")" ] \
    && ok "$e available" || no "$e missing — install postgresql-contrib"
done
for b in pg_amcheck pg_waldump pg_controldata pg_resetwal pg_checksums pg_basebackup pg_test_fsync; do
  command -v $b >/dev/null 2>&1 && ok "$b present" || no "$b missing"
done

hd "5. Filesystem layout  — gates NL-R-03, NL-W-07, NL-R-08"
DF_PGDATA=$(df -h "$PGDATA" 2>/dev/null | awk 'NR==2{print $1" "$2" avail "$4" on "$6}')
DF_ROOT=$(df -h / | awk 'NR==2{print $1}')
echo "        PGDATA on : $DF_PGDATA"
PG_DEV=$(df "$PGDATA" 2>/dev/null | awk 'NR==2{print $1}')
[ "$PG_DEV" != "$DF_ROOT" ] && ok "PGDATA is NOT on the root filesystem" \
  || no "PGDATA shares / — filling it to 100% takes the host down. NL-R-03 unsafe"
WAL_DEV=$(df "$PGDATA/pg_wal" 2>/dev/null | awk 'NR==2{print $1}')
[ -n "$WAL_DEV" ] && [ "$WAL_DEV" != "$PG_DEV" ] && ok "pg_wal on its own filesystem" \
  || wr "pg_wal shares the data filesystem — NL-W-07 will also fill PGDATA"
TT=$(q 'SHOW temp_tablespaces'); [ -n "$TT" ] && ok "temp_tablespaces = $TT" \
  || wr "temp_tablespaces unset — NL-R-08 would fill the data filesystem"

hd "6. LVM / device-mapper  — gates snapshot reset and NL-D-03"
if sudo vgs --noheadings -o vg_name,vg_free 2>/dev/null | grep -q .; then
  sudo vgs --noheadings -o vg_name,vg_size,vg_free 2>/dev/null | sed 's/^/        /'
  FREE=$(sudo vgs --noheadings -o vg_free --units g 2>/dev/null | tr -d ' g' | sort -rn | head -1)
  awk -v f="${FREE:-0}" 'BEGIN{exit !(f>0)}' \
    && ok "free extents available — snapshot reset possible" \
    || no "VFree = 0 — no snapshot reset. This is the per-run reset mechanism for all 54"
else
  no "no LVM volume group — no snapshot reset, and dm-flakey (NL-D-03) needs a DM volume"
fi
sudo dmsetup ls >/dev/null 2>&1 && ok "dmsetup usable" || no "dmsetup missing — NL-D-03 blocked"
lsblk -no NAME,TYPE 2>/dev/null | grep -q lvm && ok "PGDATA path includes an LVM device" \
  || wr "no LVM device seen — confirm PGDATA sits on device-mapper"

hd "7. Fault-injection tools"
for t in stress-ng fio; do
  command -v $t >/dev/null 2>&1 && ok "$t present" || no "$t missing (sudo dnf/apt install $t)"
done
command -v pgbackrest >/dev/null 2>&1 && ok "pgbackrest present" || wr "pgbackrest missing — NL-N-06, NL-W-06"
command -v tc >/dev/null 2>&1 && ok "tc present (NL-N-05)" || wr "tc missing — iproute2"

hd "8. cgroup control  — gates NL-R-01, NL-R-02, NL-N-02"
[ -f /sys/fs/cgroup/cgroup.controllers ] && ok "cgroup v2: $(cat /sys/fs/cgroup/cgroup.controllers)" \
  || wr "cgroup v1 — memory/cpu limits still work, paths differ"
SVC=$(systemctl list-units --type=service --no-legend 2>/dev/null | grep -iE 'postgres|shaktidb' | awk '{print $1}' | head -1)
[ -n "$SVC" ] && ok "service unit: $SVC" || wr "no systemd unit found — start/stop must use pg_ctl"
[ -n "$SVC" ] && echo "        CPUQuota now : $(systemctl show -p CPUQuota --value "$SVC" 2>/dev/null)"
sudo test -w /proc/1/oom_score_adj && ok "oom_score_adj writable (NL-R-02)" || no "cannot set oom_score_adj"

hd "9. Operational interference  — invalidates results silently"
systemctl is-enabled unattended-upgrades 2>/dev/null | grep -q enabled \
  && no "unattended-upgrades ENABLED — a reboot mid-run invalidates it" || ok "no unattended-upgrades"
[ -f /var/run/reboot-required ] && no "a reboot is already pending" || ok "no pending reboot"
for a in checkmk check-mk-agent wazuh-agent; do
  systemctl is-active "$a" 2>/dev/null | grep -q '^active' && wr "$a running — will alert on intentional faults"
done
[ -n "$SVC" ] && R=$(systemctl show -p Restart --value "$SVC" 2>/dev/null) && \
  { [ "$R" = "no" ] && ok "Restart=no — unattended-recovery scenarios are valid" \
    || no "Restart=$R — systemd restarts the DB for you; NL-C/NL-R recovery results are invalid"; }
systemctl is-active chronyd 2>/dev/null | grep -q '^active' \
  && wr "chronyd active — NL-M-08 needs permission to stop it and shift the clock" || ok "chronyd not active"

hd "10. Power control  — gates 6 P0 scenarios"
systemd-detect-virt -q && echo "        virtualised: $(systemd-detect-virt)" || echo "        bare metal"
command -v ipmitool >/dev/null 2>&1 && ok "ipmitool present" || no "no ipmitool"
[ -c /dev/ipmi0 ] && ok "BMC device present — physical power cut possible" || no "no BMC — needs hypervisor/API force-off"
awk -v m="$(cat /proc/sys/kernel/sysrq 2>/dev/null)" 'BEGIN{exit !(and(m,128))}' 2>/dev/null \
  && wr "sysrq reboot bit on — OS-level crash only; does NOT test the disk cache" \
  || echo "        sysrq mask: $(cat /proc/sys/kernel/sysrq 2>/dev/null)"

hd "11. Resources"
echo "        vCPU : $(nproc)    RAM : $(free -g | awk 'NR==2{print $2}') GB"
[ "$(nproc)" -ge 8 ] && ok "8+ vCPU (NL-R-06)" || wr "<8 vCPU — CPU saturation less representative"
[ "$(free -g | awk 'NR==2{print $2}')" -ge 16 ] && ok "16+ GB RAM" || wr "<16 GB — OOM scenarios need a known fixed figure"

printf '\n\033[1mRead the NO lines top to bottom — each one names the scenarios it blocks.\033[0m\n\n'
