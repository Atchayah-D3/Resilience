#!/usr/bin/env python3
"""Fake pgbench for the harness's unit tests: a small interpreter of the pgbench subset the
harness's scripts use, against a fake database kept in files. No PostgreSQL needed.

Faithful where the harness depends on pgbench's behaviour (recorded on ShaktiDB 17 pgbench):
- `\\setshell` / `\\shell` run through /bin/sh for real, so the record steps, their pipes and
  their failure handling are exercised exactly as with real pgbench: a command of 255 bytes
  or more, a non-integer `\\setshell` result or a non-zero `\\shell` exit aborts the client
  ("aborted in command N (shell) ... execution of meta-command failed", exit 2);
- `:name` is substituted only in a whole meta-command argument, and anywhere in SQL;
- a serialization failure at COMMIT skips the rest of the script and the client goes on with
  its next transaction (`--max-tries=1`);
- a broken connection aborts the client ("aborted in command N (SQL) of script 0; perhaps the
  backend died while processing", exit 2); no connection at start exits 1.

The fake database, in the directory $FAKE_PGBENCH_DB:
  down             present: the server is down (abort on the next statement; refuse new clients)
  lose             present: COMMIT is acknowledged but nothing persists (a lying database)
  hang             present: COMMIT never returns
  auth-fail        present: new clients are refused for their password
  stmt-error       present: the next statement fails with ERROR (the connection stays up)
  commit-delay     contains S: every COMMIT takes S seconds
  drop-<launch>    present: that launch's connection is terminated at its next statement
  serialize-every  contains N: every Nth COMMIT of each client fails with a serialization error
  serialize-delay  contains S: ... after waiting S seconds
  exit-after       contains N: the process exits 0 by itself after N transactions
  committed        written: one line "<uuid> <commit wall time>" per committed marker
  lists.json       written: the Elle lists, {key: [values]}
  churn            written: one line per committed churn transaction
"""

import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid as uuidlib

SHELL_COMMAND_SIZE = 256


def die(msg, code):
    print(msg, file=sys.stderr, flush=True)
    sys.exit(code)


def main():
    argv = sys.argv[1:]
    if "--version" in argv:
        print(os.environ.get("FAKE_PGBENCH_VERSION", "pgbench (PostgreSQL) 17.11.1.0"))
        return 0
    variables, script, i = {}, None, 0
    while i < len(argv):
        a = argv[i]
        if a in ("-c", "-j", "-T", "-h", "-p", "-U"):
            i += 2
            continue
        if a == "-D":
            k, v = argv[i + 1].split("=", 1)
            variables[k] = v
            i += 2
            continue
        if a == "-f":
            script = argv[i + 1]
            i += 2
            continue
        i += 1
    db = os.environ.get("FAKE_PGBENCH_DB")
    if not db or not script:
        die("pgbench: error: fake pgbench needs FAKE_PGBENCH_DB and -f", 1)
    if os.path.exists(os.path.join(db, "down")):
        die('pgbench: error: connection to server at "127.0.0.1", port 5433 failed: Connection refused\n'
            "pgbench: error: could not create connection for setup", 1)
    if os.path.exists(os.path.join(db, "auth-fail")):
        die('pgbench: error: connection to server at "127.0.0.1", port 5433 failed: FATAL:  '
            'password authentication failed for user "harness"\n'
            "pgbench: error: could not create connection for setup", 1)
    commands = parse(open(script).read())
    exit_after = read_int(db, "exit-after")
    serialize_every = read_int(db, "serialize-every")
    txns = 0
    while True:
        run_transaction(commands, variables, db, serialize_every, txns)
        txns += 1
        if exit_after and txns >= exit_after:
            return 0


def read_int(db, name):
    try:
        return int(open(os.path.join(db, name)).read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def parse(text):
    commands = []
    for line in text.splitlines():
        if not line.strip():
            continue
        if line.startswith("\\"):
            words = line[1:].split()
            commands.append(("meta", words[0], words[1:]))
        else:
            gset = line.rstrip().endswith("\\gset")
            commands.append(("sql", line.rstrip()[: -len("\\gset")] if gset else line, gset))
    return commands


def subst_words(words, variables):
    return [variables[w[1:]] if w.startswith(":") and w[1:] in variables else w for w in words]


def subst_sql(sql, variables):
    return re.sub(r"(?<!:):([A-Za-z_]\w*)", lambda m: variables.get(m.group(1), m.group(0)), sql)


def evaluate(expr, variables):
    expr = re.sub(r":([A-Za-z_]\w*)", lambda m: variables[m.group(1)], expr)
    if not re.fullmatch(r"[\d\s+\-*/%()<>=!]+", expr):
        raise ValueError(expr)
    return int(eval(expr.replace("/", "//")))   # noqa: S307 -- digits and operators only


def run_transaction(commands, variables, db, serialize_every, txns):
    txn = {"markers": [], "appends": [], "churn": 0}
    skip = []   # \if stack: True when the branch is not taken
    for idx, (kind, a, b) in enumerate(commands):
        if kind == "meta":
            name, args = a, b
            if name == "if":
                skip.append(not evaluate(" ".join(args), variables) if not any(skip) else True)
                continue
            if name == "else":
                skip[-1] = not skip[-1] if not any(skip[:-1]) else True
                continue
            if name == "endif":
                skip.pop()
                continue
            if any(skip):
                continue
            if name == "set":
                variables[args[0]] = str(evaluate(" ".join(args[1:]), variables))
            elif name in ("shell", "setshell"):
                words = subst_words(args[1:] if name == "setshell" else args, variables)
                command = " ".join(words)
                if len(command) >= SHELL_COMMAND_SIZE - 1:
                    print(f"pgbench: error: {words[0]}: shell command is too long", file=sys.stderr)
                    abort_meta(idx, name)
                if name == "shell":
                    if subprocess.run(["/bin/sh", "-c", command]).returncode != 0:
                        print(f"pgbench: error: {words[0]}: could not launch shell command", file=sys.stderr)
                        abort_meta(idx, name)
                else:
                    out = subprocess.run(["/bin/sh", "-c", command], stdout=subprocess.PIPE, text=True).stdout
                    first = (out.splitlines() or [""])[0].strip()
                    if not re.fullmatch(r"-?\d+", first):
                        print(f"pgbench: error: {words[0]}: shell command must return an integer (not \"{first}\")",
                              file=sys.stderr)
                        abort_meta(idx, name)
                    variables[args[0]] = first
            elif name == "sleep":
                time.sleep(int(args[0]) / 1e6 if len(args) > 1 and args[1] == "us" else int(args[0]))
            continue
        if any(skip):
            continue
        sql = subst_sql(a, variables)
        connection_check(db, variables, idx)
        if not execute(sql, a, b, txn, variables, db, serialize_every, txns):
            return   # serialization failure: the rest of the script is skipped


def connection_check(db, variables, idx):
    if os.path.exists(os.path.join(db, "down")):
        die(f"pgbench: error: client 0 aborted in command {idx} (SQL) of script 0; "
            "perhaps the backend died while processing", 2)
    if os.path.exists(os.path.join(db, "stmt-error")):
        die(f"pgbench: error: client 0 script 0 aborted in command {idx} query 0: ERROR:  "
            'column "ts" of relation "markers" does not exist', 2)
    drop = os.path.join(db, f"drop-{variables.get('launch')}")
    if os.path.exists(drop):
        os.unlink(drop)
        die(f"pgbench: error: client 0 script 0 aborted in command {idx} query 0: FATAL:  "
            "terminating connection due to administrator command", 2)


def abort_meta(idx, name):
    die(f"pgbench: error: client 0 aborted in command {idx} ({name}) of script 0; "
        "execution of meta-command failed", 2)


def execute(sql, raw, gset, txn, variables, db, serialize_every, txns):
    m = re.search(r"md5\('([^']*)' \|\| (\d+)\)", sql)
    if m and "resilience.markers" in sql:
        txn["markers"].append(str(uuidlib.UUID(hashlib.md5((m.group(1) + m.group(2)).encode()).hexdigest())))
    m = re.search(r"elle_lists AS l \(k, v\) VALUES \((\d+), ARRAY\[(\d+)::bigint\]\)", sql)
    if m:
        txn["appends"].append((m.group(1), int(m.group(2))))
    if "resilience.churn" in sql and not sql.startswith("DELETE"):
        txn["churn"] += 1
    if gset:
        read(sql, txn, variables, db)
    if sql.strip().upper().startswith("COMMIT"):
        delay = os.path.join(db, "commit-delay")
        if os.path.exists(delay):
            time.sleep(float(open(delay).read().strip()))
        if os.path.exists(os.path.join(db, "hang")):
            while True:
                time.sleep(1)
        if serialize_every and (txns + 1) % serialize_every == 0:
            delay = os.path.join(db, "serialize-delay")
            if os.path.exists(delay):
                time.sleep(float(open(delay).read().strip()))
            return False
        commit(txn, db)
    return True


def read(sql, txn, variables, db):
    m = re.search(r"AS (r\d)len", sql)
    key = re.search(r"WHERE k = (\d+)", sql).group(1)
    vbase = int(re.search(r"e - (\d+)", sql).group(1))
    lists = load_lists(db)
    values = list(lists.get(key, [])) + [v for k, v in txn["appends"] if k == key]
    x = ".".join(str(v - vbase) for v in values) if (key in lists or values) else "nil"
    variables[f"{m.group(1)}len"] = str(len(x))
    for start, size, name in re.findall(r"substr\(x, (\d+), (\d+)\) AS (\w+)", sql):
        variables[name] = x[int(start) - 1: int(start) - 1 + int(size)]


def load_lists(db):
    try:
        return json.load(open(os.path.join(db, "lists.json")))
    except FileNotFoundError:
        return {}


def commit(txn, db):
    if os.path.exists(os.path.join(db, "lose")):
        return
    with open(os.path.join(db, "lock"), "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        now = time.time()
        with open(os.path.join(db, "committed"), "a") as f:
            for u in txn["markers"]:
                f.write(f"{u} {now}\n")
        if txn["appends"]:
            lists = load_lists(db)
            for k, v in txn["appends"]:
                lists.setdefault(k, []).append(v)
            with open(os.path.join(db, "lists.json.tmp"), "w") as f:
                json.dump(lists, f)
            os.replace(os.path.join(db, "lists.json.tmp"), os.path.join(db, "lists.json"))
        if txn["churn"]:
            with open(os.path.join(db, "churn"), "a") as f:
                f.write("1\n")


if __name__ == "__main__":
    sys.exit(main())
