"""How the harness runs a program with higher rights on the target (execution/remote.py
`privileged`), in the two forms the environment file chooses:

    sudo_shell true (default)  sudo -n [-u user] -- sh -c '[cd /tmp && ]program args'
    sudo_shell false           [cd /tmp && ]sudo [-n] [-u user] -- program args

The default must give exactly today's command lines, so no existing environment changes. The
direct form is for targets whose sudo policy forbids shells (BDB-QA-U22-3): only the program
itself runs with higher rights, by its full path.
"""

from __future__ import annotations

import asyncio
import shlex

import pytest

from resilience_tests.control.profile import SSHEndpoint, load_profile
from resilience_tests.execution.injectors import os_ssh, process
from resilience_tests.execution.remote import RemoteCommandError, RemoteResult, as_root, as_user, privileged
from tests.test_adapter_seam import NODE

STANDARD = SSHEndpoint(host="t", port=22, user="u")
DIRECT = SSHEndpoint(host="t", port=22, user="u", sudo_shell=False, sudo_noninteractive=False)
DIRECT_N = SSHEndpoint(host="t", port=22, user="u", sudo_shell=False)          # e.g. a service account


def test_the_default_form_is_exactly_the_original_one():
    assert privileged(STANDARD, ["tail", "-n0", "-F", "/d/logfile"]) == as_root("tail -n0 -F /d/logfile")
    assert privileged(STANDARD, ["df", "-P", "/d"]) == as_root("df -P /d")
    cmd = privileged(STANDARD, ["/b/pg_amcheck", "-p", "5434", "-d", "resilience"], user="postgres", cwd="/tmp")
    assert cmd == as_user("postgres", "cd /tmp && /b/pg_amcheck -p 5434 -d resilience")


def test_the_direct_form_runs_only_the_program_with_higher_rights():
    cmd = privileged(DIRECT, ["/b/pg_amcheck", "-p", "5434", "-d", "resilience"], user="postgres", cwd="/tmp")
    assert cmd == "cd /tmp && sudo -u postgres -- /b/pg_amcheck -p 5434 -d resilience"
    assert " sh " not in f" {cmd} " and "-n" not in shlex.split(cmd)
    assert privileged(DIRECT, ["systemctl", "kill", "-s", "SIGKILL", "x.service"]) == \
        "sudo -- systemctl kill -s SIGKILL x.service"
    assert privileged(DIRECT_N, ["df", "-P", "/d"]) == "sudo -n -- df -P /d"


def test_arguments_stay_arguments_never_shell_syntax():
    sql = "SELECT 'a b'; DROP TABLE x; $(reboot)"
    for ep in (STANDARD, DIRECT):
        cmd = privileged(ep, ["/b/psql", "-c", sql], user="postgres", cwd="/tmp")
        words = shlex.split(cmd)
        if ep.sudo_shell:                       # the inner command of sh -c parses back to the argv
            words = shlex.split(words[words.index("-c") + 1])
        assert words[-2:] == ["-c", sql]
    with pytest.raises(ValueError):
        privileged(DIRECT, [])


def test_the_new_lab_target_uses_the_direct_form_and_the_others_keep_the_original():
    u22_3 = load_profile("e2-u22-3").nodes[0].ssh
    assert (u22_3.sudo_shell, u22_3.sudo_noninteractive) == (False, False)
    old = load_profile("e2-dedicated-vm").nodes[0].ssh
    assert (old.sudo_shell, old.sudo_noninteractive) == (True, True)


# --- each converted command, in both forms ----------------------------------------------------

def _node(ep):
    return NODE.model_copy(update={"ssh": ep})


@pytest.mark.parametrize("ep", [STANDARD, DIRECT])
def test_the_database_tools_run_as_the_cluster_user_in_both_forms(ep):
    from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
    a = PostgreSQLAdapter(_node(ep))
    cmd = a._psql_as_os_user("SELECT 1")
    fault = os_ssh.psql_command(_node(ep), "SELECT 1", "SELECT 2")
    for c in (cmd, fault):
        words = shlex.split(c)
        assert ["-u", NODE.os_user] == words[words.index("-u"):words.index("-u") + 2]
        assert f"{NODE.pg_bin}/psql" in c
        assert (" sh -c " in c) == ep.sudo_shell
        assert c.startswith("sudo -n -u ") if ep.sudo_shell else c.startswith("cd /tmp && sudo -u ")


class FakeHost:
    def __init__(self, ep, result):
        self.endpoint, self.result, self.commands = ep, result, []

    async def run(self, command, *, timeout_s, check=True):
        self.commands.append(command)
        return self.result


@pytest.mark.parametrize("ep", [STANDARD, DIRECT])
def test_reading_a_process_in_both_forms(ep):
    host = FakeHost(ep, RemoteResult(0, "4242 (postgres) S 1 ...", ""))
    assert asyncio.run(process.proc_stat(host, _node(host.endpoint), 4242)).startswith("4242")
    assert host.commands[0].endswith("cat /proc/4242/stat") == (not ep.sudo_shell)


def test_a_process_that_does_not_exist_reads_as_empty():
    host = FakeHost(DIRECT, RemoteResult(1, "", "cat: /proc/4242/stat: No such file or directory\n"))
    assert asyncio.run(process.proc_stat(host, _node(host.endpoint), 4242)) == ""


def test_a_refused_read_is_an_error_never_a_gone_process():
    # empty output means "the process is gone"; a read sudo refused must never confirm a kill
    host = FakeHost(DIRECT, RemoteResult(1, "", "sudo: a terminal is required to read the password\n"))
    with pytest.raises(RemoteCommandError, match="terminal is required"):
        asyncio.run(process.proc_stat(host, _node(host.endpoint), 4242))
