import sys
import os
import stat
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
import awg_helper  # noqa: E402


def _has_match_clause(cmd: list[str], direction: str, ip: str) -> bool:
    needle = ["match", "ip", direction, f"{ip}/32"]
    return any(cmd[i:i + len(needle)] == needle for i in range(len(cmd) - len(needle) + 1))


def _patch_root_owned_regular_lstat(monkeypatch, policy: Path):
    original_lstat = awg_helper.Path.lstat

    def fake_lstat(self):
        st = original_lstat(self)
        if self == policy:
            values = list(st)
            values[0] = stat.S_IFREG | 0o640  # st_mode: regular file with safe perms
            values[4] = 0  # st_uid: root
            return os.stat_result(values)
        return st

    monkeypatch.setattr(awg_helper.Path, "lstat", fake_lstat)


def test_load_policy_uses_host_interface_when_present(tmp_path: Path, monkeypatch):
    policy = tmp_path / "policy.json"
    policy.write_text('{"container":"awg","interface":"awg0","host_interface":"amn0"}', encoding="utf-8")
    policy.chmod(0o640)
    _patch_root_owned_regular_lstat(monkeypatch, policy)

    container, interface, host_interface = awg_helper._load_policy(policy)

    assert container == "awg"
    assert interface == "awg0"
    assert host_interface == "amn0"


def test_load_policy_falls_back_to_interface_when_host_missing(tmp_path: Path, monkeypatch):
    policy = tmp_path / "policy.json"
    policy.write_text('{"container":"awg","interface":"awg0"}', encoding="utf-8")
    policy.chmod(0o640)
    _patch_root_owned_regular_lstat(monkeypatch, policy)

    _, interface, host_interface = awg_helper._load_policy(policy)

    assert interface == "awg0"
    assert host_interface == "awg0"


def test_qos_set_uses_host_interface(monkeypatch, capsys):
    host_calls = []
    docker_calls = []

    monkeypatch.setattr(awg_helper, "_load_policy", lambda path=None: ("awg", "awg0", "amn0"))
    monkeypatch.setattr(awg_helper, "_run", lambda args, stdin_text=None: host_calls.append(args) or "")
    monkeypatch.setattr(awg_helper, "_docker_exec", lambda container, cmd, stdin_text=None: docker_calls.append((container, cmd)) or "")

    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "qos-set", "--ip", "10.8.1.11", "--rate-mbit", "50"])

    rc = awg_helper.main()

    assert rc == 0
    assert all(cmd[cmd.index("dev") + 1] == "amn0" for cmd in host_calls)
    assert all(container == "awg" for container, _ in docker_calls)
    assert all(cmd[cmd.index("dev") + 1] == "awg0" for _, cmd in docker_calls)
    assert any(_has_match_clause(cmd, "dst", "10.8.1.11") for cmd in host_calls)
    assert any(_has_match_clause(cmd, "src", "10.8.1.11") for cmd in host_calls)
    assert any(_has_match_clause(cmd, "dst", "10.8.1.11") for _, cmd in docker_calls)
    assert any(_has_match_clause(cmd, "src", "10.8.1.11") for _, cmd in docker_calls)
    assert "qos set 10.8.1.11 50mbit" in capsys.readouterr().out


def test_qos_set_falls_back_to_delete_add_when_replace_not_supported(monkeypatch):
    calls = []

    def fake_run(args, stdin_text=None):
        calls.append(args)
        if args[:3] == ["tc", "qdisc", "replace"]:
            raise RuntimeError("Error: Change operation not supported by specified qdisc.")
        if args[:3] == ["tc", "qdisc", "del"]:
            raise RuntimeError("RTNETLINK answers: No such file or directory")
        return ""

    monkeypatch.setattr(awg_helper, "_run", fake_run)

    awg_helper._ensure_qos_root_qdisc("amn0")

    assert calls == [
        ["tc", "qdisc", "replace", "dev", "amn0", "root", "handle", "1:", "htb", "default", "9999"],
        ["tc", "qdisc", "del", "dev", "amn0", "root"],
        ["tc", "qdisc", "add", "dev", "amn0", "root", "handle", "1:", "htb", "default", "9999"],
    ]


def test_qos_sync_uses_host_interface(monkeypatch, capsys):
    host_calls = []
    docker_calls = []

    monkeypatch.setattr(awg_helper, "_load_policy", lambda path=None: ("awg", "awg0", "amn0"))
    monkeypatch.setattr(awg_helper, "_run", lambda args, stdin_text=None: host_calls.append(args) or "")
    monkeypatch.setattr(awg_helper, "_docker_exec", lambda container, cmd, stdin_text=None: docker_calls.append((container, cmd)) or "")
    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "qos-sync"])
    monkeypatch.setattr(sys, "stdin", type("FakeStdin", (), {"read": lambda self: "10.8.1.11,50\n"})())

    rc = awg_helper.main()

    assert rc == 0
    assert all(cmd[cmd.index("dev") + 1] == "amn0" for cmd in host_calls)
    assert all(container == "awg" for container, _ in docker_calls)
    assert all(cmd[cmd.index("dev") + 1] == "awg0" for _, cmd in docker_calls)
    assert any(_has_match_clause(cmd, "dst", "10.8.1.11") for cmd in host_calls)
    assert any(_has_match_clause(cmd, "src", "10.8.1.11") for cmd in host_calls)
    assert any(_has_match_clause(cmd, "dst", "10.8.1.11") for _, cmd in docker_calls)
    assert any(_has_match_clause(cmd, "src", "10.8.1.11") for _, cmd in docker_calls)
    assert "qos synced 1" in capsys.readouterr().out


def test_show_uses_awg_interface(monkeypatch, capsys):
    monkeypatch.setattr(awg_helper, "_load_policy", lambda path=None: ("awg", "awg0", "amn0"))
    docker_calls = []

    def fake_exec(container, cmd, stdin_text=None):
        docker_calls.append((container, cmd))
        return "ok"

    monkeypatch.setattr(awg_helper, "_docker_exec", fake_exec)
    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "show"])

    rc = awg_helper.main()

    assert rc == 0
    assert docker_calls == [("awg", ["awg", "show", "awg0"])]
    assert "ok" in capsys.readouterr().out


def test_installer_policy_writer_includes_host_interface_field():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")

    assert '"host_interface": "${host_interface}"' in script
    assert 'host_interface="$(get_env_value WG_HOST_INTERFACE)"' in script
    assert "detect_host_qos_interface" in script
    assert "WG_HOST_INTERFACE (хост для tc/QoS)" in script
