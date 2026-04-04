import sys
import os
import stat
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
import awg_helper  # noqa: E402


def _patch_root_owned_regular_lstat(monkeypatch, policy: Path):
    original_lstat = awg_helper.Path.lstat

    def fake_lstat(self):
        st = original_lstat(self)
        if self == policy:
            values = list(st)
            values[0] = stat.S_IFREG | 0o640
            values[4] = 0
            return os.stat_result(values)
        return st

    monkeypatch.setattr(awg_helper.Path, "lstat", fake_lstat)


def test_load_policy_reads_container_interface(tmp_path: Path, monkeypatch):
    policy = tmp_path / "policy.json"
    policy.write_text('{"container":"awg","interface":"awg0"}', encoding="utf-8")
    policy.chmod(0o640)
    _patch_root_owned_regular_lstat(monkeypatch, policy)

    container, interface = awg_helper._load_policy(policy)

    assert container == "awg"
    assert interface == "awg0"


def test_parser_has_no_qos_commands():
    parser = awg_helper.build_parser()
    choices = set(parser._subparsers._group_actions[0].choices.keys())
    assert "qos-set" not in choices
    assert "qos-clear" not in choices
    assert "qos-sync" not in choices


def test_denylist_sync_targets_policy_container_and_interface(monkeypatch):
    monkeypatch.setattr(awg_helper, "_load_policy", lambda path=None: ("awg", "awg0"))
    calls = []

    def fake_run_nft(container, args, stdin_text=None):
        calls.append((container, list(args), stdin_text))
        return ""

    monkeypatch.setattr(awg_helper, "_run_nft", fake_run_nft)
    monkeypatch.setattr(awg_helper, "_run_nft_script", lambda container, script: calls.append((container, ["-f", "-"], script)) or "")
    monkeypatch.setattr(awg_helper, "_ensure_denylist_primitives", lambda container: calls.append((container, ["ensure"], None)))

    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "denylist-sync", "--vpn-subnet", "10.8.0.0/24"])
    monkeypatch.setattr(sys, "stdin", io.StringIO("1.1.1.1/32\n"))
    assert awg_helper.main() == 0

    assert ("awg", ["ensure"], None) in calls
    assert ("awg", ["flush", "chain", "inet", "filter", "awg_forward"], None) in calls
    assert ("awg", ["flush", "set", "inet", "filter", "awg_denylist"], None) in calls
    scripts = [entry[2] for entry in calls if entry[1] == ["-f", "-"]]
    assert any('add element inet filter awg_denylist { 1.1.1.1/32 }' in script for script in scripts)
    assert any('iifname "awg0" ip saddr 10.8.0.0/24 ip daddr @awg_denylist drop' in script for script in scripts)
