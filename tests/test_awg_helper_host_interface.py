import sys
import os
import stat
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
