import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

from config_validate import read_helper_policy, validate_helper_policy


class _Logger:
    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def test_read_helper_policy_container_interface(tmp_path: Path):
    policy = tmp_path / "helper.json"
    policy.write_text('{"container":"awg","interface":"awg0"}', encoding="utf-8")

    container, interface, error = read_helper_policy(policy)

    assert error == ""
    assert container == "awg"
    assert interface == "awg0"


def test_validate_helper_policy_detects_interface_mismatch(tmp_path: Path):
    policy = tmp_path / "helper.json"
    policy.write_text('{"container":"awg","interface":"awg0"}', encoding="utf-8")

    try:
        validate_helper_policy(
            policy_path=str(policy),
            docker_container="awg",
            wg_interface="awg1",
            logger=_Logger(),
        )
    except RuntimeError as e:
        assert "AWG helper policy mismatch" in str(e)
        assert "env=awg/awg1" in str(e)
        assert "policy=awg/awg0" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_read_helper_policy_reports_parse_error(tmp_path: Path):
    policy = tmp_path / "helper.json"
    policy.write_text("{container: awg", encoding="utf-8")

    container, interface, error = read_helper_policy(policy)

    assert container == ""
    assert interface == ""
    assert error.startswith("helper policy parse failed:")
