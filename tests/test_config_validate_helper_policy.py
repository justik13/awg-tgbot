import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

from config_validate import read_helper_policy, validate_helper_policy


class _Logger:
    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def test_read_helper_policy_falls_back_host_interface_to_interface(tmp_path: Path):
    policy = tmp_path / "helper.json"
    policy.write_text('{"container":"awg","interface":"awg0"}', encoding="utf-8")

    container, interface, host_interface, error = read_helper_policy(policy)

    assert error == ""
    assert container == "awg"
    assert interface == "awg0"
    assert host_interface == "awg0"


def test_validate_helper_policy_detects_host_interface_mismatch(tmp_path: Path):
    policy = tmp_path / "helper.json"
    policy.write_text('{"container":"awg","interface":"awg0","host_interface":"awg0"}', encoding="utf-8")

    try:
        validate_helper_policy(
            policy_path=str(policy),
            docker_container="awg",
            wg_interface="awg0",
            wg_host_interface="amn0",
            logger=_Logger(),
        )
    except RuntimeError as e:
        assert "AWG helper policy mismatch" in str(e)
        assert "env=awg/awg0/amn0" in str(e)
        assert "policy=awg/awg0/awg0" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_config_wires_wg_host_interface_into_helper_policy_validation():
    body = Path("bot/config.py").read_text(encoding="utf-8")

    assert "WG_HOST_INTERFACE = env_with_runtime_default('WG_HOST_INTERFACE', '') or WG_INTERFACE" in body
    assert "wg_host_interface=WG_HOST_INTERFACE" in body
