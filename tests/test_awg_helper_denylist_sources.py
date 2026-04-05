import sys
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
import awg_helper  # noqa: E402


def test_bridge_mode_uses_container_ipv4(monkeypatch):
    monkeypatch.setattr(
        awg_helper,
        "_inspect_container_network",
        lambda _container: {
            "HostConfig": {"NetworkMode": "bridge"},
            "NetworkSettings": {
                "Networks": {
                    "bridge": {"IPAddress": "172.29.172.2"},
                }
            },
        },
    )

    selectors, source_desc = awg_helper._derive_denylist_sources("amnezia-awg2", "10.8.1.0/24")

    assert selectors == ["172.29.172.2/32"]
    assert source_desc == "container bridge IP 172.29.172.2"


def test_host_mode_uses_vpn_subnet(monkeypatch):
    monkeypatch.setattr(
        awg_helper,
        "_inspect_container_network",
        lambda _container: {
            "HostConfig": {"NetworkMode": "host"},
            "NetworkSettings": {"Networks": {}},
        },
    )

    selectors, source_desc = awg_helper._derive_denylist_sources("amnezia-awg2", "10.8.1.0/24")

    assert selectors == ["10.8.1.0/24"]
    assert source_desc == "VPN subnet 10.8.1.0/24"


def test_multiple_container_ipv4s_are_all_reflected(monkeypatch):
    monkeypatch.setattr(
        awg_helper,
        "_inspect_container_network",
        lambda _container: {
            "HostConfig": {"NetworkMode": "awg-net"},
            "NetworkSettings": {
                "Networks": {
                    "net1": {"IPAddress": "172.29.172.2"},
                    "net2": {"IPAddress": "172.30.0.7"},
                }
            },
        },
    )

    selectors, source_desc = awg_helper._derive_denylist_sources("amnezia-awg2", "10.8.1.0/24")

    assert selectors == ["172.29.172.2/32", "172.30.0.7/32"]
    assert source_desc == "container bridge IPs 172.29.172.2, 172.30.0.7"
    assert awg_helper._render_denylist_rule(selectors) == (
        'add rule inet filter awg_forward ip saddr { 172.29.172.2/32, 172.30.0.7/32 } '
        'ip daddr @awg_denylist drop comment "awg_denylist"\n'
    )


def test_rendered_rule_is_deterministic_for_idempotent_sync():
    selectors = ["172.29.172.2/32"]

    first = awg_helper._render_denylist_rule(selectors)
    second = awg_helper._render_denylist_rule(selectors)

    assert first == second


def test_soft_mode_falls_back_to_vpn_subnet_on_source_discovery_failure(monkeypatch):
    def _raise(*_args, **_kwargs):
        raise RuntimeError("inspect failed")

    monkeypatch.setattr(awg_helper, "_derive_denylist_sources", _raise)

    selectors, source_desc = awg_helper._resolve_denylist_sources("amnezia-awg2", "10.8.1.0/24", "soft")

    assert selectors == ["10.8.1.0/24"]
    assert source_desc == "VPN subnet 10.8.1.0/24 (fallback: source discovery failed)"


def test_strict_mode_raises_on_source_discovery_failure(monkeypatch):
    def _raise(*_args, **_kwargs):
        raise RuntimeError("inspect failed")

    monkeypatch.setattr(awg_helper, "_derive_denylist_sources", _raise)

    try:
        awg_helper._resolve_denylist_sources("amnezia-awg2", "10.8.1.0/24", "strict")
    except RuntimeError as e:
        assert "cannot derive denylist source selector in strict mode" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_safe_ipv4_cidr_rejects_ipv6_with_clear_error():
    try:
        awg_helper._safe_ipv4_cidr("2001:db8::/64", "denylist entry")
    except ValueError as e:
        assert "IPv6 CIDR is not supported for denylist entry" in str(e)
        assert "IPv4-only" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_denylist_sync_rejects_ipv6_entry_from_stdin(monkeypatch, capsys):
    monkeypatch.setattr(awg_helper, "_load_policy", lambda: ("amnezia-awg2", "awg0"))
    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "denylist-sync", "--vpn-subnet", "10.8.1.0/24", "--mode", "soft"])
    monkeypatch.setattr(sys, "stdin", io.StringIO("2001:db8::/64\n"))

    rc = awg_helper.main()
    captured = capsys.readouterr()

    assert rc == 1
    assert "IPv6 CIDR is not supported for denylist entry" in captured.err


def test_denylist_clear_rejects_ipv6_vpn_subnet(monkeypatch, capsys):
    monkeypatch.setattr(awg_helper, "_load_policy", lambda: ("amnezia-awg2", "awg0"))
    monkeypatch.setattr(sys, "argv", ["awg_helper.py", "denylist-clear", "--vpn-subnet", "2001:db8::/64"])

    rc = awg_helper.main()
    captured = capsys.readouterr()

    assert rc == 1
    assert "IPv6 CIDR is not supported for vpn-subnet" in captured.err
