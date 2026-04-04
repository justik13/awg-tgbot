from pathlib import Path


def test_manual_install_flow_prompts_denylist_defaults():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "EGRESS_DENYLIST_ENABLED" in script
    assert "EGRESS_DENYLIST_MODE" in script


def test_status_shows_awg_target_and_policy_target():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "AWG target (.env):" in script
    assert "AWG target (helper policy):" in script
