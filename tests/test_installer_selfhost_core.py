from pathlib import Path


def test_manual_install_flow_prompts_denylist_defaults():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "EGRESS_DENYLIST_ENABLED" in script
    assert "EGRESS_DENYLIST_MODE" in script


def test_status_shows_awg_target_and_policy_target():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "AWG target (.env):" in script
    assert "AWG target (helper policy):" in script


def test_write_awg_helper_policy_writes_valid_json_without_trailing_comma():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert '"container": "${container}",' in script
    assert '"interface": "${interface}"' in script
    assert '"interface": "${interface}",' not in script


def test_logs_doctor_marks_helper_policy_parse_error_as_critical():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "helper policy parse failed:" in script
    assert "КРИТИЧНО: исправь JSON в /etc/awg-bot-helper.json." in script
    assert '&& -z "$policy_error"' in script
