from pathlib import Path


def test_installer_defaults_point_to_real_repo_and_main_branch():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'DEFAULT_REPO_BRANCH="main"' in script
    assert 'REPO_OWNER="${REPO_OWNER:-${DETECTED_REPO_OWNER:-Just1k13}}"' in script
    assert 'REPO_NAME="${REPO_NAME:-${DETECTED_REPO_NAME:-awg-tgbot}}"' in script
    assert "awg-tgbot-selfhost" not in script


def test_installer_has_restore_flow_in_menu_and_actions():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "restore_from_backup()" in script
    assert "restore) restore_from_backup ;;" in script
    assert "Restore backup" in script


def test_restore_flow_repairs_owner_and_permissions_for_env_and_db():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'repair_runtime_file_access "$db_file" 600' in script
    assert 'repair_runtime_file_access "$ENV_FILE" 600' in script
    assert 'chown "$BOT_USER:$BOT_USER" "$target_path"' in script


def test_manual_install_flow_prompts_network_policy_defaults():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "QOS_ENABLED" in script
    assert "DEFAULT_KEY_RATE_MBIT" in script
    assert "QOS_STRICT" in script
    assert "EGRESS_DENYLIST_ENABLED" in script
    assert "EGRESS_DENYLIST_MODE" in script
