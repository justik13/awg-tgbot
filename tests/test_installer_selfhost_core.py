from pathlib import Path


def test_installer_defaults_point_to_main_branch_and_not_old_owner():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'DEFAULT_REPO_BRANCH="main"' in script
    assert 'REPO_OWNER="Just1k13"' not in script


def test_installer_has_restore_flow_in_menu_and_actions():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "restore_from_backup()" in script
    assert "restore) restore_from_backup ;;" in script
    assert "Restore backup" in script
