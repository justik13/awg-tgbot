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


def test_helper_policy_non_object_json_is_handled_as_structured_error():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "if not isinstance(data, dict):" in script
    assert 'error = "helper policy must be a JSON object"' in script


def test_reinstall_includes_runtime_snapshot_smokecheck_and_rollback_hooks():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "create_runtime_snapshot_before_reinstall" in script
    assert "run_post_restart_smokecheck" in script
    assert "rollback_failed_reinstall" in script
    assert "Переустановка не прошла smokecheck. Выполнен rollback" in script
    assert '"$AWG_HELPER_TARGET" check-awg' in script
    assert "AWG container reachable" not in script
    assert "awg_check_rc=$?" in script
    assert '"$awg_check_rc" -ne 0' in script
    assert "runtime_not_ready:" in script
    assert "schema_not_ready" in script
    assert "from dotenv import load_dotenv" in script
    assert "load_dotenv(env_file, override=True)" in script
    assert "load_dotenv(env_file, override=False)" not in script
    assert "schema_not_ready:path=" in script
    assert "runtime_not_ready:path=" in script
    assert "install_dir = os.path.dirname(bot_dir)" in script
    assert "os.chdir(install_dir)" in script
    assert 'for raw in open(env_file, "r", encoding="utf-8").read().splitlines()' not in script
    assert 'line.split("=", 1)' not in script
    assert "os.environ.setdefault(" not in script


def test_restore_includes_post_restore_smokecheck_and_rollback_message():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "Восстановление не прошло post-restore smokecheck. Запускаю rollback." in script
    assert "Восстановление не удалось; rollback выполнен полностью" in script
    assert "Восстановление не удалось; rollback выполнен частично" in script
    assert "sync_awg_helper_policy_from_env" in script


def test_installer_dependencies_include_nftables_for_denylist():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "nftables" in script


def test_rollback_restores_state_metadata_and_realigns_dependencies():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "version_file.before" in script
    assert "repo_branch.before" in script
    assert "ensure_venv_and_requirements" in script
    assert "Rollback: не удалось переустановить зависимости" in script


def test_reinstall_rollback_messages_report_full_or_partial_outcome():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "Переустановка не удалась; rollback выполнен полностью" in script
    assert "Переустановка не удалась; rollback выполнен частично" in script


def test_reinstall_stops_service_before_runtime_snapshot():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    stop_pos = script.find("stop_service_if_exists")
    runtime_snapshot_pos = script.find("create_runtime_snapshot_before_reinstall pre-reinstall")
    assert stop_pos != -1 and runtime_snapshot_pos != -1
    assert stop_pos < runtime_snapshot_pos


def test_rollback_stops_service_before_restoring_repo_and_runtime():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'systemctl stop "$SERVICE_NAME" 2>/dev/null || true' in script
    assert 'local repo_snapshot_dir="$1" runtime_snapshot_dir="$2" pending_log_archive="$3"' in script
    assert "Rollback: останавливаю текущий неудачный сервис перед восстановлением runtime." in script
    rollback_pos = script.find("rollback_failed_reinstall()")
    stop_pos = script.find('systemctl stop "$SERVICE_NAME" 2>/dev/null || true', rollback_pos)
    restore_log_pos = script.find('restore_bot_log_after_failed_reinstall "$pending_log_archive"', rollback_pos)
    restore_repo_pos = script.find('restore_repo_snapshot_after_failed_reinstall "$repo_snapshot_dir"', rollback_pos)
    restore_db_pos = script.find('install -m 600 "$runtime_snapshot_dir/db.before" "$db_file"', rollback_pos)
    assert stop_pos != -1 and restore_log_pos != -1 and restore_repo_pos != -1 and restore_db_pos != -1
    assert stop_pos < restore_log_pos
    assert restore_log_pos < restore_repo_pos
    assert stop_pos < restore_db_pos
