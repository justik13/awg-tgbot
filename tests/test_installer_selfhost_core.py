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


def test_installer_does_not_grant_bot_write_access_to_entire_install_dir():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'chown -R "$BOT_USER:$BOT_USER" "$INSTALL_DIR" "$APP_LOG_DIR"' not in script
    assert "enforce_root_owned_code_paths" in script
    assert 'for path in "$INSTALL_DIR/awg-tgbot.sh" "$BOT_DIR" "$INSTALL_DIR/scripts" "$INSTALL_DIR/packaging" "$VENV_DIR"' in script


def test_installer_uses_runtime_db_dir_with_absolute_default_path():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'RUNTIME_DIR="${INSTALL_DIR}/runtime"' in script
    assert 'DEFAULT_DB_PATH="${RUNTIME_DIR}/${DEFAULT_DB_BASENAME}"' in script
    assert 'set_env_value DB_PATH "$DEFAULT_DB_PATH"' in script
    assert 'set_env_value DB_PATH "vpn_bot.db"' not in script


def test_runtime_dir_permissions_are_narrow_and_bot_writable():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'mkdir -p "$RUNTIME_DIR"' in script
    assert 'chown "$BOT_USER:$BOT_USER" "$RUNTIME_DIR"' in script
    assert 'chmod 750 "$RUNTIME_DIR"' in script


def test_legacy_default_db_migration_copies_main_db_and_sqlite_sidecars():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "migrate_legacy_default_db_path" in script
    assert "copy_sqlite_runtime_bundle" in script
    assert 'copy_sqlite_runtime_bundle "$old_db_file" "$DEFAULT_DB_PATH"' in script
    assert 'for suffix in "-wal" "-shm"; do' in script
    assert 'set_env_value DB_PATH "$DEFAULT_DB_PATH"' in script
    assert "migrate_legacy_default_db_path || die" in script


def test_sqlite_bundle_helpers_are_shared_across_snapshot_restore_and_backup_flows():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "snapshot_sqlite_runtime_bundle()" in script
    assert "restore_sqlite_runtime_bundle()" in script
    assert "sqlite_runtime_quick_check()" in script
    assert "validate_backup_archive_payload()" in script
    assert "wait_for_service_stopped_state()" in script
    assert "wait_for_service_active_state()" in script
    assert "collect_existing_sqlite_bundle_basenames()" in script
    assert 'snapshot_sqlite_runtime_bundle "$db_file" "$snapshot_dir" "db.before"' in script
    assert 'restore_sqlite_runtime_bundle "$runtime_snapshot_dir/db.before" "$db_file"' in script
    assert 'mapfile -t db_bundle_names < <(collect_existing_sqlite_bundle_basenames "$snapshot_dir/$db_basename")' in script


def test_backup_and_restore_include_sqlite_wal_sidecars_when_present():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert '-C "$snapshot_dir" "${db_bundle_names[@]}"' in script
    assert 'restore_members+=("${db_basename}-wal")' in script
    assert 'restore_members+=("${db_basename}-shm")' in script
    assert 'restore_sqlite_runtime_bundle "$tmp_restore/$db_basename" "$db_file"' in script
    assert 'if ! systemctl stop "$SERVICE_NAME" 2>/dev/null; then' in script
    assert 'validate_backup_archive_payload "$archive_file" "$db_basename"' in script
    assert 'sqlite_runtime_quick_check "$tmp_restore/$db_basename"' in script


def test_backup_fail_closed_for_active_service_stop_and_restart_are_validated():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'if ! systemctl stop "$SERVICE_NAME" 2>/dev/null; then' in script
    assert "бэкап отменён (fail-closed для active service)" in script
    assert 'if [[ "$service_active_before" == "active" ]]; then' in script
    assert "не перешёл в безопасное stopped-state после stop" in script
    assert 'elif [[ "$service_active_before" != "inactive" && "$service_active_before" != "failed" ]]; then' in script
    assert 'if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then' in script
    assert "if ! wait_for_service_active_state; then" in script
    assert "Бот может остаться остановленным. Проверьте вручную: systemctl status" in script


def test_backup_success_reporting_happens_after_restart_validation():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    create_backup_pos = script.find("create_local_backup()")
    start_pos = script.find('if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then', create_backup_pos)
    active_check_pos = script.find("if ! wait_for_service_active_state; then", create_backup_pos)
    success_pos = script.find('ok "Бэкап сохранён: ${archive_file}"', create_backup_pos)
    assert start_pos != -1 and active_check_pos != -1 and success_pos != -1
    assert start_pos < active_check_pos < success_pos


def test_restore_fail_closed_when_stop_fails_or_service_stays_active():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "Restore отменён: не удалось остановить" in script
    assert "Restore отменён: ${SERVICE_NAME} не перешёл в безопасное stopped-state после stop" in script
    assert 'service_state_after_stop="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"' in script
    assert 'elif [[ "$service_active_before" != "inactive" && "$service_active_before" != "failed" ]]; then' in script


def test_service_stop_wait_poll_logic_requires_inactive_or_failed_states():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "wait_for_service_stopped_state()" in script
    assert 'for ((attempt = 1; attempt <= max_attempts; attempt++)); do' in script
    assert 'if [[ "$state" == "inactive" || "$state" == "failed" ]]; then' in script
    assert 'sleep "$delay_seconds"' in script


def test_service_start_wait_poll_logic_requires_active_state():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "wait_for_service_active_state()" in script
    assert 'for ((attempt = 1; attempt <= max_attempts; attempt++)); do' in script
    assert 'if [[ "$state" == "active" ]]; then' in script
    assert 'sleep "$delay_seconds"' in script


def test_remove_keep_db_and_env_preserves_sqlite_bundle_not_only_main_db():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'snapshot_sqlite_runtime_bundle "$db_file" "$db_tmp" "db.keep"' in script
    assert 'restore_sqlite_runtime_bundle "$db_tmp/db.keep" "$db_path"' in script


def test_custom_db_path_is_not_treated_as_legacy_default_during_migration():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "elif [[ \"$current_db_path\" == \"$LEGACY_DB_PATH\" ]]; then" in script
    assert "else" in script
    assert "return 0" in script


def test_generated_service_wants_docker_service():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "After=network-online.target docker.service" in script
    assert "Wants=network-online.target docker.service" in script


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


def test_selfhost_deploy_prunes_dev_only_paths_from_runtime_install_dir():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "prune_selfhost_runtime_footprint()" in script
    assert 'for target in "$INSTALL_DIR/tests" "$INSTALL_DIR/.github"; do' in script
    deploy_pos = script.find("deploy_repo()")
    prune_call_pos = script.find("&& prune_selfhost_runtime_footprint; then", deploy_pos)
    assert deploy_pos != -1 and prune_call_pos != -1
    assert deploy_pos < prune_call_pos


def test_reinstall_rollback_restore_reapplies_runtime_prune_contract():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    restore_repo_pos = script.find("restore_repo_snapshot_after_failed_reinstall()")
    prune_call_pos = script.find("prune_selfhost_runtime_footprint", restore_repo_pos)
    assert restore_repo_pos != -1 and prune_call_pos != -1
    assert restore_repo_pos < prune_call_pos


def test_pruning_targets_runtime_install_dir_only_not_repo_tree():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert '"$INSTALL_DIR/tests"' in script
    assert '"$INSTALL_DIR/.github"' in script
    assert '"$src_dir/tests"' not in script
    assert '"$src_dir/.github"' not in script


def test_prepare_bot_log_for_reinstall_returns_newline_terminated_read_payload():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "printf '%s\\t%s\\n' \"$pending_archive\" \"$final_archive\"" in script
    assert "printf '%s\\t%s' \"$pending_archive\" \"$final_archive\"" not in script


def test_rollback_stops_service_before_restoring_repo_and_runtime():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'systemctl stop "$SERVICE_NAME" 2>/dev/null || true' in script
    assert 'local repo_snapshot_dir="$1" runtime_snapshot_dir="$2" pending_log_archive="$3"' in script
    assert "Rollback: останавливаю текущий неудачный сервис перед восстановлением runtime." in script
    rollback_pos = script.find("rollback_failed_reinstall()")
    stop_pos = script.find('systemctl stop "$SERVICE_NAME" 2>/dev/null || true', rollback_pos)
    restore_log_pos = script.find('restore_bot_log_after_failed_reinstall "$pending_log_archive"', rollback_pos)
    restore_repo_pos = script.find('restore_repo_snapshot_after_failed_reinstall "$repo_snapshot_dir"', rollback_pos)
    restore_db_pos = script.find('restore_sqlite_runtime_bundle "$runtime_snapshot_dir/db.before" "$db_file"', rollback_pos)
    assert stop_pos != -1 and restore_log_pos != -1 and restore_repo_pos != -1 and restore_db_pos != -1
    assert stop_pos < restore_log_pos
    assert restore_log_pos < restore_repo_pos
    assert stop_pos < restore_db_pos
