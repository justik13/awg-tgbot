from pathlib import Path


def test_installer_defaults_point_to_real_repo_and_main_branch():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'DEFAULT_REPO_BRANCH="main"' in script
    assert 'REPO_OWNER="${REPO_OWNER:-${DETECTED_REPO_OWNER:-justik13}}"' in script
    assert 'REPO_NAME="${REPO_NAME:-${DETECTED_REPO_NAME:-awg-tgbot}}"' in script
    assert "awg-tgbot-selfhost" not in script


def test_installer_has_restore_flow_in_menu_and_actions():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "restore_from_backup()" in script
    assert "restore) restore_from_backup ;;" in script
    assert "Восстановить из бэкапа" in script


def test_restore_flow_repairs_owner_and_permissions_for_env_and_db():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'repair_runtime_file_access "$db_file" 600' in script
    assert 'repair_runtime_file_access "$ENV_FILE" 600' in script
    assert 'chown "$BOT_USER:$BOT_USER" "$target_path"' in script


def test_manual_install_flow_prompts_network_policy_defaults():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "QOS_ENABLED" in script
    assert "DEFAULT_KEY_RATE_MBIT" in script
    assert 'pick_existing_or_default "$(get_env_value DEFAULT_KEY_RATE_MBIT)" "$SELFHOST_DEFAULT_KEY_RATE_MBIT_DEFAULT"' in script
    assert "QOS_STRICT" in script
    assert "WG_HOST_INTERFACE (хост для tc/QoS)" in script
    assert "EGRESS_DENYLIST_ENABLED" in script
    assert "EGRESS_DENYLIST_MODE" in script


def test_auto_manual_install_share_selfhost_default_population():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "ensure_selfhost_network_defaults()" in script
    assert 'ensure_selfhost_network_defaults' in script
    assert 'set_env_value QOS_ENABLED "$SELFHOST_QOS_ENABLED_DEFAULT"' in script
    assert 'set_env_value EGRESS_DENYLIST_MODE "$SELFHOST_EGRESS_DENYLIST_MODE_DEFAULT"' in script
    assert 'set_env_value AUTO_BACKUP_ENABLED "$SELFHOST_AUTO_BACKUP_ENABLED_DEFAULT"' in script
    assert 'set_env_value AUTO_BACKUP_KEEP_COUNT "$SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT"' in script


def test_installer_status_and_detection_show_host_interface_for_qos():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "detect_host_qos_interface()" in script
    assert 'DETECTED_HOST_INTERFACE="$(pick_existing_or_default "$(get_env_value WG_HOST_INTERFACE)" "$(detect_host_qos_interface "$DETECTED_INTERFACE")")"' in script
    assert "QoS host-интерфейс (.env):" in script
    assert "QoS host-интерфейс (helper policy):" in script


def test_installer_integrates_autobackup_timer_and_manual_prompts():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'AUTO_BACKUP_ENABLED=1' not in script  # must come from defaults/prompt, not hardcoded runtime overrides
    assert "prompt_with_default 'Включить autobackup (1=ON,0=OFF)'" in script
    assert "prompt_with_default 'Сколько autobackup хранить (шт)'" in script
    assert "configure_autobackup_timer || die" in script
    assert "systemctl enable --now \"$AUTO_BACKUP_TIMER_NAME\"" in script
    assert "systemctl disable --now \"$AUTO_BACKUP_TIMER_NAME\"" in script


def test_deploy_rollback_preserves_scripts_and_packaging():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert '[[ -d "$INSTALL_DIR/scripts" ]] && mv "$INSTALL_DIR/scripts" "$backup_dir/scripts"' in script
    assert '[[ -d "$INSTALL_DIR/packaging" ]] && mv "$INSTALL_DIR/packaging" "$backup_dir/packaging"' in script
    assert '[[ -d "$backup_dir/scripts" ]] && mv "$backup_dir/scripts" "$INSTALL_DIR/scripts"' in script
    assert '[[ -d "$backup_dir/packaging" ]] && mv "$backup_dir/packaging" "$INSTALL_DIR/packaging"' in script


def test_deploy_optional_copy_and_chmod_use_grouped_conditions_not_invalid_test_syntax():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert '{ [[ ! -d "$src_dir/scripts" ]] || cp -a "$src_dir/scripts" "$INSTALL_DIR/scripts"; }' in script
    assert '{ [[ ! -d "$src_dir/packaging" ]] || cp -a "$src_dir/packaging" "$INSTALL_DIR/packaging"; }' in script
    assert '{ [[ ! -f "$AUTO_BACKUP_SCRIPT" ]] || chmod +x "$AUTO_BACKUP_SCRIPT"; }' in script
    assert '[[ ! -d "$src_dir/scripts" || cp -a "$src_dir/scripts" "$INSTALL_DIR/scripts" ]]' not in script
    assert '[[ ! -d "$src_dir/packaging" || cp -a "$src_dir/packaging" "$INSTALL_DIR/packaging" ]]' not in script
    assert '[[ ! -f "$AUTO_BACKUP_SCRIPT" || chmod +x "$AUTO_BACKUP_SCRIPT" ]]' not in script


def test_installer_does_not_have_unused_autobackup_schedule_setting():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "AUTO_BACKUP_TIMER_ONCALENDAR" not in script


def test_normal_remove_preserves_local_backups_and_full_remove_still_deletes_everything():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'find "$BACKUP_ROOT" -maxdepth 1 -type f -name \'awg-tgbot-backup-*.tar.gz\'' in script
    assert 'if ! mv "$BACKUP_ROOT" "$backup_stash"; then' in script
    assert 'if mv "$backup_stash" "$BACKUP_ROOT"; then' in script
    assert "Удаление остановлено до удаления данных, потому что не удалось безопасно сохранить backup-архивы." in script
    assert "Сохранены: БД, .env и локальные backup-архивы." in script
    assert 'cleanup_backup_tmp=0' in script
    assert 'warn "Архивы сохранены для ручного восстановления: ${backup_tmp_root}"' in script
    assert 'warn "Полное удаление уничтожит код, сервис, БД, .env и логи."' in script
    assert 'cp -a "$BACKUP_ROOT/." "$backup_tmp/" 2>/dev/null || true' not in script
    assert 'remove_everything' in script


def test_normal_remove_snapshots_backups_before_destructive_remove():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    snapshot_pos = script.find('if ! mv "$BACKUP_ROOT" "$backup_stash"; then')
    remove_pos = script.find("  remove_everything", snapshot_pos)
    assert snapshot_pos != -1
    assert remove_pos != -1
    assert snapshot_pos < remove_pos


def test_success_message_for_backup_preservation_is_guarded_by_restore_flags():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'if [[ "$REMOVE_BACKUPS_WERE_PRESENT" == "1" && "$REMOVE_BACKUPS_RESTORED" == "1" ]]; then' in script
    assert 'ok "Удалено приложение и сервис. Сохранены: БД, .env и локальные backup-архивы."' in script


def test_restore_fail_path_keeps_stash_and_reports_recovery_path():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'recovery_root="/var/tmp/awg-tgbot-backups-recovery-' in script
    assert 'if mv "$backup_tmp_root" "$recovery_root" 2>/dev/null; then' in script
    assert 'if [[ "$cleanup_backup_tmp" == "1" && -n "$backup_tmp_root" && -d "$backup_tmp_root" ]]; then' in script


def test_readme_contains_owner_operator_sections():
    readme = Path("README.md").read_text(encoding="utf-8")
    assert "## Что это" in readme
    assert "## Быстрый старт" in readme
    assert "## Как устроены бэкапы" in readme
    assert "AUTO_BACKUP_ENABLED" in readme
    assert "## Где смотреть код" in readme
    assert "раз в день" in readme
    assert "пункт `1) Установить`" in readme


def test_remove_confirmation_mentions_backups():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "оставив БД, .env и локальные backup-архивы?" in script


def test_remove_menu_line_mentions_local_backup_archives():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert "Обычное удаление (сохранить БД, .env и локальные backup-архивы)" in script


def test_ci_workflow_exists_and_runs_pytest_on_push_and_pr():
    workflow = Path(".github/workflows/ci.yml")
    assert workflow.exists()
    body = workflow.read_text(encoding="utf-8")
    assert "push:" in body
    assert "pull_request:" in body
    assert "pytest" in body
