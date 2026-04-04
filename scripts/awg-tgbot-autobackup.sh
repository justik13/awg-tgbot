#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKUP_DIR="${INSTALL_DIR}/backups"
ENV_FILE="${INSTALL_DIR}/.env"
MANUAL_BACKUP_CMD=("${INSTALL_DIR}/awg-tgbot.sh" backup)

DEFAULT_KEEP="14"
DEFAULT_ENABLED="1"

log() {
  printf '[autobackup] %s\n' "$*"
}

env_value() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] || return 0
  awk -F= -v k="$key" '$1==k {print substr($0, index($0, "=")+1); exit}' "$ENV_FILE" 2>/dev/null || true
}

enabled="${AUTO_BACKUP_ENABLED:-$(env_value AUTO_BACKUP_ENABLED)}"
keep_count="${AUTO_BACKUP_KEEP_COUNT:-$(env_value AUTO_BACKUP_KEEP_COUNT)}"

[[ -n "$enabled" ]] || enabled="$DEFAULT_ENABLED"
[[ -n "$keep_count" ]] || keep_count="$DEFAULT_KEEP"
if [[ ! "$keep_count" =~ ^[0-9]+$ ]] || (( keep_count < 1 )); then
  keep_count="$DEFAULT_KEEP"
fi

if [[ "$enabled" != "1" ]]; then
  log "skip: AUTO_BACKUP_ENABLED=${enabled}"
  exit 0
fi

if ! "${MANUAL_BACKUP_CMD[@]}" >/dev/null; then
  log "backup failed"
  exit 1
fi

latest_archive="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' | sort -r | head -n1 || true)"
mapfile -t archives < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' | sort -r)

pruned=0
prune_errors=0
if (( ${#archives[@]} > keep_count )); then
  for old_archive in "${archives[@]:keep_count}"; do
    if rm -f -- "$old_archive"; then
      pruned=$((pruned + 1))
    else
      prune_errors=$((prune_errors + 1))
    fi
  done
fi

log "backup created: ${latest_archive:-unknown}"
log "retention keep=${keep_count}, pruned=${pruned}"
if (( prune_errors > 0 )); then
  log "retention warnings: failed_to_prune=${prune_errors}"
fi
