#!/usr/bin/env bash
set -Eeuo pipefail

# Handle case when script is run via curl | bash (stdin has no path)
# BASH_SOURCE[0] may be unset when running from stdin, so we check it safely
if [[ -n "${BASH_SOURCE[0]:-}" ]] && [[ -f "${BASH_SOURCE[0]:-}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
  SCRIPT_DIR="$(pwd)"
fi
ORIGIN_URL="$(git -C "$SCRIPT_DIR" config --get remote.origin.url 2>/dev/null || true)"
if [[ "$ORIGIN_URL" =~ github\.com[:/]([^/]+)/([^/.]+)(\.git)?$ ]]; then
  DETECTED_REPO_OWNER="${BASH_REMATCH[1]}"
  DETECTED_REPO_NAME="${BASH_REMATCH[2]}"
else
  DETECTED_REPO_OWNER=""
  DETECTED_REPO_NAME=""
fi
REPO_OWNER="${REPO_OWNER:-${DETECTED_REPO_OWNER:-justik13}}"
REPO_NAME="${REPO_NAME:-${DETECTED_REPO_NAME:-awg-tgbot}}"
DEFAULT_REPO_BRANCH="main"
INSTALL_DIR="/opt/amnezia/bot"
RUNTIME_DIR="${INSTALL_DIR}/runtime"
DEFAULT_DB_BASENAME="vpn_bot.db"
DEFAULT_DB_PATH="${RUNTIME_DIR}/${DEFAULT_DB_BASENAME}"
LEGACY_DB_BASENAME="vpn_bot.db"
LEGACY_DB_PATH="${INSTALL_DIR}/${LEGACY_DB_BASENAME}"
STATE_DIR="${INSTALL_DIR}/.state"
REPO_BRANCH_FILE="${STATE_DIR}/repo_branch"
REPO_BRANCH="${REPO_BRANCH:-$(cat "$REPO_BRANCH_FILE" 2>/dev/null | tr -d '\r\n' || true)}"
REPO_BRANCH="${REPO_BRANCH:-$DEFAULT_REPO_BRANCH}"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}"
RAW_BASE_URL="https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${REPO_BRANCH}"
COMMIT_API_URL="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/commits/${REPO_BRANCH}"

BOT_DIR="${INSTALL_DIR}/bot"
ENV_FILE="${INSTALL_DIR}/.env"
VENV_DIR="${INSTALL_DIR}/.venv"
SERVICE_NAME="vpn-bot.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"
BOT_USER="awg-bot"
VERSION_FILE="${STATE_DIR}/release_sha"
INSTALL_LOG="/var/log/awg-tgbot-install.log"
APP_LOG_DIR="/var/log/awg-tgbot"
APP_LOG_FILE="${APP_LOG_DIR}/bot.log"
PYTHON_BIN="$(command -v python3 || echo "/usr/bin/python3")"
AWG_HELPER_TARGET="/usr/local/libexec/awg-bot-helper"
AWG_HELPER_SUDOERS="/etc/sudoers.d/awg-bot-helper"
AWG_HELPER_POLICY="/etc/awg-bot-helper.json"
TTY_DEVICE="/dev/tty"
# FD 3 и FD 4 будут открыты позже в setup_tty_fd после require_root
SELF_SYMLINK="/usr/local/bin/awg-tgbot"
SELFHOST_EGRESS_DENYLIST_ENABLED_DEFAULT="1"
SELFHOST_EGRESS_DENYLIST_MODE_DEFAULT="soft"
SELFHOST_EGRESS_DENYLIST_REFRESH_MINUTES_DEFAULT="30"
SELFHOST_AUTO_BACKUP_ENABLED_DEFAULT="1"
SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT="14"
AUTO_BACKUP_SCRIPT_REL="scripts/awg-tgbot-autobackup.sh"
AUTO_BACKUP_SCRIPT="${INSTALL_DIR}/${AUTO_BACKUP_SCRIPT_REL}"
AUTO_BACKUP_SERVICE_NAME="awg-tgbot-backup.service"
AUTO_BACKUP_TIMER_NAME="awg-tgbot-backup.timer"
AUTO_BACKUP_SERVICE_FILE="/etc/systemd/system/${AUTO_BACKUP_SERVICE_NAME}"
AUTO_BACKUP_TIMER_FILE="/etc/systemd/system/${AUTO_BACKUP_TIMER_NAME}"
PLATEGA_WEBHOOK_SERVICE_NAME="platega-webhook.service"
PLATEGA_WEBHOOK_SERVICE_FILE="/etc/systemd/system/${PLATEGA_WEBHOOK_SERVICE_NAME}"
BACKUP_ROOT="${INSTALL_DIR}/backups"
SAFETY_SNAPSHOT_PREFIX="${INSTALL_DIR}/.safety-snapshot"

DETECTED_CONTAINER=""
DETECTED_INTERFACE=""
DETECTED_CONFIG_PATH=""
DETECTED_PUBLIC_KEY=""
DETECTED_LISTEN_PORT=""
DETECTED_SERVER_IP=""
DETECTED_SERVER_NAME=""
DETECTED_PUBLIC_HOST=""
DETECTED_AWG_JC=""
DETECTED_AWG_JMIN=""
DETECTED_AWG_JMAX=""
DETECTED_AWG_S1=""
DETECTED_AWG_S2=""
DETECTED_AWG_S3=""
DETECTED_AWG_S4=""
DETECTED_AWG_H1=""
DETECTED_AWG_H2=""
DETECTED_AWG_H3=""
DETECTED_AWG_H4=""
DETECTED_AWG_I1=""
DETECTED_AWG_I2=""
DETECTED_AWG_I3=""
DETECTED_AWG_I4=""
DETECTED_AWG_I5=""

STATE_DOCKER_INSTALLED=0
STATE_DOCKER_DAEMON=0
STATE_AWG_CONTAINER_FOUND=0
STATE_AWG_INTERFACE_FOUND=0
STATE_AWG_CONFIG_FOUND=0
STATE_AWG_FOUND=0
STATE_BOT_SERVICE_FOUND=0
STATE_BOT_DIR_FOUND=0
STATE_BOT_APP_FOUND=0
STATE_BOT_SYMLINK_FOUND=0
STATE_BOT_ENV_FOUND=0
STATE_BOT_STATE_FOUND=0
STATE_BOT_INSTALLED=0
STATE_BOT_RESIDUAL=0
STATE_KERNEL_SUPPORTED=0
STATE_AMNEZIAWG_INSTALLED=0
STARTUP_STATE_CODE="unknown"
UPDATE_STATUS="not_applicable"
UPDATE_REMOTE_SHA=""
UPDATE_REMOTE_TITLE=""
UPDATE_LOCAL_SHA=""
UPDATE_CHECK_TS=0
UPDATE_CACHE_TTL=15
UPDATE_CACHE_BRANCH=""
REMOVE_BACKUPS_WERE_PRESENT=0
REMOVE_BACKUPS_RESTORED=0
REINSTALL_GUARD_ACTIVE=0
REINSTALL_GUARD_ROLLING_BACK=0
REINSTALL_GUARD_REPO_SNAPSHOT=""
REINSTALL_GUARD_RUNTIME_SNAPSHOT=""
REINSTALL_GUARD_PENDING_LOG=""

print_line() { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '[*] %s\n' "$*" >&2; }
ok() { printf '[+] %s\n' "$*" >&2; }
warn() { printf '[!] %s\n' "$*" >&2; }
error() { printf '[ERROR] %s\n' "$*" >&2; }
die() { error "$*"; exit 1; }

clear_reinstall_guard() {
  REINSTALL_GUARD_ACTIVE=0
  REINSTALL_GUARD_ROLLING_BACK=0
  REINSTALL_GUARD_REPO_SNAPSHOT=""
  REINSTALL_GUARD_RUNTIME_SNAPSHOT=""
  REINSTALL_GUARD_PENDING_LOG=""
}

set_reinstall_guard() {
  local repo_snapshot_dir="$1" runtime_snapshot_dir="$2"
  REINSTALL_GUARD_ACTIVE=1
  REINSTALL_GUARD_ROLLING_BACK=0
  REINSTALL_GUARD_REPO_SNAPSHOT="$repo_snapshot_dir"
  REINSTALL_GUARD_RUNTIME_SNAPSHOT="$runtime_snapshot_dir"
  REINSTALL_GUARD_PENDING_LOG=""
}

register_reinstall_guard_pending_log() {
  local pending_log_archive="${1:-}"
  REINSTALL_GUARD_PENDING_LOG="$pending_log_archive"
}

on_error_trap() {
  local line_no="${1:-unknown}" exit_code="${2:-1}"
  # Close TTY file descriptors to prevent leaks
  exec 3>&- 2>/dev/null || true
  exec 4>&- 2>/dev/null || true
  if [[ "$REINSTALL_GUARD_ACTIVE" == "1" && "$REINSTALL_GUARD_ROLLING_BACK" != "1" ]]; then
    REINSTALL_GUARD_ROLLING_BACK=1
    warn "Сработал аварийный rollback-guard для reinstall (line=${line_no}, rc=${exit_code})."
    set +e
    rollback_failed_reinstall "$REINSTALL_GUARD_REPO_SNAPSHOT" "$REINSTALL_GUARD_RUNTIME_SNAPSHOT" "$REINSTALL_GUARD_PENDING_LOG"
    set -e
    clear_reinstall_guard
  fi
  printf "[!] Ошибка на строке %s (rc=%s). Подробности: %s\n" "$line_no" "$exit_code" "$INSTALL_LOG" >&2
}
trap 'on_error_trap "$LINENO" "$?"' ERR
trap 'exec 3>&- 2>/dev/null || true; exec 4>&- 2>/dev/null || true' EXIT

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Запусти скрипт от root: sudo bash awg-tgbot.sh"
    echo "Или одной командой: curl -fsSL ${RAW_BASE_URL}/awg-tgbot.sh | sudo REPO_BRANCH=${REPO_BRANCH} bash -s --"
    exit 1
  fi
}

setup_logging() {
  mkdir -p "$(dirname "$INSTALL_LOG")" "$APP_LOG_DIR"
  touch "$INSTALL_LOG" "$APP_LOG_FILE"
  chmod 640 "$INSTALL_LOG" "$APP_LOG_FILE" || true
  # Sanitize logs to prevent leaking sensitive data
  # Use process substitution with explicit error handling
  if ! exec > >(tee -a "$INSTALL_LOG" | sed -e 's/BOT_TOKEN=[^ ]*/BOT_TOKEN=***REDACTED***/g' -e 's/PLATEGA_MERCHANT_ID=[^ ]*/PLATEGA_MERCHANT_ID=***REDACTED***/g' -e 's/PLATEGA_SECRET_KEY=[^ ]*/PLATEGA_SECRET_KEY=***REDACTED***/g') 2>&1; then
    warn "Не удалось настроить перенаправление логов, продолжаем без логирования в файл."
  fi
}

setup_tty_fd() {
  # Закрываем любые существующие FD 3 и FD 4 перед открытием новых
  exec 3>&- 2>/dev/null || true
  exec 4>&- 2>/dev/null || true
  
  if [[ -c "$TTY_DEVICE" ]]; then
    exec 3<>"$TTY_DEVICE"
    return 0
  fi
  # Fallback: используем stdin/stdout
  exec 3<&0 4>&1
}

has_tty() { [[ -t 3 ]]; }

supports_color() {
  has_tty && [[ "${TERM:-}" != "dumb" ]]
}

color_red() {
  local value="$1"
  if supports_color; then
    printf '\033[1;31m%s\033[0m' "$value"
  else
    printf '%s' "$value"
  fi
}

pause_if_tty() {
  if has_tty; then
    echo
    read -r -u 3 -p "Нажми Enter, чтобы продолжить..." _dummy || true
  fi
}

clear_if_tty() {
  if has_tty; then
    clear || true
  fi
}

screen_line() {
  if has_tty; then
    printf '%s\n' "------------------------------------------------------------" >&3
  else
    print_line
  fi
}

screen_echo() {
  if has_tty; then
    printf '%s\n' "$*" >&3
  else
    printf '%s\n' "$*"
  fi
}

screen_run() {
  if has_tty; then
    "$@" >&3 2>&1 || true
  else
    "$@" || true
  fi
}

prompt_raw() {
  local prompt="$1"
  local __resultvar="$2"
  local __input=""
  
  # Явно выводим подсказку перед чтением ввода
  printf '%s' "$prompt" >&3 2>/dev/null || printf '%s' "$prompt"
  
  # При запуске через curl | bash stdin перенаправлен, поэтому всегда читаем из /dev/tty
  # Это единственный способ получить интерактивный ввод пользователя
  if [[ -e "/dev/tty" ]]; then
    if ! read -r __input < /dev/tty; then
      warn "Не удалось прочитать ввод с /dev/tty."
      __input=""
    fi
  elif [[ -t 0 ]]; then
    # Fallback: stdin является терминалом (редкий случай)
    if ! read -r __input; then
      warn "Не удалось прочитать ввод со stdin."
      __input=""
    fi
  elif [[ -t 3 ]]; then
    # Fallback: fd 3 является терминалом
    if ! read -r __input <&3; then
      warn "Не удалось прочитать ввод из fd 3."
      __input=""
    fi
  else
    # Последний шанс: читаем из stdin (не интерактивно)
    if ! read -r __input; then
      warn "Не удалось прочитать ввод (fallback)."
      __input=""
    fi
  fi
  
  # Trim whitespace
  __input="${__input#"${__input%%[![:space:]]*}"}"
  __input="${__input%"${__input##*[![:space:]]}"}"
  
  printf -v "$__resultvar" '%s' "$__input"
}

prompt_menu_key() {
  local prompt="$1"
  local __resultvar="$2"
  local __input=""
  if ! has_tty; then
    die "Невозможно запросить ввод без TTY (menu: ${prompt}). Запусти скрипт в интерактивном терминале."
  fi
  if ! read -r -u 3 -n 1 -p "$prompt" __input; then
    __input=""
  fi
  echo >&3
  printf -v "$__resultvar" '%s' "$__input"
}

prompt_with_default() {
  local prompt="$1"
  local default="${2:-}"
  local __resultvar="$3"
  local input_value=""
  while true; do
    if [[ -n "$default" ]]; then
      prompt_raw "$prompt [$default]: " input_value
      input_value="${input_value:-$default}"
    else
      prompt_raw "$prompt: " input_value
    fi
    if [[ -n "$input_value" ]]; then
      printf -v "$__resultvar" '%s' "$input_value"
      return 0
    fi
    warn "Значение не может быть пустым."
  done
}


confirm_explicit() {
  local prompt="$1"
  local value=""
  while true; do
    prompt_raw "$prompt [y/n]: " value
    case "${value,,}" in
      y|yes|д|да) return 0 ;;
      n|no|н|нет) return 1 ;;
      *) warn "Нужно явное подтверждение: введи y или n." ;;
    esac
  done
}

confirm_delete_word() {
  local typed=""
  prompt_raw "Для полного удаления введите DELETE: " typed
  [[ "$typed" == "DELETE" ]]
}

require_command() { command -v "$1" >/dev/null 2>&1; }
service_exists() { [[ -f "$SERVICE_FILE" ]]; }
is_installed() { [[ -f "$SERVICE_FILE" && -d "$BOT_DIR" && -f "$BOT_DIR/app.py" ]]; }

has_residual_files() {
  [[ -d "$INSTALL_DIR" || -e "$SELF_SYMLINK" || -f "$SERVICE_FILE" ]]
}

get_env_value() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] || return 0
  grep -m1 -E "^${key}=" "$ENV_FILE" | cut -d'=' -f2- || true
}

get_env_value_from_file() {
  local env_file="$1" key="$2"
  [[ -f "$env_file" ]] || return 0
  grep -m1 -E "^${key}=" "$env_file" | cut -d'=' -f2- || true
}

resolve_db_file_from_db_path() {
  local db_path="$1"
  [[ -n "$db_path" ]] || db_path="$DEFAULT_DB_PATH"
  if [[ "$db_path" = /* ]]; then
    printf '%s' "$db_path"
  else
    printf '%s' "$INSTALL_DIR/$db_path"
  fi
}

set_env_value() {
  local key="$1"
  local value="$2"
  mkdir -p "$INSTALL_DIR"
  touch "$ENV_FILE"
  chmod 600 "$ENV_FILE" || true
  local escaped
  escaped="$(printf '%s' "$value" | sed -e 's/[\\/&|]/\\&/g')"
  if grep -q -E "^${key}=" "$ENV_FILE" 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${escaped}|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
  return 0
}

persist_repo_branch() {
  mkdir -p "$STATE_DIR"
  printf '%s\n' "$REPO_BRANCH" > "$REPO_BRANCH_FILE"
  return 0
}

is_safe_name() {
  local value="$1"
  # Reject empty, leading/trailing dots, double dots, and dangerous patterns
  [[ -n "$value" ]] || return 1
  [[ "$value" != .* ]] || return 1
  [[ "$value" != *. ]] || return 1
  [[ ! "$value" =~ \.\. ]] || return 1
  [[ "$value" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*[a-zA-Z0-9]$ ]] || [[ "$value" =~ ^[a-zA-Z0-9]$ ]]
}

validate_awg_target_values() {
  local container="$1" interface="$2"
  if ! is_safe_name "$container"; then
    die "Некорректное значение DOCKER_CONTAINER: '${container}'. Разрешены только [a-zA-Z0-9_.-]."
  fi
  if ! is_safe_name "$interface"; then
    die "Некорректное значение WG_INTERFACE: '${interface}'. Разрешены только [a-zA-Z0-9_.-]."
  fi
  return 0
}

write_awg_helper_policy() {
  local container="$1" interface="$2"
  validate_awg_target_values "$container" "$interface"
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp" <<POLICY
{
  "container": "${container}",
  "interface": "${interface}"
}
POLICY
  install -o root -g "$BOT_USER" -m 640 "$tmp" "$AWG_HELPER_POLICY"
  rm -f "$tmp"
  return 0
}

sync_awg_helper_policy_from_env() {
  local container interface
  container="$(get_env_value DOCKER_CONTAINER)"
  interface="$(get_env_value WG_INTERFACE)"
  [[ -n "$container" ]] || die "DOCKER_CONTAINER не задан в ${ENV_FILE}. Синхронизация policy невозможна."
  [[ -n "$interface" ]] || die "WG_INTERFACE не задан в ${ENV_FILE}. Синхронизация policy невозможна."
  write_awg_helper_policy "$container" "$interface"
  ok "Helper policy синхронизирована: ${AWG_HELPER_POLICY} (${container}/${interface})"
  return 0
}

helper_policy_field() {
  local field="$1"
  [[ -f "$AWG_HELPER_POLICY" ]] || return 0
  "$PYTHON_BIN" - "$AWG_HELPER_POLICY" "$field" <<'PY' 2>/dev/null || true
import json, sys
path, key = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
value = data.get(key, "")
print(value if isinstance(value, str) else "")
PY
}

read_helper_policy_state() {
  [[ -f "$AWG_HELPER_POLICY" ]] || {
    printf '\t\t'
    return 0
  }
  "$PYTHON_BIN" - "$AWG_HELPER_POLICY" <<'PY'
import json, sys
from pathlib import Path

path = Path(sys.argv[1])
container = ""
interface = ""
error = ""
try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    error = f"helper policy parse failed: {exc}"
else:
    if not isinstance(data, dict):
        error = "helper policy must be a JSON object"
    else:
        container = data.get("container", "")
        interface = data.get("interface", "")
        if not isinstance(container, str):
            container = ""
        if not isinstance(interface, str):
            interface = ""
print(f"{container}\t{interface}\t{error}")
PY
}

print_exit_hint() {
  print_line
  echo "Выход из awg-tgbot."
  echo "Текущая ветка: ${REPO_BRANCH}"
  echo
  echo "Повторный запуск installer:"
  echo "curl -fsSL ${RAW_BASE_URL}/awg-tgbot.sh | sudo REPO_BRANCH=${REPO_BRANCH} bash -s --"
  if [[ -x "$SELF_SYMLINK" || -f "$INSTALL_DIR/awg-tgbot.sh" ]]; then
    echo
    echo "Если скрипт уже установлен локально:"
    echo "sudo awg-tgbot"
    echo "sudo bash ${INSTALL_DIR}/awg-tgbot.sh"
  fi
  print_line
  return 0
}

cleanup_transient_install_state() {
  if service_exists || [[ -d "$BOT_DIR" ]] || [[ -f "$ENV_FILE" ]] || [[ -e "$SELF_SYMLINK" ]] || [[ -f "$INSTALL_DIR/awg-tgbot.sh" ]]; then
    return 0
  fi

  if [[ -f "$REPO_BRANCH_FILE" ]]; then
    rm -f "$REPO_BRANCH_FILE" || true
  fi

  if [[ -d "$STATE_DIR" ]]; then
    rmdir "$STATE_DIR" 2>/dev/null || true
  fi

  if [[ -d "$INSTALL_DIR" ]]; then
    local entries=""
    entries="$(find "$INSTALL_DIR" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null || true)"
    if [[ -z "$entries" ]]; then
      rmdir "$INSTALL_DIR" 2>/dev/null || true
    fi
  fi

  return 0
}
fetch_remote_commit_info() {
  local payload="" parsed=""
  # Add timeout and retry logic for GitHub API calls
  payload="$(curl -fsSL --connect-timeout 10 --max-time 30 --retry 2 --retry-delay 5 "$COMMIT_API_URL" 2>/dev/null || true)"
  [[ -n "$payload" ]] || return 0
  parsed="$("$PYTHON_BIN" - "$payload" <<'PY' 2>/dev/null || true
import json
import sys

raw = sys.argv[1]
try:
    data = json.loads(raw)
except Exception:
    raise SystemExit(0)

sha = data.get("sha", "")
commit = data.get("commit", {})
message = ""
if isinstance(commit, dict):
    message = commit.get("message", "")

if isinstance(sha, str):
    sha = sha.strip()
else:
    sha = ""

if isinstance(message, str):
    title = message.splitlines()[0].strip()
else:
    title = ""

if sha:
    print(f"{sha}\t{title}")
PY
)"
  printf '%s' "$parsed"
}

fetch_remote_sha() {
  local info_line=""
  info_line="$(fetch_remote_commit_info)"
  printf '%s' "${info_line%%$'\t'*}"
}

get_local_sha() { [[ -f "$VERSION_FILE" ]] && cat "$VERSION_FILE" || true; }

dpkg_lock_free() {
  ! fuser /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock /var/cache/apt/archives/lock >/dev/null 2>&1
}

wait_for_apt_locks() {
  local waited=0 max_wait=300
  while ! dpkg_lock_free; do
    if (( waited == 0 )); then
      warn "apt/dpkg сейчас занят другим процессом. Жду освобождения блокировки..."
      info "Процессы, удерживающие блокировку:"
      fuser -v /var/lib/dpkg/lock-frontend 2>&1 || true
      fuser -v /var/lib/dpkg/lock 2>&1 || true
    fi
    sleep 5
    waited=$((waited + 5))
    if (( waited >= max_wait )); then
      die "Не удалось дождаться освобождения apt/dpkg lock за ${max_wait} секунд. Попробуй позже."
    fi
  done
  return 0
}

apt_get_safe() {
  wait_for_apt_locks
  apt-get "$@"
}

ensure_packages() {
  info "Проверяю и обновляю системные зависимости..."
  export DEBIAN_FRONTEND=noninteractive
  apt_get_safe update -y
  
  # Определяем версию Python для установки правильного python3-venv
  local py_version py_major_minor
  py_version=$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "3.12")
  py_major_minor="${py_version%%.*}"
  
  apt_get_safe install -y --no-install-recommends \
    ca-certificates curl tar gzip openssl sudo iproute2 psmisc nftables nginx certbot python3-certbot-nginx
  
  # Устанавливаем venv для конкретной версии Python
  local venv_pkg="python3-${py_version}-venv"
  if ! apt_get_safe install -y --no-install-recommends "$venv_pkg" 2>/dev/null; then
    # Fallback: пробуем общий пакет python3-venv
    warn "Не удалось установить $venv_pkg, пробую python3-venv..."
    apt_get_safe install -y --no-install-recommends python3-venv || {
      error "Не удалось установить python3-venv. Установите вручную: apt install ${venv_pkg} или apt install python3-venv"
      return 1
    }
  fi
  
  if ! require_command docker; then
    warn "Docker не найден. Устанавливаю docker.io..."
    apt_get_safe install -y --no-install-recommends docker.io
  fi
  if require_command systemctl && systemctl list-unit-files docker.service >/dev/null 2>&1; then
    systemctl enable --now docker >/dev/null 2>&1 || systemctl start docker >/dev/null 2>&1 || true
    sleep 2
  fi
  return 0
}

ensure_python_compatible() {
  "$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit(1)
print(f"python={sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
PY
}

docker_is_accessible() { require_command docker && docker ps >/dev/null 2>&1; }

ensure_docker_ready() {
  if docker_is_accessible; then
    return 0
  fi
  if require_command systemctl && systemctl list-unit-files docker.service >/dev/null 2>&1; then
    systemctl enable --now docker >/dev/null 2>&1 || systemctl start docker >/dev/null 2>&1 || true
    sleep 2
  fi
  if ! docker_is_accessible; then
    warn "Docker недоступен. Проверь, что docker установлен и daemon запущен."
    warn "Подсказка: systemctl status docker --no-pager"
    return 1
  fi
  return 0
}

pick_existing_or_default() {
  local current="$1" fallback="$2"
  if [[ -n "$current" ]]; then printf '%s' "$current"; else printf '%s' "$fallback"; fi
}

is_public_ipv4() {
  local value="$1"
  "$PYTHON_BIN" - "$value" <<'PY'
import ipaddress, sys
value = sys.argv[1].strip()
try:
    addr = ipaddress.ip_address(value)
    ok = addr.version == 4 and not (addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_multicast or addr.is_unspecified or addr.is_reserved)
    print('1' if ok else '0')
except Exception:
    print('0')
PY
}

docker_exec_capture() {
  local container="$1"; shift
  docker exec -i "$container" "$@" 2>/dev/null || true
}

docker_exec_sh() {
  local container="$1" command="$2"
  docker exec -i "$container" sh -lc "$command" 2>/dev/null || true
}

find_awg_container() {
  local current lines line name image haystack score best_score=0 best_name=""
  current="$(get_env_value DOCKER_CONTAINER)"
  if [[ -n "$current" ]] && docker_is_accessible && docker inspect "$current" >/dev/null 2>&1; then
    printf '%s' "$current"
    return 0
  fi
  if ! docker_is_accessible; then
    printf '%s' "$current"
    return 0
  fi
  lines="$(docker ps --format '{{.Names}}\t{{.Image}}' 2>/dev/null || true)"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    name="${line%%$'\t'*}"
    image="${line#*$'\t'}"
    haystack="${name,,} ${image,,}"
    score=0
    [[ "$haystack" == *"amnezia-awg2"* ]] && score=$((score+150))
    [[ "$haystack" == *"amnezia-awg"* ]] && score=$((score+100))
    [[ "$haystack" == *"awg"* ]] && score=$((score+70))
    [[ "$haystack" == *"wireguard"* ]] && score=$((score+60))
    [[ "$haystack" == *"vpn"* ]] && score=$((score+30))
    if (( score > best_score )); then
      best_score=$score
      best_name="$name"
    fi
  done <<< "$lines"
  printf '%s' "$best_name"
}

extract_awg_show_value() {
  local label="$1" content="$2"
  awk -F': ' -v k="$label" '$1 == k {print substr($0, index($0, ": ")+2); exit}' <<< "$content"
}

parse_conf_value() {
  local key="$1" content="$2"
  awk -v key="$key" '
    function trim(s) { sub(/^[ \t]+/, "", s); sub(/[ \t\r]+$/, "", s); return s }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      val=$0
      sub(/^[^=]*=/, "", val)
      print trim(val)
      exit
    }
  ' <<< "$content"
}

find_awg_config_path() {
  local container="$1" interface_hint="$2" path=""
  if [[ -n "$interface_hint" ]]; then
    path="$(docker_exec_sh "$container" "[ -f '/opt/amnezia/awg/${interface_hint}.conf' ] && printf '%s' '/opt/amnezia/awg/${interface_hint}.conf' || true")"
  fi
  if [[ -z "$path" ]]; then
    path="$(docker_exec_sh "$container" "[ -f '/opt/amnezia/awg/awg0.conf' ] && printf '%s' '/opt/amnezia/awg/awg0.conf' || true")"
  fi
  if [[ -z "$path" ]]; then
    path="$(docker_exec_sh "$container" "find /opt/amnezia -maxdepth 4 -type f -name '*.conf' 2>/dev/null | grep '/awg/' | head -n1 || true")"
  fi
  printf '%s' "$path"
}

derive_public_key_from_private() {
  local container="$1" private_key="$2" out=""
  [[ -n "$private_key" ]] || return 0
  out="$(printf '%s\n' "$private_key" | docker exec -i "$container" awg pubkey 2>/dev/null | tr -d '\r' | head -n1 || true)"
  if [[ -z "$out" ]]; then
    out="$(printf '%s\n' "$private_key" | docker exec -i "$container" wg pubkey 2>/dev/null | tr -d '\r' | head -n1 || true)"
  fi
  printf '%s' "$out"
}

get_public_host() {
  local value route
  for value in "$(get_env_value PUBLIC_HOST)" "${PUBLIC_HOST:-}"; do
    value="$(printf '%s' "$value" | tr -d '[:space:]')"
    [[ -z "$value" ]] && continue
    if [[ "$(is_public_ipv4 "$value")" == "1" ]]; then
      printf '%s' "$value"
      return 0
    fi
  done
  if require_command curl; then
    local url
    for url in 'https://api.ipify.org' 'https://ifconfig.me/ip' 'https://ipv4.icanhazip.com'; do
      value="$(curl -4 -fsSL --connect-timeout 5 "$url" 2>/dev/null | tr -d '[:space:]' || true)"
      if [[ "$(is_public_ipv4 "$value")" == "1" ]]; then
        printf '%s' "$value"
        return 0
      fi
    done
  fi
  route="$(ip -4 route get 1.1.1.1 2>/dev/null || true)"
  value="$(grep -oE '\bsrc\s+[0-9.]+\b' <<< "$route" | awk '{print $2}' | head -n1 || true)"
  if [[ "$(is_public_ipv4 "$value")" == "1" ]]; then
    printf '%s' "$value"
    return 0
  fi
  printf '%s' ""
}

detect_awg_environment() {
  DETECTED_CONTAINER=""
  DETECTED_INTERFACE=""
    DETECTED_CONFIG_PATH=""
  DETECTED_PUBLIC_KEY=""
  DETECTED_LISTEN_PORT=""
  DETECTED_SERVER_IP=""
  DETECTED_SERVER_NAME=""
  DETECTED_PUBLIC_HOST=""
  DETECTED_AWG_JC=""
  DETECTED_AWG_JMIN=""
  DETECTED_AWG_JMAX=""
  DETECTED_AWG_S1=""
  DETECTED_AWG_S2=""
  DETECTED_AWG_S3=""
  DETECTED_AWG_S4=""
  DETECTED_AWG_H1=""
  DETECTED_AWG_H2=""
  DETECTED_AWG_H3=""
  DETECTED_AWG_H4=""
  DETECTED_AWG_I1=""
  DETECTED_AWG_I2=""
  DETECTED_AWG_I3=""
  DETECTED_AWG_I4=""
  DETECTED_AWG_I5=""

  local configured_container configured_interface show_output conf_output private_key interface_name
  configured_container="$(get_env_value DOCKER_CONTAINER)"
  configured_interface="$(get_env_value WG_INTERFACE)"
  DETECTED_CONTAINER="$(pick_existing_or_default "$configured_container" "$(find_awg_container)")"
  DETECTED_INTERFACE="${configured_interface:-awg0}"
  DETECTED_SERVER_NAME="$(pick_existing_or_default "${server_name:-$(get_env_value SERVER_NAME)}" "${SERVER_NAME:-My VPN}")"
  DETECTED_PUBLIC_HOST="$(get_public_host)"

  if [[ -n "$DETECTED_CONTAINER" ]] && docker_is_accessible && docker inspect "$DETECTED_CONTAINER" >/dev/null 2>&1; then
    show_output="$(docker_exec_capture "$DETECTED_CONTAINER" awg show "$DETECTED_INTERFACE")"
    [[ -n "$show_output" ]] || show_output="$(docker_exec_capture "$DETECTED_CONTAINER" awg show)"

    interface_name="$(extract_awg_show_value 'interface' "$show_output")"
    [[ -n "$interface_name" ]] && DETECTED_INTERFACE="$interface_name"

    DETECTED_PUBLIC_KEY="$(extract_awg_show_value 'public key' "$show_output")"
    DETECTED_LISTEN_PORT="$(extract_awg_show_value 'listening port' "$show_output")"
    DETECTED_CONFIG_PATH="$(find_awg_config_path "$DETECTED_CONTAINER" "$DETECTED_INTERFACE")"
    if [[ -n "$DETECTED_CONFIG_PATH" ]]; then
      conf_output="$(docker_exec_sh "$DETECTED_CONTAINER" "cat '$DETECTED_CONFIG_PATH'")"
      [[ -n "$DETECTED_LISTEN_PORT" ]] || DETECTED_LISTEN_PORT="$(parse_conf_value 'ListenPort' "$conf_output")"
      if [[ -z "$DETECTED_PUBLIC_KEY" ]]; then
        private_key="$(parse_conf_value 'PrivateKey' "$conf_output")"
        private_key="$(printf '%s' "$private_key" | tr -d '\r' | xargs 2>/dev/null || true)"
        DETECTED_PUBLIC_KEY="$(derive_public_key_from_private "$DETECTED_CONTAINER" "$private_key")"
      fi
      DETECTED_AWG_JC="$(parse_conf_value 'Jc' "$conf_output")"
      DETECTED_AWG_JMIN="$(parse_conf_value 'Jmin' "$conf_output")"
      DETECTED_AWG_JMAX="$(parse_conf_value 'Jmax' "$conf_output")"
      DETECTED_AWG_S1="$(parse_conf_value 'S1' "$conf_output")"
      DETECTED_AWG_S2="$(parse_conf_value 'S2' "$conf_output")"
      DETECTED_AWG_S3="$(parse_conf_value 'S3' "$conf_output")"
      DETECTED_AWG_S4="$(parse_conf_value 'S4' "$conf_output")"
      DETECTED_AWG_H1="$(parse_conf_value 'H1' "$conf_output")"
      DETECTED_AWG_H2="$(parse_conf_value 'H2' "$conf_output")"
      DETECTED_AWG_H3="$(parse_conf_value 'H3' "$conf_output")"
      DETECTED_AWG_H4="$(parse_conf_value 'H4' "$conf_output")"
      DETECTED_AWG_I1="$(parse_conf_value 'I1' "$conf_output")"
      DETECTED_AWG_I2="$(parse_conf_value 'I2' "$conf_output")"
      DETECTED_AWG_I3="$(parse_conf_value 'I3' "$conf_output")"
      DETECTED_AWG_I4="$(parse_conf_value 'I4' "$conf_output")"
      DETECTED_AWG_I5="$(parse_conf_value 'I5' "$conf_output")"
    fi
  fi

  if [[ -n "$DETECTED_PUBLIC_HOST" && -n "$DETECTED_LISTEN_PORT" ]]; then
    DETECTED_SERVER_IP="${DETECTED_PUBLIC_HOST}:${DETECTED_LISTEN_PORT}"
  else
    DETECTED_SERVER_IP="$(get_env_value SERVER_IP)"
  fi
}

print_detected_awg_summary() {
  print_line
  echo "Автоподбор AWG:"
  echo "Контейнер: ${DETECTED_CONTAINER:-не найден}"
  echo "Интерфейс: ${DETECTED_INTERFACE:-не найден}"
  echo "Конфиг: ${DETECTED_CONFIG_PATH:-не найден}"
  echo "Public key: ${DETECTED_PUBLIC_KEY:-не найден}"
  echo "Endpoint: ${DETECTED_SERVER_IP:-не найден}"
  echo "Имя сервера: ${DETECTED_SERVER_NAME:-не найдено}"
  print_line
  [[ -z "$DETECTED_PUBLIC_KEY" ]] && warn "Не удалось автоматически определить SERVER_PUBLIC_KEY."
  [[ -z "$DETECTED_SERVER_IP" ]] && warn "Не удалось автоматически определить внешний SERVER_IP."
  [[ -z "$DETECTED_PUBLIC_HOST" ]] && warn "Если внешний IP не определился — укажи PUBLIC_HOST / внешний IP вручную."
  return 0
}

status_found_text() {
  [[ "${1:-0}" == "1" ]] && printf 'найден' || printf 'не найден'
}

status_installed_text() {
  [[ "${1:-0}" == "1" ]] && printf 'установлен' || printf 'не установлен'
}

status_available_text() {
  [[ "${1:-0}" == "1" ]] && printf 'доступен' || printf 'недоступен'
}

reset_system_state() {
  STATE_DOCKER_INSTALLED=0
  STATE_DOCKER_DAEMON=0
  STATE_AWG_CONTAINER_FOUND=0
  STATE_AWG_INTERFACE_FOUND=0
  STATE_AWG_CONFIG_FOUND=0
  STATE_AWG_FOUND=0
  STATE_BOT_SERVICE_FOUND=0
  STATE_BOT_DIR_FOUND=0
  STATE_BOT_APP_FOUND=0
  STATE_BOT_SYMLINK_FOUND=0
  STATE_BOT_ENV_FOUND=0
  STATE_BOT_STATE_FOUND=0
  STATE_BOT_INSTALLED=0
  STATE_BOT_RESIDUAL=0
  STATE_KERNEL_SUPPORTED=0
  STATE_AMNEZIAWG_INSTALLED=0
  STARTUP_STATE_CODE="unknown"
}

check_kernel_support() {
  local kernel
  kernel="$(uname -r 2>/dev/null || true)"
  if [[ "$kernel" =~ ^([0-9]+)\.([0-9]+) ]]; then
    local major minor
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    if (( major > 5 || (major == 5 && minor >= 6) )); then
      STATE_KERNEL_SUPPORTED=1
    fi
  fi
}

check_amneziawg_installed() {
  if [[ -d "/etc/amnezia/amneziawg" ]]; then
    STATE_AMNEZIAWG_INSTALLED=1
    return 0
  fi
  if require_command docker && docker_is_accessible; then
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -qi 'amnezia'; then
      STATE_AMNEZIAWG_INSTALLED=1
    fi
  fi
}

check_awg_installed() {
  local show_output="" interface_name=""
  STATE_DOCKER_INSTALLED=0
  STATE_DOCKER_DAEMON=0
  STATE_AWG_CONTAINER_FOUND=0
  STATE_AWG_INTERFACE_FOUND=0
  STATE_AWG_CONFIG_FOUND=0
  STATE_AWG_FOUND=0

  if require_command docker; then
    STATE_DOCKER_INSTALLED=1
  fi

  if docker_is_accessible; then
    STATE_DOCKER_DAEMON=1
  fi

  detect_awg_environment

  if [[ "$STATE_DOCKER_DAEMON" != "1" ]]; then
    return 0
  fi

  if [[ -n "$DETECTED_CONTAINER" ]] && docker inspect "$DETECTED_CONTAINER" >/dev/null 2>&1; then
    STATE_AWG_CONTAINER_FOUND=1
  else
    return 0
  fi

  show_output="$(docker_exec_capture "$DETECTED_CONTAINER" awg show "$DETECTED_INTERFACE")"
  [[ -n "$show_output" ]] || show_output="$(docker_exec_capture "$DETECTED_CONTAINER" awg show)"
  interface_name="$(extract_awg_show_value 'interface' "$show_output")"
  if [[ -n "$interface_name" ]]; then
    DETECTED_INTERFACE="$interface_name"
    STATE_AWG_INTERFACE_FOUND=1
  fi

  if [[ -n "$DETECTED_CONFIG_PATH" ]]; then
    STATE_AWG_CONFIG_FOUND=1
  fi

  if [[ "$STATE_AWG_INTERFACE_FOUND" == "1" || "$STATE_AWG_CONFIG_FOUND" == "1" ]]; then
    STATE_AWG_FOUND=1
  fi
  return 0
}

check_bot_installed() {
  STATE_BOT_SERVICE_FOUND=0
  STATE_BOT_DIR_FOUND=0
  STATE_BOT_APP_FOUND=0
  STATE_BOT_SYMLINK_FOUND=0
  STATE_BOT_ENV_FOUND=0
  STATE_BOT_STATE_FOUND=0
  STATE_BOT_INSTALLED=0
  STATE_BOT_RESIDUAL=0

  [[ -f "$SERVICE_FILE" ]] && STATE_BOT_SERVICE_FOUND=1
  [[ -d "$BOT_DIR" ]] && STATE_BOT_DIR_FOUND=1
  [[ -f "$BOT_DIR/app.py" ]] && STATE_BOT_APP_FOUND=1
  [[ -L "$SELF_SYMLINK" ]] && STATE_BOT_SYMLINK_FOUND=1
  [[ -f "$ENV_FILE" ]] && STATE_BOT_ENV_FOUND=1
  [[ -d "$STATE_DIR" ]] && STATE_BOT_STATE_FOUND=1

  if [[ "$STATE_BOT_SERVICE_FOUND" == "1" && "$STATE_BOT_DIR_FOUND" == "1" && "$STATE_BOT_APP_FOUND" == "1" ]]; then
    STATE_BOT_INSTALLED=1
  fi

  if has_residual_files || [[ "$STATE_BOT_ENV_FOUND" == "1" || "$STATE_BOT_STATE_FOUND" == "1" ]]; then
    STATE_BOT_RESIDUAL=1
  fi
  return 0
}

collect_system_state() {
  reset_system_state
  check_kernel_support
  check_amneziawg_installed
  check_awg_installed
  check_bot_installed

  if [[ "$STATE_AWG_FOUND" == "1" && "$STATE_BOT_INSTALLED" == "1" ]]; then
    STARTUP_STATE_CODE="awg_yes_bot_yes"
  elif [[ "$STATE_AWG_FOUND" == "1" && "$STATE_BOT_INSTALLED" != "1" ]]; then
    STARTUP_STATE_CODE="awg_yes_bot_no"
  elif [[ "$STATE_AWG_FOUND" != "1" && "$STATE_BOT_INSTALLED" == "1" ]]; then
    STARTUP_STATE_CODE="awg_no_bot_yes"
  else
    STARTUP_STATE_CODE="awg_no_bot_no"
  fi
  return 0
}

detect_install_state() {
  collect_system_state
}

refresh_update_status_quiet() {
  local now_ts=0 info_line=""
  UPDATE_STATUS="not_applicable"
  UPDATE_LOCAL_SHA="$(get_local_sha)"
  UPDATE_REMOTE_TITLE=""

  if [[ "$STATE_BOT_INSTALLED" != "1" ]]; then
    UPDATE_REMOTE_SHA=""
    UPDATE_CHECK_TS=0
    UPDATE_CACHE_BRANCH=""
    return 0
  fi

  now_ts="${EPOCHSECONDS:-0}"
  if [[ "$UPDATE_CHECK_TS" -gt 0 && "$UPDATE_CACHE_BRANCH" == "$REPO_BRANCH" && "$now_ts" -gt 0 ]]; then
    if (( now_ts - UPDATE_CHECK_TS < UPDATE_CACHE_TTL )); then
      if [[ -z "$UPDATE_REMOTE_SHA" ]]; then
        UPDATE_STATUS="unknown"
      elif [[ -n "$UPDATE_LOCAL_SHA" && "$UPDATE_REMOTE_SHA" == "$UPDATE_LOCAL_SHA" ]]; then
        UPDATE_STATUS="current"
      else
        UPDATE_STATUS="available"
      fi
      return 0
    fi
  fi

  info_line="$(fetch_remote_commit_info)"
  UPDATE_REMOTE_SHA="${info_line%%$'\t'*}"
  if [[ "$info_line" == *$'\t'* ]]; then
    UPDATE_REMOTE_TITLE="${info_line#*$'\t'}"
  fi
  UPDATE_CACHE_BRANCH="$REPO_BRANCH"
  UPDATE_CHECK_TS="$now_ts"

  if [[ -z "$UPDATE_REMOTE_SHA" ]]; then
    UPDATE_STATUS="unknown"
  elif [[ -n "$UPDATE_LOCAL_SHA" && "$UPDATE_REMOTE_SHA" == "$UPDATE_LOCAL_SHA" ]]; then
    UPDATE_STATUS="current"
  else
    UPDATE_STATUS="available"
  fi
  return 0
}

startup_state_message() {
  case "$STARTUP_STATE_CODE" in
    awg_yes_bot_yes) printf '%s' "AWG найден, бот установлен." ;;
    awg_yes_bot_no) printf '%s' "AWG найден, бот не установлен." ;;
    awg_no_bot_yes) printf '%s' "Установка бота найдена, но AWG сейчас не обнаружен." ;;
    awg_no_bot_no|*) printf '%s' "AWG не найден и бот не установлен." ;;
  esac
}

print_recommended_actions() {
  echo "Что делать дальше:"
  case "$STARTUP_STATE_CODE" in
    awg_yes_bot_yes)
      echo "• Открой «Статус», чтобы проверить сервис и ветку."
      echo "• Если доступно обновление — запусти «Переустановить» (пункт 3 в меню)."
      echo "• Если есть проблемы — открой «Логи» → «Что не так?»."
      ;;
    awg_yes_bot_no)
      echo "• AWG найден: можно запускать установку бота."
      echo "• Выбери «Автоматическую установку», если AWG стандартный."
      echo "• Выбери «Ручную установку», если нужно явно задать параметры."
      ;;
    awg_no_bot_yes)
      echo "• Проверь Docker и доступность контейнера AWG."
      echo "• Открой «Диагностика» и сверяй AWG-контейнер/интерфейс."
      echo "• После исправления запусти «Переустановить»."
      ;;
    awg_no_bot_no|*)
      echo "• Сначала установи и запусти AmneziaWG/AWG."
      echo "• Затем снова запусти preflight и установку бота."
      ;;
  esac
}

print_update_status_line() {
  [[ "$STATE_BOT_INSTALLED" == "1" ]] || return 0
  case "$UPDATE_STATUS" in
    available)
      echo
      printf '%s\n' "$(color_red '[!] ДОСТУПНО ОБНОВЛЕНИЕ')"
      echo "    Локальная версия: ${UPDATE_LOCAL_SHA:0:12}"
      echo "    Новая версия:    ${UPDATE_REMOTE_SHA:0:12}"
      printf '    %s\n' "$(color_red 'Открой пункт меню: 3) Переустановить')"
      ;;
    current) echo "Обновление: версия актуальна" ;;
    unknown) echo "Обновление: не удалось проверить удалённый commit" ;;
  esac
  return 0
}


print_detailed_startup_summary() {
  local ab_stats ab_latest ab_count env_container env_interface policy_container policy_interface policy_error
  print_line
  echo "Предварительная проверка:"
  echo "AWG: $(status_found_text "$STATE_AWG_FOUND")"
  echo "Бот: $(status_installed_text "$STATE_BOT_INSTALLED")"
  echo "Ветка: ${REPO_BRANCH}"
  echo "Service: $(status_found_text "$STATE_BOT_SERVICE_FOUND")"
  echo "Docker: $(status_available_text "$STATE_DOCKER_DAEMON")"
  echo "Linux kernel (>=5.6): $(status_available_text "$STATE_KERNEL_SUPPORTED")"
  echo "AmneziaWG install: $(status_found_text "$STATE_AMNEZIAWG_INSTALLED")"
  print_line
  echo "Docker CLI: $([[ "$STATE_DOCKER_INSTALLED" == "1" ]] && echo 'установлен' || echo 'не установлен')"
  echo "Docker daemon: $(status_available_text "$STATE_DOCKER_DAEMON")"
  echo "AWG контейнер: $(status_found_text "$STATE_AWG_CONTAINER_FOUND")"
  echo "AWG интерфейс: $(status_found_text "$STATE_AWG_INTERFACE_FOUND")"
  echo "AWG config: $(status_found_text "$STATE_AWG_CONFIG_FOUND")"
  echo "BOT_DIR: $(status_found_text "$STATE_BOT_DIR_FOUND")"
  echo "BOT_DIR/app.py: $(status_found_text "$STATE_BOT_APP_FOUND")"
  echo "Symlink /usr/local/bin/awg-tgbot: $(status_found_text "$STATE_BOT_SYMLINK_FOUND")"
  echo ".env: $(status_found_text "$STATE_BOT_ENV_FOUND")"
  env_container="$(get_env_value DOCKER_CONTAINER)"
  env_interface="$(get_env_value WG_INTERFACE)"
  echo "AWG target (.env): ${env_container:-не задан}/${env_interface:-не задан}"
  if [[ -f "$AWG_HELPER_POLICY" ]]; then
    IFS=$'\t' read -r policy_container policy_interface policy_error < <(read_helper_policy_state)
    echo "AWG target (helper policy): ${policy_container:-не задан}/${policy_interface:-не задан}"
    if [[ -n "$policy_error" ]]; then
      warn "$policy_error (${AWG_HELPER_POLICY})"
    fi
  fi
  echo "Служебное состояние установки: $(status_found_text "$STATE_BOT_STATE_FOUND")"
  ab_stats="$(autobackup_archive_stats)"
  ab_latest="${ab_stats%%|*}"
  ab_count="${ab_stats##*|}"
  echo "Autobackup enabled: $(autobackup_enabled && echo 'да' || echo 'нет')"
  echo "Autobackup timer: $(autobackup_timer_state)"
  echo "Autobackup keep count: $(autobackup_keep_count)"
  echo "Backup directory: ${BACKUP_ROOT}"
  echo "Backup archives: ${ab_count}, latest: ${ab_latest}"
  if [[ "$STATE_BOT_RESIDUAL" == "1" && "$STATE_BOT_INSTALLED" != "1" ]]; then
    echo "Остаточные файлы: найдены"
  fi
  print_update_status_line
  if [[ "$STATE_BOT_INSTALLED" == "1" ]]; then
    echo "Локальная версия: ${UPDATE_LOCAL_SHA:-неизвестно}"
    echo "Доступный commit: ${UPDATE_REMOTE_SHA:-не удалось получить}"
    echo "Обновление: через «Переустановить» (reinstall)"
    if [[ -n "$UPDATE_REMOTE_TITLE" ]]; then
      echo "Commit title: ${UPDATE_REMOTE_TITLE}"
    fi
  fi
  print_line
  echo "Состояние: $(startup_state_message)"
  if [[ "$STARTUP_STATE_CODE" == "awg_no_bot_no" ]]; then
    echo "Сначала установи и запусти AWG, затем вернись к установке бота."
  fi
  print_line
  print_recommended_actions
  print_line
  return 0
}

ensure_fernet_key() {
  local current key
  current="$(get_env_value FERNET_KEY)"
  if [[ -n "$current" ]]; then
    return 0
  fi
  key="$($PYTHON_BIN - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
)"
  set_env_value FERNET_KEY "$key"
}

# Валидация критических переменных окружения после генерации/обновления .env
validate_critical_env() {
  local errors=0
  local value
  
  info "Выполняю валидацию критических переменных окружения..."
  
  # Проверка ENCRYPTION_SECRET
  value="$(get_env_value ENCRYPTION_SECRET)"
  if [[ -z "$value" ]]; then
    error "Критическая ошибка: ENCRYPTION_SECRET отсутствует или пустой!"
    errors=$((errors + 1))
  elif [[ ${#value} -lt 32 ]]; then
    error "Критическая ошибка: ENCRYPTION_SECRET слишком короткий (минимум 32 символа)!"
    errors=$((errors + 1))
  else
    ok "ENCRYPTION_SECRET: OK (длина: ${#value})"
  fi
  
  # Проверка API_TOKEN
  value="$(get_env_value API_TOKEN)"
  if [[ -z "$value" ]]; then
    error "Критическая ошибка: API_TOKEN отсутствует!"
    errors=$((errors + 1))
  elif [[ ! "$value" =~ ^[0-9]+:[A-Za-z0-9_-]+$ ]]; then
    error "Критическая ошибка: API_TOKEN имеет неверный формат (ожидается XXX:YYY)!"
    errors=$((errors + 1))
  else
    ok "API_TOKEN: OK (формат верный)"
  fi
  
  # Проверка ADMIN_ID
  value="$(get_env_value ADMIN_ID)"
  if [[ -z "$value" ]]; then
    error "Критическая ошибка: ADMIN_ID отсутствует!"
    errors=$((errors + 1))
  elif [[ ! "$value" =~ ^[0-9]+$ ]]; then
    error "Критическая ошибка: ADMIN_ID должен быть числом!"
    errors=$((errors + 1))
  else
    ok "ADMIN_ID: OK"
  fi
  
  # Проверка SERVER_PUBLIC_KEY
  value="$(get_env_value SERVER_PUBLIC_KEY)"
  if [[ -z "$value" ]]; then
    error "Критическая ошибка: SERVER_PUBLIC_KEY отсутствует!"
    errors=$((errors + 1))
  else
    ok "SERVER_PUBLIC_KEY: OK"
  fi
  
  # Проверка DB_PATH
  value="$(get_env_value DB_PATH)"
  if [[ -z "$value" ]]; then
    warn "DB_PATH не установлен, будет использовано значение по умолчанию"
  else
    ok "DB_PATH: OK ($value)"
  fi
  
  # Проверка ENCRYPTION_OLD_SECRETS (опциональная, но важная для миграции)
  value="$(get_env_value ENCRYPTION_OLD_SECRETS)"
  if [[ -n "$value" ]]; then
    # Если переменная установлена, проверяем что она не пустая и не содержит только запятые
    if [[ -z "$value" || "$value" == "," ]]; then
      warn "ENCRYPTION_OLD_SECRETS установлен, но содержит пустое или некорректное значение!"
    else
      ok "ENCRYPTION_OLD_SECRETS: OK (содержит старые ключи для миграции)"
    fi
  else
    info "ENCRYPTION_OLD_SECRETS: не установлен (это нормально для новых установок)"
  fi
  
  if [[ $errors -gt 0 ]]; then
    error "Валидация не пройдена! Найдено ошибок: $errors"
    return 1
  fi
  
  ok "Все критические переменные прошли валидацию."
  return 0
}

setup_logrotate() {
  cat > /etc/logrotate.d/awg-tgbot <<ROTATE
${APP_LOG_FILE} {
  daily
  rotate 14
  compress
  delaycompress
  missingok
  notifempty
  copytruncate
  su ${BOT_USER} ${BOT_USER}
}
${INSTALL_LOG} {
  weekly
  rotate 8
  compress
  delaycompress
  missingok
  notifempty
  copytruncate
  su root root
}
ROTATE
  chmod 644 /etc/logrotate.d/awg-tgbot
}

print_startup_summary() {
  print_line
  echo "AWG: $(status_found_text "$STATE_AWG_FOUND") | Бот: $(status_installed_text "$STATE_BOT_INSTALLED") | Ветка: ${REPO_BRANCH} | Docker: $(status_available_text "$STATE_DOCKER_DAEMON")"
  print_update_status_line
  if [[ "$STATE_BOT_RESIDUAL" == "1" && "$STATE_BOT_INSTALLED" != "1" ]]; then
    echo "Остаточные файлы: найдены"
  fi
  if [[ "$STARTUP_STATE_CODE" == "awg_no_bot_no" ]]; then
    echo "Сначала установи и запусти AWG, затем вернись к установке бота."
  fi
  echo "Подробности доступны в пункте «Диагностика»."
  print_line
  return 0
}

download_repo() {
  local tmp_dir src_dir download_url ref="${1:-$REPO_BRANCH}"
  tmp_dir="$(mktemp -d)"
  download_url="https://codeload.github.com/${REPO_OWNER}/${REPO_NAME}/tar.gz/${ref}"
  info "Скачиваю код из ${REPO_URL} (ref=${ref})..."
  # Add max-time timeout to prevent hanging on slow downloads
  if ! curl -fsSL --connect-timeout 20 --max-time 300 --retry 3 --retry-delay 5 "$download_url" -o "$tmp_dir/repo.tar.gz"; then
    warn "Не удалось скачать репозиторий. Проверяю сеть..."
    curl -v --connect-timeout 10 https://github.com 2>&1 | head -5 || true
    rm -rf "$tmp_dir"
    return 1
  fi
  tar -xzf "$tmp_dir/repo.tar.gz" -C "$tmp_dir"
  src_dir="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1 || true)"
  if [[ -z "$src_dir" || ! -d "$src_dir/bot" || ! -f "$src_dir/awg-tgbot.sh" ]]; then
    warn "Не удалось скачать корректную структуру репозитория."
    ls -la "$tmp_dir" >&2 || true
    [[ -n "$src_dir" ]] && ls -la "$src_dir" >&2 || true
    rm -rf "$tmp_dir"
    return 1
  fi
  printf '%s' "$tmp_dir"
}

deploy_repo() {
  local tmp_dir="$1" src_dir backup_dir=""
  src_dir="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1 || true)"
  if [[ -z "$src_dir" || ! -d "$src_dir/bot" || ! -f "$src_dir/awg-tgbot.sh" ]]; then
    warn "Не найдены файлы репозитория для развёртывания."
    return 1
  fi
  mkdir -p "$INSTALL_DIR" "$STATE_DIR" "$(dirname "$SELF_SYMLINK")"
  if [[ -d "$BOT_DIR" || -f "$INSTALL_DIR/awg-tgbot.sh" || -d "$INSTALL_DIR/scripts" || -d "$INSTALL_DIR/packaging" ]]; then
    backup_dir="$(mktemp -d "${INSTALL_DIR}/.backup.XXXXXX")"
    [[ -d "$BOT_DIR" ]] && mv "$BOT_DIR" "$backup_dir/bot"
    [[ -f "$INSTALL_DIR/awg-tgbot.sh" ]] && mv "$INSTALL_DIR/awg-tgbot.sh" "$backup_dir/awg-tgbot.sh"
    [[ -d "$INSTALL_DIR/scripts" ]] && mv "$INSTALL_DIR/scripts" "$backup_dir/scripts"
    [[ -d "$INSTALL_DIR/packaging" ]] && mv "$INSTALL_DIR/packaging" "$backup_dir/packaging"
  fi
  rm -rf "$BOT_DIR"
  mkdir -p "$BOT_DIR"
  rm -rf "$INSTALL_DIR/scripts" "$INSTALL_DIR/packaging"
  if cp -a "$src_dir/bot/." "$BOT_DIR/" \
    && cp "$src_dir/awg-tgbot.sh" "$INSTALL_DIR/awg-tgbot.sh" \
    && { [[ ! -d "$src_dir/scripts" ]] || cp -a "$src_dir/scripts" "$INSTALL_DIR/scripts"; } \
    && { [[ ! -d "$src_dir/packaging" ]] || cp -a "$src_dir/packaging" "$INSTALL_DIR/packaging"; } \
    && chmod +x "$INSTALL_DIR/awg-tgbot.sh" \
    && { [[ ! -f "$AUTO_BACKUP_SCRIPT" ]] || chmod +x "$AUTO_BACKUP_SCRIPT"; } \
    && ln -sfn "$INSTALL_DIR/awg-tgbot.sh" "$SELF_SYMLINK"; then
    [[ -n "$backup_dir" ]] && rm -rf "$backup_dir"
    return 0
  fi
  warn "Не удалось развернуть файлы репозитория. Выполняю откат."
  rm -rf "$BOT_DIR"
  rm -f "$INSTALL_DIR/awg-tgbot.sh"
  rm -rf "$INSTALL_DIR/scripts" "$INSTALL_DIR/packaging"
  if [[ -n "$backup_dir" && -d "$backup_dir" ]]; then
    [[ -d "$backup_dir/bot" ]] && mv "$backup_dir/bot" "$BOT_DIR"
    [[ -f "$backup_dir/awg-tgbot.sh" ]] && mv "$backup_dir/awg-tgbot.sh" "$INSTALL_DIR/awg-tgbot.sh"
    [[ -d "$backup_dir/scripts" ]] && mv "$backup_dir/scripts" "$INSTALL_DIR/scripts"
    [[ -d "$backup_dir/packaging" ]] && mv "$backup_dir/packaging" "$INSTALL_DIR/packaging"
    rm -rf "$backup_dir"
  fi
  return 1
}

ensure_env_file() {
  mkdir -p "$INSTALL_DIR"
  if [[ ! -f "$ENV_FILE" ]]; then
    if [[ -f "$BOT_DIR/.env.example" ]]; then
      cp "$BOT_DIR/.env.example" "$ENV_FILE"
    else
      touch "$ENV_FILE"
    fi
    chmod 600 "$ENV_FILE" || true
  fi
  return 0
}

# Сохраняет все текущие значения из .env файла перед перезаписью
preserve_existing_env_values() {
  local backup_file="$ENV_FILE.preserve.$$"
  if [[ ! -f "$ENV_FILE" ]]; then
    return 0
  fi
  
  # Копируем текущий .env для сохранения значений
  cp "$ENV_FILE" "$backup_file" || return 0
  
  # Читаем все переменные из backup и восстанавливаем те, которые не были перезаписаны
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    # Пропускаем пустые строки и комментарии
    [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
    
    # Извлекаем ключ (убираем пробелы)
    key="${key%%=*}"
    key="$(echo "$key" | xargs)"
    
    # Если эта переменная ещё не установлена в env, восстанавливаем её
    if [[ -n "$key" && -z "$(get_env_value "$key")" ]]; then
      # Убираем кавычки из значения если есть
      value="${value#\"}"
      value="${value%\"}"
      value="${value#\'}"
      value="${value%\'}"
      set_env_value "$key" "$value"
    fi
  done < "$backup_file"
  
  rm -f "$backup_file" || true
  return 0
}

migrate_legacy_tariff_defaults() {
  local current=""
  current="$(get_env_value STARS_PRICE_7_DAYS)"
  if [[ "$current" == "15" ]]; then
    set_env_value STARS_PRICE_7_DAYS "21"
  fi

  current="$(get_env_value STARS_PRICE_90_DAYS)"
  if [[ "$current" == "120" ]]; then
    set_env_value STARS_PRICE_90_DAYS "140"
  fi
  
  # Set default Platega prices if not set
  if [[ -z "$(get_env_value PLATEGA_PRICE_7_DAYS)" ]]; then
    set_env_value PLATEGA_PRICE_7_DAYS "100"
  fi
  if [[ -z "$(get_env_value PLATEGA_PRICE_30_DAYS)" ]]; then
    set_env_value PLATEGA_PRICE_30_DAYS "250"
  fi
  if [[ -z "$(get_env_value PLATEGA_PRICE_90_DAYS)" ]]; then
    set_env_value PLATEGA_PRICE_90_DAYS "700"
  fi
  return 0
}

ensure_secret() {
  local current secret
  current="$(get_env_value ENCRYPTION_SECRET)"
  if [[ -n "$current" ]]; then printf '%s' "$current"; return 0; fi
  if require_command openssl; then
    secret="$(openssl rand -hex 32)"
  else
    secret="$($PYTHON_BIN - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
)"
  fi
  printf '%s' "$secret"
}

prompt_api_token() {
  local __resultvar="$1" __token="" __default=""
  __default="$(get_env_value API_TOKEN)"
  while true; do
    prompt_with_default 'Введите токен Telegram-бота' "$__default" __token
    if [[ "$__token" == *:* ]]; then
      printf -v "$__resultvar" '%s' "$__token"
      return 0
    fi
    warn "Нужен токен в формате 123456:ABCDEF..."
  done
}

prompt_admin_id() {
  local __resultvar="$1" __admin_input="" __default=""
  __default="$(get_env_value ADMIN_ID)"
  while true; do
    prompt_with_default 'Введите Telegram user_id администратора' "$__default" __admin_input
    if [[ "$__admin_input" =~ ^[0-9]+$ ]]; then
      printf -v "$__resultvar" '%s' "$__admin_input"
      return 0
    fi
    warn "ADMIN_ID должен быть числом."
  done
}

prompt_server_name() {
  local __resultvar="$1" __name_input="" __default=""
  __default="$(get_env_value SERVER_NAME)"
  if [[ -z "$__default" ]]; then
    __default="My VPN"
  fi
  prompt_with_default 'Введите имя сервера (отображается в конфиге)' "$__default" __name_input
  printf -v "$__resultvar" '%s' "$__name_input"
}

write_common_env() {
  local api_token="$1" admin_id="$2" server_name="$3" secret="$4"
  local db_path=""
  set_env_value API_TOKEN "$api_token"
  set_env_value ADMIN_ID "$admin_id"
  # Записываем SERVER_NAME только если он не пустой, чтобы не затереть сохранённое значение
  if [[ -n "$server_name" ]]; then
    set_env_value SERVER_NAME "$server_name"
  fi
  set_env_value ENCRYPTION_SECRET "$secret"
  db_path="$(get_env_value DB_PATH)"
  if [[ -n "$db_path" ]]; then
    set_env_value DB_PATH "$db_path"
  else
    set_env_value DB_PATH "$DEFAULT_DB_PATH"
  fi
  return 0
}

ensure_selfhost_network_defaults() {
  local current=""
  current="$(get_env_value EGRESS_DENYLIST_ENABLED)"
  [[ -n "$current" ]] || set_env_value EGRESS_DENYLIST_ENABLED "$SELFHOST_EGRESS_DENYLIST_ENABLED_DEFAULT"
  current="$(get_env_value EGRESS_DENYLIST_MODE)"
  [[ -n "$current" ]] || set_env_value EGRESS_DENYLIST_MODE "$SELFHOST_EGRESS_DENYLIST_MODE_DEFAULT"
  current="$(get_env_value EGRESS_DENYLIST_REFRESH_MINUTES)"
  [[ -n "$current" ]] || set_env_value EGRESS_DENYLIST_REFRESH_MINUTES "$SELFHOST_EGRESS_DENYLIST_REFRESH_MINUTES_DEFAULT"
  current="$(get_env_value AUTO_BACKUP_ENABLED)"
  [[ -n "$current" ]] || set_env_value AUTO_BACKUP_ENABLED "$SELFHOST_AUTO_BACKUP_ENABLED_DEFAULT"
  current="$(get_env_value AUTO_BACKUP_KEEP_COUNT)"
  [[ -n "$current" ]] || set_env_value AUTO_BACKUP_KEEP_COUNT "$SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT"
}

autobackup_enabled() {
  local enabled
  enabled="$(get_env_value AUTO_BACKUP_ENABLED)"
  [[ -n "$enabled" ]] || enabled="$SELFHOST_AUTO_BACKUP_ENABLED_DEFAULT"
  [[ "$enabled" == "1" ]]
}

autobackup_keep_count() {
  local keep
  keep="$(get_env_value AUTO_BACKUP_KEEP_COUNT)"
  [[ -n "$keep" ]] || keep="$SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT"
  if [[ ! "$keep" =~ ^[0-9]+$ ]] || (( keep < 1 )); then
    keep="$SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT"
  fi
  printf '%s' "$keep"
}

write_detected_awg_env() {
  [[ -n "$DETECTED_CONTAINER" ]] && set_env_value DOCKER_CONTAINER "$DETECTED_CONTAINER"
  [[ -n "$DETECTED_INTERFACE" ]] && set_env_value WG_INTERFACE "$DETECTED_INTERFACE"
  [[ -n "$DETECTED_PUBLIC_KEY" ]] && set_env_value SERVER_PUBLIC_KEY "$DETECTED_PUBLIC_KEY"
  [[ -n "$DETECTED_SERVER_IP" ]] && set_env_value SERVER_IP "$DETECTED_SERVER_IP"
  [[ -n "$DETECTED_PUBLIC_HOST" ]] && set_env_value PUBLIC_HOST "$DETECTED_PUBLIC_HOST"
  [[ -n "$DETECTED_AWG_JC" ]] && set_env_value AWG_JC "$DETECTED_AWG_JC"
  [[ -n "$DETECTED_AWG_JMIN" ]] && set_env_value AWG_JMIN "$DETECTED_AWG_JMIN"
  [[ -n "$DETECTED_AWG_JMAX" ]] && set_env_value AWG_JMAX "$DETECTED_AWG_JMAX"
  [[ -n "$DETECTED_AWG_S1" ]] && set_env_value AWG_S1 "$DETECTED_AWG_S1"
  [[ -n "$DETECTED_AWG_S2" ]] && set_env_value AWG_S2 "$DETECTED_AWG_S2"
  [[ -n "$DETECTED_AWG_S3" ]] && set_env_value AWG_S3 "$DETECTED_AWG_S3"
  [[ -n "$DETECTED_AWG_S4" ]] && set_env_value AWG_S4 "$DETECTED_AWG_S4"
  [[ -n "$DETECTED_AWG_H1" ]] && set_env_value AWG_H1 "$DETECTED_AWG_H1"
  [[ -n "$DETECTED_AWG_H2" ]] && set_env_value AWG_H2 "$DETECTED_AWG_H2"
  [[ -n "$DETECTED_AWG_H3" ]] && set_env_value AWG_H3 "$DETECTED_AWG_H3"
  [[ -n "$DETECTED_AWG_H4" ]] && set_env_value AWG_H4 "$DETECTED_AWG_H4"
  [[ -n "$DETECTED_AWG_I1" ]] && set_env_value AWG_I1 "$DETECTED_AWG_I1"
  [[ -n "$DETECTED_AWG_I2" ]] && set_env_value AWG_I2 "$DETECTED_AWG_I2"
  [[ -n "$DETECTED_AWG_I3" ]] && set_env_value AWG_I3 "$DETECTED_AWG_I3"
  [[ -n "$DETECTED_AWG_I4" ]] && set_env_value AWG_I4 "$DETECTED_AWG_I4"
  [[ -n "$DETECTED_AWG_I5" ]] && set_env_value AWG_I5 "$DETECTED_AWG_I5"
  return 0
}

configure_manual_awg_only() {
  local value default
  # Запрос Telegram токена и Admin ID перед настройкой AWG
  api_token=""
  admin_id=""
  server_name=""
  
  prompt_api_token api_token
  prompt_admin_id admin_id
  
  # Запрос имени сервера
  prompt_server_name server_name
  if [[ -z "$server_name" ]]; then
    server_name="My VPN"
  fi
  
  # Настройка AWG параметров
  default="$(pick_existing_or_default "$(get_env_value DOCKER_CONTAINER)" "$DETECTED_CONTAINER")"
  prompt_with_default 'DOCKER_CONTAINER' "$default" value
  set_env_value DOCKER_CONTAINER "$value"
  default="$(pick_existing_or_default "$(get_env_value WG_INTERFACE)" "$DETECTED_INTERFACE")"
  prompt_with_default 'WG_INTERFACE' "$default" value
  set_env_value WG_INTERFACE "$value"
  default="$(pick_existing_or_default "$default" "$(get_env_value WG_INTERFACE)")"
  default="$(pick_existing_or_default "$(get_env_value SERVER_PUBLIC_KEY)" "$DETECTED_PUBLIC_KEY")"
  prompt_with_default 'SERVER_PUBLIC_KEY' "$default" value
  set_env_value SERVER_PUBLIC_KEY "$value"
  default="$(pick_existing_or_default "$(get_env_value PUBLIC_HOST)" "$DETECTED_PUBLIC_HOST")"
  prompt_with_default 'PUBLIC_HOST / внешний IP' "$default" value
  set_env_value PUBLIC_HOST "$value"
  default="$(pick_existing_or_default "$(get_env_value SERVER_IP)" "$DETECTED_SERVER_IP")"
  prompt_with_default 'SERVER_IP (IP:port)' "$default" value
  set_env_value SERVER_IP "$value"
  
  # Запись основных параметров после сбора всех данных
  local secret
  secret="$(ensure_secret)"
  write_common_env "$api_token" "$admin_id" "$server_name" "$secret"
  return 0
}




ensure_bot_not_in_docker_group() {
  if ! getent group docker >/dev/null 2>&1; then
    return 0
  fi
  if id -nG "$BOT_USER" 2>/dev/null | tr ' ' '\n' | grep -qx docker; then
    gpasswd -d "$BOT_USER" docker >/dev/null 2>&1 || true
  fi
  return 0
}

ensure_venv_and_requirements() {
  info "Настраиваю Python окружение..."
  ensure_python_compatible || die "Требуется Python >= 3.10."
  
  # Проверяем, доступен ли модуль venv в текущем Python
  if ! "$PYTHON_BIN" -c 'import venv' 2>/dev/null; then
    # Модуль venv недоступен, пытаемся установить пакет
    local py_version
    py_version=$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "3.12")
    
    local venv_pkg="python3-${py_version}-venv"
    if ! apt_get_safe install -y --no-install-recommends "$venv_pkg" 2>/dev/null; then
      warn "Не удалось установить $venv_pkg, пробую python3-venv..."
      apt_get_safe install -y --no-install-recommends python3-venv || {
        error "Не удалось установить python3-venv. Установите вручную: apt install ${venv_pkg} или apt install python3-venv"
        return 1
      }
    fi
  fi
  
  [[ -d "$VENV_DIR" ]] || "$PYTHON_BIN" -m venv "$VENV_DIR" || return 1
  "$VENV_DIR/bin/pip" install --upgrade pip wheel || return 1
  
  # При переустановке используем --force-reinstall для гарантии целостности зависимостей
  local pip_install_flags="-r"
  if [[ "${FORCE_REINSTALL_DEPS:-0}" == "1" ]]; then
    pip_install_flags="--force-reinstall --no-cache-dir -r"
    info "Выполняется принудительная переустановка зависимостей (--force-reinstall)..."
  fi
  
  # shellcheck disable=SC2086
  "$VENV_DIR/bin/pip" install $pip_install_flags "$BOT_DIR/requirements.txt" || return 1
  
  # Platega SDK используется напрямую из папки bot/platega-sdk-python
  # Установка через pip не требуется - путь добавляется в platega_service.py
  local platega_merchant_id
  platega_merchant_id="$(get_env_value PLATEGA_MERCHANT_ID)"
  if [[ -n "$platega_merchant_id" && -d "$BOT_DIR/platega-sdk-python" ]]; then
    info "Platega SDK доступен локально: $BOT_DIR/platega-sdk-python"
  fi
  return 0
}

ensure_bot_user() {
  if ! id -u "$BOT_USER" >/dev/null 2>&1; then
    useradd --system --home "$INSTALL_DIR" --shell /usr/sbin/nologin "$BOT_USER" || return 1
  fi
  ensure_bot_not_in_docker_group
  enforce_root_owned_code_paths
  prepare_runtime_access_paths
  return 0
}

enforce_root_owned_code_paths() {
  local path
  mkdir -p "$INSTALL_DIR"
  chown root:root "$INSTALL_DIR" 2>/dev/null || true
  chmod 755 "$INSTALL_DIR" 2>/dev/null || true
  for path in "$INSTALL_DIR/awg-tgbot.sh" "$INSTALL_DIR/scripts" "$INSTALL_DIR/packaging" "$VENV_DIR"; do
    [[ -e "$path" ]] || continue
    chown -R root:root "$path" 2>/dev/null || true
    chmod -R go-w "$path" 2>/dev/null || true
  done
  # Директория с кодом бота должна быть доступна для записи пользователю awg-bot (для создания БД, логов и т.д.)
  if [[ -d "$BOT_DIR" ]]; then
    chown -R "$BOT_USER:$BOT_USER" "$BOT_DIR" 2>/dev/null || true
    chmod -R u+rw "$BOT_DIR" 2>/dev/null || true
  fi
  [[ -f "$INSTALL_DIR/awg-tgbot.sh" ]] && chmod 755 "$INSTALL_DIR/awg-tgbot.sh" 2>/dev/null || true
  return 0
}

prepare_runtime_access_paths() {
  local db_file
  mkdir -p "$RUNTIME_DIR"
  chown "$BOT_USER:$BOT_USER" "$RUNTIME_DIR" 2>/dev/null || true
  chmod 750 "$RUNTIME_DIR" 2>/dev/null || true
  mkdir -p "$APP_LOG_DIR"
  touch "$APP_LOG_FILE"
  chown -R "$BOT_USER:$BOT_USER" "$APP_LOG_DIR" 2>/dev/null || true
  chmod 750 "$APP_LOG_DIR" 2>/dev/null || true
  chmod 640 "$APP_LOG_FILE" 2>/dev/null || true
  db_file="$(get_bot_db_file)"
  repair_runtime_file_access "$db_file" 600
  repair_runtime_file_access "$ENV_FILE" 600
  return 0
}

copy_sqlite_runtime_bundle() {
  local src_db="$1" dst_db="$2" src_file dst_file suffix
  [[ -f "$src_db" ]] || return 1
  mkdir -p "$(dirname "$dst_db")"
  install -m 600 "$src_db" "$dst_db" || return 1
  repair_runtime_file_access "$dst_db" 600
  for suffix in "-wal" "-shm"; do
    src_file="${src_db}${suffix}"
    dst_file="${dst_db}${suffix}"
    if [[ -f "$src_file" ]]; then
      install -m 600 "$src_file" "$dst_file" || return 1
      repair_runtime_file_access "$dst_file" 600
    else
      rm -f "$dst_file"
    fi
  done
  return 0
}

snapshot_sqlite_runtime_bundle() {
  local src_db="$1" snapshot_dir="$2" snapshot_name="$3"
  local snapshot_db="${snapshot_dir}/${snapshot_name}"
  [[ -f "$src_db" ]] || return 1
  
  # Проверка целостности перед созданием snapshot
  if ! sqlite_full_integrity_check "$src_db"; then
    log "WARN" "БД ($src_db) имеет проблемы целостности. Snapshot создаётся с предупреждением."
  fi
  
  copy_sqlite_runtime_bundle "$src_db" "$snapshot_db"
}

restore_sqlite_runtime_bundle() {
  local snapshot_db="$1" target_db="$2"
  [[ -f "$snapshot_db" ]] || return 1
  
  # Проверка целостности snapshot перед восстановлением
  if ! sqlite_full_integrity_check "$snapshot_db"; then
    log "ERROR" "Snapshot ($snapshot_db) имеет проблемы целостности. Восстановление отменено."
    return 1
  fi
  
  copy_sqlite_runtime_bundle "$snapshot_db" "$target_db"
}

sqlite_runtime_quick_check() {
  local db_file="$1"
  [[ -f "$db_file" ]] || return 1
  "$PYTHON_BIN" - "$db_file" <<'PY'
import sqlite3
import sys

path = sys.argv[1]
conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
try:
    row = conn.execute("PRAGMA quick_check;").fetchone()
    value = (row[0] if row else "").strip().lower()
    raise SystemExit(0 if value == "ok" else 1)
finally:
    conn.close()
PY
}

sqlite_full_integrity_check() {
  local db_file="$1"
  [[ -f "$db_file" ]] || return 1
  "$PYTHON_BIN" - "$db_file" <<'PY'
import sqlite3
import sys

path = sys.argv[1]
conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
try:
    row = conn.execute("PRAGMA integrity_check;").fetchone()
    value = (row[0] if row else "").strip().lower()
    raise SystemExit(0 if value == "ok" else 1)
finally:
    conn.close()
PY
}

validate_backup_archive_payload() {
  local archive_file="$1" db_basename="$2"
  local -a archive_entries=()
  mapfile -t archive_entries < <(tar -tzf "$archive_file" 2>/dev/null || true)
  [[ ${#archive_entries[@]} -gt 0 ]] || return 1
  printf '%s\n' "${archive_entries[@]}" | grep -Fxq "$db_basename" || return 1
  printf '%s\n' "${archive_entries[@]}" | grep -Fxq ".env" || return 1
  printf '%s\n' "${archive_entries[@]}" | grep -Fxq "metadata.txt" || return 1
  return 0
}

wait_for_service_stopped_state() {
  local max_attempts="${1:-8}" delay_seconds="${2:-1}" attempt state
  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    if [[ "$state" == "inactive" || "$state" == "failed" ]]; then
      return 0
    fi
    sleep "$delay_seconds"
  done
  return 1
}

wait_for_service_active_state() {
  local max_attempts="${1:-8}" delay_seconds="${2:-1}" attempt state
  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    if [[ "$state" == "active" ]]; then
      return 0
    fi
    sleep "$delay_seconds"
  done
  return 1
}

collect_existing_sqlite_bundle_basenames() {
  local db_file="$1"
  local suffix candidate
  [[ -f "$db_file" ]] || return 1
  printf '%s\n' "$(basename "$db_file")"
  for suffix in "-wal" "-shm"; do
    candidate="${db_file}${suffix}"
    [[ -f "$candidate" ]] && printf '%s\n' "$(basename "$candidate")"
  done
  return 0
}

migrate_legacy_default_db_path() {
  local current_db_path old_db_file
  current_db_path="$(get_env_value DB_PATH)"
  if [[ -z "$current_db_path" ]]; then
    old_db_file="$LEGACY_DB_PATH"
  elif [[ "$current_db_path" == "$LEGACY_DB_BASENAME" ]]; then
    old_db_file="$LEGACY_DB_PATH"
  elif [[ "$current_db_path" == "$LEGACY_DB_PATH" ]]; then
    old_db_file="$LEGACY_DB_PATH"
  else
    return 0
  fi

  if [[ "$old_db_file" != "$DEFAULT_DB_PATH" ]]; then
    mkdir -p "$RUNTIME_DIR"
    if [[ -f "$old_db_file" ]]; then
      # Проверка целостности старой БД перед копированием
      log "INFO" "Проверка целостности старой БД перед миграцией..."
      if ! sqlite_full_integrity_check "$old_db_file"; then
        log "WARN" "Старая БД ($old_db_file) имеет проблемы целостности. Создание бэкапа перед миграцией..."
        local backup_on_error="${old_db_file}.corrupt.backup.$(date +%Y%m%d%H%M%S)"
        cp -p "$old_db_file" "$backup_on_error" || true
        for suffix in "-wal" "-shm"; do
          [[ -f "${old_db_file}${suffix}" ]] && cp -p "${old_db_file}${suffix}" "${backup_on_error}${suffix}" || true
        done
        log "INFO" "Бэкап повреждённой БД сохранён: $backup_on_error"
      fi
      
      copy_sqlite_runtime_bundle "$old_db_file" "$DEFAULT_DB_PATH" || return 1
      log "INFO" "БД успешно мигрирована в: $DEFAULT_DB_PATH"
    fi
  fi

  set_env_value DB_PATH "$DEFAULT_DB_PATH"
  return 0
}

install_awg_helper() {
  [[ -f "$BOT_DIR/awg_helper.py" ]] || return 1
  install -d -m 755 /usr/local/libexec
  install -o root -g root -m 750 "$BOT_DIR/awg_helper.py" "$AWG_HELPER_TARGET"
  sync_awg_helper_policy_from_env
  if id -u "$BOT_USER" >/dev/null 2>&1; then
    chown root:"$BOT_USER" "$AWG_HELPER_POLICY"
    chmod 640 "$AWG_HELPER_POLICY"
  fi
  cat > "$AWG_HELPER_SUDOERS" <<SUDOERS
${BOT_USER} ALL=(root) NOPASSWD: ${AWG_HELPER_TARGET} *
SUDOERS
  chmod 440 "$AWG_HELPER_SUDOERS"
  return 0
}

write_service() {
  mkdir -p "$APP_LOG_DIR"
  touch "$APP_LOG_FILE"
  chmod 640 "$APP_LOG_FILE" || true
  cat > "$SERVICE_FILE" <<SERVICE
[Unit]
Description=AWG Telegram Bot
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${VENV_DIR}/bin/python -u ${BOT_DIR}/app.py
Restart=always
RestartSec=3
User=${BOT_USER}
Group=${BOT_USER}
# sudo к root helper требует возможности повышения привилегий.
NoNewPrivileges=false
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
StandardOutput=append:${APP_LOG_FILE}
StandardError=append:${APP_LOG_FILE}

[Install]
WantedBy=multi-user.target
SERVICE
  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME" >/dev/null
  return 0
}

install_autobackup_units() {
  local service_src timer_src
  service_src="${INSTALL_DIR}/packaging/systemd/${AUTO_BACKUP_SERVICE_NAME}"
  timer_src="${INSTALL_DIR}/packaging/systemd/${AUTO_BACKUP_TIMER_NAME}"
  if [[ ! -f "$service_src" || ! -f "$timer_src" ]]; then
    warn "Файлы autobackup systemd unit не найдены: ${service_src}, ${timer_src}"
    return 1
  fi
  cp "$service_src" "$AUTO_BACKUP_SERVICE_FILE"
  cp "$timer_src" "$AUTO_BACKUP_TIMER_FILE"
  chmod 644 "$AUTO_BACKUP_SERVICE_FILE" "$AUTO_BACKUP_TIMER_FILE" || true
  systemctl daemon-reload
  return 0
}

configure_autobackup_timer() {
  if ! require_command systemctl; then
    warn "systemctl не найден. Автобэкап таймер не настроен."
    return 0
  fi
  install_autobackup_units || return 1
  if autobackup_enabled; then
    systemctl enable --now "$AUTO_BACKUP_TIMER_NAME" >/dev/null 2>&1 || return 1
    ok "Автобэкап включён: ${AUTO_BACKUP_TIMER_NAME} (keep=$(autobackup_keep_count))."
  else
    systemctl disable --now "$AUTO_BACKUP_TIMER_NAME" >/dev/null 2>&1 || true
    ok "Автобэкап выключен (AUTO_BACKUP_ENABLED=0)."
  fi
  return 0
}

install_platega_webhook_service() {
  local service_src
  service_src="${INSTALL_DIR}/packaging/systemd/${PLATEGA_WEBHOOK_SERVICE_NAME}"
  if [[ ! -f "$service_src" ]]; then
    warn "Файл platega-webhook systemd unit не найден: ${service_src}"
    return 1
  fi
  cp "$service_src" "$PLATEGA_WEBHOOK_SERVICE_FILE"
  chmod 644 "$PLATEGA_WEBHOOK_SERVICE_FILE" || true
  systemctl daemon-reload
  return 0
}

install_certbot_renewal_timer() {
  local timer_src service_src
  timer_src="${INSTALL_DIR}/packaging/systemd/certbot-renewal.timer"
  service_src="${INSTALL_DIR}/packaging/systemd/certbot-renewal.service"
  
  if [[ ! -f "$timer_src" ]]; then
    warn "Файл certbot-renewal.timer не найден: ${timer_src}"
    return 1
  fi
  if [[ ! -f "$service_src" ]]; then
    warn "Файл certbot-renewal.service не найден: ${service_src}"
    return 1
  fi
  
  cp "$timer_src" /etc/systemd/system/certbot-renewal.timer
  cp "$service_src" /etc/systemd/system/certbot-renewal.service
  chmod 644 /etc/systemd/system/certbot-renewal.timer /etc/systemd/system/certbot-renewal.service || true
  systemctl daemon-reload
  systemctl enable --now certbot-renewal.timer >/dev/null 2>&1 || {
    warn "Не удалось активировать certbot-renewal.timer"
    return 1
  }
  ok "certbot-renewal.timer установлен и активирован (ежедневное обновление SSL)."
  return 0
}

configure_platega_webhook() {
  if ! require_command systemctl; then
    warn "systemctl не найден. Platega webhook сервис не настроен."
    return 0
  fi
  
  # Настраиваем сервис только если Platega включен
  local merchant_id
  merchant_id="$(get_env_value PLATEGA_MERCHANT_ID)"
  if [[ -z "$merchant_id" ]]; then
    info "Platega не настроен (нет Merchant ID). Webhook сервис не активируется."
    systemctl disable --now "$PLATEGA_WEBHOOK_SERVICE_NAME" >/dev/null 2>&1 || true
    return 0
  fi
  
  install_platega_webhook_service || return 1
  systemctl enable --now "$PLATEGA_WEBHOOK_SERVICE_NAME" >/dev/null 2>&1 || return 1
  
  # Показываем URL для настройки Callback в Platega
  local server_ip
  server_ip="$(get_env_value SERVER_IP)"
  local webhook_port
  webhook_port="$(get_env_value PLATEGA_WEBHOOK_PORT)"
  local webhook_domain
  webhook_domain="$(get_env_value PLATEGA_WEBHOOK_DOMAIN)"
  
  local callback_url=""
  if [[ -n "$webhook_domain" ]]; then
    callback_url="https://${webhook_domain}/webhook"
  elif [[ -n "$server_ip" && -n "$webhook_port" ]]; then
    # Извлекаем только IP из SERVER_IP (формат может быть IP:port)
    local webhook_host
    webhook_host="${server_ip%%:*}"
    callback_url="https://${webhook_host}:${webhook_port}/webhook"
  fi
  
  if [[ -n "$callback_url" ]]; then
    ok "Platega webhook сервис запущен: ${PLATEGA_WEBHOOK_SERVICE_NAME}"
    info ""
    info "╔═══════════════════════════════════════════════════════════╗"
    info "║  ВАЖНО: Настройте Callback URL в личном кабинете Platega  ║"
    info "╠═══════════════════════════════════════════════════════════╣"
    info "║  URL для ввода в настройках Platega:                      ║"
    info "║  ${callback_url}"
    info "║                                                           ║"
    if [[ -n "$webhook_domain" ]]; then
      info "║  SSL-сертификат настроен автоматически.                    ║"
    else
      info "║  Убедитесь, что порт ${webhook_port} открыт в firewall!              ║"
    fi
    info "╚═══════════════════════════════════════════════════════════╝"
    info ""
  else
    ok "Platega webhook сервис запущен: ${PLATEGA_WEBHOOK_SERVICE_NAME}"
    warn "Не удалось определить SERVER_IP, PLATEGA_WEBHOOK_PORT или PLATEGA_WEBHOOK_DOMAIN для отображения Callback URL"
  fi
  
  return 0
}

persist_release_sha() {
  local sha="${1:-}"
  mkdir -p "$STATE_DIR"
  if [[ -n "$sha" ]]; then
    printf '%s\n' "$sha" > "$VERSION_FILE"
    return 0
  fi
  rm -f "$VERSION_FILE" || true
  warn "release_sha не записан: точный commit развернутого кода не удалось подтвердить."
  return 0
}

# Проверка состояния firewall (только если systemctl доступен)
is_firewall_active() {
  if ! require_command systemctl; then
    return 1
  fi
  
  # Проверяем UFW
  if command -v ufw >/dev/null 2>&1; then
    if ufw status 2>/dev/null | grep -q "Status: active"; then
      return 0
    fi
  fi
  
  # Проверяем firewalld
  if systemctl is-active firewalld >/dev/null 2>&1; then
    return 0
  fi
  
  return 1
}

# Открытие портов в firewall (только если он активен)
open_firewall_ports() {
  local ports=("$@")
  
  if ! is_firewall_active; then
    info "Firewall не активен, пропускаем настройку правил."
    return 0
  fi
  
  info "Открываю порты в firewall..."
  
  # UFW
  if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
    for port in "${ports[@]}"; do
      if ufw status | grep -q "${port}/tcp.*ALLOW"; then
        info "Порт ${port}/tcp уже открыт в UFW."
      else
        ufw allow "${port}/tcp" >/dev/null 2>&1 && ok "Порт ${port}/tcp открыт в UFW." || warn "Не удалось открыть порт ${port}/tcp в UFW."
      fi
    done
    return 0
  fi
  
  # firewalld
  if systemctl is-active firewalld >/dev/null 2>&1; then
    for port in "${ports[@]}"; do
      if firewall-cmd --list-ports 2>/dev/null | grep -q "${port}/tcp"; then
        info "Порт ${port}/tcp уже открыт в firewalld."
      else
        firewall-cmd --permanent --add-port="${port}/tcp" >/dev/null 2>&1 && \
        firewall-cmd --reload >/dev/null 2>&1 && \
        ok "Порт ${port}/tcp открыт в firewalld." || warn "Не удалось открыть порт ${port}/tcp в firewalld."
      fi
    done
    return 0
  fi
  
  return 0
}

# Проверка какие процессы слушают указанный порт (альтернатива ss)
check_port_listeners() {
  local port="$1"
  local result=""
  
  # Пробуем ss если доступен
  if command -v ss >/dev/null 2>&1; then
    result=$(ss -tlnp 2>/dev/null | grep ":${port} " || true)
    if [[ -n "$result" ]]; then
      echo "$result"
      return 0
    fi
  fi
  
  # Пробуем netstat если ss недоступен
  if command -v netstat >/dev/null 2>&1; then
    result=$(netstat -tlnp 2>/dev/null | grep ":${port} " || true)
    if [[ -n "$result" ]]; then
      echo "$result"
      return 0
    fi
  fi
  
  # Используем Python как fallback
  result=$("$PYTHON_BIN" - "$port" <<'PY'
import socket, os, subprocess, sys
try:
    port = int(sys.argv[1])
    # Пытаемся получить информацию через /proc/net/tcp
    listeners = []
    for tcp_file in ['/proc/net/tcp', '/proc/net/tcp6']:
        if not os.path.exists(tcp_file):
            continue
        with open(tcp_file, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) < 4:
                    continue
                local_addr = parts[1]
                state = parts[3]
                if state != '0A':  # Только LISTEN (0A)
                    continue
                ip_hex, port_hex = local_addr.split(':')
                local_port = int(port_hex, 16)
                if local_port == port:
                    listeners.append(f"LISTEN 0.0.0.0:{port}")
    if listeners:
        for l in listeners:
            print(l)
        sys.exit(0)
    sys.exit(1)
except Exception as e:
    sys.exit(1)
PY
  )
  
  if [[ -n "$result" ]]; then
    echo "$result"
    return 0
  fi
  
  return 1
}

# Настройка Nginx и SSL сертификата
setup_nginx_and_ssl() {
  local domain="$1"
  
  if [[ -z "$domain" ]]; then
    warn "Домен не указан, пропускаю настройку Nginx и SSL."
    return 1
  fi
  
  info "Настраиваю Nginx и SSL для домена ${domain}..."
  
  # Гарантируем что nginx и certbot установлены ПЕРЕД настройкой
  info "Проверяю наличие nginx и certbot..."
  if ! command -v nginx >/dev/null 2>&1; then
    info "Устанавливаю nginx и certbot..."
    export DEBIAN_FRONTEND=noninteractive
    apt_get_safe update -y
    apt_get_safe install -y --no-install-recommends nginx certbot python3-certbot-nginx || {
      error "Не удалось установить nginx и certbot."
      return 1
    }
  fi
  
  # Проверяем что директории nginx существуют и создаём их если нет
  if [[ ! -d "/etc/nginx/sites-available" ]]; then
    mkdir -p /etc/nginx/sites-available
  fi
  if [[ ! -d "/etc/nginx/sites-enabled" ]]; then
    mkdir -p /etc/nginx/sites-enabled
  fi
  
  # Проверяем, что основной конфиг nginx включает sites-enabled
  if [[ -f "/etc/nginx/nginx.conf" ]]; then
    if ! grep -q "sites-enabled" /etc/nginx/nginx.conf 2>/dev/null; then
      info "Добавляю включение sites-enabled в nginx.conf..."
      if ! grep -q "include /etc/nginx/sites-enabled/" /etc/nginx/nginx.conf 2>/dev/null; then
        # Добавляем include перед закрывающей скобкой http блока или в конец файла
        if grep -q "^http {" /etc/nginx/nginx.conf; then
          # Находим строку с http { и добавляем include после неё
          sed -i '/^http {/a\    include /etc/nginx/sites-enabled/*;' /etc/nginx/nginx.conf
        else
          # Если структуры http {} нет, просто добавляем в конец
          echo "include /etc/nginx/sites-enabled/*;" >> /etc/nginx/nginx.conf
        fi
      fi
      ok "sites-enabled добавлен в nginx.conf"
    fi
  fi
  
  # Предварительная проверка: нет ли уже процессов на порту 80
  info "Предварительная проверка порта 80..."
  local existing_port80
  existing_port80=$(check_port_listeners 80 || true)
  if [[ -n "$existing_port80" ]]; then
    info "Порт 80 уже занят:"
    echo "$existing_port80"
    
    # Проверяем, это nginx или Docker
    local is_nginx=false
    local is_docker=false
    
    if echo "$existing_port80" | grep -q "nginx"; then
      is_nginx=true
      info "Обнаружен работающий Nginx на порту 80."
    fi
    
    # Проверяем, не Docker ли это
    if command -v docker &>/dev/null; then
      local docker_containers
      docker_containers=$(docker ps --format "table {{.Names}}\t{{.Ports}}" 2>/dev/null | grep -E ":80|:443" || true)
      if [[ -n "$docker_containers" ]]; then
        is_docker=true
        warn "Обнаружены Docker-контейнеры на портах 80/443:"
        echo "$docker_containers"
        echo ""
        echo "Это может конфликтовать с получением SSL сертификата."
      fi
    fi
    
    # Автоматически останавливаем nginx если он мешает
    if [[ "$is_nginx" == "true" ]]; then
      info "Автоматически останавливаю Nginx для настройки..."
      if command -v systemctl &>/dev/null; then
        systemctl stop nginx 2>/dev/null || true
        sleep 1
      elif command -v service &>/dev/null; then
        service nginx stop 2>/dev/null || true
        sleep 1
      fi
      
      # Проверяем что порт освободился
      if check_port_listeners 80 | grep -q ":80 "; then
        warn "Не удалось освободить порт 80. Попробуйте вручную остановить конфликтующие процессы."
        echo "Команды для ручной остановки:"
        echo "  sudo systemctl stop nginx"
        echo "  sudo docker stop <container_name>"
        if ! confirm_explicit "Попробовать продолжить несмотря на занятый порт?"; then
          return 1
        fi
      else
        ok "Порт 80 освобожден."
      fi
    elif [[ "$is_docker" == "true" ]]; then
      warn "Docker-контейнеры занимают порт 80/443."
      echo "Рекомендуется временно остановить их командой:"
      echo "  docker stop <container_name>"
      echo ""
      if ! confirm_explicit "Продолжить несмотря на возможные конфликты?"; then
        return 1
      fi
    fi
  fi
  
  # Проверка DNS
  info "Проверяю DNS записи для ${domain}..."
  if ! "$PYTHON_BIN" - "$domain" <<'PY'
import socket, sys
domain = sys.argv[1]
try:
    result = socket.gethostbyname(domain)
    print(f"DNS OK: {domain} -> {result}")
    sys.exit(0)
except socket.gaierror:
    print(f"DNS ERROR: не удалось разрешить {domain}")
    sys.exit(1)
PY
  then
    warn "DNS проверка не пройдена. Убедитесь, что домен ${domain} указывает на этот сервер."
    if ! confirm_explicit "Продолжить несмотря на ошибку DNS?"; then
      return 1
    fi
  fi
  
  # Открываем порты 80 и 443 в firewall ДО настройки Nginx и получения сертификата
  info "Открываю порты 80 и 443 в firewall..."
  open_firewall_ports 80 443

  # Очищаем старые конфигурации для этого домена
  local old_nginx_link="/etc/nginx/sites-enabled/${domain}"
  local old_nginx_conf="/etc/nginx/sites-available/${domain}"
  if [[ -L "$old_nginx_link" ]] || [[ -f "$old_nginx_link" ]]; then
    rm -f "$old_nginx_link"
  fi
  if [[ -f "$old_nginx_conf" ]]; then
    rm -f "$old_nginx_conf"
  fi
  
  # Отключаем default конфиг, чтобы он не перехватывал порт 80
  local default_nginx_link="/etc/nginx/sites-enabled/default"
  if [[ -L "$default_nginx_link" ]] || [[ -f "$default_nginx_link" ]]; then
    info "Отключаю стандартный конфиг Nginx (default)..."
    rm -f "$default_nginx_link"
  fi
  
  # ============================================================
  # ШАГ 1: Создаем директорию для ACME challenge
  # ============================================================
  info "Создаю директорию для ACME challenge..."
  if ! mkdir -p /var/www/certbot/.well-known/acme-challenge; then
    error "Не удалось создать директорию /var/www/certbot/.well-known/acme-challenge"
    return 1
  fi
  if ! chown -R www-data:www-data /var/www/certbot; then
    warn "Не удалось изменить владельца /var/www/certbot"
  fi
  if ! chmod -R 755 /var/www/certbot; then
    warn "Не удалось установить права на /var/www/certbot"
  fi
  ok "Директория /var/www/certbot/.well-known/acme-challenge создана."
  
  # ============================================================
  # ШАГ 2: Создаем ВРЕМЕННУЮ HTTP-only конфигурацию Nginx
  # (без SSL блока, чтобы nginx -t прошёл до получения сертификата)
  # ============================================================
  local webhook_port="${PLATEGA_WEBHOOK_PORT:-8081}"
  
  local nginx_conf="/etc/nginx/sites-available/${domain}"
  local nginx_link="/etc/nginx/sites-enabled/${domain}"
  
  # Создаём временный конфиг ТОЛЬКО с HTTP сервером
  cat > "$nginx_conf" <<'NGINX_HTTP'
# HTTP server - только для ACME challenge (временная конфигурация)
server {
    listen 80;
    server_name __DOMAIN_PLACEHOLDER__;
    
    # ACME challenge для Let's Encrypt
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
        try_files $uri =404;
    }

    # Временный ответ для всех остальных запросов
    location / {
        return 301 https://$host$request_uri;
        # Примечание: HTTPS будет настроен после получения сертификата
    }
}
NGINX_HTTP
  
  # Заменяем плейсхолдер на реальный домен
  sed -i "s/__DOMAIN_PLACEHOLDER__/${domain}/g" "$nginx_conf"
  
  # Создаем симлинк и проверяем что он создан
  if ! ln -sf "$nginx_conf" "$nginx_link"; then
    error "Не удалось создать символическую ссылку на конфигурацию Nginx."
    return 1
  fi
  if [[ ! -L "$nginx_link" ]] && [[ ! -f "$nginx_link" ]]; then
    error "Символическая ссылка не была создана."
    return 1
  fi
  ok "Временная HTTP конфигурация Nginx создана."
  
  # Проверяем конфигурацию Nginx (HTTP-only, без SSL - файлы сертификатов ещё не нужны)
  if ! nginx -t >/dev/null 2>&1; then
    error "Ошибка в конфигурации Nginx после создания HTTP конфига для ${domain}"
    nginx -t 2>&1 || true
    rm -f "$nginx_link"
    return 1
  fi
  
  # Перезапускаем Nginx с HTTP-only конфигом
  info "Перезапускаю Nginx с временной HTTP конфигурацией..."
  if require_command systemctl; then
    if ! systemctl daemon-reload 2>&1; then
      warn "daemon-reload failed, но продолжаем..."
    fi
    if ! systemctl restart nginx 2>&1; then
      error "Не удалось перезапустить Nginx через systemctl."
      systemctl status nginx --no-pager || true
      return 1
    fi
    sleep 2
    if ! systemctl is-active --quiet nginx; then
      error "Nginx сервис не активен после restart"
      systemctl status nginx --no-pager || true
      journalctl -u nginx -n 30 --no-pager || true
      return 1
    fi
    ok "Nginx перезапущен с HTTP конфигурацией."
  else
    if ! service nginx restart 2>&1; then
      error "Не удалось перезапустить Nginx через service."
      service nginx status || true
      return 1
    fi
    ok "Nginx перезапущен с HTTP конфигурацией."
  fi
  
  # Даём время на старт Nginx
  sleep 2
  
  # Проверяем, что Nginx действительно слушает порт 80
  info "Проверяю, что Nginx слушает порт 80..."
  if ! check_port_listeners 80 | grep -q ":80 "; then
    warn "Nginx не слушает порт 80!"
    check_port_listeners 80 | grep -E ":80|nginx" || true
    return 1
  fi
  ok "Nginx слушает порт 80 и готов для ACME challenge."
  
  # ============================================================
  # ШАГ 3: Получаем SSL сертификат через Certbot
  # ============================================================
  info "Получаю SSL сертификат через Let's Encrypt..."
  local certbot_log certbot_err_log
  certbot_log="$(mktemp)"
  certbot_err_log="$(mktemp)"
  
  # Retry logic для certbot (3 попытки с паузой)
  local max_attempts=3 attempt=1 certbot_success=false
  while (( attempt <= max_attempts )); do
    info "Попытка ${attempt}/${max_attempts} получения сертификата..."
    
    if certbot --nginx --non-interactive --agree-tos --register-unsafely-without-email -d "$domain" --verbose >"$certbot_log" 2>"$certbot_err_log"; then
      certbot_success=true
      break
    else
      warn "Попытка ${attempt} не удалась."
      if (( attempt < max_attempts )); then
        info "Жду 10 секунд перед следующей попыткой..."
        sleep 10
      fi
    fi
    ((attempt++))
  done
  
  if [[ "$certbot_success" != "true" ]]; then
    echo ""
    echo "[ОШИБКА] Не удалось получить SSL сертификат после ${max_attempts} попыток."
    echo ""
    echo "=== Лог stdout ==="
    cat "$certbot_log"
    echo ""
    echo "=== Лог stderr (ОШИБКИ) ==="
    cat "$certbot_err_log"
    echo "==========================="
    echo ""
    echo "=== Дополнительная диагностика ==="
    echo ""
    echo "1. Проверка доступности домена извне:"
    curl -v --connect-timeout 5 "http://${domain}/.well-known/acme-challenge/" 2>&1 | head -20 || echo "  [Не удалось подключиться]"
    echo ""
    echo "2. Текущие сертификаты (если есть):"
    ls -la /etc/letsencrypt/live/${domain}/ 2>/dev/null || echo "  [Сертификатов нет]"
    echo ""
    echo "3. Статус портов:"
    check_port_listeners 80 | grep -E ":80" || check_port_listeners 443 | grep -E ":443" || echo "  [Порты 80/443 не слушаются]"
    echo ""
    echo "4. Конфигурация Nginx для домена:"
    cat /etc/nginx/sites-available/"$domain" 2>/dev/null || echo "  [Конфиг не найден]"
    echo ""
    echo "5. Проверка firewall:"
    if command -v ufw >/dev/null 2>&1; then
      ufw status 2>/dev/null | head -10 || true
    fi
    if command -v iptables >/dev/null 2>&1; then
      iptables -L -n 2>/dev/null | grep -E "80|443" || true
    fi
    echo ""
    
    echo "Возможные причины:"
    echo "  1. Порт 80 закрыт фаерволом провайдера"
    echo "  2. DNS запись еще не обновилась"
    echo "  3. Домен уже имеет сертификат"
    echo "  4. Директория /.well-known/acme-challenge/ недоступна"
    echo "  5. Другой процесс перехватывает порт 80"
    echo "  6. Домен за Cloudflare proxy"
    echo ""
    echo "Для ручной диагностики:"
    echo "  curl -v http://${domain}/.well-known/acme-challenge/test"
    echo "  lsof -i :80"
    echo "  certbot --nginx -d ${domain} --verbose --debug"
    echo ""
    
    rm -f "$certbot_log" "$certbot_err_log"
    rm -f "$nginx_link"
    rm -f "$nginx_conf"
    return 1
  fi
  
  rm -f "$certbot_log" "$certbot_err_log"
  ok "SSL сертификат успешно получен для ${domain}."
  
  # ============================================================
  # ШАГ 4: Обновляем конфиг Nginx с полноценной HTTPS поддержкой
  # ============================================================
  info "Обновляю конфигурацию Nginx с HTTPS поддержкой..."
  
  cat > "$nginx_conf" <<NGINX_FULL
# HTTP server - redirect to HTTPS + ACME challenge
server {
    listen 80;
    server_name ${domain};
    
    # ACME challenge для Let's Encrypt (для продления сертификата)
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
        try_files \$uri =404;
    }

    # Redirect all other HTTP requests to HTTPS
    location / {
        return 301 https://\$host\$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl http2;
    server_name ${domain};
    
    ssl_certificate /etc/letsencrypt/live/${domain}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${domain}/privkey.pem;
    
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;
    
    location /webhook {
        proxy_pass http://127.0.0.1:${webhook_port};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        proxy_connect_timeout 5s;
        proxy_read_timeout 30s;
    }
    
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
}
NGINX_FULL
  
  # Проверяем обновлённую конфигурацию с SSL
  if ! nginx -t >/dev/null 2>&1; then
    error "Ошибка в конфигурации Nginx после добавления HTTPS блока"
    nginx -t 2>&1 || true
    return 1
  fi
  
  # Перезапускаем Nginx с полной конфигурацией
  info "Перезапускаю Nginx с HTTPS конфигурацией..."
  if require_command systemctl; then
    if ! systemctl reload nginx 2>&1; then
      if ! systemctl restart nginx 2>&1; then
        error "Не удалось перезагрузить/перезапустить Nginx"
        systemctl status nginx --no-pager || true
        return 1
      fi
    fi
    ok "Nginx перезапущен с HTTPS конфигурацией."
  else
    if ! service nginx reload 2>&1; then
      if ! service nginx restart 2>&1; then
        error "Не удалось перезагрузить/перезапустить Nginx через service"
        return 1
      fi
    fi
    ok "Nginx перезапущен с HTTPS конфигурацией."
  fi
  
  # Проверяем что оба порта слушаются
  sleep 2
  if ! check_port_listeners 80 | grep -q ":80"; then
    error "Nginx не слушает порт 80"
    return 1
  fi
  if ! check_port_listeners 443 | grep -q ":443"; then
    error "Nginx не слушает порт 443"
    return 1
  fi
  ok "Порты 80 и 443 активны."
  
  # Проверяем, слушает ли Nginx на 0.0.0.0 (все интерфейсы), а не только на 127.0.0.1
  local nginx_listen_info
  nginx_listen_info=$(check_port_listeners 80 || true)
  if echo "$nginx_listen_info" | grep -q "127.0.0.1:80"; then
    if ! echo "$nginx_listen_info" | grep -qE "0\\.0\\.0\\.0:80|\\*:80|\\[::\\]:80"; then
      warn "ВНИМАНИЕ: Nginx слушает ТОЛЬКО на localhost (127.0.0.1:80), но НЕ на внешних интерфейсах!"
      echo "Это означает, что Let's Encrypt не сможет подключиться к вашему серверу."
      echo ""
      echo "Возможные причины:"
      echo "  1. Другой процесс (например, Docker-контейнер) уже занял порт 80"
      echo "  2. Nginx сконфигурирован слушать только на 127.0.0.1"
      echo ""
      echo "Проверьте конфигурацию Nginx:"
      grep -r "listen" /etc/nginx/sites-enabled/ 2>/dev/null || true
      echo ""
      echo "Попробуйте найти и остановить конфликтующий процесс:"
      echo "  docker ps 2>/dev/null | grep -E ':80|nginx' || true"
      echo "  lsof -i :80 2>/dev/null || true"
      if ! confirm_explicit "Попробовать получить сертификат несмотря на это?"; then
        return 1
      fi
    fi
  fi
  
  # Проверяем, что порт 80 доступен извне перед получением сертификата
  info "Проверяю доступность порта 80 из интернета..."
  sleep 2
  if ! "$PYTHON_BIN" - "$domain" <<'PY'
import socket, sys, time
domain = sys.argv[1]
for attempt in range(3):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((socket.gethostbyname(domain), 80))
        sock.close()
        if result == 0:
            print(f"Порт 80 открыт и доступен")
            sys.exit(0)
    except Exception as e:
        pass
    time.sleep(2)
print("Порт 80 недоступен из интернета. Let's Encrypt не сможет проверить домен.")
sys.exit(1)
PY
  then
    warn "Порт 80 не доступен из интернета. Это может быть из-за:"
    echo "  1. Фаервола вашего хостинг-провайдера (нужно открыть в панели управления)"
    echo "  2. DNS еще не обновился (подождите 5-10 минут)"
    echo "  3. Nginx не слушает порт 80 (проверьте systemctl status nginx)"
    if ! confirm_explicit "Попробовать получить сертификат несмотря на это?"; then
      return 1
    fi
  fi
  
  # Тестовая проверка доступности ACME challenge directory
  info "Тестирую доступность /.well-known/acme-challenge/ локально..."
  local test_file="/var/www/certbot/.well-known/acme-challenge/test-connectivity"
  echo "test-ok" > "$test_file"
  sleep 1
  local curl_result
  curl_result=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1/.well-known/acme-challenge/test-connectivity" 2>/dev/null || echo "000")
  rm -f "$test_file"
  if [[ "$curl_result" == "200" ]]; then
    ok "ACME challenge директория доступна локально."
  else
    warn "ACME challenge директория НЕ доступна локально (HTTP ${curl_result})."
    echo "Проверьте конфигурацию Nginx и права доступа к /var/www/certbot"
    ls -la /var/www/certbot/.well-known/ 2>/dev/null || true
    cat /etc/nginx/sites-available/"$domain" 2>/dev/null || true
    echo ""
    echo "Проверка nginx.conf:"
    grep -A5 "acme-challenge" /etc/nginx/sites-available/"$domain" 2>/dev/null || echo "[Не найдено]"
    return 1
  fi
  
  # Проверка на Cloudflare proxy перед запуском certbot
  info "Проверяю, не находится ли домен за Cloudflare proxy..."
  local cf_check
  cf_check=$(curl -s --connect-timeout 5 -A "Mozilla/5.0" "https://api.cloudflare.com/client/v4/dns/records?name=${domain}&type=A" 2>/dev/null | grep -i "cloudflare" || true)
  if [[ -n "$cf_check" ]]; then
    warn "Домен ${domain} может быть за Cloudflare proxy."
    echo "Если включен orange cloud (proxy), Let's Encrypt не сможет пройти HTTP validation."
    echo "Варианты решения:"
    echo "  1. Временно отключить Cloudflare proxy (gray cloud) для получения сертификата"
    echo "  2. Использовать DNS challenge вместо HTTP (требует API ключ Cloudflare)"
    echo ""
    if ! confirm_explicit "Продолжить несмотря на возможный Cloudflare proxy?"; then
      return 1
    fi
  fi
  
  ok "Nginx и SSL настроены для ${domain}."
  info "Callback URL: https://${domain}/webhook"
  
  return 0
}

start_service() {
  # Проверка наличия systemctl
  if ! require_command systemctl; then
    warn "systemctl не найден. Пропускаю управление сервисом через systemd."
    warn "Если вы в контейнере или среде без systemd, запустите бота вручную:"
    warn "  su -s /bin/bash \"$BOT_USER\" -c \"cd $BOT_DIR && $VENV_DIR/bin/python app.py\""
    warn ""
    warn "ПРЕДУПРЕЖДЕНИЕ: Сервис НЕ будет запущен автоматически."
    warn "Убедитесь, что пользователь $BOT_USER существует перед ручным запуском."
    return 0
  fi
  
  # Проверка существования пользователя BOT_USER перед любыми действиями
  if ! id "$BOT_USER" &>/dev/null; then
    error "Пользователь $BOT_USER не найден. Невозможно инициализировать БД или запустить сервис."
    error "Сначала создайте пользователя: useradd -r -s /bin/false $BOT_USER"
    return 1
  fi
  
  info "Запускаю сервис..."
  
  # Принудительная инициализация базы данных перед запуском сервиса
  # Это необходимо, чтобы БД создалась с правильными правами от имени пользователя awg-bot
  info "Инициализирую базу данных..."
  if [[ -d "$BOT_DIR" && -f "$BOT_DIR/app.py" && -f "$BOT_DIR/database.py" ]]; then
    # Вызываем асинхронную функцию init_db напрямую через asyncio.run
    # Это гарантирует создание БД до запуска основного сервиса
    local init_output
    local init_rc=0
    
    # Обёртываем весь вызов su в timeout для предотвращения зависания
    # Используем увеличенный таймаут (120 сек) и добавляем диагностику через strace при зависании
    local init_cmd="cd '$BOT_DIR' && '$VENV_DIR'/bin/python -c 'import asyncio; from database import init_db; asyncio.run(init_db())'"
    
    # Перед инициализацией проверяем наличие заблокированных WAL файлов от предыдущих запусков
    local db_file_pre
    db_file_pre="$(get_bot_db_file)"
    if [[ -f "${db_file_pre}-wal" ]]; then
      warn "Обнаружен остаточный WAL файл перед инициализацией: ${db_file_pre}-wal"
      # Пытаемся корректно закрыть WAL через checkpoint, если возможно
      if command -v sqlite3 &>/dev/null; then
        sqlite3 "$db_file_pre" "PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null || true
      fi
      # Удаляем WAL и SHM файлы если они есть (это безопасно, т.к. БД не активна)
      rm -f "${db_file_pre}-wal" "${db_file_pre}-shm" 2>/dev/null || true
      info "WAL/SHM файлы удалены перед инициализацией."
    fi
    
    init_output=$(timeout 120 bash -c "su -s /bin/bash \"$BOT_USER\" -c \"$init_cmd\"" 2>&1) || init_rc=$?

    if [[ $init_rc -eq 124 ]]; then
      warn "Инициализация БД превысила таймаут (120 сек). Это может быть связано с блокировкой SQLite."
      warn "Вывод: $init_output"
      warn "Попытка диагностики: проверяем наличие WAL/SHM файлов и процессов..."
      local db_file_diag
      db_file_diag="$(get_bot_db_file)"
      if [[ -f "${db_file_diag}-wal" ]]; then
        warn "Обнаружен WAL файл: ${db_file_diag}-wal"
        ls -la "${db_file_diag}"* 2>/dev/null || true
      fi
      if command -v lsof &>/dev/null; then
        lsof -n "$db_file_diag"* 2>/dev/null || true
      fi
      if command -v fuser &>/dev/null; then
        fuser -v "$db_file_diag"* 2>/dev/null || true
      fi
      # Принудительная очистка WAL при таймауте
      if [[ -f "${db_file_diag}-wal" ]]; then
        warn "Принудительная очистка WAL/SHM файлов после таймаута..."
        rm -f "${db_file_diag}-wal" "${db_file_diag}-shm" 2>/dev/null || true
      fi
    elif [[ $init_rc -ne 0 ]]; then
      warn "Не удалось инициализировать базу данных (код: $init_rc). Вывод: $init_output"
      warn "Попытка продолжения работы..."
    else
      ok "База данных успешно инициализирована."
    fi
    
    # Проверка и исправление прав на созданный файл БД
    local db_file
    db_file="$(get_bot_db_file)"
    if [[ -f "$db_file" ]]; then
      chown "$BOT_USER:$BOT_USER" "$db_file" 2>/dev/null || true
      chmod 600 "$db_file" 2>/dev/null || true
      info "База данных инициализирована: $db_file"
    else
      warn "База данных не была создана при инициализации. Ожидаем создание при старте сервиса."
    fi
  fi
  
  # Дополнительная гарантия прав доступа к runtime файлам перед запуском
  # Исправляем права на .env и БД если они существуют
  if [[ -f "$ENV_FILE" ]]; then
    chown "$BOT_USER:$BOT_USER" "$ENV_FILE" 2>/dev/null || true
    chmod 600 "$ENV_FILE" 2>/dev/null || true
  fi
  
  local db_file_check
  db_file_check="$(get_bot_db_file)"
  if [[ -f "$db_file_check" ]]; then
    chown "$BOT_USER:$BOT_USER" "$db_file_check" 2>/dev/null || true
    chmod 600 "$db_file_check" 2>/dev/null || true
  fi
  
  # daemon-reload с проверкой
  if ! systemctl daemon-reload 2>&1; then
    warn "daemon-reload failed, но продолжаем..."
  fi
  
  # Stop old instance
  systemctl stop "$SERVICE_NAME" 2>/dev/null || true
  sleep 1
  
  # Start service
  if ! systemctl start "$SERVICE_NAME" 2>&1; then
    error "Не удалось запустить сервис ${SERVICE_NAME}"
    systemctl status "$SERVICE_NAME" --no-pager || true
    journalctl -u "$SERVICE_NAME" -n 50 --no-pager || true
    return 1
  fi
  
  # HEALTHCHECK: ждем пока сервис станет активным
  info "Проверяю статус сервиса..."
  local max_wait=30 waited=0
  while (( waited < max_wait )); do
    if systemctl is-active --quiet "$SERVICE_NAME"; then
      ok "Сервис ${SERVICE_NAME} успешно запущен и активен."
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
    info "Жду запуска сервиса... (${waited}/${max_wait}с)"
  done
  
  # Сервис не стал активным
  error "Сервис ${SERVICE_NAME} не перешел в состояние active за ${max_wait}с"
  systemctl status "$SERVICE_NAME" --no-pager || true
  journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
  return 1
}

autobackup_timer_state() {
  local state
  if ! require_command systemctl; then
    printf '%s' 'unavailable'
    return 0
  fi
  state="$(systemctl is-active "$AUTO_BACKUP_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$state" ]] || state="inactive"
  printf '%s' "$state"
}

autobackup_archive_stats() {
  local latest count
  latest="$(find "$BACKUP_ROOT" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' -printf '%T@ %f\n' 2>/dev/null | sort -nr | head -n1 | cut -d' ' -f2- || true)"
  count="$(find "$BACKUP_ROOT" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' 2>/dev/null | wc -l | tr -d ' ' || true)"
  [[ -n "$count" ]] || count="0"
  printf '%s|%s' "${latest:-нет}" "$count"
}

stop_service_if_exists() {
  if service_exists; then
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
  fi
  return 0
}

prepare_bot_log_for_reinstall() {
  local pending_archive="" final_archive="" ts log_dir
  log_dir="$(dirname "$APP_LOG_FILE")"
  mkdir -p "$log_dir" 2>/dev/null || true

  if [[ -s "$APP_LOG_FILE" ]]; then
    ts="$(date +%Y%m%d_%H%M%S)"
    pending_archive="${APP_LOG_FILE}.pending-pre-reinstall-${ts}"
    final_archive="${APP_LOG_FILE}.pre-reinstall-${ts}"
    if ! mv "$APP_LOG_FILE" "$pending_archive" 2>/dev/null; then
      warn "Не удалось архивировать bot.log перед переустановкой (${APP_LOG_FILE})."
      pending_archive=""
      final_archive=""
    fi
  fi

  if ! touch "$APP_LOG_FILE" 2>/dev/null; then
    warn "Не удалось подготовить новый bot.log (${APP_LOG_FILE})."
    printf '%s\t%s\n' "$pending_archive" "$final_archive"
    return 0
  fi
  chown "${BOT_USER}:${BOT_USER}" "$APP_LOG_FILE" 2>/dev/null || warn "Не удалось выставить владельца ${BOT_USER}:${BOT_USER} для ${APP_LOG_FILE}."
  chmod 640 "$APP_LOG_FILE" 2>/dev/null || warn "Не удалось выставить права 640 для ${APP_LOG_FILE}."
  printf '%s\t%s\n' "$pending_archive" "$final_archive"
}

finalize_bot_log_reinstall_archive() {
  local pending_archive="$1" final_archive="$2"
  [[ -n "$pending_archive" && -n "$final_archive" ]] || return 0
  [[ -f "$pending_archive" ]] || return 0
  if mv "$pending_archive" "$final_archive" 2>/dev/null; then
    printf '%s' "$final_archive"
  else
    warn "Не удалось завершить архивирование bot.log после переустановки (${pending_archive} -> ${final_archive})."
  fi
}

restore_bot_log_after_failed_reinstall() {
  local pending_archive="$1"
  [[ -n "$pending_archive" ]] || return 0
  [[ -f "$pending_archive" ]] || return 0
  rm -f "$APP_LOG_FILE" 2>/dev/null || true
  if ! mv "$pending_archive" "$APP_LOG_FILE" 2>/dev/null; then
    warn "Не удалось восстановить предыдущий bot.log после неудачной переустановки (${pending_archive})."
    return 0
  fi
  chown "${BOT_USER}:${BOT_USER}" "$APP_LOG_FILE" 2>/dev/null || true
  chmod 640 "$APP_LOG_FILE" 2>/dev/null || true
}

show_status() {
  local active_state enabled_state local_sha branch_info env_state env_container env_interface policy_container policy_interface policy_error docker_membership
  local ab_stats ab_latest ab_count
  local remote_sha
  detect_install_state
  refresh_update_status_quiet
  print_line
  branch_info="$(cat "$REPO_BRANCH_FILE" 2>/dev/null | tr -d '\r\n' || true)"
  [[ -n "$branch_info" ]] || branch_info="$REPO_BRANCH"
  active_state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
  enabled_state="$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$active_state" ]] || active_state="not-found"
  [[ -n "$enabled_state" ]] || enabled_state="not-found"
  local_sha="$(get_local_sha | cut -c1-12)"
  [[ -n "$local_sha" ]] || local_sha="неизвестно"
  remote_sha="${UPDATE_REMOTE_SHA:0:12}"
  [[ -n "$remote_sha" ]] || remote_sha="не удалось получить"
  env_state="нет"
  [[ -f "$ENV_FILE" ]] && env_state="есть"
  env_container="$(get_env_value DOCKER_CONTAINER)"
  env_interface="$(get_env_value WG_INTERFACE)"
  IFS=$'\t' read -r policy_container policy_interface policy_error < <(read_helper_policy_state)
  ab_stats="$(autobackup_archive_stats)"
  ab_latest="${ab_stats%%|*}"
  ab_count="${ab_stats##*|}"

  echo "Проект: ${REPO_OWNER}/${REPO_NAME}"
  echo "Установлен: $([[ -d "$INSTALL_DIR" ]] && echo 'да' || echo 'нет')"
  echo "Код: ${INSTALL_DIR}"
  echo "ENV: ${ENV_FILE} (${env_state})"
  echo "Сервис: ${SERVICE_NAME}"
  echo "Статус сервиса: ${active_state}"
  echo "Автозапуск: ${enabled_state}"
  echo "Ветка: ${branch_info}"
  echo "Локальная версия: ${local_sha}"
  echo "Доступная версия: ${remote_sha}"
  echo "Обновление: через «Переустановить» (reinstall)"
  echo "Логи приложения: ${APP_LOG_FILE}"
  echo "Лог установки: ${INSTALL_LOG}"
  echo "Autobackup enabled: $(autobackup_enabled && echo 'да' || echo 'нет')"
  echo "Autobackup timer: $(autobackup_timer_state)"
  echo "Autobackup keep count: $(autobackup_keep_count)"
  echo "Backup directory: ${BACKUP_ROOT}"
  echo "Backup archives: ${ab_count}, latest: ${ab_latest}"
  if id -u "$BOT_USER" >/dev/null 2>&1 && id -nG "$BOT_USER" 2>/dev/null | tr ' ' '\n' | grep -qx docker; then
    docker_membership="в группе docker (небезопасно)"
  else
    docker_membership="не в группе docker"
  fi
  echo "${BOT_USER}: ${docker_membership}"
  echo "AWG target (.env): ${env_container:-не задан}/${env_interface:-не задан}"
  if [[ -f "$AWG_HELPER_POLICY" ]]; then
    echo "AWG target (helper policy): ${policy_container:-не задан}/${policy_interface:-не задан}"
    if [[ -n "$policy_error" ]]; then
      warn "$policy_error (${AWG_HELPER_POLICY})"
    elif [[ -n "$env_container" && -n "$env_interface" ]] && [[ "$env_container" != "$policy_container" || "$env_interface" != "$policy_interface" ]]; then
      warn "Обнаружен рассинхрон .env и helper policy. Выполни: sudo awg-tgbot sync-helper-policy"
    fi
  else
    warn "Helper policy не найдена: ${AWG_HELPER_POLICY}"
  fi
  if is_installed; then
    echo
    echo "Health summary:"
    if [[ "$active_state" == "active" ]]; then
      ok "Сервис запущен."
    else
      warn "Сервис не активен."
    fi
  fi
  print_line
  return 0
}

check_updates() {
  detect_install_state
  refresh_update_status_quiet
  print_line
  echo "Ветка : ${REPO_BRANCH}"
  echo "Remote: ${UPDATE_REMOTE_SHA:-не удалось получить}"
  echo "Local : ${UPDATE_LOCAL_SHA:-нет локальной версии}"
  if [[ -n "$UPDATE_REMOTE_TITLE" ]]; then
    echo "Commit title: ${UPDATE_REMOTE_TITLE}"
  fi
  case "$UPDATE_STATUS" in
    current)
      ok "Обновления не найдены. Установлена актуальная версия."
      ;;
    available)
      warn "Доступно обновление. В personal selfhost используй пункт «Переустановить» в меню."
      ;;
    unknown|*)
      warn "Не удалось проверить удалённый commit."
      echo "Для обновления в personal selfhost используй «Переустановить»."
      ;;
  esac
  print_line
  return 0
}

install_or_reinstall_flow() {
  local mode="$1" tmp_dir choice deploy_sha=""
  local api_token="" admin_id="" server_name="" secret=""
  local pre_reinstall_runtime_snapshot="" pre_reinstall_repo_snapshot="" pre_reinstall_log_pending="" pre_reinstall_log_final="" pre_reinstall_log_archive=""
  clear_reinstall_guard
  detect_install_state
  if [[ "$STATE_KERNEL_SUPPORTED" != "1" ]]; then
    die "Ядро Linux слишком старое для AWG (нужно >= 5.6)."
  fi
  if [[ "$STATE_AMNEZIAWG_INSTALLED" != "1" ]]; then
    die "AmneziaWG не обнаружен. Сначала установи AmneziaWG."
  fi
  if [[ "$mode" == "install" && "$STATE_AWG_FOUND" != "1" ]]; then
    print_startup_summary
    die "AWG не обнаружен. Установка доступна только после явной подготовки и запуска AWG."
  fi
  if [[ "$mode" == "reinstall" && "$STATE_AWG_FOUND" != "1" ]]; then
    warn "AWG сейчас не обнаружен. Переустановка может потребовать ручной проверки Docker/AWG."
  fi
  print_line
  if [[ "$mode" == "install" ]]; then
    info "Установка AWG Telegram Bot"
    echo "1) Автоматическая установка"
    echo "   Подходит для типового сценария: скрипт сам подставит найденные значения AWG."
    echo "2) Ручная установка"
    echo "   Подходит, если нужно явно проверить и ввести параметры AWG вручную."
    echo "0) Отмена"
  else
    info "Переустановка AWG Telegram Bot"
    echo "1) Быстрая переустановка"
    echo "   Использует текущие значения из .env (токен, admin_id, цены и т.д.)."
    echo "2) Автоматическая переустановка"
    echo "   Автоопределение AWG + ручной ввод основных параметров."
    echo "3) Ручная переустановка"
    echo "   Расширенный вариант: вручную проверить параметры перед запуском."
    echo "0) Отмена"
  fi
  prompt_raw "Выбор: " choice
  case "$choice" in
    1|2|3) ;;
    *) warn "Действие отменено."; return 0 ;;
  esac

  # Быстрая переустановка: используем текущие env значения
  if [[ "$mode" == "reinstall" && "$choice" == "1" ]]; then
    # Проверяем, что .env существует и содержит необходимые значения
    if [[ ! -f "$ENV_FILE" ]]; then
      die ".env файл не найден. Быстрая переустановка невозможна."
    fi
    
    api_token="$(get_env_value API_TOKEN)"
    admin_id="$(get_env_value ADMIN_ID)"
    server_name="$(get_env_value SERVER_NAME)"
    
    if [[ -z "$api_token" || -z "$admin_id" ]]; then
      die "Не найдены API_TOKEN или ADMIN_ID в .env. Быстрая переустановка невозможна."
    fi
    
    # Если SERVER_NAME не задан, используем значение по умолчанию
    if [[ -z "$server_name" ]]; then
      server_name="My VPN"
    fi
    
    # В быстрой переустановке НЕ спрашиваем имя сервера, используем текущее из .env
    info "Используем текущее имя сервера: ${server_name}"
    
    secret="$(ensure_secret)"
    
    ensure_packages || die "Не удалось установить системные зависимости."
    ensure_docker_ready || die "Docker недоступен."
    detect_awg_environment
    print_detected_awg_summary
  fi

  # Общий код для всех режимов переустановки/установки
  if [[ "$mode" == "reinstall" ]]; then
    pre_reinstall_repo_snapshot="$(create_repo_snapshot_before_reinstall)"
    ok "Создан snapshot файлов приложения перед переустановкой: ${pre_reinstall_repo_snapshot}"
  fi

  deploy_sha="$(fetch_remote_sha)"
  if [[ -n "$deploy_sha" ]]; then
    info "Целевой commit для развёртывания: ${deploy_sha}"
    tmp_dir="$(download_repo "$deploy_sha")" || die "Не удалось скачать код проекта из GitHub (commit=${deploy_sha})."
  else
    warn "Не удалось определить remote SHA. Использую ветку ${REPO_BRANCH} без commit pinning."
    tmp_dir="$(download_repo "$REPO_BRANCH")" || die "Не удалось скачать код проекта из GitHub (branch=${REPO_BRANCH})."
  fi
  stop_service_if_exists
  if [[ "$mode" == "reinstall" ]]; then
    pre_reinstall_runtime_snapshot="$(create_runtime_snapshot_before_reinstall pre-reinstall)"
    ok "Создан snapshot runtime (DB/.env/state) после остановки сервиса: ${pre_reinstall_runtime_snapshot}"
    set_reinstall_guard "$pre_reinstall_repo_snapshot" "$pre_reinstall_runtime_snapshot"
  fi
  deploy_repo "$tmp_dir" || { rm -rf "$tmp_dir"; die "Не удалось развернуть файлы проекта."; }
  rm -rf "$tmp_dir"
  ensure_env_file
  migrate_legacy_tariff_defaults
  migrate_legacy_default_db_path || die "Не удалось подготовить путь БД для runtime."

  # Для быстрой переустановки (choice=1) сохраняем текущие env значения
  if [[ "$mode" == "reinstall" && "$choice" == "1" ]]; then
    # Быстрая переустановка: сохраняем все текущие значения из .env
    # Сначала восстанавливаем существующие значения (включая SERVER_NAME)
    preserve_existing_env_values
    # Затем записываем обновлённые основные поля
    write_common_env "$api_token" "$admin_id" "$server_name" "$secret"
    # Принудительная переустановка зависимостей
    export FORCE_REINSTALL_DEPS=1
  else
    write_common_env "$api_token" "$admin_id" "$server_name" "$secret"
    # При полной переустановке с новым конфигом тоже переустанавливаем зависимости
    if [[ "$mode" == "reinstall" ]]; then
      export FORCE_REINSTALL_DEPS=1
    fi
  fi
  ensure_selfhost_network_defaults
  ensure_fernet_key
  
  # Обработка выбора режима для AWG и других настроек
  if [[ "$mode" == "reinstall" && "$choice" == "1" ]]; then
    # Быстрая переустановка: используем текущие значения AWG из .env
    : # Ничего не делаем, оставляем текущие значения
  elif [[ "$mode" == "reinstall" && "$choice" == "2" ]]; then
    # Автоматическая переустановка: автоопределение AWG + ручной ввод основных параметров
    api_token=""
    admin_id=""
    server_name=""
    
    prompt_api_token api_token
    prompt_admin_id admin_id
    
    # Запрос имени сервера
    prompt_server_name server_name
    if [[ -z "$server_name" ]]; then
      server_name="My VPN"
    fi
    
    write_detected_awg_env
    if [[ -z "$(get_env_value SERVER_PUBLIC_KEY)" ]]; then
      warn "Не удалось автоматически определить SERVER_PUBLIC_KEY. Нужен один ручной шаг."
      prompt_with_default 'SERVER_PUBLIC_KEY' "$DETECTED_PUBLIC_KEY" value
      set_env_value SERVER_PUBLIC_KEY "$value"
    fi
    if [[ -z "$(get_env_value SERVER_IP)" ]]; then
      warn "Не удалось автоматически определить SERVER_IP. Укажи внешний IP и порт."
      default="$(pick_existing_or_default "$(get_env_value PUBLIC_HOST)" "$DETECTED_PUBLIC_HOST")"
      prompt_with_default 'PUBLIC_HOST / внешний IP' "$default" value
      set_env_value PUBLIC_HOST "$value"
      if [[ -n "$DETECTED_LISTEN_PORT" && -n "$value" ]]; then
        set_env_value SERVER_IP "${value}:${DETECTED_LISTEN_PORT}"
      else
        prompt_with_default 'SERVER_IP (IP:port)' "$DETECTED_SERVER_IP" value
        set_env_value SERVER_IP "$value"
      fi
    fi
    
    # Запись основных параметров после сбора всех данных
    secret="$(ensure_secret)"
    write_common_env "$api_token" "$admin_id" "$server_name" "$secret"
    
    # Настройка Platega
    echo ""
    echo "--- Настройка Platega (опционально, нажмите Enter для пропуска) ---"
    
    platega_merchant_id=""
    default="$(pick_existing_or_default "$(get_env_value PLATEGA_MERCHANT_ID)" "")"
    prompt_with_default 'Platega Merchant ID' "$default" platega_merchant_id
    
    if [[ -n "$platega_merchant_id" ]]; then
      set_env_value PLATEGA_MERCHANT_ID "$platega_merchant_id"
      
      platega_secret_key=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_SECRET_KEY)" "")"
      prompt_with_default 'Platega Secret Key' "$default" platega_secret_key
      set_env_value PLATEGA_SECRET_KEY "$platega_secret_key"
      
      # Запрос домена для webhook
      platega_domain=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_DOMAIN)" "")"
      prompt_with_default 'Домен для webhook Platega (например: example.com)' "$default" platega_domain
      
      if [[ -n "$platega_domain" ]]; then
        set_env_value PLATEGA_WEBHOOK_DOMAIN "$platega_domain"
        set_env_value PLATEGA_WEBHOOK_PORT "443"
        
        # Автоматическая настройка Nginx и SSL
        setup_nginx_and_ssl "$platega_domain"
      else
        # Если домен не указан, используем старый режим с IP:порт
        platega_webhook_port=""
        default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_PORT)" "8081")"
        prompt_with_default 'Порт webhook Platega' "$default" platega_webhook_port
        set_env_value PLATEGA_WEBHOOK_PORT "$platega_webhook_port"
      fi
      
      platega_price_7_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_7_DAYS)" "100")"
      prompt_with_default 'Цена 7 дней через Platega (руб)' "$default" platega_price_7_days
      set_env_value PLATEGA_PRICE_7_DAYS "$platega_price_7_days"
      
      platega_price_30_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_30_DAYS)" "250")"
      prompt_with_default 'Цена 30 дней через Platega (руб)' "$default" platega_price_30_days
      set_env_value PLATEGA_PRICE_30_DAYS "$platega_price_30_days"
      
      platega_price_90_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_90_DAYS)" "700")"
      prompt_with_default 'Цена 90 дней через Platega (руб)' "$default" platega_price_90_days
      set_env_value PLATEGA_PRICE_90_DAYS "$platega_price_90_days"
      
      echo "[+] Platega настроен."
    else
      echo "[*] Platega пропущен."
      set_env_value PLATEGA_MERCHANT_ID ""
      set_env_value PLATEGA_SECRET_KEY ""
    fi
  elif [[ "$choice" == "1" ]]; then
    # Автоматическая установка (не reinstall): запрос Telegram токена и Admin ID перед настройкой AWG
    api_token=""
    admin_id=""
    server_name=""
    
    prompt_api_token api_token
    prompt_admin_id admin_id
    
    # Запрос имени сервера
    prompt_server_name server_name
    if [[ -z "$server_name" ]]; then
      server_name="My VPN"
    fi
    
    write_detected_awg_env
    if [[ -z "$(get_env_value SERVER_PUBLIC_KEY)" ]]; then
      warn "Не удалось автоматически определить SERVER_PUBLIC_KEY. Нужен один ручной шаг."
      prompt_with_default 'SERVER_PUBLIC_KEY' "$DETECTED_PUBLIC_KEY" value
      set_env_value SERVER_PUBLIC_KEY "$value"
    fi
    if [[ -z "$(get_env_value SERVER_IP)" ]]; then
      warn "Не удалось автоматически определить SERVER_IP. Укажи внешний IP и порт."
      default="$(pick_existing_or_default "$(get_env_value PUBLIC_HOST)" "$DETECTED_PUBLIC_HOST")"
      prompt_with_default 'PUBLIC_HOST / внешний IP' "$default" value
      set_env_value PUBLIC_HOST "$value"
      if [[ -n "$DETECTED_LISTEN_PORT" && -n "$value" ]]; then
        set_env_value SERVER_IP "${value}:${DETECTED_LISTEN_PORT}"
      else
        prompt_with_default 'SERVER_IP (IP:port)' "$DETECTED_SERVER_IP" value
        set_env_value SERVER_IP "$value"
      fi
    fi
    
    # Запись основных параметров после сбора всех данных
    secret="$(ensure_secret)"
    write_common_env "$api_token" "$admin_id" "$server_name" "$secret"
    
    # Настройка Platega даже в автоматическом режиме
    echo ""
    echo "--- Настройка Platega (опционально, нажмите Enter для пропуска) ---"
    
    platega_merchant_id=""
    default="$(pick_existing_or_default "$(get_env_value PLATEGA_MERCHANT_ID)" "")"
    prompt_with_default 'Platega Merchant ID' "$default" platega_merchant_id
    
    if [[ -n "$platega_merchant_id" ]]; then
      set_env_value PLATEGA_MERCHANT_ID "$platega_merchant_id"
      
      platega_secret_key=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_SECRET_KEY)" "")"
      prompt_with_default 'Platega Secret Key' "$default" platega_secret_key
      set_env_value PLATEGA_SECRET_KEY "$platega_secret_key"
      
      # Запрос домена для webhook
      platega_domain=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_DOMAIN)" "")"
      prompt_with_default 'Домен для webhook Platega (например: example.com)' "$default" platega_domain
      
      if [[ -n "$platega_domain" ]]; then
        set_env_value PLATEGA_WEBHOOK_DOMAIN "$platega_domain"
        set_env_value PLATEGA_WEBHOOK_PORT "443"
        
        # Автоматическая настройка Nginx и SSL
        setup_nginx_and_ssl "$platega_domain"
      else
        # Если домен не указан, используем старый режим с IP:порт
        platega_webhook_port=""
        default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_PORT)" "8081")"
        prompt_with_default 'Порт webhook Platega' "$default" platega_webhook_port
        set_env_value PLATEGA_WEBHOOK_PORT "$platega_webhook_port"
      fi
      
      platega_price_7_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_7_DAYS)" "100")"
      prompt_with_default 'Цена 7 дней через Platega (руб)' "$default" platega_price_7_days
      set_env_value PLATEGA_PRICE_7_DAYS "$platega_price_7_days"
      
      platega_price_30_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_30_DAYS)" "250")"
      prompt_with_default 'Цена 30 дней через Platega (руб)' "$default" platega_price_30_days
      set_env_value PLATEGA_PRICE_30_DAYS "$platega_price_30_days"
      
      platega_price_90_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_90_DAYS)" "700")"
      prompt_with_default 'Цена 90 дней через Platega (руб)' "$default" platega_price_90_days
      set_env_value PLATEGA_PRICE_90_DAYS "$platega_price_90_days"
      
      echo "[+] Platega настроен."
    else
      echo "[*] Platega пропущен."
      set_env_value PLATEGA_MERCHANT_ID ""
      set_env_value PLATEGA_SECRET_KEY ""
    fi
  else
    # Ручная установка или ручная переустановка (choice=2 при install, choice=3 при reinstall)
    configure_manual_awg_only
    default="$(pick_existing_or_default "$(get_env_value STARS_PRICE_7_DAYS)" "21")"
    prompt_with_default 'Цена 7 дней в Telegram Stars' "$default" value
    set_env_value STARS_PRICE_7_DAYS "$value"
    default="$(pick_existing_or_default "$(get_env_value STARS_PRICE_30_DAYS)" "50")"
    prompt_with_default 'Цена 30 дней в Telegram Stars' "$default" value
    set_env_value STARS_PRICE_30_DAYS "$value"
    
    echo ""
    echo "--- Настройка цен Platega (руб) ---"
    
    platega_merchant_id=""
    default="$(pick_existing_or_default "$(get_env_value PLATEGA_MERCHANT_ID)" "")"
    prompt_with_default 'Platega Merchant ID' "$default" platega_merchant_id
    
    if [[ -n "$platega_merchant_id" ]]; then
      set_env_value PLATEGA_MERCHANT_ID "$platega_merchant_id"
      
      platega_secret_key=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_SECRET_KEY)" "")"
      prompt_with_default 'Platega Secret Key' "$default" platega_secret_key
      set_env_value PLATEGA_SECRET_KEY "$platega_secret_key"
      
      # Запрос домена для webhook (во второй ветке тоже)
      platega_domain=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_DOMAIN)" "")"
      prompt_with_default 'Домен для webhook Platega (например: example.com)' "$default" platega_domain
      
      if [[ -n "$platega_domain" ]]; then
        set_env_value PLATEGA_WEBHOOK_DOMAIN "$platega_domain"
        set_env_value PLATEGA_WEBHOOK_PORT "443"
        
        # Автоматическая настройка Nginx и SSL
        setup_nginx_and_ssl "$platega_domain"
      else
        # Если домен не указан, используем старый режим с IP:порт
        platega_webhook_port=""
        default="$(pick_existing_or_default "$(get_env_value PLATEGA_WEBHOOK_PORT)" "8081")"
        prompt_with_default 'Порт webhook Platega' "$default" platega_webhook_port
        set_env_value PLATEGA_WEBHOOK_PORT "$platega_webhook_port"
      fi
      
      platega_price_7_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_7_DAYS)" "100")"
      prompt_with_default 'Цена 7 дней через Platega (руб)' "$default" platega_price_7_days
      set_env_value PLATEGA_PRICE_7_DAYS "$platega_price_7_days"
      
      platega_price_30_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_30_DAYS)" "250")"
      prompt_with_default 'Цена 30 дней через Platega (руб)' "$default" platega_price_30_days
      set_env_value PLATEGA_PRICE_30_DAYS "$platega_price_30_days"
      
      platega_price_90_days=""
      default="$(pick_existing_or_default "$(get_env_value PLATEGA_PRICE_90_DAYS)" "700")"
      prompt_with_default 'Цена 90 дней через Platega (руб)' "$default" platega_price_90_days
      set_env_value PLATEGA_PRICE_90_DAYS "$platega_price_90_days"
      
      echo "[+] Platega настроен."
    else
      echo "[*] Platega пропущен."
      set_env_value PLATEGA_MERCHANT_ID ""
      set_env_value PLATEGA_SECRET_KEY ""
    fi
    
    default="$(pick_existing_or_default "$(get_env_value DOWNLOAD_URL)" "https://github.com/amnezia-vpn/amnezia-client/releases/latest")"
    prompt_with_default 'Ссылка на Amnezia / инструкцию скачивания' "$default" value
    set_env_value DOWNLOAD_URL "$value"
    default="$(get_env_value SUPPORT_USERNAME)"
    prompt_with_default 'Username поддержки (можно @username)' "${default:-@support}" value
    set_env_value SUPPORT_USERNAME "$value"
    default="$(pick_existing_or_default "$(get_env_value EGRESS_DENYLIST_ENABLED)" "$SELFHOST_EGRESS_DENYLIST_ENABLED_DEFAULT")"
    prompt_with_default 'Включить egress denylist (1=ON,0=OFF)' "$default" value
    set_env_value EGRESS_DENYLIST_ENABLED "$value"
    default="$(pick_existing_or_default "$(get_env_value EGRESS_DENYLIST_MODE)" "$SELFHOST_EGRESS_DENYLIST_MODE_DEFAULT")"
    prompt_with_default 'Режим denylist (soft/strict)' "$default" value
    set_env_value EGRESS_DENYLIST_MODE "$value"
    default="$(pick_existing_or_default "$(get_env_value EGRESS_DENYLIST_REFRESH_MINUTES)" "$SELFHOST_EGRESS_DENYLIST_REFRESH_MINUTES_DEFAULT")"
    prompt_with_default 'Интервал обновления denylist (мин)' "$default" value
    set_env_value EGRESS_DENYLIST_REFRESH_MINUTES "$value"
    default="$(pick_existing_or_default "$(get_env_value AUTO_BACKUP_ENABLED)" "$SELFHOST_AUTO_BACKUP_ENABLED_DEFAULT")"
    prompt_with_default 'Включить autobackup (1=ON,0=OFF)' "$default" value
    set_env_value AUTO_BACKUP_ENABLED "$value"
    default="$(pick_existing_or_default "$(get_env_value AUTO_BACKUP_KEEP_COUNT)" "$SELFHOST_AUTO_BACKUP_KEEP_COUNT_DEFAULT")"
    prompt_with_default 'Сколько autobackup хранить (шт)' "$default" value
    set_env_value AUTO_BACKUP_KEEP_COUNT "$value"
  fi

  # Валидация критических переменных после заполнения всех полей .env
  validate_critical_env || die "Валидация критических переменных не пройдена. Проверьте .env файл."

  ensure_venv_and_requirements || die "Не удалось установить Python зависимости."
  ensure_bot_user || die "Не удалось подготовить service пользователя."
  install_awg_helper || die "Не удалось установить helper для AWG."
  setup_logrotate || die "Не удалось настроить logrotate."
  write_service || die "Не удалось создать systemd сервис."
  configure_autobackup_timer || die "Не удалось настроить systemd timer autobackup."
  configure_platega_webhook || warn "Не удалось настроить Platega webhook сервис."
  # SSL таймер устанавливается ПОСЛЕ успешного smokecheck (см. ниже)
  persist_repo_branch
  persist_release_sha "$deploy_sha"
  if [[ "$mode" == "reinstall" ]]; then
    IFS=$'\t' read -r pre_reinstall_log_pending pre_reinstall_log_final < <(prepare_bot_log_for_reinstall)
    register_reinstall_guard_pending_log "$pre_reinstall_log_pending"
    if ! start_service; then
      restore_bot_log_after_failed_reinstall "$pre_reinstall_log_pending"
      die "Не удалось запустить сервис."
    fi
  else
    start_service || die "Не удалось запустить сервис."
  fi
  if run_post_restart_smokecheck; then
    if [[ "$mode" == "reinstall" ]]; then
      clear_reinstall_guard
      pre_reinstall_log_archive="$(finalize_bot_log_reinstall_archive "$pre_reinstall_log_pending" "$pre_reinstall_log_final")"
      ok "Smokecheck после переустановки пройден."
      if [[ -n "$pre_reinstall_log_archive" ]]; then
        info "bot.log очищен для новой версии; предыдущий лог сохранён в ${pre_reinstall_log_archive}"
      fi
    else
      ok "Smokecheck после установки пройден."
    fi
    # Установка certbot-renewal.timer ПОСЛЕ успешного smokecheck
    install_certbot_renewal_timer || warn "Не удалось установить certbot-renewal.timer (SSL не будет обновляться автоматически)."
  else
    if [[ "$mode" == "reinstall" ]]; then
      clear_reinstall_guard
      rollback_failed_reinstall "$pre_reinstall_repo_snapshot" "$pre_reinstall_runtime_snapshot" "$pre_reinstall_log_pending"
      die "Переустановка не прошла smokecheck. Выполнен rollback к предыдущему рабочему состоянию."
    else
      die "Установка не прошла post-start smokecheck. Проверь логи и диагностику."
    fi
  fi
  ok "Готово. Бот установлен/переустановлен."
  show_status
  echo "Быстрый запуск меню потом: sudo bash ${INSTALL_DIR}/awg-tgbot.sh"
  echo "Или коротко: sudo awg-tgbot"
  return 0
}

get_bot_db_file() {
  local db_path
  db_path="$(get_env_value DB_PATH)"
  resolve_db_file_from_db_path "$db_path"
}

repair_runtime_file_access() {
  local target_path="$1"
  local mode="$2"
  [[ -f "$target_path" ]] || return 0
  chown "$BOT_USER:$BOT_USER" "$target_path" 2>/dev/null || true
  chmod "$mode" "$target_path" 2>/dev/null || true
}

create_runtime_snapshot_before_reinstall() {
  local snapshot_label="$1"
  local db_file snapshot_dir service_active="unknown" service_enabled="unknown"
  db_file="$(get_bot_db_file)"
  snapshot_dir="$(mktemp -d "${SAFETY_SNAPSHOT_PREFIX}-${snapshot_label}-$(date -u +%Y%m%d_%H%M%S)-XXXXXX")"
  chmod 700 "$snapshot_dir" || true
  if [[ -f "$db_file" ]]; then
    snapshot_sqlite_runtime_bundle "$db_file" "$snapshot_dir" "db.before" || return 1
  fi
  if [[ -f "$ENV_FILE" ]]; then
    cp -a "$ENV_FILE" "$snapshot_dir/.env.before"
  fi
  if [[ -f "$VERSION_FILE" ]]; then
    cp -a "$VERSION_FILE" "$snapshot_dir/version_file.before"
  fi
  if [[ -f "$REPO_BRANCH_FILE" ]]; then
    cp -a "$REPO_BRANCH_FILE" "$snapshot_dir/repo_branch.before"
  fi
  if service_exists && require_command systemctl; then
    service_active="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    service_enabled="$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
  fi
  printf 'service_active=%s\nservice_enabled=%s\n' "${service_active:-unknown}" "${service_enabled:-unknown}" > "$snapshot_dir/service_state.before"
  printf '%s' "$snapshot_dir"
}

create_repo_snapshot_before_reinstall() {
  local snapshot_dir
  snapshot_dir="$(mktemp -d "${SAFETY_SNAPSHOT_PREFIX}-repo-$(date -u +%Y%m%d_%H%M%S)-XXXXXX")"
  chmod 700 "$snapshot_dir" || true
  [[ -d "$BOT_DIR" ]] && cp -a "$BOT_DIR" "$snapshot_dir/bot"
  [[ -f "$INSTALL_DIR/awg-tgbot.sh" ]] && cp -a "$INSTALL_DIR/awg-tgbot.sh" "$snapshot_dir/awg-tgbot.sh"
  [[ -d "$INSTALL_DIR/scripts" ]] && cp -a "$INSTALL_DIR/scripts" "$snapshot_dir/scripts"
  [[ -d "$INSTALL_DIR/packaging" ]] && cp -a "$INSTALL_DIR/packaging" "$snapshot_dir/packaging"
  printf '%s' "$snapshot_dir"
}

restore_repo_snapshot_after_failed_reinstall() {
  local repo_snapshot_dir="$1"
  [[ -d "$repo_snapshot_dir" ]] || return 1
  rm -rf "$BOT_DIR" "$INSTALL_DIR/scripts" "$INSTALL_DIR/packaging"
  rm -f "$INSTALL_DIR/awg-tgbot.sh"
  [[ -d "$repo_snapshot_dir/bot" ]] && cp -a "$repo_snapshot_dir/bot" "$BOT_DIR"
  [[ -f "$repo_snapshot_dir/awg-tgbot.sh" ]] && cp -a "$repo_snapshot_dir/awg-tgbot.sh" "$INSTALL_DIR/awg-tgbot.sh"
  [[ -d "$repo_snapshot_dir/scripts" ]] && cp -a "$repo_snapshot_dir/scripts" "$INSTALL_DIR/scripts"
  [[ -d "$repo_snapshot_dir/packaging" ]] && cp -a "$repo_snapshot_dir/packaging" "$INSTALL_DIR/packaging"
  [[ -f "$INSTALL_DIR/awg-tgbot.sh" ]] && chmod +x "$INSTALL_DIR/awg-tgbot.sh" || true
  [[ -f "$AUTO_BACKUP_SCRIPT" ]] && chmod +x "$AUTO_BACKUP_SCRIPT" || true
  ln -sfn "$INSTALL_DIR/awg-tgbot.sh" "$SELF_SYMLINK" || true
  return 0
}

run_post_restart_smokecheck() {
  local failed=0 env_container env_interface policy_container policy_interface policy_error db_result runtime_python awg_check_output awg_check_rc=0

  if ! service_exists || [[ "$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)" != "active" ]]; then
    warn "Smokecheck: сервис ${SERVICE_NAME} не в состоянии active."
    failed=1
  fi

  env_container="$(get_env_value DOCKER_CONTAINER)"
  env_interface="$(get_env_value WG_INTERFACE)"
  IFS=$'\t' read -r policy_container policy_interface policy_error < <(read_helper_policy_state)
  if [[ -n "$policy_error" ]]; then
    warn "Smokecheck: helper policy недоступна/невалидна: ${policy_error}"
    failed=1
  fi
  if [[ -z "$env_container" || -z "$env_interface" ]]; then
    warn "Smokecheck: DOCKER_CONTAINER/WG_INTERFACE не заданы в .env."
    failed=1
  fi
  if [[ -n "$env_container" && -n "$env_interface" && -z "$policy_error" ]] && [[ "$policy_container" != "$env_container" || "$policy_interface" != "$env_interface" ]]; then
    warn "Smokecheck: helper policy не совпадает с .env (${policy_container}/${policy_interface} != ${env_container}/${env_interface})."
    failed=1
  fi

  if [[ ! -x "$AWG_HELPER_TARGET" ]]; then
    warn "Проверка после перезапуска: helper ${AWG_HELPER_TARGET} не найден/не исполняемый."
    failed=1
  elif [[ -n "$policy_error" ]]; then
    warn "Проверка после перезапуска: check-awg пропущен из-за ошибки helper policy."
    failed=1
  else
    set +e
    awg_check_output="$("$AWG_HELPER_TARGET" check-awg 2>&1)"
    awg_check_rc=$?
    set -e
    if [[ "$awg_check_rc" -ne 0 ]]; then
      warn "Проверка после перезапуска: AWG check-awg завершился ошибкой (rc=${awg_check_rc}, output=${awg_check_output:-no-output})."
      failed=1
    fi
  fi

  runtime_python="$PYTHON_BIN"
  [[ -x "${VENV_DIR}/bin/python" ]] && runtime_python="${VENV_DIR}/bin/python"

  # Retry loop для проверки БД: даём боту время на инициализацию схемы
  local max_attempts=5 attempt_delay=2 db_result="" attempt=0
  while (( attempt < max_attempts )); do
    attempt=$((attempt + 1))
    db_result="$( "$runtime_python" - "$BOT_DIR" "$ENV_FILE" <<'PY' 2>/dev/null || true
import asyncio
import os
import sys
from dotenv import load_dotenv

bot_dir, env_file = sys.argv[1], sys.argv[2]
install_dir = os.path.dirname(bot_dir)
os.chdir(install_dir)
load_dotenv(env_file, override=True)
sys.path.insert(0, bot_dir)
from config import DB_PATH  # noqa: E402
from database import db_health_info  # noqa: E402

info = asyncio.run(db_health_info())
if not info.get("schema_ready"):
    print(f"schema_not_ready:path={DB_PATH}")
    raise SystemExit(1)
if not info.get("runtime_ready"):
    integrity = info.get("instance_integrity") if isinstance(info.get("instance_integrity"), dict) else {}
    issues = integrity.get("issues") if isinstance(integrity.get("issues"), list) else []
    suffix = "; ".join(str(item) for item in issues if str(item).strip()) or "no_integrity_details"
    print(f"runtime_not_ready:path={DB_PATH}:{suffix}")
    raise SystemExit(1)
print("runtime_ready")
PY
)"
    # Проверяем результат
    if [[ "$db_result" == "runtime_ready" ]]; then
      break
    fi
    # Если не готова и это не последняя попытка — ждём и пробуем снова
    if (( attempt < max_attempts )); then
      sleep "$attempt_delay"
    fi
  done

  if [[ "$db_result" == schema_not_ready:path=* ]]; then
    local schema_db_path="${db_result#schema_not_ready:path=}"
    warn "Проверка после перезапуска: проверка БД не пройдена (schema_ready=false, path=${schema_db_path})."
    failed=1
  elif [[ "$db_result" == "schema_not_ready" ]]; then
    warn "Проверка после перезапуска: проверка БД не пройдена (schema_ready=false)."
    failed=1
  elif [[ "$db_result" == runtime_not_ready:path=*:* ]]; then
    local runtime_payload="${db_result#runtime_not_ready:path=}"
    local runtime_db_path="${runtime_payload%%:*}"
    local runtime_suffix="${runtime_payload#*:}"
    warn "Проверка после перезапуска: проверка БД не пройдена (runtime_ready=false, path=${runtime_db_path}, ${runtime_suffix})."
    failed=1
  elif [[ "$db_result" == runtime_not_ready:* ]]; then
    warn "Проверка после перезапуска: проверка БД не пройдена (runtime_ready=false, ${db_result#runtime_not_ready:})."
    failed=1
  elif [[ "$db_result" != "runtime_ready" ]]; then
    warn "Проверка после перезапуска: проверка БД не пройдена (${db_result:-unknown})."
    failed=1
  fi

  [[ "$failed" == "0" ]]
}

restore_service_state_from_snapshot() {
  local runtime_snapshot_dir="$1"
  local state_file="${runtime_snapshot_dir}/service_state.before"
  local service_active_before="" service_enabled_before=""
  [[ -f "$state_file" ]] || return 0
  service_active_before="$(get_env_value_from_file "$state_file" service_active)"
  service_enabled_before="$(get_env_value_from_file "$state_file" service_enabled)"

  if require_command systemctl && service_exists; then
    if [[ "$service_enabled_before" == "enabled" ]]; then
      systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
    elif [[ "$service_enabled_before" == "disabled" ]]; then
      systemctl disable "$SERVICE_NAME" >/dev/null 2>&1 || true
    fi

    case "$service_active_before" in
      active)
        start_service || true
        ;;
      inactive|failed)
        systemctl stop "$SERVICE_NAME" 2>/dev/null || true
        ;;
      *)
        ;;
    esac
  fi
  return 0
}

rollback_failed_reinstall() {
  local repo_snapshot_dir="$1" runtime_snapshot_dir="$2" pending_log_archive="$3"
  local db_file restore_repo_ok=0 restore_runtime_ok=0 restore_db_ok=0 restore_env_ok=0 restore_meta_ok=0 deps_ok=0 helper_sync_ok=0 post_rollback_smoke_ok=0
  local service_state_restore_ok=0
  db_file="$(get_bot_db_file)"
  warn "Переустановка завершилась с ошибкой smokecheck. Выполняю аварийный rollback."
  warn "Rollback: останавливаю текущий неудачный сервис перед восстановлением runtime."
  if service_exists; then
    systemctl stop "$SERVICE_NAME" 2>/dev/null || true
    sleep 1
  fi
  restore_bot_log_after_failed_reinstall "$pending_log_archive"

  if restore_repo_snapshot_after_failed_reinstall "$repo_snapshot_dir"; then
    restore_repo_ok=1
  else
    warn "Rollback: не удалось восстановить файлы репозитория из ${repo_snapshot_dir}."
  fi

  if [[ -f "$runtime_snapshot_dir/db.before" ]]; then
    if restore_sqlite_runtime_bundle "$runtime_snapshot_dir/db.before" "$db_file"; then
      restore_db_ok=1
    fi
  else
    restore_db_ok=1
  fi
  if [[ -f "$runtime_snapshot_dir/.env.before" ]]; then
    if install -m 600 "$runtime_snapshot_dir/.env.before" "$ENV_FILE"; then
      repair_runtime_file_access "$ENV_FILE" 600
      restore_env_ok=1
    fi
  else
    restore_env_ok=1
  fi
  if [[ "$restore_db_ok" == "1" && "$restore_env_ok" == "1" ]]; then
    restore_runtime_ok=1
  fi

  if [[ -f "$runtime_snapshot_dir/version_file.before" ]]; then
    if install -m 600 "$runtime_snapshot_dir/version_file.before" "$VERSION_FILE"; then
      restore_meta_ok=1
    fi
  else
    rm -f "$VERSION_FILE" || true
    restore_meta_ok=1
  fi
  if [[ -f "$runtime_snapshot_dir/repo_branch.before" ]]; then
    if install -m 600 "$runtime_snapshot_dir/repo_branch.before" "$REPO_BRANCH_FILE"; then
      restore_meta_ok=1
    fi
  fi

  if ensure_venv_and_requirements; then
    deps_ok=1
  else
    warn "Rollback: не удалось переустановить зависимости для восстановленного requirements.txt."
  fi

  if sync_awg_helper_policy_from_env; then
    helper_sync_ok=1
  else
    warn "Rollback: не удалось синхронизировать helper policy из восстановленного .env."
  fi
  restore_service_state_from_snapshot "$runtime_snapshot_dir"
  service_state_restore_ok=1
  if require_command systemctl && service_exists && [[ "$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)" == "active" ]]; then
    if run_post_restart_smokecheck; then
      post_rollback_smoke_ok=1
      warn "Rollback выполнен: предыдущий runtime снова проходит проверку после перезапуска."
    else
      warn "Rollback выполнен частично: сервис после rollback всё ещё не проходит проверку после перезапуска."
    fi
  else
    post_rollback_smoke_ok=1
    warn "Rollback: сервис восстановлен в неактивное состояние (как до reinstall), post-rollback smokecheck пропущен."
  fi

  if [[ "$restore_repo_ok" == "1" && "$restore_runtime_ok" == "1" && "$restore_meta_ok" == "1" && "$deps_ok" == "1" && "$helper_sync_ok" == "1" && "$service_state_restore_ok" == "1" && "$post_rollback_smoke_ok" == "1" ]]; then
    warn "Переустановка не удалась; rollback выполнен полностью (код+DB+.env+state+deps+service-state), helper policy: ok."
  else
    warn "Переустановка не удалась; rollback выполнен частично (repo=${restore_repo_ok}, runtime=${restore_runtime_ok}, state=${restore_meta_ok}, deps=${deps_ok}, helper policy=${helper_sync_ok}, service_state=${service_state_restore_ok}, post-rollback проверка=${post_rollback_smoke_ok})."
  fi
}

create_local_backup() {
  local db_file db_basename timestamp archive_file meta_dir meta_file local_sha
  local snapshot_dir service_active_before service_was_active=0 service_state_after_start=""
  local -a db_bundle_names=()
  
  # Проверяем наличие БД и .env, даже если бот не полностью установлен
  # Это позволяет сделать бэкап перед переустановкой или на новом сервере
  if [[ ! -f "$ENV_FILE" ]]; then
    warn "Файл .env не найден: ${ENV_FILE}"
    return 1
  fi
  
  db_file="$(get_bot_db_file)"
  if [[ ! -f "$db_file" ]]; then
    warn "Файл БД не найден: ${db_file}"
    return 1
  fi
  db_basename="$(basename "$db_file")"

  timestamp="$(date -u +%Y%m%d_%H%M%S)"
  archive_file="${BACKUP_ROOT}/awg-tgbot-backup-${timestamp}.tar.gz"
  meta_dir="$(mktemp -d)"
  snapshot_dir="$(mktemp -d)"
  meta_file="${meta_dir}/metadata.txt"
  mkdir -p "$BACKUP_ROOT"
  chmod 700 "$BACKUP_ROOT" || true

  if require_command systemctl && service_exists; then
    service_active_before="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    if [[ "$service_active_before" == "active" ]]; then
      warn "Локальный бэкап: временно останавливаю ${SERVICE_NAME} для консистентного snapshot SQLite."
      if ! systemctl stop "$SERVICE_NAME" 2>/dev/null; then
        rm -rf "$meta_dir" "$snapshot_dir"
        warn "Не удалось остановить ${SERVICE_NAME}; бэкап отменён (fail-closed для active service)."
        return 1
      fi
      if ! wait_for_service_stopped_state; then
        service_active_before="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        rm -rf "$meta_dir" "$snapshot_dir"
        warn "Сервис ${SERVICE_NAME} не перешёл в безопасное stopped-state после stop (state=${service_active_before:-unknown}); бэкап отменён (fail-closed)."
        return 1
      fi
      service_was_active=1
    elif [[ "$service_active_before" != "inactive" && "$service_active_before" != "failed" ]]; then
      rm -rf "$meta_dir" "$snapshot_dir"
      warn "Бэкап отменён: ${SERVICE_NAME} в переходном/небезопасном состоянии (${service_active_before:-unknown}). Дождитесь stable inactive/failed или active."
      return 1
    fi
  fi

  if ! snapshot_sqlite_runtime_bundle "$db_file" "$snapshot_dir" "$db_basename"; then
    rm -rf "$meta_dir" "$snapshot_dir"
    warn "Не удалось подготовить консистентный snapshot SQLite для бэкапа: ${db_file}"
    if [[ "$service_was_active" == "1" ]] && require_command systemctl && service_exists; then
      if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки snapshot."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
      if ! wait_for_service_active_state; then
        service_state_after_start="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки snapshot (state=${service_state_after_start:-unknown})."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
    fi
    return 1
  fi
  mapfile -t db_bundle_names < <(collect_existing_sqlite_bundle_basenames "$snapshot_dir/$db_basename")
  if [[ ${#db_bundle_names[@]} -eq 0 ]]; then
    rm -rf "$meta_dir" "$snapshot_dir"
    warn "Не удалось собрать SQLite bundle для бэкапа: ${db_file}"
    if [[ "$service_was_active" == "1" ]] && require_command systemctl && service_exists; then
      if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки bundle-сборки."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
      if ! wait_for_service_active_state; then
        service_state_after_start="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки bundle-сборки (state=${service_state_after_start:-unknown})."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
    fi
    return 1
  fi

  local_sha="$(cat "$VERSION_FILE" 2>/dev/null | tr -d '\r\n' || true)"
  if [[ -z "$local_sha" && -d "$INSTALL_DIR/.git" ]]; then
    local_sha="$(git -C "$INSTALL_DIR" rev-parse --short HEAD 2>/dev/null || true)"
  fi
  [[ -n "$local_sha" ]] || local_sha="unknown"

  cat > "$meta_file" <<EOF
created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
repo_branch=${REPO_BRANCH}
local_sha=${local_sha}
db_file=$(basename "$db_file")
db_bundle=$(IFS=,; echo "${db_bundle_names[*]}")
env_file=.env
EOF

  if ! tar -czf "$archive_file" \
    -C "$snapshot_dir" "${db_bundle_names[@]}" \
    -C "$INSTALL_DIR" ".env" \
    -C "$meta_dir" "metadata.txt"; then
    rm -rf "$meta_dir" "$snapshot_dir"
    rm -f "$archive_file"
    warn "Не удалось создать архив бэкапа."
    if [[ "$service_was_active" == "1" ]] && require_command systemctl && service_exists; then
      if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки архивации."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
      if ! wait_for_service_active_state; then
        service_state_after_start="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки архивации (state=${service_state_after_start:-unknown})."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
    fi
    return 1
  fi

  rm -rf "$meta_dir" "$snapshot_dir"
  chmod 600 "$archive_file" || true
  if ! validate_backup_archive_payload "$archive_file" "$db_basename"; then
    rm -f "$archive_file"
    warn "Архив бэкапа не прошёл валидацию payload (db/.env/metadata)."
    if [[ "$service_was_active" == "1" ]] && require_command systemctl && service_exists; then
      if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки валидации архива."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
      if ! wait_for_service_active_state; then
        service_state_after_start="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        warn "Не удалось автоматически вернуть ${SERVICE_NAME} после ошибки валидации архива (state=${service_state_after_start:-unknown})."
        warn "Бот может остаться остановленным. Проверьте вручную: systemctl status ${SERVICE_NAME}"
        return 1
      fi
    fi
    return 1
  fi

  if [[ "$service_was_active" == "1" ]] && require_command systemctl && service_exists; then
    if ! systemctl start "$SERVICE_NAME" 2>/dev/null; then
      warn "Архив создан: ${archive_file}"
      warn "Не удалось автоматически запустить ${SERVICE_NAME} после backup. Проверьте вручную: systemctl status ${SERVICE_NAME}"
      return 1
    fi
    if ! wait_for_service_active_state; then
      service_state_after_start="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
      warn "Архив создан: ${archive_file}"
      warn "Сервис ${SERVICE_NAME} не восстановлен автоматически (state=${service_state_after_start:-unknown}). Проверьте: systemctl status ${SERVICE_NAME}"
      return 1
    fi
  fi
  ok "Бэкап сохранён: ${archive_file}"
  return 0
}

restore_from_backup() {
  local backup_root="${BACKUP_ROOT}"
  local selected_index="" selected_archive="" confirm_restore=""
  local original_db_file payload_db_basename meta_content snapshot_dir tmp_restore service_active_before service_enabled_before service_state_after_stop=""
  local archive_env_file archive_db_path="" archive_db_file="" archive_db_basename=""
  local restore_target_db_file="" rollback_target_db_file="" rollback_cleanup_db_file=""
  local -a archive_entries=() restore_members=()
  local restore_ok=0 rollback_ok=0 smokecheck_ok=0 helper_sync_ok=0 rollback_helper_sync_ok=0 rollback_smoke_ok=0 restore_blocked=0
  local restored_bundle_written=0
  
  # Создаём директорию для бэкапов, если она не существует
  mkdir -p "$backup_root"
  chmod 755 "$backup_root" || true
  
  # Проверяем наличие бэкапов, даже если бот не установлен
  mapfile -t archives < <(find "$backup_root" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' | sort -r 2>/dev/null || true)
  
  # Если бэкапы не найдены, предлагаем загрузить их вручную
  if [[ ${#archives[@]} -eq 0 ]]; then
    warn "Локальные бэкапы не найдены: ${backup_root}"
    echo ""
    echo "Если у вас есть бэкап с другого сервера:"
    echo "1) Скопируйте файл бэкапа в директорию: ${backup_root}"
    echo "   Пример: scp user@old-server:/opt/amnezia/bot/backups/awg-tgbot-backup-*.tar.gz ${backup_root}/"
    echo "2) Запустите восстановление снова"
    echo ""
    echo "Ожидаемый формат имени файла: awg-tgbot-backup-YYYYMMDD_HHMMSS.tar.gz"
    return 1
  fi

  print_line
  echo "Доступные бэкапы:"
  local i=1
  for archive in "${archives[@]}"; do
    echo "${i}) $(basename "$archive")"
    i=$((i + 1))
  done
  print_line
  prompt_raw "Выберите номер бэкапа: " selected_index
  if [[ ! "$selected_index" =~ ^[0-9]+$ ]] || (( selected_index < 1 || selected_index > ${#archives[@]} )); then
    warn "Некорректный выбор."
    return 1
  fi
  selected_archive="${archives[$((selected_index - 1))]}"
  # Определяем original_db_file, даже если бот не установлен
  original_db_file="$(get_bot_db_file)"
  rollback_target_db_file="$original_db_file"
  
  # Сначала извлекаем metadata и .env чтобы определить правильный db_file из бэкапа
  tmp_restore="$(mktemp -d)"
  if ! tar -xzf "$selected_archive" -C "$tmp_restore" ".env" "metadata.txt" 2>/dev/null; then
    warn "Не удалось извлечь .env и metadata.txt из архива."
    return 1
  fi
  archive_env_file="$tmp_restore/.env"
  
  # Определяем payload_db_basename из metadata.txt бэкапа
  meta_content="$(cat "$tmp_restore/metadata.txt" 2>/dev/null || true)"
  if [[ -n "$meta_content" ]]; then
    payload_db_basename="$(printf '%s\n' "$meta_content" | awk -F= '/^db_file=/{print $2}' | tail -n1)"
  fi
  [[ -n "$payload_db_basename" ]] || payload_db_basename="$(basename "$original_db_file")"
  
  mapfile -t archive_entries < <(tar -tzf "$selected_archive" 2>/dev/null || true)
  if [[ ${#archive_entries[@]} -eq 0 ]]; then
    warn "Архив повреждён или пуст: $(basename "$selected_archive")"
    rm -rf "$tmp_restore"
    return 1
  fi
  
  restore_members=("$payload_db_basename")
  if printf '%s\n' "${archive_entries[@]}" | grep -Fxq "${payload_db_basename}-wal"; then
    restore_members+=("${payload_db_basename}-wal")
  fi
  if printf '%s\n' "${archive_entries[@]}" | grep -Fxq "${payload_db_basename}-shm"; then
    restore_members+=("${payload_db_basename}-shm")
  fi
  if ! printf '%s\n' "${archive_entries[@]}" | grep -Fxq ".env"; then
    warn "Архив не содержит .env: $(basename "$selected_archive")"
    return 1
  fi
  if ! printf '%s\n' "${archive_entries[@]}" | grep -Fxq "$payload_db_basename"; then
    warn "Архив не содержит основной SQLite файл ${payload_db_basename}."
    return 1
  fi

  echo "Выбран архив: $(basename "$selected_archive")"
  echo "Будет восстановлено: ${payload_db_basename} (+sidecars при наличии), .env"
  if ! confirm_explicit "Продолжить восстановление?"; then
    warn "Восстановление отменено."
    rm -rf "$tmp_restore"
    return 1
  fi

  # archive_env_file уже извлечён выше, используем его для переопределения параметров

  # Сначала детектируем AWG параметры с текущего сервера (если возможно)
  local detected_container="" detected_interface="" detected_public_key="" detected_server_ip=""
  
  # Пробуем найти AWG контейнер и интерфейс через docker
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    # Ищем контейнер AmneziaWG
    detected_container="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -iE 'amnezia|awg|wg' | head -n1 || true)"
    
    if [[ -n "$detected_container" ]]; then
      # Получаем public key из контейнера
      detected_public_key="$(docker exec "$detected_container" awg show 2>/dev/null | grep -i 'public key' | awk -F: '{print $2}' | tr -d ' ' || true)"
      
      # Получаем интерфейс
      detected_interface="$(docker exec "$detected_container" awg show 2>/dev/null | grep -i '^interface:' | awk -F: '{print $2}' | tr -d ' ' || true)"
      [[ -z "$detected_interface" ]] && detected_interface="awg0"
      
      # Получаем порт и вычисляем SERVER_IP
      local listen_port
      listen_port="$(docker exec "$detected_container" awg show 2>/dev/null | grep -i 'listening port' | awk -F: '{print $2}' | tr -d ' ' || true)"
      if [[ -n "$listen_port" ]]; then
        # Пытаемся получить внешний IP
        local public_host
        public_host="$(curl -sS https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}' || true)"
        if [[ -n "$public_host" ]]; then
          detected_server_ip="${public_host}:${listen_port}"
        fi
      fi
    fi
  fi
  
  # Если не удалось получить через docker, пробуем из текущей .env если она есть
  if [[ -z "$detected_server_ip" && -f "$ENV_FILE" ]]; then
    detected_server_ip="$(get_env_value SERVER_IP)"
  fi
  if [[ -z "$detected_public_key" && -f "$ENV_FILE" ]]; then
    detected_public_key="$(get_env_value SERVER_PUBLIC_KEY)"
  fi
  if [[ -z "$detected_interface" && -f "$ENV_FILE" ]]; then
    detected_interface="$(get_env_value WG_INTERFACE)"
  fi
  if [[ -z "$detected_container" && -f "$ENV_FILE" ]]; then
    detected_container="$(get_env_value DOCKER_CONTAINER)"
  fi

  # Показываем текущие значения и предлагаем переопределить критические параметры
  echo ""
  echo "=== Переопределение параметров для нового сервера ==="
  echo ""
  
  # ============================================
  # ШАГ 1: Параметры Telegram бота и платежей (обязательно новые)
  # ============================================
  echo "--- Параметры Telegram бота и платежной системы ---"
  echo "Эти параметры нужно указать заново для текущего сервера:"
  echo ""
  
  local env_override="" new_value=""
  local -a manual_vars=("BOT_TOKEN" "PLATEGA_MERCHANT_ID" "PLATEGA_SECRET_KEY")
  
  for var in "${manual_vars[@]}"; do
    local current_value
    current_value="$(get_env_value_from_file "$archive_env_file" "$var")"
    if [[ -n "$current_value" ]]; then
      if [[ "$var" == "BOT_TOKEN" ]]; then
        echo "  Старое значение $var = ${current_value:0:10}...***REDACTED***"
      else
        echo "  Старое значение $var = ***REDACTED***"
      fi
    else
      echo "  $var = (не задано)"
    fi
    prompt_raw "Введите новое значение $var: " new_value
    while [[ -z "$new_value" ]]; do
      warn "$var не может быть пустым. Введите значение или нажмите Ctrl+C для отмены."
      prompt_raw "Введите новое значение $var: " new_value
    done
    
    local escaped_new
    escaped_new="$(printf '%s\n' "$new_value" | sed 's/[&/\]/\\&/g')"
    if [[ -n "$current_value" ]]; then
      sed -i "s/^${var}=.*/${var}=${escaped_new}/" "$archive_env_file"
    else
      echo "${var}=${escaped_new}" >> "$archive_env_file"
    fi
    env_override=1
    ok "$var обновлён"
  done
  
  # ============================================
  # ШАГ 1.5: ENCRYPTION_SECRET (новый + старый для миграции)
  # ============================================
  echo ""
  echo "--- Ключ шифрования данных (ENCRYPTION_SECRET) ---"
  local old_encryption_secret
  old_encryption_secret="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_SECRET")"
  
  if [[ -n "$old_encryption_secret" ]]; then
    echo "  Старый ENCRYPTION_SECRET = ${old_encryption_secret:0:8}...***REDACTED*** (${#old_encryption_secret} символов)"
    echo ""
    echo "Выберите действие:"
    echo "  1) Сгенерировать новый ENCRYPTION_SECRET (рекомендуется для нового сервера)"
    echo "     Старый ключ будет сохранён в ENCRYPTION_OLD_SECRETS для расшифровки существующих данных"
    echo "  2) Оставить старый ENCRYPTION_SECRET (если переносите бота на другой сервер с теми же данными)"
    echo "     Ключ шифрования не изменится, все данные расшифруются текущим ключом"
    echo ""
    local enc_choice=""
    prompt_raw "Ваш выбор (1 или 2): " enc_choice
    
    if [[ "$enc_choice" == "1" ]]; then
      # Генерируем новый ENCRYPTION_SECRET
      local new_encryption_secret
      new_encryption_secret="$(openssl rand -hex 32 2>/dev/null || python3 -c 'import secrets; print(secrets.token_hex(32))' || dd if=/dev/urandom bs=32 count=1 2>/dev/null | xxd -p)"
      
      if [[ -n "$new_encryption_secret" && ${#new_encryption_secret} -ge 64 ]]; then
        # Сохраняем старый в ENCRYPTION_OLD_SECRETS
        local existing_old_secrets
        existing_old_secrets="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_OLD_SECRETS")"
        
        if [[ -n "$existing_old_secrets" ]]; then
          # Добавляем к существующим, если ещё не добавлен
          if [[ "$existing_old_secrets" != *"$old_encryption_secret"* ]]; then
            local combined_old_secrets="${existing_old_secrets},${old_encryption_secret}"
            local escaped_combined
            escaped_combined="$(printf '%s\n' "$combined_old_secrets" | sed 's/[&/\]/\\&/g')"
            sed -i "s/^ENCRYPTION_OLD_SECRETS=.*/ENCRYPTION_OLD_SECRETS=${escaped_combined}/" "$archive_env_file"
            ok "Старый ключ добавлен к существующим ENCRYPTION_OLD_SECRETS"
          else
            echo "  Старый ключ уже присутствует в ENCRYPTION_OLD_SECRETS"
          fi
        else
          # Создаём новую запись
          local escaped_old
          escaped_old="$(printf '%s\n' "$old_encryption_secret" | sed 's/[&/\]/\\&/g')"
          echo "ENCRYPTION_OLD_SECRETS=${escaped_old}" >> "$archive_env_file"
          ok "ENCRYPTION_OLD_SECRETS создан со старым ключом"
        fi
        
        # Устанавливаем новый ENCRYPTION_SECRET
        local escaped_new_enc
        escaped_new_enc="$(printf '%s\n' "$new_encryption_secret" | sed 's/[&/\]/\\&/g')"
        sed -i "s/^ENCRYPTION_SECRET=.*/ENCRYPTION_SECRET=${escaped_new_enc}/" "$archive_env_file"
        
        ok "ENCRYPTION_SECRET обновлён (старый сохранён в ENCRYPTION_OLD_SECRETS)"
        env_override=1
      else
        warn "Не удалось сгенерировать новый ENCRYPTION_SECRET. Оставляем старый."
      fi
    else
      # Опция 2: Оставляем старый ENCRYPTION_SECRET
      # НЕ добавляем его в ENCRYPTION_OLD_SECRETS, так как он уже является активным
      echo "  Оставляем старый ENCRYPTION_SECRET без изменений"
      echo "  Ключ шифрования не изменён, существующие данные будут расшифрованы текущим ключом"
      
      # Проверяем, есть ли уже ENCRYPTION_OLD_SECRETS (от предыдущих миграций)
      local existing_old_secrets
      existing_old_secrets="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_OLD_SECRETS")"
      
      if [[ -n "$existing_old_secrets" ]]; then
        echo "  ENCRYPTION_OLD_SECRETS содержит старые ключи от предыдущих миграций (не изменяется)"
      else
        echo "  ENCRYPTION_OLD_SECRETS не установлен (будет создан при следующей смене ключа)"
      fi
    fi
  else
    echo "  ENCRYPTION_SECRET не найден в бэкапе. Будет сгенерирован новый."
    local new_encryption_secret
    new_encryption_secret="$(openssl rand -hex 32 2>/dev/null || python3 -c 'import secrets; print(secrets.token_hex(32))' || dd if=/dev/urandom bs=32 count=1 2>/dev/null | xxd -p)"
    
    if [[ -n "$new_encryption_secret" && ${#new_encryption_secret} -ge 64 ]]; then
      echo "ENCRYPTION_SECRET=${new_encryption_secret}" >> "$archive_env_file"
      ok "ENCRYPTION_SECRET сгенерирован"
      env_override=1
    else
      warn "Не удалось сгенерировать ENCRYPTION_SECRET!"
    fi
  fi
  
  # Выводим итоговые значения для проверки
  echo ""
  echo "Итоговые значения:"
  local final_encryption_preview final_old_secrets_preview
  final_encryption_preview="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_SECRET")"
  final_old_secrets_preview="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_OLD_SECRETS")"
  echo "  ENCRYPTION_SECRET: ${final_encryption_preview:0:8}...***REDACTED*** (${#final_encryption_preview} символов)"
  if [[ -n "$final_old_secrets_preview" ]]; then
    # Показываем количество старых ключей
    local old_count
    old_count="$(echo "$final_old_secrets_preview" | tr ',' '\n' | grep -c . || echo 0)"
    echo "  ENCRYPTION_OLD_SECRETS: содержит $old_count ключ(ей) для миграции старых данных"
  else
    echo "  ENCRYPTION_OLD_SECRETS: не установлен (все данные шифруются текущим ключом)"
  fi
  echo ""
  echo "Важно: Если в базе данных есть зашифрованные ключи, убедитесь что ENCRYPTION_SECRET"
  echo "         соответствует ключу, которым они были зашифрованы, либо добавьте старый ключ"
  echo "         в ENCRYPTION_OLD_SECRETS через опцию 1 при следующей переустановке."

  # ============================================
  # ШАГ 1.6: Валидация ENCRYPTION_SECRET после модификации
  # ============================================
  local final_encryption_secret
  final_encryption_secret="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_SECRET")"
  
  if [[ -z "$final_encryption_secret" ]]; then
    error "Критическая ошибка: ENCRYPTION_SECRET отсутствует или пустой после модификации!"
    error "Восстановление невозможно без корректного ключа шифрования."
    rm -rf "$tmp_restore"
    return 1
  elif [[ ${#final_encryption_secret} -lt 32 ]]; then
    error "Критическая ошибка: ENCRYPTION_SECRET слишком короткий (минимум 32 символа, текущая длина: ${#final_encryption_secret})!"
    error "Восстановление невозможно без корректного ключа шифрования."
    rm -rf "$tmp_restore"
    return 1
  else
    ok "ENCRYPTION_SECRET прошёл валидацию (длина: ${#final_encryption_secret})"
  fi

  # Проверка ENCRYPTION_OLD_SECRETS если он был установлен
  local old_secrets_value
  old_secrets_value="$(get_env_value_from_file "$archive_env_file" "ENCRYPTION_OLD_SECRETS")"
  if [[ -n "$old_secrets_value" ]]; then
    # Проверяем, что не пустой и содержит хотя бы один ключ
    if [[ -z "$old_secrets_value" || "$old_secrets_value" == "," ]]; then
      warn "ENCRYPTION_OLD_SECRETS установлен, но содержит пустое или некорректное значение."
    else
      ok "ENCRYPTION_OLD_SECRETS: OK (содержит старые ключи для миграции)"
    fi
  fi

  echo ""
  echo "--- Параметры AmneziaWG (авто-подстановка с текущего сервера) ---"
  echo "Следующие параметры будут автоматически взяты с текущего сервера:"
  echo ""
  
  # ============================================
  # ШАГ 2: Параметры AWG (авто-подстановка)
  # ============================================
  local -a auto_vars=("SERVER_IP" "SERVER_PUBLIC_KEY" "WG_INTERFACE" "DOCKER_CONTAINER")
  
  for var in "${auto_vars[@]}"; do
    local current_value auto_value
    current_value="$(get_env_value_from_file "$archive_env_file" "$var")"
    
    # Определяем авто-значение для текущего сервера
    case "$var" in
      SERVER_IP) auto_value="$detected_server_ip" ;;
      SERVER_PUBLIC_KEY) auto_value="$detected_public_key" ;;
      WG_INTERFACE) auto_value="$detected_interface" ;;
      DOCKER_CONTAINER) auto_value="$detected_container" ;;
    esac
    
    if [[ -n "$auto_value" ]]; then
      # Есть авто-значение - используем его
      echo "  $var = $auto_value (с текущего сервера)"
      # Заменяем значение из бэкапа на авто-значение
      if [[ -n "$current_value" ]]; then
        local escaped_auto
        escaped_auto="$(printf '%s\n' "$auto_value" | sed 's/[&/\]/\\&/g')"
        sed -i "s|^${var}=.*|${var}=${escaped_auto}|" "$archive_env_file"
      else
        echo "${var}=${auto_value}" >> "$archive_env_file"
      fi
      env_override=1
    elif [[ -n "$current_value" ]]; then
      # Нет авто-значения, но есть в бэкапе - оставляем старое
      echo "  $var = $current_value (из бэкапа, авто-подстановка недоступна)"
    else
      # Нет ни авто-значения, ни в бэкапе - просим ввести вручную
      echo "  $var = (требуется ручное указание)"
      prompt_raw "Введите значение $var: " new_value
      if [[ -n "$new_value" ]]; then
        local escaped_new
        escaped_new="$(printf '%s\n' "$new_value" | sed 's/[&/\]/\\&/g')"
        echo "${var}=${escaped_new}" >> "$archive_env_file"
        env_override=1
      fi
    fi
  done
  
  # Проверяем, удалось ли определить критические AWG параметры
  if [[ -z "$detected_container" || -z "$detected_interface" || -z "$detected_public_key" ]]; then
    echo ""
    warn "Не удалось автоматически определить некоторые параметры AmneziaWG!"
    echo "Убедитесь, что AmneziaWG установлен и запущен перед восстановлением бота."
    echo "После восстановления может потребоваться ручная настройка .env"
  fi

  # Переопределение цен на подписки
  echo ""
  echo "=== Переопределение цен на подписки ==="
  local -a price_vars=("STARS_PRICE_7_DAYS" "STARS_PRICE_30_DAYS" "STARS_PRICE_90_DAYS" "PLATEGA_PRICE_7_DAYS" "PLATEGA_PRICE_30_DAYS" "PLATEGA_PRICE_90_DAYS")
  
  for var in "${price_vars[@]}"; do
    local current_value
    current_value="$(get_env_value_from_file "$archive_env_file" "$var")"
    if [[ -n "$current_value" ]]; then
      echo "  $var = $current_value"
    else
      echo "  $var = (не задано)"
    fi
    prompt_raw "Переопределить $var (оставить пустым для сохранения текущего): " new_value
    if [[ -n "$new_value" ]]; then
      # Проверяем, что введено число
      if [[ "$new_value" =~ ^[0-9]+$ ]]; then
        local escaped_new
        escaped_new="$(printf '%s\n' "$new_value" | sed 's/[&/\]/\\&/g')"
        if [[ -n "$current_value" ]]; then
          sed -i "s/^${var}=.*/${var}=${escaped_new}/" "$archive_env_file"
        else
          # Добавляем переменную, если её не было
          echo "${var}=${escaped_new}" >> "$archive_env_file"
        fi
        env_override=1
      else
        warn "Некорректное значение для $var (должно быть число). Пропущено."
      fi
    fi
  done

  
  if [[ -n "$env_override" ]]; then
    echo ""
    echo "Обновлённые значения в .env:"
    for var in "${manual_vars[@]}" "${auto_vars[@]}" "${price_vars[@]}"; do
      local updated_value
      updated_value="$(get_env_value_from_file "$archive_env_file" "$var")"
      if [[ -n "$updated_value" ]]; then
        if [[ "$var" == "BOT_TOKEN" || "$var" == "PLATEGA_MERCHANT_ID" || "$var" == "PLATEGA_SECRET_KEY" ]]; then
          echo "  $var = ***REDACTED***"
        else
          echo "  $var = $updated_value"
        fi
      fi
    done
    echo ""
  fi

  # Если сервис существует, останавливаем его для безопасного восстановления
  if require_command systemctl && service_exists; then
    service_active_before="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    service_enabled_before="$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
    if [[ "$service_active_before" == "active" ]]; then
      if ! systemctl stop "$SERVICE_NAME" 2>/dev/null; then
        warn "Restore отменён: не удалось остановить ${SERVICE_NAME} (fail-closed для active service)."
        return 1
      fi
      if ! wait_for_service_stopped_state; then
        service_state_after_stop="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
        warn "Restore отменён: ${SERVICE_NAME} не перешёл в безопасное stopped-state после stop (state=${service_state_after_stop:-unknown})."
        return 1
      fi
    elif [[ "$service_active_before" != "inactive" && "$service_active_before" != "failed" ]]; then
      warn "Restore отменён: ${SERVICE_NAME} в переходном/небезопасном состоянии (${service_active_before:-unknown}). Дождитесь stable inactive/failed или active."
      return 1
    fi
  fi

  # Создаём snapshot текущих данных для rollback (если они есть)
  snapshot_dir="$(mktemp -d "${backup_root}/pre-restore-$(date -u +%Y%m%d_%H%M%S)-XXXXXX")"
  chmod 700 "$snapshot_dir" || true
  if [[ -f "$original_db_file" ]]; then snapshot_sqlite_runtime_bundle "$original_db_file" "$snapshot_dir" "db.before" || true; fi
  if [[ -f "$ENV_FILE" ]]; then cp -a "$ENV_FILE" "$snapshot_dir/.env.before"; fi

  # Извлекаем файлы из архива (используем обновлённый archive_env_file)
  tmp_restore="$(mktemp -d)"
  if tar -xzf "$selected_archive" -C "$tmp_restore" "${restore_members[@]}" 2>/dev/null; then
    # Копируем модифицированный .env вместо извлечения из архива
    cp "$archive_env_file" "$tmp_restore/.env"
    
    archive_db_path="$(get_env_value_from_file "$archive_env_file" DB_PATH)"
    archive_db_file="$(resolve_db_file_from_db_path "$archive_db_path")"
    archive_db_basename="$(basename "$archive_db_file")"
    restore_target_db_file="$archive_db_file"
    rollback_cleanup_db_file="$restore_target_db_file"
    if [[ "$archive_db_basename" != "$payload_db_basename" ]]; then
      warn "Restore: DB_PATH из .env указывает на другой basename (${archive_db_basename}), архив содержит ${payload_db_basename}."
      warn "Восстановление отменено, чтобы не разложить .env и SQLite в разные пути."
      restore_blocked=1
    else
      mkdir -p "$(dirname "$restore_target_db_file")" "$INSTALL_DIR"
    fi
    if [[ "$restore_blocked" == "1" ]]; then
      :
    elif [[ ! -f "$tmp_restore/$payload_db_basename" ]]; then
      warn "Restore: основной SQLite файл не извлечён из архива (${payload_db_basename})."
    elif ! sqlite_runtime_quick_check "$tmp_restore/$payload_db_basename"; then
      warn "SQLite quick_check не пройден для ${payload_db_basename} из архива."
    elif restore_sqlite_runtime_bundle "$tmp_restore/$payload_db_basename" "$restore_target_db_file"; then
      install -m 600 "$archive_env_file" "$ENV_FILE"
      repair_runtime_file_access "$ENV_FILE" 600
      restored_bundle_written=1
      restore_ok=1
    else
      warn "Ошибка восстановления SQLite bundle из архива."
    fi
  else
    warn "Ошибка извлечения файлов из архива."
  fi
  rm -rf "$tmp_restore"

  if [[ "$restore_ok" != "1" ]]; then
    if [[ -f "$snapshot_dir/db.before" ]]; then restore_sqlite_runtime_bundle "$snapshot_dir/db.before" "$rollback_target_db_file" && rollback_ok=1; fi
    if [[ -f "$snapshot_dir/.env.before" ]]; then install -m 600 "$snapshot_dir/.env.before" "$ENV_FILE" && rollback_ok=1; fi
    repair_runtime_file_access "$ENV_FILE" 600
    if [[ "$restored_bundle_written" == "1" && -n "$rollback_cleanup_db_file" && "$rollback_cleanup_db_file" != "$rollback_target_db_file" ]]; then
      rm -f "$rollback_cleanup_db_file" "${rollback_cleanup_db_file}-wal" "${rollback_cleanup_db_file}-shm" || true
    fi
    warn "Восстановление не завершено. Откат: $([[ "$rollback_ok" == "1" ]] && echo 'выполнен' || echo 'частично/не выполнен')."
    if [[ "$service_active_before" == "active" ]] && require_command systemctl && service_exists; then
      systemctl start "$SERVICE_NAME" 2>/dev/null || true
    fi
    return 1
  fi

  if [[ "$restore_ok" == "1" ]]; then
    # Дополнительная валидация восстановленного .env файла
    info "Выполняю финальную валидацию восстановленного .env файла..."
    
    # Копируем временный .env для проверки
    local temp_env_check
    temp_env_check="$(mktemp)"
    cp "$ENV_FILE" "$temp_env_check"
    
    # Временная подмена ENV_FILE для валидации
    local original_env_file="$ENV_FILE"
    ENV_FILE="$temp_env_check"
    
    if ! validate_critical_env; then
      error "Валидация восстановленного .env файла не пройдена!"
      error "Откат изменений..."
      ENV_FILE="$original_env_file"
      rm -f "$temp_env_check"
      
      # Откат
      if [[ -f "$snapshot_dir/db.before" ]]; then restore_sqlite_runtime_bundle "$snapshot_dir/db.before" "$rollback_target_db_file" && rollback_ok=1; fi
      if [[ -f "$snapshot_dir/.env.before" ]]; then install -m 600 "$snapshot_dir/.env.before" "$ENV_FILE" && rollback_ok=1; fi
      repair_runtime_file_access "$ENV_FILE" 600
      if [[ "$restored_bundle_written" == "1" && -n "$rollback_cleanup_db_file" && "$rollback_cleanup_db_file" != "$rollback_target_db_file" ]]; then
        rm -f "$rollback_cleanup_db_file" "${rollback_cleanup_db_file}-wal" "${rollback_cleanup_db_file}-shm" || true
      fi
      warn "Восстановление отменено из-за некорректного .env. Откат: $([[ "$rollback_ok" == "1" ]] && echo 'выполнен' || echo 'частично/не выполнен')."
      if [[ "$service_active_before" == "active" ]] && require_command systemctl && service_exists; then
        systemctl start "$SERVICE_NAME" 2>/dev/null || true
      fi
      return 1
    fi
    
    # Восстанавливаем оригинальный путь
    ENV_FILE="$original_env_file"
    rm -f "$temp_env_check"
    ok "Валидация восстановленного .env файла пройдена успешно."
    
    # Синхронизируем helper policy только если сервис существует
    if service_exists; then
      if ! sync_awg_helper_policy_from_env; then
        warn "Восстановление: не удалось синхронизировать helper policy из восстановленного .env."
        restore_ok=0
      else
        helper_sync_ok=1
      fi
    else
      # Бот ещё не установлен, helper policy будет настроен при установке
      ok "Helper policy будет настроен при установке бота."
      helper_sync_ok=1
    fi
  fi

  # Запускаем сервис только если он существует
  if require_command systemctl && service_exists; then
    systemctl start "$SERVICE_NAME" 2>/dev/null || true
  fi

  # Запускаем smokecheck только если сервис существует
  if [[ "$restore_ok" == "1" ]] && service_exists; then
    if run_post_restart_smokecheck; then
      smokecheck_ok=1
    fi
  elif [[ "$restore_ok" == "1" ]]; then
    # Если сервис ещё не установлен, считаем smokecheck пройденным
    smokecheck_ok=1
  fi

  if [[ "$restore_ok" != "1" || "$smokecheck_ok" != "1" ]]; then
    warn "Восстановление не прошло post-restore smokecheck. Запускаю rollback."
    if [[ -f "$snapshot_dir/db.before" ]]; then restore_sqlite_runtime_bundle "$snapshot_dir/db.before" "$rollback_target_db_file" && rollback_ok=1; fi
    if [[ -f "$snapshot_dir/.env.before" ]]; then install -m 600 "$snapshot_dir/.env.before" "$ENV_FILE" && rollback_ok=1; fi
    repair_runtime_file_access "$ENV_FILE" 600
    if [[ "$restored_bundle_written" == "1" && -n "$rollback_cleanup_db_file" && "$rollback_cleanup_db_file" != "$rollback_target_db_file" ]]; then
      rm -f "$rollback_cleanup_db_file" "${rollback_cleanup_db_file}-wal" "${rollback_cleanup_db_file}-shm" || true
    fi
    # Синхронизируем helper policy только если сервис существует
    if service_exists; then
      if sync_awg_helper_policy_from_env; then
        rollback_helper_sync_ok=1
      else
        warn "Rollback restore: не удалось синхронизировать helper policy."
      fi
    else
      rollback_helper_sync_ok=1
    fi
    if require_command systemctl && service_exists; then
      systemctl restart "$SERVICE_NAME" 2>/dev/null || true
    fi
    if service_exists && run_post_restart_smokecheck; then
      rollback_smoke_ok=1
    elif ! service_exists; then
      rollback_smoke_ok=1
    fi
    if [[ "$rollback_ok" == "1" && "$rollback_helper_sync_ok" == "1" && "$rollback_smoke_ok" == "1" ]]; then
      warn "Восстановление не удалось; rollback выполнен полностью (runtime восстановлен, helper policy: ok, post-rollback проверка: ok)."
    else
      warn "Восстановление не удалось; rollback выполнен частично (runtime=${rollback_ok}, helper policy=${rollback_helper_sync_ok}, post-rollback проверка=${rollback_smoke_ok})."
    fi
    return 1
  fi

  echo "Восстановление завершено."
  echo "Права файлов восстановлены для ${BOT_USER}: ${restore_target_db_file}, ${ENV_FILE}"
  if [[ "$helper_sync_ok" == "1" ]]; then
    echo "Helper policy sync: успешно."
  fi
  if require_command systemctl && service_exists; then
    echo "Сервис: $(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true) (enabled: ${service_enabled_before:-unknown})"
  else
    echo "Сервис ещё не установлен. Выполните установку бота для завершения восстановления."
  fi
  return 0
}

list_bot_managed_peer_keys() {
  local db_file="$1"
  [[ -f "$db_file" ]] || return 0
  "$PYTHON_BIN" - "$db_file" <<'PY'
import sqlite3
import sys

path = sys.argv[1]
con = sqlite3.connect(path)
try:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute("PRAGMA table_info(keys)").fetchall()}
    if not cols:
        raise SystemExit(0)

    where = [
        "public_key IS NOT NULL",
        "TRIM(public_key) != ''",
        "public_key NOT LIKE 'pending:%'",
    ]
    if "bot_managed" not in cols:
        raise SystemExit(0)
    where.append("bot_managed = 1")
    if "state" in cols:
        where.append("(state IS NULL OR state != 'deleted')")

    query = "SELECT DISTINCT public_key FROM keys WHERE " + " AND ".join(where) + " ORDER BY id"
    for (key,) in cur.execute(query).fetchall():
        if isinstance(key, str) and key.strip():
            print(key.strip())
finally:
    con.close()
PY
}

remove_peer_from_awg_full_delete() {
  local public_key="$1"
  local container interface
  if [[ -x "$AWG_HELPER_TARGET" && -f "$AWG_HELPER_POLICY" ]]; then
    "$AWG_HELPER_TARGET" remove-peer --public-key "$public_key" >/dev/null
    return 0
  fi

  container="$(get_env_value DOCKER_CONTAINER)"
  interface="$(get_env_value WG_INTERFACE)"
  [[ -n "$container" ]] || { warn "DOCKER_CONTAINER не задан; fallback-удаление peer невозможно."; return 1; }
  [[ -n "$interface" ]] || { warn "WG_INTERFACE не задан; fallback-удаление peer невозможно."; return 1; }
  validate_awg_target_values "$container" "$interface"
  docker exec -i "$container" awg set "$interface" peer "$public_key" remove >/dev/null
  return 0
}

remove_bot_managed_peers_from_awg() {
  local db_file
  local -a peer_keys=()
  local total=0 removed=0 failed=0
  local public_key=""

  db_file="$(get_bot_db_file)"
  if [[ ! -f "$db_file" ]]; then
    info "База данных бота не найдена (${db_file}). Удалять peer по БД нечего."
    return 0
  fi

  if [[ -f "$ENV_FILE" ]]; then
    sync_awg_helper_policy_from_env || {
      warn "Не удалось синхронизировать helper policy из .env перед удалением peer."
      return 1
    }
  fi

  ensure_docker_ready || {
    warn "Docker недоступен. Нельзя безопасно удалить peer перед полным удалением."
    return 1
  }

  mapfile -t peer_keys < <(list_bot_managed_peer_keys "$db_file")
  total="${#peer_keys[@]}"
  if (( total == 0 )); then
    info "В текущей БД нет peer, созданных ботом. Удалять в AWG нечего."
    return 0
  fi

  info "Удаляю из AWG peer, созданные ботом и найденные в БД: ${total}"
  for public_key in "${peer_keys[@]}"; do
    [[ -n "$public_key" ]] || continue
    if remove_peer_from_awg_full_delete "$public_key"; then
      removed=$((removed + 1))
    else
      failed=$((failed + 1))
      warn "Не удалось удалить peer из AWG: ${public_key}"
    fi
  done

  if (( failed > 0 )); then
    warn "Удаление peer остановлено: успешно ${removed}, с ошибкой ${failed}."
    warn "Полное удаление отменено, чтобы не потерять БД до завершения очистки AWG."
    return 1
  fi

  ok "Из AWG удалены peer, созданные ботом: ${removed}"
  return 0
}

remove_everything() {
  systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
  systemctl disable --now "$AUTO_BACKUP_TIMER_NAME" 2>/dev/null || true
  rm -f "$SERVICE_FILE"
  rm -f "$AUTO_BACKUP_SERVICE_FILE" "$AUTO_BACKUP_TIMER_FILE"
  systemctl daemon-reload || true
  systemctl reset-failed || true
  rm -f "$SELF_SYMLINK"
  rm -f "$AWG_HELPER_SUDOERS" "$AWG_HELPER_TARGET"
  rm -rf "$INSTALL_DIR" "$APP_LOG_DIR"
  rm -f "$INSTALL_LOG"
  return 0
}

remove_keep_db_and_env() {
  local db_path db_file db_tmp env_tmp restored_dir backup_tmp_root backup_stash backup_count recovery_root
  local cleanup_backup_tmp=1
  REMOVE_BACKUPS_WERE_PRESENT=0
  REMOVE_BACKUPS_RESTORED=0
  db_path="$(get_env_value DB_PATH)"
  [[ -n "$db_path" ]] || db_path="$DEFAULT_DB_PATH"
  if [[ "$db_path" = /* ]]; then
    db_file="$db_path"
  else
    db_file="$INSTALL_DIR/$db_path"
  fi
  db_tmp=""
  env_tmp=""
  if [[ -f "$db_file" ]]; then
    db_tmp="$(mktemp -d)"
    snapshot_sqlite_runtime_bundle "$db_file" "$db_tmp" "db.keep" || return 1
  fi
  if [[ -f "$ENV_FILE" ]]; then
    env_tmp="$(mktemp)"
    cp -a "$ENV_FILE" "$env_tmp"
  fi
  backup_tmp_root=""
  backup_stash=""
  backup_count="$(find "$BACKUP_ROOT" -maxdepth 1 -type f -name 'awg-tgbot-backup-*.tar.gz' 2>/dev/null | wc -l | tr -d ' ' || true)"
  [[ -n "$backup_count" ]] || backup_count="0"
  if [[ "$backup_count" != "0" ]]; then
    REMOVE_BACKUPS_WERE_PRESENT=1
    backup_tmp_root="$(mktemp -d)"
    backup_stash="${backup_tmp_root}/backups"
    if ! mv "$BACKUP_ROOT" "$backup_stash"; then
      warn "Не удалось сохранить локальные backup-архивы из ${BACKUP_ROOT}. Удаление отменено."
      rm -rf "$backup_tmp_root"
      return 1
    fi
  fi
  remove_everything
  mkdir -p "$INSTALL_DIR"
  chmod 755 "$INSTALL_DIR" || true
  if [[ -n "$db_tmp" && -f "$db_tmp/db.keep" ]]; then
    if [[ "$db_path" = /* ]]; then
      restored_dir="$(dirname "$db_path")"
      mkdir -p "$restored_dir"
      restore_sqlite_runtime_bundle "$db_tmp/db.keep" "$db_path" || return 1
    else
      restore_sqlite_runtime_bundle "$db_tmp/db.keep" "$INSTALL_DIR/$db_path" || return 1
    fi
    rm -rf "$db_tmp"
  fi
  if [[ -n "$env_tmp" && -f "$env_tmp" ]]; then
    cp -a "$env_tmp" "$ENV_FILE"
    chmod 600 "$ENV_FILE" || true
    rm -f "$env_tmp"
  fi
  if [[ -n "$backup_stash" && -d "$backup_stash" ]]; then
    if mv "$backup_stash" "$BACKUP_ROOT"; then
      chmod 700 "$BACKUP_ROOT" || true
      REMOVE_BACKUPS_RESTORED=1
    else
      recovery_root="/var/tmp/awg-tgbot-backups-recovery-$(date -u +%Y%m%d_%H%M%S)-$$"
      if mv "$backup_tmp_root" "$recovery_root" 2>/dev/null; then
        backup_tmp_root="$recovery_root"
      fi
      cleanup_backup_tmp=0
      warn "Локальные backup-архивы не восстановлены в ${BACKUP_ROOT}."
      warn "Архивы сохранены для ручного восстановления: ${backup_tmp_root}"
    fi
  fi
  if [[ "$cleanup_backup_tmp" == "1" && -n "$backup_tmp_root" && -d "$backup_tmp_root" ]]; then
    rm -rf "$backup_tmp_root"
  fi
  return 0
}

remove_default() {
  if ! confirm_explicit "Удалить приложение и сервис, оставив БД, .env и локальные backup-архивы?"; then
    warn "Удаление отменено."
    return 0
  fi
  if ! remove_keep_db_and_env; then
    warn "Удаление остановлено до удаления данных, потому что не удалось безопасно сохранить backup-архивы."
    return 1
  fi
  if [[ "$REMOVE_BACKUPS_WERE_PRESENT" == "1" && "$REMOVE_BACKUPS_RESTORED" == "1" ]]; then
    ok "Удалено приложение и сервис. Сохранены: БД, .env и локальные backup-архивы."
  elif [[ "$REMOVE_BACKUPS_WERE_PRESENT" == "1" ]]; then
    warn "Удалено приложение и сервис. Сохранены: БД и .env. Локальные backup-архивы восстановлены не полностью."
  else
    ok "Удалено приложение и сервис. Сохранены: БД и .env."
  fi
  return 0
}

remove_full() {
  print_line
  warn "Полное удаление уничтожит код, сервис, БД, .env и логи."
  warn "Перед удалением будут удалены AWG peer, созданные ботом и найденные в текущей БД."
  warn "Если удаление хотя бы одного peer завершится ошибкой, полное удаление будет остановлено."
  if ! confirm_delete_word; then
    warn "Полное удаление отменено (неверное подтверждение)."
    return 0
  fi
  remove_bot_managed_peers_from_awg || return 1
  remove_everything
  ok "Выполнено полное удаление. Peer, созданные ботом и найденные в БД, удалены из AWG."
  return 0
}

remove_bot() {
  local choice=""
  print_line
  if ! has_residual_files; then
    warn "Бот уже удалён."
    return 0
  fi
  echo "1) Обычное удаление (сохранить БД, .env и локальные backup-архивы)"
  echo "2) Полное удаление (удалить всё)"
  echo "0) Отмена"
  prompt_raw "Выбор: " choice
  case "$choice" in
    1) remove_default ;;
    2) remove_full ;;
    *)
      warn "Удаление отменено."
      ;;
  esac
  print_line
  return 0
}



screen_warn() {
  local msg="$*"
  if has_tty; then
    printf '[!] %s\n' "$msg" >&3
  else
    warn "$msg"
  fi
}

print_file_tail_tty_safe() {
  local file="$1" lines="${2:-50}"
  if [[ -f "$file" ]]; then
    if has_tty; then
      tail -n "$lines" "$file" >&3 2>&1 || true
    else
      tail -n "$lines" "$file" || true
    fi
  else
    screen_warn "Файл не найден: $file"
  fi
}

get_service_active_since() {
  if ! service_exists; then
    return 0
  fi
  local active="" started_at=""
  active="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
  if [[ "$active" != "active" ]]; then
    return 0
  fi
  started_at="$(systemctl show -p ActiveEnterTimestamp --value "$SERVICE_NAME" 2>/dev/null | tr -d '\r' || true)"
  if [[ -n "$started_at" && "$started_at" != "n/a" ]]; then
    printf '%s' "$started_at"
  fi
}

read_service_journal() {
  local limit="${1:-400}"
  local since=""
  since="$(get_service_active_since)"
  if [[ -n "$since" ]]; then
    journalctl -u "$SERVICE_NAME" --since "$since" -n "$limit" --no-pager 2>/dev/null || true
  else
    journalctl -u "$SERVICE_NAME" -n "$limit" --no-pager 2>/dev/null || true
  fi
}


print_journal_tail_tty_safe() {
  local lines="${1:-50}"
  local raw_logs="" filtered_logs=""
  if service_exists; then
    raw_logs="$(journalctl -u "$SERVICE_NAME" -n 400 --no-pager 2>/dev/null || true)"
    filtered_logs="$(
      printf '%s\n' "$raw_logs" | grep -Eiv \
        'sudo\[[0-9]+\]: pam_unix\(sudo:session\): session (opened|closed) for user root|sudo\[[0-9]+\]:[[:space:]]+awg-bot[[:space:]]*: .*COMMAND=/usr/local/libexec/awg-bot-helper (show|denylist-clear --vpn-subnet )' \
        || true
    )"
    if [[ -n "$filtered_logs" ]]; then
      if has_tty; then
        printf '%s\n' "$filtered_logs" | tail -n "$lines" >&3 2>/dev/null || true
      else
        printf '%s\n' "$filtered_logs" | tail -n "$lines" 2>/dev/null || true
      fi
    else
      screen_warn "После фильтрации служебного sudo-шума журнал пуст, показываю raw-лог."
      screen_run journalctl -u "$SERVICE_NAME" -n "$lines" --no-pager
    fi
  else
    screen_warn "Сервис $SERVICE_NAME не найден."
  fi
}

print_journal_matches_tty_safe() {
  local pattern="$1" lines="${2:-20}"
  local raw_logs=""
  if service_exists; then
    raw_logs="$(read_service_journal 400)"
    if has_tty; then
      printf '%s\n' "$raw_logs" | grep -Ei "$pattern" | tail -n "$lines" >&3 2>/dev/null || true
    else
      printf '%s\n' "$raw_logs" | grep -Ei "$pattern" | tail -n "$lines" 2>/dev/null || true
    fi
  else
    screen_warn "Сервис $SERVICE_NAME не найден."
  fi
}

print_service_error_context_tty_safe() {
  local lines="${1:-20}"
  local raw_logs="" filtered_logs="" meaningful_logs="" fallback_logs="" app_tail="" status_tail=""

  if ! service_exists; then
    screen_warn "Сервис $SERVICE_NAME не найден."
    return 0
  fi

  raw_logs="$(journalctl -u "$SERVICE_NAME" -n 2000 --no-pager 2>/dev/null || true)"
  filtered_logs="$(printf '%s\n' "$raw_logs" | grep -Ei 'error|failed|traceback|exception|permission denied|main process exited|code=exited|status=[0-9]+' || true)"
  meaningful_logs="$(printf '%s\n' "$filtered_logs" | grep -Eiv "Failed with result[ =]+'?exit-code'?|Scheduled restart job, restart counter is at" || true)"

  if [[ -z "$filtered_logs" ]]; then
    screen_echo "Явных ошибок сервиса в последних записях journalctl не найдено."
    fallback_logs="$(printf '%s\n' "$raw_logs" | tail -n "$lines")"
    if [[ -n "$fallback_logs" ]]; then
      screen_line
      screen_echo "Последние строки сервиса:"
      if has_tty; then
        printf '%s\n' "$fallback_logs" >&3 2>/dev/null || true
      else
        printf '%s\n' "$fallback_logs" 2>/dev/null || true
      fi
    fi
    return 0
  fi

  if [[ -n "$meaningful_logs" ]]; then
    if has_tty; then
      printf '%s\n' "$meaningful_logs" | tail -n "$lines" >&3 2>/dev/null || true
    else
      printf '%s\n' "$meaningful_logs" | tail -n "$lines" 2>/dev/null || true
    fi
    return 0
  fi

  screen_warn "Найдены только повторы 'Failed with result=exit-code' без причины. Показываю расширенный контекст."
  status_tail="$(systemctl status "$SERVICE_NAME" -n "$lines" --no-pager 2>/dev/null || true)"
  if [[ -n "$status_tail" ]]; then
    screen_line
    screen_echo "systemctl status (последние строки):"
    if has_tty; then
      printf '%s\n' "$status_tail" >&3 2>/dev/null || true
    else
      printf '%s\n' "$status_tail" 2>/dev/null || true
    fi
  fi

  fallback_logs="$(printf '%s\n' "$raw_logs" | tail -n "$lines")"
  if [[ -n "$fallback_logs" ]]; then
    screen_line
    screen_echo "Расширенный контекст journalctl:"
    if has_tty; then
      printf '%s\n' "$fallback_logs" >&3 2>/dev/null || true
    else
      printf '%s\n' "$fallback_logs" 2>/dev/null || true
    fi
  fi

  if [[ -f "$APP_LOG_FILE" ]]; then
    app_tail="$(tail -n "$lines" "$APP_LOG_FILE" 2>/dev/null || true)"
    if [[ -n "$app_tail" ]]; then
      screen_line
      screen_echo "Последние строки bot.log (для причины падения):"
      if has_tty; then
        printf '%s\n' "$app_tail" >&3 2>/dev/null || true
      else
        printf '%s\n' "$app_tail" 2>/dev/null || true
      fi
    fi
  fi
}

run_log_snapshot() {
  local mode="$1" variant="${2:-last}"
  case "$mode:$variant" in
    service:last) print_journal_tail_tty_safe 50 ;;
    service:error) print_service_error_context_tty_safe 20 ;;
    bot:last) print_file_tail_tty_safe "$APP_LOG_FILE" 50 ;;
    bot:warn)
      if [[ -f "$APP_LOG_FILE" ]]; then
        if has_tty; then
          grep -E '\| (WARNING|ERROR) \|' "$APP_LOG_FILE" | grep -Ev 'Received SIGTERM signal' | tail -n 20 >&3 2>/dev/null || true
        else
          grep -E '\| (WARNING|ERROR) \|' "$APP_LOG_FILE" | grep -Ev 'Received SIGTERM signal' | tail -n 20 2>/dev/null || true
        fi
      else
        screen_warn "Файл не найден: $APP_LOG_FILE"
      fi
      ;;
    install:last) print_file_tail_tty_safe "$INSTALL_LOG" 50 ;;
    paths:show)
      screen_echo "Пути логов:"
      screen_echo "• Runtime (bot.log): ${APP_LOG_FILE}"
      screen_echo "• Installer (install log): ${INSTALL_LOG}"
      screen_echo "• Service (systemd unit): ${SERVICE_NAME}"
      ;;
    *)
      screen_warn "Неизвестный режим логов: ${mode}:${variant}"
      ;;
  esac
  return 0
}

watch_logs_live() {
  local mode="$1" key=""
  while true; do
    clear_if_tty
    screen_line
    case "$mode" in
      service)
        screen_echo "Лог сервиса — live"
        screen_echo "Обновление каждые 2 сек. Нажми q для возврата."
        screen_line
        print_journal_tail_tty_safe 40
        ;;
      bot)
        screen_echo "Лог бота — live"
        screen_echo "Обновление каждые 2 сек. Нажми q для возврата."
        screen_line
        print_file_tail_tty_safe "$APP_LOG_FILE" 40
        ;;
      install)
        screen_echo "install log — live"
        screen_echo "Обновление каждые 2 сек. Нажми q для возврата."
        screen_line
        print_file_tail_tty_safe "$INSTALL_LOG" 40
        ;;
      *)
        screen_warn "Неизвестный live-режим: $mode"
        return 0
        ;;
    esac
    screen_line
    if has_tty; then
      if read -r -u 3 -t 2 -n 1 key 2>/dev/null; then
        echo >&3
        case "$key" in
          q|Q|й|Й|0) clear_if_tty; return 0 ;;
          *) ;;
        esac
      fi
    else
      sleep 2
    fi
  done
}

show_logs_doctor() {
  local active="unknown" enabled="unknown"
  local journal_hits="" bot_hits=""
  local env_container="" env_interface="" policy_container="" policy_interface="" policy_error=""
  detect_install_state
  refresh_update_status_quiet
  clear_if_tty
  screen_line
  screen_echo "Что не так?"
  screen_line

  if service_exists; then
    active="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
    enabled="$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
    screen_echo "Сервис: ${SERVICE_NAME} (${active:-unknown}, ${enabled:-unknown})"
  else
    screen_warn "Сервис ${SERVICE_NAME} не найден."
  fi

  screen_echo "Docker daemon: $(status_available_text "$STATE_DOCKER_DAEMON")"
  screen_echo "AWG: $(status_found_text "$STATE_AWG_FOUND")"
  screen_echo "ENV: $(status_found_text "$STATE_BOT_ENV_FOUND")"
  screen_echo "bot.log: $( [[ -f "$APP_LOG_FILE" ]] && printf 'найден' || printf 'не найден' )"
  screen_echo "install log: $( [[ -f "$INSTALL_LOG" ]] && printf 'найден' || printf 'не найден' )"

  if [[ "$STATE_DOCKER_DAEMON" != "1" ]]; then
    screen_warn "Docker daemon недоступен."
  fi
  if [[ "$STATE_AWG_FOUND" != "1" ]]; then
    screen_warn "AWG сейчас не обнаружен."
  fi
  if service_exists && [[ "$active" != "active" ]]; then
    screen_warn "Сервис не запущен или работает нестабильно."
  fi
  env_container="$(get_env_value DOCKER_CONTAINER)"
  env_interface="$(get_env_value WG_INTERFACE)"
  IFS=$'\t' read -r policy_container policy_interface policy_error < <(read_helper_policy_state)
  if [[ -n "$policy_error" ]]; then
    screen_warn "$policy_error (${AWG_HELPER_POLICY})"
  fi

  screen_line
  screen_echo "Последние важные сообщения сервиса:"
  screen_line
  print_journal_matches_tty_safe 'error|failed|traceback|exception|permission denied' 10

  screen_line
  screen_echo "Последние WARNING / ERROR бота:"
  screen_line
  if [[ -f "$APP_LOG_FILE" ]]; then
    if has_tty; then
      grep -E '\| (WARNING|ERROR) \|' "$APP_LOG_FILE" | grep -Ev 'Received SIGTERM signal' | tail -n 10 >&3 2>/dev/null || true
    else
      grep -E '\| (WARNING|ERROR) \|' "$APP_LOG_FILE" | grep -Ev 'Received SIGTERM signal' | tail -n 10 2>/dev/null || true
    fi
  else
    screen_warn "Файл не найден: $APP_LOG_FILE"
  fi

  screen_line
  screen_echo "Где искать проблему:"
  screen_echo "• app-level: «Лог бота» (${APP_LOG_FILE})"
  screen_echo "• service-level: «Лог сервиса» (journalctl -u ${SERVICE_NAME})"
  screen_echo "• installer-level: «Дополнительно → install log» (${INSTALL_LOG})"
  screen_line
  screen_echo "Рекомендуемые действия:"
  if [[ "$STATE_DOCKER_DAEMON" != "1" ]]; then
    screen_echo "• Запусти Docker daemon и повтори диагностику."
  fi
  if [[ "$STATE_AWG_FOUND" != "1" ]]; then
    screen_echo "• Проверь контейнер AWG и имя интерфейса в .env (DOCKER_CONTAINER / WG_INTERFACE)."
  fi
  if [[ -n "$policy_error" ]]; then
    screen_echo "• КРИТИЧНО: исправь JSON в /etc/awg-bot-helper.json."
    screen_echo "• Проверь container/interface в helper policy и .env."
    screen_echo "• Затем перезапусти vpn-bot.service."
  elif [[ -n "$env_container" && -n "$env_interface" ]] && [[ "$policy_container" != "$env_container" || "$policy_interface" != "$env_interface" ]]; then
    screen_echo "• Проверь container/interface в /etc/awg-bot-helper.json и синхронизируй policy."
  fi
  if service_exists && [[ "$active" != "active" ]]; then
    screen_echo "• Открой «Лог сервиса» и посмотри последние ошибки перед перезапуском."
  fi
  if [[ "$STATE_DOCKER_DAEMON" == "1" && "$STATE_AWG_FOUND" == "1" && -z "$policy_error" ]] && { ! service_exists || [[ "$active" == "active" ]]; }; then
    screen_echo "• Критичных проблем не найдено. Если есть жалобы, открой «Лог бота» и «Лог сервиса»."
  fi

  screen_line
  pause_if_tty
  clear_if_tty
  return 0
}

show_bot_logs_menu() {
  local choice=""
  while true; do
    screen_line
    screen_echo "Лог бота:"
    screen_echo "1) Последние 50 строк"
    screen_echo "2) Только WARNING / ERROR"
    screen_echo "3) Live просмотр"
    screen_echo "0) Назад"
    screen_line
    prompt_menu_key "Выбор: " choice
    case "$choice" in
      1) screen_line; run_log_snapshot bot last; screen_line; pause_if_tty; clear_if_tty ;;
      2) screen_line; run_log_snapshot bot warn; screen_line; pause_if_tty; clear_if_tty ;;
      3) watch_logs_live bot ;;
      0) clear_if_tty; return 0 ;;
      *) screen_warn "Неизвестный пункт меню."; pause_if_tty; clear_if_tty ;;
    esac
  done
}

show_service_logs_menu() {
  local choice=""
  while true; do
    screen_line
    screen_echo "Лог сервиса:"
    screen_echo "1) Последние 50 строк"
    screen_echo "2) Только ошибки"
    screen_echo "3) Live просмотр"
    screen_echo "0) Назад"
    screen_line
    prompt_menu_key "Выбор: " choice
    case "$choice" in
      1) screen_line; run_log_snapshot service last; screen_line; pause_if_tty; clear_if_tty ;;
      2) screen_line; run_log_snapshot service error; screen_line; pause_if_tty; clear_if_tty ;;
      3) watch_logs_live service ;;
      0) clear_if_tty; return 0 ;;
      *) screen_warn "Неизвестный пункт меню."; pause_if_tty; clear_if_tty ;;
    esac
  done
}

show_extra_logs_menu() {
  local choice=""
  while true; do
    screen_line
    screen_echo "Дополнительно:"
    screen_echo "1) install log — последние 50 строк"
    screen_echo "2) install log — live просмотр"
    screen_echo "3) Пути логов"
    screen_echo "0) Назад"
    screen_line
    prompt_menu_key "Выбор: " choice
    case "$choice" in
      1) screen_line; run_log_snapshot install last; screen_line; pause_if_tty; clear_if_tty ;;
      2) watch_logs_live install ;;
      3) screen_line; run_log_snapshot paths show; screen_line; pause_if_tty; clear_if_tty ;;
      0) clear_if_tty; return 0 ;;
      *) screen_warn "Неизвестный пункт меню."; pause_if_tty; clear_if_tty ;;
    esac
  done
}

show_logs() {
  local choice=""
  if ! has_residual_files; then
    print_line
    warn "Бот не установлен."
    print_line
    return 0
  fi

  while true; do
    screen_line
    screen_echo "Логи:"
    screen_echo "1) Что не так?"
    screen_echo "2) Лог бота"
    screen_echo "3) Лог сервиса"
    screen_echo "4) Дополнительно"
    screen_echo "0) Назад"
    screen_line
    prompt_menu_key "Выбор: " choice
    case "$choice" in
      1) show_logs_doctor ;;
      2) show_bot_logs_menu ;;
      3) show_service_logs_menu ;;
      4) show_extra_logs_menu ;;
      0) clear_if_tty; return 0 ;;
      *) screen_warn "Неизвестный пункт меню."; pause_if_tty; clear_if_tty ;;
    esac
  done
}


print_menu_awg_yes_bot_no() {
  echo "Доступные действия:"
  echo "1) Установить"
  echo "2) Диагностика"
  echo "3) Повторить проверку"
  echo "4) Бэкап"
  echo "5) Восстановить из бэкапа"
  echo "0) Выход"
  print_line
}

print_menu_awg_yes_bot_yes() {
  echo "Доступные действия:"
  echo "1) Статус"
  echo "2) Логи"
  echo "3) Переустановить"
  echo "4) Бэкап"
  echo "5) Восстановить из бэкапа"
  echo "6) Удалить"
  echo "7) Диагностика"
  echo "0) Выход"
  print_line
}

print_menu_awg_no_bot_yes() {
  echo "Доступные действия:"
  echo "1) Статус"
  echo "2) Логи"
  echo "3) Переустановить"
  echo "4) Бэкап"
  echo "5) Восстановить из бэкапа"
  echo "6) Удалить"
  echo "7) Диагностика"
  echo "8) Повторить проверку"
  echo "0) Выход"
  print_line
}

print_menu_awg_no_bot_no() {
  echo "Доступные действия:"
  echo "1) Диагностика"
  echo "2) Повторить проверку"
  echo "3) Восстановить из бэкапа"
  echo "0) Выход"
  print_line
}

run_action() {
  local action="${1:-}"
  case "$action" in
    install) install_or_reinstall_flow install ;;
    reinstall) install_or_reinstall_flow reinstall ;;
    update|check-updates|choose-branch)
      warn "Команда '$action' отключена в personal MVP. Используй reinstall для обновления."
      ;;
    status) show_status ;;
    logs) show_logs ;;
    backup) create_local_backup ;;
    restore) restore_from_backup ;;
    diagnostics) detect_install_state; refresh_update_status_quiet; print_detailed_startup_summary ;;
    preflight|detect-install-state) detect_install_state; refresh_update_status_quiet; print_detailed_startup_summary ;;
    sync-helper-policy) sync_awg_helper_policy_from_env ;;
    remove-default) remove_default ;;
    remove-full) remove_full ;;
    remove|uninstall) remove_bot ;;
    *) return 0 ;;
  esac
}

main_menu() {
  local choice="" should_pause=1
  while true; do
    should_pause=1
    detect_install_state
    refresh_update_status_quiet
    print_startup_summary
    case "$STARTUP_STATE_CODE" in
      awg_yes_bot_yes)
        print_menu_awg_yes_bot_yes
        prompt_menu_key "Выбери действие: " choice
        case "$choice" in
          1) show_status ;;
          2) show_logs ;;
          3) install_or_reinstall_flow reinstall ;;
          4) create_local_backup ;;
          5) restore_from_backup ;;
          6) remove_bot ;;
          7) print_detailed_startup_summary ;;
          0) cleanup_transient_install_state; clear_if_tty; print_exit_hint; exit 0 ;;
          *) warn "Неизвестный пункт меню." ;;
        esac
        ;;
      awg_yes_bot_no)
        print_menu_awg_yes_bot_no
        prompt_menu_key "Выбери действие: " choice
        case "$choice" in
          1) install_or_reinstall_flow install ;;
          2) print_detailed_startup_summary ;;
          3) should_pause=0 ;;
          4) create_local_backup ;;
          5) restore_from_backup ;;
          0) cleanup_transient_install_state; clear_if_tty; print_exit_hint; exit 0 ;;
          *) warn "Неизвестный пункт меню." ;;
        esac
        ;;
      awg_no_bot_yes)
        print_menu_awg_no_bot_yes
        prompt_menu_key "Выбери действие: " choice
        case "$choice" in
          1) show_status ;;
          2) show_logs ;;
          3) install_or_reinstall_flow reinstall ;;
          4) create_local_backup ;;
          5) restore_from_backup ;;
          6) remove_bot ;;
          7) print_detailed_startup_summary ;;
          8) should_pause=0 ;;
          0) cleanup_transient_install_state; clear_if_tty; print_exit_hint; exit 0 ;;
          *) warn "Неизвестный пункт меню." ;;
        esac
        ;;
      awg_no_bot_no|*)
        print_menu_awg_no_bot_no
        prompt_menu_key "Выбери действие: " choice
        case "$choice" in
          1) print_detailed_startup_summary ;;
          2) should_pause=0 ;;
          3) restore_from_backup ;;
          0) cleanup_transient_install_state; clear_if_tty; print_exit_hint; exit 0 ;;
          *) warn "Неизвестный пункт меню." ;;
        esac
        ;;
    esac
    if [[ "$should_pause" == "1" ]]; then
      pause_if_tty
    fi
    clear_if_tty
  done
}

if [[ "${AWG_TGBOT_SOURCE_ONLY:-0}" == "1" ]]; then
  return 0 2>/dev/null || exit 0
fi

require_root
setup_tty_fd
setup_logging

if [[ $# -gt 0 ]]; then
  run_action "$1"
  exit 0
fi

if ! has_tty; then
  warn "Интерактивное меню требует TTY и не может читать ответы из stdin pipe."
  warn "Используй action-команды (например: status, reinstall, diagnostics, sync-helper-policy) без prompt-ов."
  die "Для первичной установки запусти команду в интерактивной сессии с TTY (SSH/console)."
fi

main_menu
