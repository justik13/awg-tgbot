#!/usr/bin/env bash
# Скрипт мониторинга состояния бота для production эксплуатации
set -euo pipefail

INSTALL_DIR="/opt/amnezia/bot"
RUNTIME_DIR="${INSTALL_DIR}/runtime"
ENV_FILE="${INSTALL_DIR}/.env"
APP_LOG_FILE="/var/log/awg-tgbot/bot.log"
SERVICE_NAME="vpn-bot.service"
ADMIN_ID=""
BOT_TOKEN=""
TELEGRAM_API="https://api.telegram.org"
LAST_ALERT_FILE="${RUNTIME_DIR}/.last_alert"
ALERT_COOLDOWN=300  # 5 минут между повторными алертами

# Чтение переменных из .env
if [[ -f "$ENV_FILE" ]]; then
  ADMIN_ID=$(grep -m1 "^ADMIN_ID=" "$ENV_FILE" | cut -d'=' -f2- || true)
  BOT_TOKEN=$(grep -m1 "^API_TOKEN=" "$ENV_FILE" | cut -d'=' -f2- || true)
fi

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$APP_LOG_FILE"
}

send_telegram_alert() {
  local message="$1"
  if [[ -z "$BOT_TOKEN" || -z "$ADMIN_ID" ]]; then
    log "ALERT: Telegram не настроен (TOKEN или ADMIN_ID пустые). Сообщение: $message"
    return 0
  fi
  
  # Проверка cooldown
  if [[ -f "$LAST_ALERT_FILE" ]]; then
    local last_alert
    last_alert=$(cat "$LAST_ALERT_FILE" 2>/dev/null || echo 0)
    local now
    now=$(date +%s)
    if (( now - last_alert < ALERT_COOLDOWN )); then
      log "ALERT suppressed (cooldown): $message"
      return 0
    fi
  fi
  
  # Отправка уведомления
  local escaped_message
  escaped_message=$(printf '%s' "$message" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&#39;/g')
  
  if curl -fsSL -X POST "${TELEGRAM_API}/bot${BOT_TOKEN}/sendMessage" \
    -d "chat_id=${ADMIN_ID}" \
    -d "text=${escaped_message}" \
    -d "parse_mode=HTML" \
    --connect-timeout 10 \
    --max-time 30 2>/dev/null; then
    echo "$(date +%s)" > "$LAST_ALERT_FILE"
    log "ALERT sent: $message"
  else
    log "ALERT failed to send: $message"
  fi
}

check_service_status() {
  local status
  if ! systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
    status="inactive"
  else
    status="active"
  fi
  echo "$status"
}

check_port_listening() {
  local port="${1:-8444}"
  if ss -tlnp 2>/dev/null | grep -qE ":${port}\\s"; then
    echo "listening"
  else
    echo "not_listening"
  fi
}

check_disk_usage() {
  local threshold="${1:-90}"
  local usage
  usage=$(df "$INSTALL_DIR" 2>/dev/null | awk 'NR==2 {gsub(/%/,""); print $5}' || echo 0)
  if (( usage >= threshold )); then
    echo "critical:${usage}"
  else
    echo "ok:${usage}"
  fi
}

check_awg_container() {
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qE '^amnezia-wg|awg'; then
    echo "running"
  else
    echo "not_running"
  fi
}

check_database_health() {
  local db_path
  db_path=$(grep -m1 "^DB_PATH=" "$ENV_FILE" 2>/dev/null | cut -d'=' -f2- || echo "${RUNTIME_DIR}/vpn_bot.db")
  if [[ -f "$db_path" ]]; then
    # Проверка целостности БД
    if sqlite3 "$db_path" "PRAGMA quick_check;" 2>/dev/null | grep -q "^ok$"; then
      echo "healthy"
    else
      echo "corrupted"
    fi
  else
    echo "missing"
  fi
}

get_bot_uptime() {
  local pid
  pid=$(systemctl show "$SERVICE_NAME" --property=MainPID 2>/dev/null | cut -d'=' -f2 || echo 0)
  if [[ "$pid" -gt 0 && -d "/proc/$pid" ]]; then
    local start_time
    start_time=$(stat -c %Y "/proc/$pid" 2>/dev/null || echo 0)
    local now
    now=$(date +%s)
    local uptime=$((now - start_time))
    local days=$((uptime / 86400))
    local hours=$(((uptime % 86400) / 3600))
    local mins=$(((uptime % 3600) / 60))
    echo "${days}d ${hours}h ${mins}m"
  else
    echo "unknown"
  fi
}

run_full_check() {
  local issues=()
  local warnings=()
  local status_report=""
  
  # Проверка сервиса
  local svc_status
  svc_status=$(check_service_status)
  if [[ "$svc_status" != "active" ]]; then
    issues+=("❌ Сервис бота НЕ активен (${svc_status})")
  else
    status_report+="✅ Сервис: активен\n"
  fi
  
  # Проверка порта Node API
  local node_port
  node_port=$(grep -m1 "^NODE_API_PORT=" "$ENV_FILE" 2>/dev/null | cut -d'=' -f2- || echo "8444")
  local port_status
  port_status=$(check_port_listening "$node_port")
  if [[ "$port_status" != "listening" ]]; then
    warnings+=("⚠️ Порт Node API ${node_port} не слушается")
  else
    status_report+="✅ Node API порт ${node_port}: слушается\n"
  fi
  
  # Проверка диска
  local disk_status
  disk_status=$(check_disk_usage 90)
  if [[ "$disk_status" == critical:* ]]; then
    local disk_usage="${disk_status#critical:}"
    issues+=("❌ Диск заполнен на ${disk_usage}%")
  else
    local disk_usage="${disk_status#ok:}"
    status_report+="✅ Диск: ${disk_usage}%\n"
  fi
  
  # Проверка AWG контейнера
  local awg_status
  awg_status=$(check_awg_container)
  if [[ "$awg_status" != "running" ]]; then
    warnings+=("⚠️ AWG контейнер не запущен")
  else
    status_report+="✅ AWG контейнер: работает\n"
  fi
  
  # Проверка БД
  local db_status
  db_status=$(check_database_health)
  case "$db_status" in
    healthy)
      status_report+="✅ База данных: здорова\n"
      ;;
    corrupted)
      issues+=("❌ База данных повреждена!")
      ;;
    missing)
      issues+=("❌ Файл базы данных отсутствует")
      ;;
  esac
  
  # Uptime
  local uptime
  uptime=$(get_bot_uptime)
  status_report+="⏱ Uptime: ${uptime}\n"
  
  # Формирование отчета
  local report="🔍 <b>Мониторинг AWG-TGBOT</b>\n\n"
  report+="${status_report}\n"
  
  if [[ ${#warnings[@]} -gt 0 ]]; then
    report+="<b>Предупреждения:</b>\n"
    for w in "${warnings[@]}"; do
      report+="${w}\n"
    done
    report+="\n"
  fi
  
  if [[ ${#issues[@]} -gt 0 ]]; then
    report+="<b>Критические проблемы:</b>\n"
    for i in "${issues[@]}"; do
      report+="${i}\n"
    done
    # Отправка алерта при проблемах
    send_telegram_alert "$report"
    echo "CRITICAL"
    return 1
  else
    report+="<b>Все системы в норме!</b>"
    echo "$report"
    return 0
  fi
}

# Main
case "${1:-check}" in
  check)
    run_full_check
    ;;
  alert-test)
    send_telegram_alert "🧪 Тестовое уведомление от системы мониторинга AWG-TGBOT"
    echo "Тестовый алерт отправлен"
    ;;
  status)
    echo "Service: $(check_service_status)"
    echo "Port ${node_port:-8444}: $(check_port_listening "${node_port:-8444}")"
    echo "Disk: $(check_disk_usage)"
    echo "AWG: $(check_awg_container)"
    echo "Database: $(check_database_health)"
    echo "Uptime: $(get_bot_uptime)"
    ;;
  *)
    echo "Usage: $0 {check|alert-test|status}"
    exit 1
    ;;
esac
