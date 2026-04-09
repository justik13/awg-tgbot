# awg-tgbot (selfhost)

## Что это
`awg-tgbot` — selfhost Telegram-бот для управления доступом к AWG / AmneziaWG на одном сервере.
Проект ориентирован на персональный или небольшой private setup (один оператор/админ).

## Актуальные возможности
- Установка и переустановка через `awg-tgbot.sh`.
- Операционные команды: `status`, `logs`, `diagnostics`.
- Локальные бэкапы и восстановление (`backup`, `restore`).
- Ежедневный autobackup через systemd timer (`awg-tgbot-backup.timer`).
- Admin-first интерфейс в Telegram (выдача ключей, управление пользователями).
- Оплата подписок через Telegram Stars.
- Реферальная система.
- Промокоды.
- Рассылки (broadcast jobs).
- Egress denylist (домены + CIDR, режимы `soft`/`strict`).
- Гибкие текстовые настройки (контент/override).

## Что не является целью сейчас
- Multi-country / multi-server оркестрация.
- Web-панель.
- Автоматический refund flow.

## Быстрый старт
1. Клонировать репозиторий на сервер.
2. Запустить установщик:
   ```bash
   sudo bash awg-tgbot.sh
   ```
3. В интерактивном меню выбрать `Установить`.
4. Проверить состояние:
   ```bash
   sudo bash awg-tgbot.sh status
   ```
5. Открыть бота в Telegram и проверить admin-панель.

## Операторские команды
- Переустановка/обновление: `sudo bash awg-tgbot.sh reinstall`
- Статус: `sudo bash awg-tgbot.sh status`
- Логи: `sudo bash awg-tgbot.sh logs`
- Диагностика: `sudo bash awg-tgbot.sh diagnostics`
- Ручной бэкап: `sudo bash awg-tgbot.sh backup`
- Восстановление: `sudo bash awg-tgbot.sh restore`
- Синхронизация helper policy с `.env`: `sudo bash awg-tgbot.sh sync-helper-policy`
- Удаление (с выбором режима): `sudo bash awg-tgbot.sh remove`

## Бэкапы
- В архив попадают: `.env`, runtime-БД, `metadata.txt`.
- Локальное хранилище: `/opt/amnezia/bot/backups`.
- `restore` делает pre-restore snapshot и пытается rollback при ошибках.
- `autobackup` использует `awg-tgbot.sh backup` и применяет retention по `AUTO_BACKUP_KEEP_COUNT`.

## Критичные переменные `.env`
Минимум, без которого бот не стартует:
- `API_TOKEN` — токен Telegram-бота.
- `ADMIN_ID` — Telegram user id администратора.
- `SERVER_PUBLIC_KEY` — public key сервера AWG.
- `SERVER_IP` — endpoint в формате `IPv4:port`.
- `ENCRYPTION_SECRET` — ключ шифрования конфигов.

Базовые selfhost-настройки:
- `DOCKER_CONTAINER`, `WG_INTERFACE` — где живёт AWG.
- `CONFIGS_PER_USER` — лимит активных конфигов на пользователя.
- `SUPPORT_USERNAME`, `DOWNLOAD_URL`, `SERVER_NAME` — контент и UX.
- `STARS_PRICE_7_DAYS`, `STARS_PRICE_30_DAYS`, `STARS_PRICE_90_DAYS` — цены подписок.
- `REFERRAL_ENABLED`, `REFERRAL_INVITEE_BONUS_DAYS`, `REFERRAL_INVITER_BONUS_DAYS`.
- `EGRESS_DENYLIST_ENABLED`, `EGRESS_DENYLIST_MODE`, `EGRESS_DENYLIST_REFRESH_MINUTES`, `EGRESS_DENYLIST_DOMAINS`, `EGRESS_DENYLIST_CIDRS`.
- `AUTO_BACKUP_ENABLED`, `AUTO_BACKUP_KEEP_COUNT`.

## Если что-то сломалось
1. `status` — здоровье сервиса + таймер autobackup.
2. `logs` — логи приложения и installer.
3. `diagnostics` — Docker/AWG/env/помощник.
4. Проверить соответствие `.env` и helper policy (`sync-helper-policy`).
5. При необходимости восстановиться из локального backup-архива.


## Зависимости Python
Актуальные pinned-версии (проверено **2026-04-09**):
- `aiogram==3.27.0`
- `aiosqlite==0.22.1`
- `cryptography==46.0.7`
- `python-dotenv==1.2.2`
- `APScheduler==3.11.2`

## Локальная разработка
Из корня репозитория:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r bot/requirements.txt
python -m unittest -v tests/test_support_useful.py
```

## Структура проекта
- `awg-tgbot.sh` — установщик и операторские команды.
- `bot/app.py` — запуск приложения и фоновые worker'ы.
- `bot/handlers_admin.py` — admin handlers.
- `bot/handlers_user.py` — user handlers.
- `bot/network_policy.py` — denylist-логика.
- `bot/payments.py` — биллинг/Stars.
- `bot/referrals.py` — реферальная логика.
- `tests/` — тесты.
