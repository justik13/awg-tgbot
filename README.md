# awg-tgbot (selfhost)

## Что это
Это selfhost Telegram-бот для личного управления VPN-доступом через AWG / AmneziaWG.
Репозиторий рассчитан на одного оператора (владелец сервера) и небольшой домашний/личный setup.

## Что уже умеет
- Установка и переустановка через `awg-tgbot.sh`.
- `status`, `logs`, `diagnostics` для операционного контроля.
- Ручные `backup` и `restore` локально на сервере.
- Кнопочный admin-first интерфейс в Telegram.
- Поиск и проверка платежей.
- Промокоды.
- Информация по трафику и ключам.
- QoS / egress denylist политики.
- Настройки сервиса и текстовые overrides.

## Что не является целью сейчас
- Multi-country / multi-server оркестрация.
- Web-панель.
- Автоматический refund flow.

## Быстрый старт
1. Залить репозиторий на сервер (`git clone ...` или загрузка архива).
2. Запустить установщик:
   ```bash
   sudo bash awg-tgbot.sh
   ```
3. В меню выбрать пункт `1) Установить`, затем сценарий:
   - автоустановка;
   - ручная установка (с явными prompt-ами).
4. Проверить состояние:
   ```bash
   sudo bash awg-tgbot.sh status
   ```
5. Открыть бот в Telegram и проверить admin-панель.

## Основные действия после установки
- Переустановка/обновление: `sudo bash awg-tgbot.sh reinstall`
- Статус: `sudo bash awg-tgbot.sh status`
- Логи: `sudo bash awg-tgbot.sh logs`
- Диагностика: `sudo bash awg-tgbot.sh diagnostics`
- Ручной бэкап: `sudo bash awg-tgbot.sh backup`
- Восстановление: `sudo bash awg-tgbot.sh restore`
- Удаление: `sudo bash awg-tgbot.sh remove`

## Как устроены бэкапы
- В бэкап попадают: `.env`, рабочая БД бота, `metadata.txt`.
- Архивы хранятся локально: `/opt/amnezia/bot/backups`.
- Ручной restore:
  - выбор архива;
  - подтверждение;
  - pre-restore snapshot;
  - попытка rollback при ошибке;
  - восстановление runtime-пермиссий файлов.
- Autobackup:
  - запускается systemd timer-ом раз в день;
  - использует существующий `awg-tgbot.sh backup`;
  - применяет retention по `AUTO_BACKUP_KEEP_COUNT`.

## Важные selfhost настройки
- `API_TOKEN` — токен Telegram-бота.
- `ADMIN_ID` — ваш Telegram user_id администратора.
- `DOCKER_CONTAINER` — контейнер AWG/AmneziaWG.
- `WG_INTERFACE` — WG-интерфейс в контейнере.
- `SERVER_PUBLIC_KEY` — public key сервера для клиентских конфигов.
- `SERVER_IP` — endpoint сервера (`IP:port`).
- `CONFIGS_PER_USER` — сколько активных конфигов разрешено на пользователя.
- `QOS_ENABLED` — включение QoS.
- `DEFAULT_KEY_RATE_MBIT` — дефолтная скорость на ключ.
- `QOS_STRICT` — strict/soft режим ограничений QoS.
- `EGRESS_DENYLIST_ENABLED` — включение denylist по egress.
- `EGRESS_DENYLIST_MODE` — режим denylist (`soft`/`strict`).
- `AUTO_BACKUP_ENABLED` — включение daily autobackup.
- `AUTO_BACKUP_KEEP_COUNT` — сколько последних архивов хранить.

## Что проверять, если что-то сломалось
1. `status` — сервис/автозапуск/ветка/autobackup timer.
2. `logs` — ошибки приложения и installer-лог.
3. `diagnostics` — состояние Docker/AWG/env.
4. Проверить доступность Docker и AWG контейнера/интерфейса.
5. При критике — восстановиться из локального backup архива.

## Как я бы обновлял этот repo
1. Открыть PR с точечными изменениями.
2. Просмотреть diff и риски для selfhost.
3. Merge в `main`.
4. На сервере выполнить `reinstall`.
5. Smoke-check: запуск бота, выдача ключа, backup/restore, базовые admin actions.

## Где смотреть код
- `awg-tgbot.sh` — installer/операторские команды.
- `bot/app.py` — запуск и wiring приложения.
- `bot/handlers_admin.py` — admin handlers.
- `bot/handlers_user.py` — user handlers.
- `bot/network_policy.py` — QoS/denylist логика сети.
- `bot/content_settings.py` — тексты/override контента.
- `tests/` — регрессии и selfhost-проверки.
