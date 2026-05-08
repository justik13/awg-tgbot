# 🌐 awg-tgbot (multi-node)

Telegram-бот для управления доступом к **AmneziaWG** с поддержкой **распределённой multi-node архитектуры**.

Проект эволюционировал из single-server решения в масштабируемую систему с центральным сервером управления и удалёнными нодами в разных локациях.

---

## 📖 Оглавление

- [Возможности](#-возможности)
- [Архитектура](#-архитектура)
- [Требования](#-требования)
- [Установка Main-сервера](#-установка-main-сервера)
- [Настройка удалённых нод](#-настройка-удалённых-нод)
- [ENV переменные](#-env-переменные)
- [Запуск и управление](#-запуск-и-управление)
- [Бэкапы](#-бэкапы)
- [Структура проекта](#-структура-проекта)
- [Troubleshooting](#-troubleshooting)
- [FAQ](#-faq)
- [Roadmap](#-roadmap)

---

## ✨ Возможности

### Для пользователей:
- 🔑 Генерация WireGuard конфигов через Telegram
- 🌍 Выбор страны/ноды для подключения
- 📱 До 2 устройств на пользователя (настраивается)
- 💳 Оплата подписки через Telegram Stars (7/30/90 дней)
- 🔄 Перевыпуск ключей, смена страны
- 📊 Статистика трафика по устройствам
- 👥 Реферальная система с бонусами
- 🎁 Промокоды

### Для администраторов:
- ⚙️ Управление пользователями и подписками
- 📢 Рассылки с прогрессом и статистикой
- 🏷 Промокоды (создание, отключение, статистика)
- 💰 Управление ценами в Stars
- 🌐 Управление нодами (capacity, visibility)
- 🔒 Egress denylist (домены + CIDR, soft/strict режимы)
- 🛠 Maintenance mode
- 📈 Health checks и диагностика
- 📝 Audit log событий

### Multi-node возможности:
- 🌍 Распределение пользователей между нодами
- ⚖️ Балансировка по capacity
- 🔄 Graceful degradation при потере связи
- 📡 Heartbeat мониторинг (60 сек)
- 🚀 Централизованное управление командами

---

## 🏗 Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    MAIN SERVER                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │ Telegram    │  │ Node API     │  │ SQLite          │    │
│  │ Bot         │  │ :8443        │  │ Database        │    │
│  │ (aiogram)   │◄─┤ (aiohttp)    │◄─┤ - users         │    │
│  └─────────────┘  └──────────────┘  │ - keys          │    │
│         ▲               ▲           │ - nodes         │    │
│         │               │           │ - devices       │    │
│         │               │           │ - node_commands │    │
│         │               │           └─────────────────┘    │
└─────────┼───────────────┼──────────────────────────────────┘
          │               │
          │ HTTPS         │ HTTPS
          │ (heartbeat)   │ (register/heartbeat)
          │               │
    ┌─────▼──────┐  ┌─────▼──────┐  ┌─────▼──────┐
    │  Node 1    │  │  Node 2    │  │  Node N    │
    │  (agent)   │  │  (agent)   │  │  (agent)   │
    │  🇩🇪 DE     │  │  🇺🇸 US     │  │  🇫🇮 FI     │
    │  awg0      │  │  awg0      │  │  awg0      │
    └────────────┘  └────────────┘  └────────────┘
```

### Принцип работы

**Main Server:**
- Принимает входящие HTTPS запросы от агентов
- Хранит состояние всех нод в БД
- Очереди команд (`node_commands`) для управления пирами
- Node API на порту `NODE_API_PORT` (default: 8443)

**Agent (на удалённой ноде):**
- Регистрируется на Main через `/api/v1/node/register`
- Отправляет heartbeat каждые 60 сек через `/api/v1/node/heartbeat`
- Получает и применяет команды (add_peer, remove_peer, update_denylist)
- Работает в режиме graceful degradation при недоступности Main
- Детектирует params drift (изменение параметров awg0)

### Flow создания устройства

1. Пользователь выбирает слот → показывается список активных нод
2. Выбор страны → проверка capacity ноды
3. Генерация ключей WireGuard (private/public/psk)
4. Атомарное создание записи в `devices` + инкремент `active_configs`
5. Команда `add_peer` ставится в очередь `node_commands`
6. Агент получает команду через heartbeat → добавляет пира в awg0

---

## 📋 Требования

### Main Server:
- Ubuntu 20.04+ / Debian 11+
- Docker с контейнером AmneziaWG
- Python 3.10+
- Git
- Systemd

### Remote Node:
- Ubuntu 20.04+ / Debian 11+
- AmneziaWG установлен (`awg` команда в PATH)
- Python 3.10+ (для агента)
- Systemd
- Доступ к Main Server по HTTPS

---

## 🚀 Установка Main-сервера

### 1. Клонирование репозитория

```bash
git clone https://github.com/justik13/awg-tgbot.git
cd awg-tgbot
```

### 2. Запуск установщика

```bash
sudo bash awg-tgbot.sh
```

### 3. Интерактивное меню

Выберите `Установить` → следуйте инструкциям:
- Введите `API_TOKEN` от BotFather
- Введите `ADMIN_ID` (ваш Telegram user ID)
- Подтвердите параметры AWG (контейнер, интерфейс, public key)

### 4. Проверка статуса

```bash
sudo bash awg-tgbot.sh status
```

---

## 🌐 Настройка удалённых нод

### Вариант A: Автоматическая регистрация

1. Установите AmneziaWG на сервер
2. Создайте файл `/opt/amnezia/agent/node.env`:

```ini
MAIN_API_URL=https://your-main-server.com:8443
```

3. Запустите агента:

```bash
python3 /path/to/agent/agent.py
```

Агент автоматически зарегистрируется на Main и получит `api_token`.

### Вариант B: Ручная регистрация

1. Агент запустится без токена → будет ждать создания `node.env`
2. На Main сервере создайте запись в БД:

```sql
INSERT INTO nodes (name, ip, port, status, is_visible, capacity, country, flag_emoji, api_token)
VALUES ('Germany', '1.2.3.4', 51820, 'ready', 1, 50, 'Germany', '🇩🇪', 'your-secret-token');
```

3. Создайте `/opt/amnezia/agent/node.env`:

```ini
MAIN_API_URL=https://your-main-server.com:8443
```

4. Перезапустите агента

### Мониторинг ноды

```bash
# Логи агента
tail -f /opt/amnezia/agent/agent.log

# Состояние
cat /opt/amnezia/agent/state.json
```

---

## ⚙️ ENV переменные

### Обязательные (Main Server)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `API_TOKEN` | Токен Telegram-бота | `123456:ABC-DEF...` |
| `ADMIN_ID` | Telegram user ID админа | `123456789` |
| `SERVER_PUBLIC_KEY` | Public key сервера AWG | `base64...` |
| `SERVER_IP` | Endpoint в формате `IPv4:port` | `1.2.3.4:51820` |
| `ENCRYPTION_SECRET` | Ключ шифрования конфигов | `random-secret` |
| `NODE_API_PORT` | Порт Node API | `8443` |

### Базовые настройки

| Переменная | Описание | Default |
|------------|----------|---------|
| `DOCKER_CONTAINER` | Имя Docker контейнера AWG | `amnezia-awg2` |
| `WG_INTERFACE` | Интерфейс WireGuard | `awg0` |
| `CONFIGS_PER_USER` | Лимит устройств на пользователя | `2` |
| `SUPPORT_USERNAME` | Username поддержки | `@support` |
| `DOWNLOAD_URL` | Ссылка на скачивание клиента | (Amnezia WG) |
| `SERVER_NAME` | Название сервера | `My VPN` |

### Цены (Telegram Stars)

| Переменная | Описание | Default |
|------------|----------|---------|
| `STARS_PRICE_7_DAYS` | Цена 7 дней | `21` |
| `STARS_PRICE_30_DAYS` | Цена 30 дней | `50` |
| `STARS_PRICE_90_DAYS` | Цена 90 дней | `140` |

### Реферальная система

| Переменная | Описание | Default |
|------------|----------|---------|
| `REFERRAL_ENABLED` | Включить рефералы | `1` |
| `REFERRAL_INVITEE_BONUS_DAYS` | Бонус приглашённому | `5` |
| `REFERRAL_INVITER_BONUS_DAYS` | Бонус пригласившему | `3` |
| `REFERRAL_RECURRING_INVITER_BONUS_DAYS` | Рекуррентный бонус | `2` |

### Egress Denylist

| Переменная | Описание | Default |
|------------|----------|---------|
| `EGRESS_DENYLIST_ENABLED` | Включить denylist | `1` |
| `EGRESS_DENYLIST_MODE` | Режим (`soft`/`strict`) | `soft` |
| `EGRESS_DENYLIST_REFRESH_MINUTES` | Частота обновления | `30` |
| `EGRESS_DENYLIST_DOMAINS` | Список доменов | (gosuslugi, nalog...) |
| `EGRESS_DENYLIST_CIDRS` | Список CIDR | (пусто) |

### Бэкапы

| Переменная | Описание | Default |
|------------|----------|---------|
| `AUTO_BACKUP_ENABLED` | Включить автобэкапы | `1` |
| `AUTO_BACKUP_KEEP_COUNT` | Хранить последних бэкапов | `14` |

### Node (Agent)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `MAIN_API_URL` | URL Main сервера | `https://1.2.3.4:8443` |

---

## ▶️ Запуск и управление

### Операторские команды (Main Server)

```bash
# Статус сервиса
sudo bash awg-tgbot.sh status

# Логи приложения
sudo bash awg-tgbot.sh logs

# Диагностика (Docker/AWG/env)
sudo bash awg-tgbot.sh diagnostics

# Ручной бэкап
sudo bash awg-tgbot.sh backup

# Восстановление из бэкапа
sudo bash awg-tgbot.sh restore

# Обновление / переустановка
sudo bash awg-tgbot.sh reinstall

# Синхронизация helper policy с .env
sudo bash awg-tgbot.sh sync-helper-policy

# Удаление (с выбором режима)
sudo bash awg-tgbot.sh remove
```

### Управление агентом (Node)

```bash
# Статус systemd сервиса
systemctl status awg-tgbot-agent

# Логи
journalctl -u awg-tgbot-agent -f

# Перезапуск
sudo systemctl restart awg-tgbot-agent

# Остановка
sudo systemctl stop awg-tgbot-agent
```

---

## 💾 Бэкапы

### Что备份ируется:
- `.env` — конфигурация
- `runtime/vpn_bot.db` — база данных
- `metadata.txt` — метаданные

### Хранилище:
`/opt/amnezia/bot/backups/`

### Autobackup:
- Выполняется ежедневно через `awg-tgbot-backup.timer`
- Retention: `AUTO_BACKUP_KEEP_COUNT` (default: 14)
- Pre-restore snapshot при восстановлении

### Ручной бэкап:
```bash
sudo bash awg-tgbot.sh backup
```

### Восстановление:
```bash
sudo bash awg-tgbot.sh restore
```

---

## 📁 Структура проекта

```
awg-tgbot/
├── agent/                      # Агент для удалённых нод
│   └── agent.py                # Heartbeat + команды
├── bot/                        # Main сервер (бот)
│   ├── app.py                  # Точка входа + workers
│   ├── node_api.py             # HTTPS API для нод
│   ├── handlers_admin.py       # Admin handlers
│   ├── handlers_user.py        # User handlers
│   ├── database.py             # DB helpers + migrations
│   ├── migration_phase1.py     # Multi-node миграция
│   ├── awg_backend.py          # AWG операции
│   ├── payments.py             # Telegram Stars
│   ├── referrals.py            # Реферальная система
│   ├── network_policy.py       # Egress denylist
│   ├── config*.py              # Конфигурация
│   └── keyboards.py            # Inline клавиатуры
├── packaging/systemd/          # Systemd unit файлы
│   ├── awg-tgbot-backup.service
│   └── awg-tgbot-backup.timer
├── scripts/
│   └── awg-tgbot-autobackup.sh # Скрипт автобэкапа
├── tests/                      # Pytest тесты
├── awg-tgbot.sh                # Installer + CLI
└── README.md                   # Этот файл
```

---

## 🔧 Troubleshooting

### Main Server

#### Бот не запускается
```bash
# Проверка статуса
sudo bash awg-tgbot.sh status

# Логи
sudo bash awg-tgbot.sh logs

# Диагностика
sudo bash awg-tgbot.sh diagnostics
```

#### Ошибка "AWG недоступен"
- Проверьте статус контейнера: `docker ps | grep amnezia`
- Проверьте интерфейс: `awg show awg0`
- Синхронизируйте policy: `sudo bash awg-tgbot.sh sync-helper-policy`

#### Node API не доступен
- Проверьте порт: `netstat -tlnp | grep 8443`
- Проверьте firewall: `ufw status`
- Логи: `journalctl -u vpn-bot -f`

### Remote Node

#### Агент не регистрируется
- Проверьте `MAIN_API_URL` в `node.env`
- Проверьте доступность Main: `curl -k https://main:8443`
- Логи: `tail -f /opt/amnezia/agent/agent.log`

#### Params drift detected
- Изменились параметры awg0 (port, s1-s4, h1-h4)
- Требуется внимание администратора
- Проверьте конфиг AWG на ноде

#### Node offline
- Проверьте heartbeat в БД: `SELECT last_seen FROM nodes WHERE id = ?`
- Если > 5 минут — статус `degraded`
- Агент продолжит работать с последним состоянием

### Database

#### Проверка целостности
```python
# В Python консоли бота
from database import db_health_info
import asyncio
asyncio.run(db_health_info())
```

#### Очистка orphan peers
```python
from awg_backend import get_orphan_awg_peers
import asyncio
asyncio.run(get_orphan_awg_peers())
```

---

## ❓ FAQ

### Q: Как добавить новую ноду?
**A:** Установите AmneziaWG на сервер, настройте агент с `MAIN_API_URL`, агент автоматически зарегистрируется.

### Q: Как ограничить количество пользователей на ноде?
**A:** Измените `capacity` в таблице `nodes`:
```sql
UPDATE nodes SET capacity = 100 WHERE id = ?;
```

### Q: Как скрыть ноду от выбора?
**A:** Установите `is_visible = 0`:
```sql
UPDATE nodes SET is_visible = 0 WHERE id = ?;
```

### Q: Что происходит при потере связи с Main?
**A:** Агент работает в режиме graceful degradation:
- Сохраняет последнее состояние
- Продолжает обслуживать текущих пиров
- Пытается reconnect с экспоненциальным бэкоффом (5s → 120s)

### Q: Как обновить denylist на всех нодах?
**A:** Измените `EGRESS_DENYLIST_DOMAINS`/`CIDRS` в `.env` → примените через admin panel → команды отправятся в очереди `node_commands`.

### Q: Можно ли использовать без remote nodes?
**A:** Да. Main сервер работает как single-node установка. Таблица `nodes` будет содержать одну запись (localhost).

---

## 🗺 Roadmap

### Phase 1 ✅ (реализовано)
- ✅ Multi-node схема БД
- ✅ Миграция existing users
- ✅ Выбор ноды пользователем
- ✅ Capacity management

### Phase 2 ✅ (реализовано)
- ✅ Node API (register/heartbeat)
- ✅ Agent с heartbeat loop
- ✅ Command queue (add/remove peer)
- ✅ Params drift detection

### Phase 3 🚧 (в работе)
- ⏳ Admin UI для управления нодами
- ⏳ Мониторинг и алерты
- ⏳ Load balancing
- ⏳ Geo-based routing

### Phase 4 📋 (планируется)
- 🔜 Denylist sync на ноды
- 🔜 Traffic stats aggregation
- 🔜 Auto-scaling hints

---

## 📄 Лицензия

MIT License

---

## 🤝 Contributing

1. Fork репозиторий
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

---

## 📞 Контакты

- GitHub: [@justik13](https://github.com/justik13)
- Issues: [GitHub Issues](https://github.com/justik13/awg-tgbot/issues)
