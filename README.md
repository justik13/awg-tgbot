# 🤖 awg-tgbot

Telegram-бот для управления доступом к AmneziaWG на одном сервере. Selfhost решение для персонального использования.

## ✨ Возможности

- 🔧 Установка и управление через единый скрипт
- 👤 Admin-панель в Telegram (пользователи, ключи, подписки)
- 💳 Оплата через Telegram Stars
- 🔗 Реферальная система и промокоды
- 📦 Автобэкапы через systemd timer
- 🛡️ Egress denylist (домены + CIDR)
- 📢 Рассылки пользователям

## 🚀 Установка

```bash
curl -fsSL https://raw.githubusercontent.com/YOUR_USERNAME/awg-tgbot/main/awg-tgbot.sh | sudo bash
```

Или клонируйте репозиторий и запустите:

```bash
git clone https://github.com/YOUR_USERNAME/awg-tgbot.git
cd awg-tgbot
sudo bash awg-tgbot.sh
```

## ⚙️ Конфигурация

Создайте файл `.env` с необходимыми переменными:

```env
# Обязательные
API_TOKEN=your_bot_token
ADMIN_ID=your_telegram_id
SERVER_PUBLIC_KEY=wg_public_key
SERVER_IP=x.x.x.x:51820
ENCRYPTION_SECRET=your_secret_key

# Опциональные
CONFIGS_PER_USER=3
STARS_PRICE_7_DAYS=100
STARS_PRICE_30_DAYS=300
REFERRAL_ENABLED=true
AUTO_BACKUP_ENABLED=true
```

## ▶️ Запуск

После установки бот запускается автоматически через systemd:

```bash
# Проверка статуса
sudo bash awg-tgbot.sh status

# Просмотр логов
sudo bash awg-tgbot.sh logs

# Диагностика
sudo bash awg-tgbot.sh diagnostics
```

## 📁 Структура проекта

```
awg-tgbot/
├── awg-tgbot.sh          # Установщик и CLI утилиты
├── bot/
│   ├── app.py            # Точка входа
│   ├── handlers_admin.py # Админ-хендлеры
│   ├── handlers_user.py  # Пользовательские хендлеры
│   ├── network_policy.py # Egress denylist
│   ├── payments.py       # Биллинг (Stars)
│   └── referrals.py      # Реферальная система
├── tests/                # Тесты
└── packaging/            # Пакеты для дистрибуции
```

## 🛠️ Команды

| Команда | Описание |
|---------|----------|
| `status` | Проверка состояния сервиса |
| `logs` | Просмотр логов |
| `diagnostics` | Диагностика системы |
| `backup` | Ручной бэкап |
| `restore` | Восстановление из бэкапа |
| `reinstall` | Переустановка |
| `remove` | Удаление |
| `sync-helper-policy` | Синхронизация с `.env` |

## 📦 Технологии

- **Python 3.10+**
- **aiogram 3.x** — Telegram Bot API
- **Docker** — контейнеризация AWG
- **SQLite** — хранение данных
- **systemd** — управление сервисом

## 📋 Зависимости

```txt
aiogram==3.27.0
aiosqlite==0.22.1
cryptography==46.0.7
python-dotenv==1.2.2
APScheduler==3.11.2
```

## 🧪 Разработка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r bot/requirements.txt
python -m unittest -v tests/
```

## 📄 License

MIT License
