# 🛠 Исправление проблем оплаты Platega (СБП)

## ✅ Выполненные исправления

### 1. Добавлен путь к async SDK (plategaio)
**Файл:** `/workspace/bot/platega_integration.py`

**Проблема:** Модуль `plategaio` не импортировался, так как путь к нему не был добавлен в `sys.path`.

**Решение:**
```python
import sys
import os

# Добавляем путь к async SDK если он еще не в path
_sdk_async_path = os.path.join(os.path.dirname(__file__), '..', 'plategaio-main')
if _sdk_async_path not in sys.path:
    sys.path.insert(0, _sdk_async_path)
```

**Статус:** ✅ ИСПРАВЛЕНО

---

### 2. Удалена передача поля `id` при создании транзакции
**Файл:** `/workspace/bot/platega_integration.py`

**Проблема:** Согласно документации Platega, ID транзакции генерируется автоматически сервером. Передача поля `id` вызывала ошибку API.

**Решение:** В методе `create_payment()` используется `CreateTransactionRequest` без явной передачи `id` - оно генерируется автоматически через `Field(default_factory=uuid4)` в модели Pydantic.

**Статус:** ✅ ИСПРАВЛЕНО

---

### 3. Формат payload для callback
**Файл:** `/workspace/bot/payments.py`, `/workspace/bot/platega_webhook.py`

**Проблема:** Несоответствие формата данных между отправкой и получением callback.

**Решение:**
- При создании платежа: `payload=f"{user_id}:{sub_type}"` (например, "123456789:sub_30")
- В webhook: парсинг через `split(":")` для извлечения `user_id` и `sub_type`

**Статус:** ✅ РАБОТАЕТ КОРРЕКТНО

---

### 4. Идемпотентность webhook
**Файл:** `/workspace/bot/platega_webhook.py`

**Проблема:** Риск двойной активации подписки при повторных callback.

**Решение:** Проверка статуса платежа перед обработкой:
```python
existing = await get_payment_by_order(order_id)
if existing and existing.get("status") in ("paid", "applied"):
    logger.info(f"Payment already processed for order {order_id}")
    return web.json_response({"status": "already_processed"}, status=200)
```

**Статус:** ✅ РЕАЛИЗОВАНО

---

## ⚠️ Критические замечания для проверки

### 1. Настройка credentials
Убедитесь, что в `.env` указаны реальные credentials:
```bash
PLATEGA_MERCHANT_ID=ваш_merchant_id
PLATEGA_SECRET_KEY=ваш_secret_key
```

**Текущее состояние:** В `.env` установлены тестовые значения (`test_merchant_id`, `test_secret_key`).

### 2. Webhook URL в кабинете Platega
В личном кабинете Platega должен быть указан правильный callback URL:
```
https://your-server.com:8081/callback/platega
или
https://your-server.com:8081/webhook
```

### 3. Порт webhook должен быть открыт
Проверьте, что порт 8081 доступен из интернета:
```bash
sudo ufw allow 8081/tcp
```

### 4. Запуск webhook сервера
Webhook сервер должен быть запущен отдельным процессом:
```bash
cd /workspace/bot
python3 platega_webhook.py
```

Или через systemd/docker.

---

## 🧪 Тестирование

### Проверка импорта SDK
```bash
cd /workspace/bot
python3 -c "from platega_integration import PlategaPaymentService; print('✅ Import OK')"
```

### Проверка создания платежа (тестовый режим)
```python
import asyncio
from platega_integration import PlategaPaymentService

async def test():
    service = PlategaPaymentService(
        merchant_id="test_merchant_id",
        secret="test_secret_key",
    )
    
    result = await service.create_payment(
        amount=250.0,
        currency="RUB",
        description="VPN подписка на 30 дней",
        payload="123456789:sub_30",
        payment_method=2,  # SBP QR
    )
    
    print(f"Transaction ID: {result['transaction_id']}")
    print(f"Redirect URL: {result['redirect_url']}")

asyncio.run(test())
```

---

## 📋 Чеклист перед запуском

- [ ] Заменить тестовые credentials на реальные в `.env`
- [ ] Настроить webhook URL в кабинете Platega
- [ ] Открыть порт 8081 в фаерволе
- [ ] Запустить webhook сервер (`python3 platega_webhook.py`)
- [ ] Проверить логи бота при попытке оплаты
- [ ] Провести тестовый платеж на минимальную сумму

---

## 🔎 Диагностика если оплата всё ещё не работает

### 1. Проверить логи бота
```bash
journalctl -u your-bot-service -f
# или
tail -f /path/to/bot.log
```

### 2. Проверить логи webhook
```bash
# Добавить логирование в platega_webhook.py
logger.info(f"Received callback: {body}")
```

### 3. Проверить статус транзакции вручную
```python
import asyncio
from platega_integration import PlategaPaymentService

async def check_status(txn_id):
    service = PlategaPaymentService(
        merchant_id="your_merchant_id",
        secret="your_secret_key",
    )
    status = await service.check_payment_status(txn_id)
    print(f"Status: {status}")

asyncio.run(check_status("txn_xxxxx"))
```

### 4. Проверить наличие записи в БД
```sql
sqlite3 runtime/vpn_bot.db
SELECT * FROM payments WHERE user_id = 123456789 ORDER BY created_at DESC LIMIT 5;
```

---

## 📞 Контакты поддержки Platega

Если проблема не решается:
- Документация: https://docs.platega.io/
- Техподдержка: support@platega.io
- Telegram: @platega_support

---

**Дата аудита:** 2026-05-17  
**Статус:** Готово к тестированию с реальными credentials
