# Полный аудит Telegram-бота: Оплата и выдача подписок через Platega

## 📋 Резюме

Аудит проведен на основе:
- Исходного кода бота (`/workspace/bot/`)
- Официальной документации Platega.io API
- Схем данных и callback

**Общая оценка:** Система работает, но содержит **критические архитектурные проблемы** и **несоответствия документации API**.

---

## 🔴 КРИТИЧЕСКИЕ ПРОБЛЕМЫ

### 1. Несоответствие формату `order_id` (КРИТИЧНО)

**Проблема:** В документации Platega указано:
> **ID транзакции генерируется системой автоматически — не передавайте поле `id` в запросе.**

**В коде:**
```python
# platega_integration.py:54
tx_request = CreateTransactionRequest(
    id=uuid.uuid4(),  # ❌ ПЕРЕДАЕМ ID - ЭТО ОШИБКА!
    ...
)
```

**Последствия:** 
- API может отклонять запросы
- Конфликты с внутренней генерацией ID Platega
- Непредсказуемое поведение при создании платежей

**Решение:** Удалить поле `id` из запроса создания транзакции.

---

### 2. Дублирование клиентов Platega (КРИТИЧНО)

**Проблема:** Существуют два разных клиента:
1. `platega_service.py` - синхронный SDK (`platega-sdk-python`)
2. `platega_integration.py` - асинхронный SDK (`plategaio`)

**В коде:**
```python
# payments.py:67, 77-90
from platega_integration import PlategaPaymentService
from platega_service import platega_service

def get_platega_service() -> PlategaPaymentService | None:
    global _platega_service
    if _platega_service is None:
        _platega_service = PlategaPaymentService(...)  # plategaio
    return _platega_service
```

**Последствия:**
- Путаница какой клиент использовать
- Разная логика обработки ошибок
- Дублирование кода проверки статуса
- Webhook использует один клиент, создание платежа - другой

**Решение:** Унифицировать на одном SDK (рекомендуется `plategaio` для async).

---

### 3. Некорректная обработка `payload` в callback (КРИТИЧНО)

**Проблема:** В webhook ожидается `payload` как строка `user_id:sub_type`, но:

**В документации Platega:**
```json
{
  "id": "txn_xxx",
  "status": "CONFIRMED",
  "payload": "user_id:sub_type"  // Наш custom payload
}
```

**В коде webhook (platega_webhook.py:51):**
```python
order_id = payload.get("payload")  # ✅ Верно
```

**НО в `platega_integration.py:59`:**
```python
payload=json.dumps({"user_id": cb.from_user.id, "tariff": payload}),
```

**Проблема:** При создании платежа передается JSON, а в webhook ожидается строка!

**Последствия:**
- Webhook не сможет распарсить `order_id`
- Подписка не активируется после оплаты
- Потерянные платежи

**Решение:** Привести формат payload к единому виду (строка `user_id:sub_type`).

---

### 4. Отсутствие идемпотентности при активации (КРИТИЧНО)

**Проблема:** Функция `activate_subscription` вызывается из webhook без защиты от повторных вызовов.

**В коде (platega_webhook.py:98-105):**
```python
success = await activate_subscription(user_id, sub_type, "platega", transaction_id)
if success:
    await update_payment_status_by_order(order_id, "paid", transaction_id)
else:
    logger.error(f"Failed to activate subscription for user {user_id}")
```

**Проблема:** Если webhook придет дважды (Platega делает до 3 retry), возможна двойная выдача ключей.

**Решение:** Добавить проверку `payment_already_processed` ДО активации.

---

## 🟡 ПРОБЛЕМЫ СРЕДНЕЙ ВАЖНОСТИ

### 5. Неправильный формат `order_id` в БД

**Проблема:** В `activate_subscription` создается `order_id = f"{user_id}:{sub_type}"`, но:

**В коде (payments.py:1245-1248):**
```python
existing = await get_payment_by_order(f"{user_id}:{sub_type}")
if existing and existing.get("status") in ("paid", "applied"):
    return True  # Уже обработан
```

**Проблема:** Эта проверка выполняется ПОСЛЕ сохранения платежа в БД (строка 1258-1268), а не ДО.

**Решение:** Переместить проверку перед `save_payment`.

---

### 6. Магические числа статусов платежей

**В коде:**
```python
# platega_integration.py:16-23
PLATEGA_METHOD_SBP_QR = 2
PLATEGA_STATUS_PENDING = "PENDING"
```

**Проблема:** Дублирование констант между `platega.py`, `platega_integration.py`, `callback.py`.

**Решение:** Импортировать константы из основного SDK.

---

### 7. Отсутствие таймаутов БД в webhook

**В коде (platega_webhook.py):** Нет явных таймаутов на операции БД.

**Риск:** При высокой нагрузке webhook может завершиться по таймауту (60 сек), Platega сделает retry.

**Решение:** Добавить таймауты на DB операции.

---

### 8. Логирование чувствительных данных

**В коде (platega_webhook.py:40):**
```python
logger.info(f"Received Platega callback: {body[:200]}...")
```

**Проблема:** В логах могут быть полные данные платежа включая суммы.

**Решение:** Логировать только `transaction_id` и `status`.

---

## 🟢 МИНОРЫ

### 9. Неиспользуемый метод `get_qr_code`

В `platega_service.py` есть метод `get_qr_code`, но он не используется в основном потоке оплаты.

### 10. Ручное управление транзакциями БД

В `update_payment_status_by_order` используется ручное управление транзакцией:
```python
await db.execute("BEGIN IMMEDIATE")
```

Рекомендуется использовать контекстный менеджер.

---

## 📊 АНАЛИЗ ПОТОКА ОПЛАТЫ

### Текущий поток (Platega SBP):

1. **Создание платежа** (`pay_platega_handler`):
   ```
   User → CB_PAY_PLATEGA_PREFIX → create_payment() → save_payment(status="pending")
   ```

2. **Оплата пользователем**:
   ```
   User переходит по redirect URL → Оплачивает через СБП
   ```

3. **Callback от Platega** (`handle_platega_callback`):
   ```
   Platega → POST /callback/platega → validate_callback() → activate_subscription()
   ```

4. **Активация подписки** (`activate_subscription`):
   ```
   ensure_user_exists() → check existing payment → save_payment() → process_payment_provisioning()
   → issue_subscription() → finalize_payment_and_job() → send key to user
   ```

### Проблемные места:

| Шаг | Проблема | Риск |
|-----|----------|------|
| Создание платежа | Передается `id=uuid.uuid4()` | Отказ API |
| Создание платежа | `payload=json.dumps({...})` | Несоответствие webhook |
| Webhook | Проверка идемпотентности после активации | Двойная выдача |
| Активация | Сохранение платежа до проверки существующего | Дубликаты в БД |

---

## ✅ ПЛАН ИСПРАВЛЕНИЙ

### Приоритет 1 (Критично - исправить немедленно):

1. **Удалить поле `id` из CreateTransactionRequest**
   ```python
   # platega_integration.py:52-60
   tx_request = CreateTransactionRequest(
       # id=uuid.uuid4(),  # ❌ УДАЛИТЬ
       payment_method=payment_method,
       ...
   )
   ```

2. **Исправить формат payload**
   ```python
   # payments.py:313
   payload=f"{cb.from_user.id}:{payload}",  # ✅ Строка вместо JSON
   ```

3. **Добавить идемпотентность в webhook**
   ```python
   # platega_webhook.py:74-78
   existing = await get_payment_by_order(order_id)
   if existing and existing.get("status") == "paid":
       logger.info(f"Payment already processed for order {order_id}")
       return web.json_response({"status": "already_processed"}, status=200)
   ```

4. **Переместить проверку существующего платежа в activate_subscription**
   ```python
   # payments.py:1244-1250
   # ПЕРЕМЕСТИТЬ ПЕРЕД save_payment!
   existing = await get_payment_by_order(f"{user_id}:{sub_type}")
   if existing and existing.get("status") in ("paid", "applied"):
       logger.info(f"Payment already processed for user {user_id}")
       return True
   ```

### Приоритет 2 (Важно - в течение недели):

5. **Унифицировать SDK**
   - Удалить `platega_service.py` или сделать оберткой над `plategaio`
   - Обновить все импорты

6. **Добавить таймауты БД**
   ```python
   # platega_webhook.py
   import asyncio
   async with asyncio.timeout(30):
       # DB operations
   ```

7. **Улучшить логирование**
   ```python
   logger.info(f"Callback: txn={transaction_id}, status={status}, order={order_id}")
   ```

### Приоритет 3 (Улучшения):

8. Добавить мониторинг failed платежей
9. Создать админ-команды для ручного восстановления
10. Написать тесты на идемпотентность

---

## 🔐 БЕЗОПАСНОСТЬ

### Что реализовано хорошо:
- ✅ Валидация webhook через `X-MerchantId` и `X-Secret`
- ✅ Блокировки на уровне БД (`claim_payment_and_job_for_provisioning`)
- ✅ Audit log для всех операций
- ✅ Retry механизм с экспоненциальной задержкой

### Что требует улучшения:
- ⚠️ Нет rate limiting на webhook endpoint
- ⚠️ Нет шифрования sensitive данных в логах
- ⚠️ Нет alert'ов при множественных failed попытках

---

## 📈 МЕТРИКИ ДЛЯ МОНИТОРИНГА

Рекомендуется добавить метрики:
1. Количество платежей в статусе `pending` > 1 часа
2. Процент успешных активаций после webhook
3. Среднее время между оплатой и выдачей ключа
4. Количество retry webhook от Platega

---

## ВЫВОД

Система оплаты через Platega **работоспособна**, но содержит критические уязвимости в обработке `payload` и идемпотентности. 

**Рекомендация:** Немедленно применить исправления Приоритета 1 перед приемом реальных платежей.

