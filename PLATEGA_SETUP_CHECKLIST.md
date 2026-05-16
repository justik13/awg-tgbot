# 📋 ЧЕКЛИСТ НАСТРОЙКИ PLATEGA ДЛЯ ОПЛАТЫ ПО СБП

## ⚠️ КРИТИЧЕСКИ ВАЖНО - ВЫПОЛНИТЬ ПЕРЕД ЗАПУСКОМ

### 1. Получите реальные credentials из личного кабинета Platega
- Зайдите в личный кабинет Platega.io
- Скопируйте ваш `Merchant ID` (UUID формат)
- Скопируйте ваш `Secret Key` (API ключ)

### 2. Настройте файл .env
```bash
cd /workspace/bot
nano .env
```

Замените тестовые значения на реальные:
```env
PLATEGA_MERCHANT_ID=ваш_real_merchant_id_из_кабинета
PLATEGA_SECRET_KEY=ваш_real_secret_key_из_кабинета
PLATEGA_WEBHOOK_PORT=8081
PLATEGA_WEBHOOK_DOMAIN=https://ваш-домен.com
PLATEGA_PRICE_7_DAYS=100
PLATEGA_PRICE_30_DAYS=250
PLATEGA_PRICE_90_DAYS=700
PLATEGA_BASE_URL=https://app.platega.io
```

### 3. Настройте Webhook URL в кабинете Platega
- В личном кабинете Platega перейдите в: Настройки → Callback URLs
- Укажите URL: `https://ваш-домен.com:8081/callback/platega`
- Или через reverse proxy (nginx): `https://ваш-домен.com/platega/callback`

**Важно:** URL должен быть доступен из интернета и использовать HTTPS!

### 4. Откройте порт 8081 в фаерволе
```bash
# Для UFW
sudo ufw allow 8081/tcp

# Для firewalld
sudo firewall-cmd --permanent --add-port=8081/tcp
sudo firewall-cmd --reload

# Для iptables
sudo iptables -A INPUT -p tcp --dport 8081 -j ACCEPT
```

### 5. Запустите webhook сервер
```bash
cd /workspace/bot
python3 platega_webhook.py &
```

Или используйте скрипт:
```bash
./run_webhook.sh
```

### 6. Проверьте что webhook сервер запущен
```bash
curl http://localhost:8081/health
# Должен вернуть: {"status": "ok", "service": "platega-webhook"}

ps aux | grep platega_webhook
netstat -tlnp | grep 8081
```

### 7. Перезапустите бота
```bash
# Остановите текущего бота (Ctrl+C если запущен в терминале)
# Затем запустите снова
cd /workspace/bot
python3 app.py
```

### 8. Протестируйте оплату
1. Запустите бота в Telegram
2. Нажмите `/start`
3. Выберите тариф (например, 30 дней)
4. Выберите способ оплаты "💳 СБП"
5. Нажмите "Оплатить через СБП"
6. Должна появиться ссылка на оплату

### 9. Проверьте логи
```bash
# Логи бота
tail -f /workspace/bot/runtime/bot.log

# Логи webhook (в отдельном терминале)
tail -f /var/log/syslog | grep platega
```

## 🔍 Диагностика проблем

### Проблема: "Оплата через СБП временно недоступна"
**Решение:**
1. Проверьте что credentials настроены в .env
2. Проверьте логи бота на ошибки импорта plategaio
3. Убедитесь что plategaio доступен:
   ```bash
   python3 -c "import sys; sys.path.insert(0, '/workspace/plategaio-main'); from plategaio import PlategaAsyncClient; print('OK')"
   ```

### Проблема: Не приходит callback от Platega
**Решение:**
1. Проверьте что webhook URL правильно настроен в кабинете Platega
2. Проверьте что порт 8081 открыт в фаерволе
3. Проверьте что webhook сервер запущен:
   ```bash
   curl http://localhost:8081/health
   ```
4. Проверьте логи webhook сервера

### Проблема: Оплата прошла но подписка не активирована
**Решение:**
1. Проверьте логи на наличие ошибок при обработке callback
2. Проверьте что order_id передается в формате `user_id:sub_type`
3. Проверьте базу данных на наличие записей о платеже

## 📝 Формат order_id

При создании платежа мы передаем `payload` в формате:
```
{user_id}:{sub_type}
```

Примеры:
- `123456789:sub_7` - 7 дней для пользователя 123456789
- `123456789:sub_30` - 30 дней
- `123456789:sub_90` - 90 дней

Этот payload возвращается в callback и используется для активации подписки.

## 🔐 Безопасность

1. **Никогда не коммитьте .env с реальными credentials в git**
2. Используйте HTTPS для webhook URL
3. Регулярно обновляйте Secret Key в кабинете Platega
4. Мониторьте логи на подозрительную активность

## 📊 Мониторинг

Рекомендуется настроить мониторинг:
- Доступность webhook сервера
- Количество успешных/неуспешных платежей
- Время обработки callback
- Ошибки активации подписок

---
**После выполнения всех шагов оплата через СБП должна работать корректно.**
