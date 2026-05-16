#!/bin/bash
# Запуск webhook сервера для Platega
cd /workspace/bot
python3 platega_webhook.py &
echo "Webhook server started on port 8081"
