"""
Webhook сервер для приема callback от Platega.
Запускается как отдельный процесс на порту 8081.
"""
import asyncio
import json
import logging
from aiohttp import web
from typing import Dict, Any

from database import db, get_payment_by_order, update_payment_status_by_order
from platega_service import platega_service

logger = logging.getLogger(__name__)


async def handle_platega_callback(request: web.Request) -> web.Response:
    """
    Обработчик входящих callback от Platega.
    
    Ожидает:
    - Headers: X-MerchantId, X-Secret
    - Body: JSON с данными транзакции
    
    Возвращает:
    - 200 OK при успешной обработке
    - 401/400 при ошибке валидации
    """
    try:
        # Получаем заголовки
        headers = dict(request.headers)
        
        # Получаем тело запроса
        body = await request.text()
        
        if not body:
            logger.warning("Empty callback body received")
            return web.json_response({"error": "Empty body"}, status=400)
        
        logger.info(f"Received Platega callback: {body[:200]}...")
        
        # Валидируем callback через сервис
        is_valid, payload, error = platega_service.validate_callback(headers, body)
        
        if not is_valid:
            logger.warning(f"Invalid callback: {error}")
            return web.json_response({"error": error}, status=401)
        
        # Извлекаем данные из payload
        status = payload.get("status")
        order_id = payload.get("payload")  # Наш order_id передается в payload
        transaction_id = payload.get("id")
        amount = payload.get("amount")
        
        logger.info(f"Callback validated: Order={order_id}, Status={status}, Txn={transaction_id}")
        
        if not order_id:
            logger.error("No order_id in callback payload")
            return web.json_response({"error": "No order_id"}, status=400)
        
        # Парсим order_id (формат: user_id:sub_type)
        try:
            user_id_str, sub_type = order_id.split(":")
            user_id = int(user_id_str)
        except ValueError:
            logger.error(f"Invalid order_id format: {order_id}")
            return web.json_response({"error": "Invalid order_id format"}, status=400)
        
        # Обрабатываем статус платежа
        if status == "CONFIRMED":
            # Успешная оплата
            logger.info(f"Payment confirmed for user {user_id}, sub {sub_type}")
            
            # Проверяем существующий платеж
            existing = await db.get_payment_by_order(order_id)
            if existing and existing.get("status") == "paid":
                logger.info(f"Payment already processed for order {order_id}")
                return web.json_response({"status": "already_processed"}, status=200)
            
            # Активируем подписку
            from payments import activate_subscription
            
            # Получаем информацию о тарифе
            from config import PLATEGA_PRICE_7_DAYS, PLATEGA_PRICE_30_DAYS, PLATEGA_PRICE_90_DAYS
            
            tariff_map = {
                "sub_7": int(PLATEGA_PRICE_7_DAYS),
                "sub_30": int(PLATEGA_PRICE_30_DAYS),
                "sub_90": int(PLATEGA_PRICE_90_DAYS),
            }
            
            expected_amount = tariff_map.get(sub_type)
            if expected_amount and abs(float(amount) - expected_amount) > 0.01:
                logger.warning(f"Amount mismatch: expected {expected_amount}, got {amount}")
                # Не прерываем обработку, так как сумма может отличаться из-за комиссий
            
            # Активируем подписку
            success = await activate_subscription(user_id, sub_type, "platega", transaction_id)
            
            if success:
                logger.info(f"Subscription activated for user {user_id}")
                # Обновляем запись о платеже
                await db.update_payment_status(order_id, "paid", transaction_id)
            else:
                logger.error(f"Failed to activate subscription for user {user_id}")
                
        elif status == "CANCELED":
            logger.info(f"Payment canceled for order {order_id}")
            await db.update_payment_status(order_id, "canceled", transaction_id)
            
        elif status == "CHARGEBACKED":
            logger.warning(f"Chargeback for order {order_id}")
            # TODO: Обработка chargeback (возможно блокировка пользователя)
            await db.update_payment_status(order_id, "chargeback", transaction_id)
        else:
            logger.info(f"Payment status {status} for order {order_id}")
        
        return web.json_response({"status": "ok"}, status=200)
        
    except Exception as e:
        logger.exception(f"Error processing callback: {e}")
        return web.json_response({"error": str(e)}, status=500)


async def handle_health(request: web.Request) -> web.Response:
    """Endpoint для проверки здоровья сервиса."""
    return web.json_response({"status": "ok", "service": "platega-webhook"})


def create_app() -> web.Application:
    """Создает и настраивает веб-приложение."""
    app = web.Application()
    
    # Роуты
    app.router.add_post('/callback/platega', handle_platega_callback)
    app.router.add_get('/health', handle_health)
    
    return app


def run_webhook_server(host: str = '0.0.0.0', port: int | None = None):
    """Запускает webhook сервер."""
    from config import PLATEGA_WEBHOOK_PORT
    if port is None:
        port = PLATEGA_WEBHOOK_PORT
    
    app = create_app()
    
    logger.info(f"Starting Platega webhook server on {host}:{port}")
    web.run_app(app, host=host, port=port, print=lambda x: logger.info(x))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_webhook_server()
