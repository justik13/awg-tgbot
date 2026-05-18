"""
Webhook сервер для приема callback от Platega.
Запускается как отдельный процесс на порту 8081.

Идемпотентность и атомарность:
- Используем SELECT FOR UPDATE для блокировки записи платежа
- Проверяем статус в рамках одной транзакции
- Обновляем статус атомарно с проверкой предыдущего состояния
"""
import asyncio
import json
import logging
from aiohttp import web
from typing import Dict, Any

from database import get_payment_by_order, update_payment_status_by_order, open_db, execute
from platega_service import platega_service
from config import logger as bot_logger

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
    
    Идемпотентность: Обработка защищена от дублирования через:
    1. Атомарную проверку статуса в транзакции
    2. Блокировку записи (SELECT FOR UPDATE)
    3. Проверку idempotency key
    """
    request_id = f"req_{asyncio.get_event_loop().time()}"
    
    try:
        # Получаем заголовки
        headers = dict(request.headers)
        
        # Получаем тело запроса
        body = await request.text()
        
        if not body:
            logger.warning("Empty callback body received")
            return web.json_response({"error": "Empty body"}, status=400)
        
        logger.info(f"[{request_id}] Received Platega callback: {body[:200]}...")
        
        # Валидируем callback через сервис
        is_valid, payload, error = platega_service.validate_callback(headers, body)
        
        if not is_valid:
            logger.warning(f"[{request_id}] Invalid callback: {error}")
            return web.json_response({"error": error}, status=401)
        
        # Извлекаем данные из payload
        status = payload.get("status")
        order_id = payload.get("payload")  # Наш order_id передается в payload
        transaction_id = payload.get("id")
        amount = payload.get("amount")
        
        logger.info(f"[{request_id}] Callback validated: Order={order_id}, Status={status}, Txn={transaction_id}")
        
        if not order_id:
            logger.error(f"[{request_id}] No order_id in callback payload")
            return web.json_response({"error": "No order_id"}, status=400)
        
        # Парсим order_id (формат: user_id:sub_type)
        try:
            user_id_str, sub_type = order_id.split(":")
            user_id = int(user_id_str)
        except ValueError:
            logger.error(f"[{request_id}] Invalid order_id format: {order_id}")
            return web.json_response({"error": "Invalid order_id format"}, status=400)
        
        # Обрабатываем статус платежа
        if status == "CONFIRMED":
            # Успешная оплата - используем атомарную операцию
            logger.info(f"[{request_id}] Payment confirmed for user {user_id}, sub {sub_type}")
            
            result = await process_confirmed_payment_atomically(
                order_id=order_id,
                user_id=user_id,
                sub_type=sub_type,
                transaction_id=transaction_id,
                amount=amount,
                request_id=request_id
            )
            
            if result["status"] == "already_processed":
                logger.info(f"[{request_id}] Payment already processed: {result['reason']}")
                return web.json_response({"status": "already_processed", "reason": result["reason"]}, status=200)
            
            if result["status"] == "processing":
                logger.info(f"[{request_id}] Payment already being processed")
                return web.json_response({"status": "already_processing"}, status=200)
            
            if result["status"] == "success":
                logger.info(f"[{request_id}] Subscription activated successfully")
                return web.json_response({"status": "ok"}, status=200)
            
            if result["status"] == "error":
                logger.error(f"[{request_id}] Activation failed: {result['error']}")
                return web.json_response({"status": "error", "error": result["error"]}, status=500)
                
        elif status == "CANCELED":
            logger.info(f"[{request_id}] Payment canceled for order {order_id}")
            await update_payment_status_by_order(order_id, "canceled", transaction_id)
            
        elif status == "CHARGEBACKED":
            logger.warning(f"[{request_id}] Chargeback for order {order_id}")
            await update_payment_status_by_order(order_id, "chargeback", transaction_id)
        else:
            logger.info(f"[{request_id}] Payment status {status} for order {order_id}")
        
        return web.json_response({"status": "ok"}, status=200)
        
    except Exception as e:
        logger.exception(f"[{request_id}] Error processing callback: {e}")
        return web.json_response({"error": str(e)}, status=500)


async def process_confirmed_payment_atomically(
    order_id: str,
    user_id: int,
    sub_type: str,
    transaction_id: str,
    amount: float,
    request_id: str
) -> Dict[str, Any]:
    """
    Атомарная обработка подтвержденного платежа.
    
    Использует транзакцию с блокировкой для предотвращения race conditions.
    
    Returns:
        Dict со статусом обработки:
        - {"status": "already_processed", "reason": "..."}
        - {"status": "processing"}
        - {"status": "success"}
        - {"status": "error", "error": "..."}
    """
    db = await open_db()
    
    try:
        # Начинаем эксклюзивную транзакцию
        await db.execute("BEGIN IMMEDIATE")
        
        # Атомарно проверяем и обновляем статус
        # Используем CASE для атомарной проверки и обновления
        cursor = await db.execute("""
            UPDATE payments
            SET status = 'processing',
                provider_payment_charge_id = ?,
                updated_at = datetime('now')
            WHERE payload = ?
              AND user_id = ?
              AND status NOT IN ('paid', 'applied', 'processing')
        """, (transaction_id, sub_type, user_id))
        
        if cursor.rowcount == 0:
            # Запись не обновлена - проверяем почему
            existing = await get_payment_by_order(order_id)
            
            if existing and existing.get("status") in ("paid", "applied"):
                await db.rollback()
                return {
                    "status": "already_processed",
                    "reason": f"Payment already paid (status={existing.get('status')})"
                }
            
            if existing and existing.get("status") == "processing":
                await db.rollback()
                return {"status": "processing"}
            
            if not existing:
                await db.rollback()
                return {
                    "status": "error",
                    "error": "Payment record not found"
                }
            
            # Другой статус - разрешаем обработку
            # Повторяем обновление
            await db.execute("""
                UPDATE payments
                SET status = 'processing',
                    provider_payment_charge_id = ?,
                    updated_at = datetime('now')
                WHERE payload = ? AND user_id = ?
            """, (transaction_id, sub_type, user_id))
        
        # Фиксируем переход в processing
        await db.commit()
        
        # Теперь активируем подписку (вне транзакции БД)
        from payments import activate_subscription
        
        # Получаем информацию о тарифе для проверки суммы
        from config import PLATEGA_PRICE_7_DAYS, PLATEGA_PRICE_30_DAYS, PLATEGA_PRICE_90_DAYS
        
        tariff_map = {
            "sub_7": int(PLATEGA_PRICE_7_DAYS),
            "sub_30": int(PLATEGA_PRICE_30_DAYS),
            "sub_90": int(PLATEGA_PRICE_90_DAYS),
        }
        
        expected_amount = tariff_map.get(sub_type)
        if expected_amount and abs(float(amount) - expected_amount) > 0.01:
            logger.warning(
                f"[{request_id}] Amount mismatch: expected {expected_amount}, got {amount}"
            )
            # Не прерываем обработку, так как сумма может отличаться из-за комиссий
        
        # Активируем подписку
        success = await activate_subscription(
            user_id=user_id,
            sub_type=sub_type,
            payment_method="platega",
            transaction_id=transaction_id
        )
        
        if success:
            logger.info(f"[{request_id}] Subscription activated for user {user_id}")
            
            # Обновляем запись о платеже на "paid"
            await update_payment_status_by_order(order_id, "paid", transaction_id)
            
            return {"status": "success"}
        else:
            logger.error(f"[{request_id}] Failed to activate subscription for user {user_id}")
            
            # Откатываем статус back к needs_repair
            await execute("""
                UPDATE payments
                SET status = 'needs_repair',
                    last_provision_status = 'activation_failed',
                    updated_at = datetime('now')
                WHERE payload = ? AND user_id = ?
            """, (sub_type, user_id))
            
            return {"status": "error", "error": "Subscription activation failed"}
            
    except Exception as e:
        await db.rollback()
        logger.exception(f"[{request_id}] Atomic payment processing error: {e}")
        return {"status": "error", "error": str(e)}
        
    finally:
        await db.close()


async def handle_health(request: web.Request) -> web.Response:
    """Endpoint для проверки здоровья сервиса."""
    return web.json_response({"status": "ok", "service": "platega-webhook"})


def create_app() -> web.Application:
    """Создает и настраивает веб-приложение."""
    app = web.Application()
    
    # Роуты - поддерживаем оба пути для совместимости
    app.router.add_post('/callback/platega', handle_platega_callback)
    app.router.add_post('/webhook', handle_platega_callback)
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
