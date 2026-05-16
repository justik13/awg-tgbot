"""
Сервис для работы с платежной системой Platega.
Обертка над официальным SDK.
"""
import logging
import os
import sys
from typing import Optional, Dict, Any

# Добавляем путь к локальному SDK если он еще не в path
_sdk_path = os.path.join(os.path.dirname(__file__), 'platega-sdk-python')
if _sdk_path not in sys.path:
    sys.path.insert(0, _sdk_path)

try:
    # Импорт из загруженного SDK
    from platega import Platega, PlategaCallback, PlategaAPIError
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False
    Platega = None
    PlategaCallback = None
    PlategaAPIError = Exception

from config import PLATEGA_MERCHANT_ID, PLATEGA_SECRET_KEY

logger = logging.getLogger(__name__)


class PlategaService:
    """Сервис для взаимодействия с API Platega."""

    def __init__(self):
        if not SDK_AVAILABLE:
            logger.error("Platega SDK not installed. Please install requirements.")
            self.client = None
            return

        if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET_KEY:
            logger.warning("Platega credentials not configured in .env")
            self.client = None
            return

        try:
            # Инициализация клиента
            self.client = Platega(
                merchant_id=PLATEGA_MERCHANT_ID,
                secret=PLATEGA_SECRET_KEY
            )
            logger.info("Platega client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Platega client: {e}")
            self.client = None

    async def create_payment(
        self,
        amount: float,
        currency: str = "RUB",
        order_id: str = "",
        description: str = "Оплата подписки",
        return_url: Optional[str] = None,
        failed_url: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Создает платеж в Platega через СБП QR (метод 2).
        
        :param amount: Сумма платежа
        :param currency: Валюта (по умолчанию RUB)
        :param order_id: Уникальный ID заказа в нашей системе (user_id:sub_type)
        :param description: Описание платежа
        :param return_url: URL возврата при успехе (опционально)
        :param failed_url: URL возврата при ошибке (опционально)
        :return: dict с данными платежа (url, transaction_id) или None при ошибке
        """
        if not self.client:
            logger.error("Platega client is not initialized.")
            return None

        try:
            logger.info(f"Creating Platega payment for order {order_id}, amount {amount} {currency}")
            
            # Создание платежа через SDK (метод 2 = СБП QR)
            response = self.client.create_payment(
                amount=amount,
                currency=currency,
                payment_method=Platega.METHOD_SBP_QR,  # Метод 2 - СБП QR
                description=description,
                return_url=return_url,
                failed_url=failed_url,
                payload=order_id  # Передаем order_id как payload для callback
            )
            
            # Ожидаемая структура ответа:
            # {
            #   "transactionId": "txn_xxxxx",
            #   "redirect": "https://...",
            #   "status": "PENDING",
            #   "paymentMethod": "SBP_QR",
            #   "expiresIn": "..."
            # }
            
            if not response or "transactionId" not in response:
                logger.error(f"Invalid response from Platega: {response}")
                return None

            return {
                "transaction_id": response["transactionId"],
                "payment_url": response.get("redirect"),
                "status": response.get("status", "PENDING"),
                "payment_method": response.get("paymentMethod"),
                "expires_in": response.get("expiresIn"),
                "amount": amount,
                "currency": currency
            }

        except PlategaAPIError as e:
            logger.error(f"Platega API error: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error creating Platega payment: {e}")
            return None

    async def check_status(self, transaction_id: str) -> Optional[str]:
        """
        Проверяет статус транзакции.
        
        :param transaction_id: ID транзакции в Platega
        :return: Статус ('CONFIRMED', 'PENDING', 'CANCELED') или None
        """
        if not self.client:
            return None

        try:
            response = self.client.get_payment_status(transaction_id)
            return response.get("status")
        except Exception as e:
            logger.error(f"Error checking status for {transaction_id}: {e}")
            return None

    async def get_qr_code(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает QR-код для платежа через API Platega.
        
        :param transaction_id: ID транзакции в Platega
        :return: dict с данными QR (qr_url, qr_base64) или None
        """
        if not self.client:
            logger.error("Platega client is not initialized.")
            return None

        try:
            # Вызываем новый метод SDK для получения QR-кода
            response = self.client.get_qr_code(transaction_id)
            
            # API Platega возвращает QR данные в поле qr
            # Ожидаемая структура: {"qr": {"url": "...", "base64": "..."}, "redirect": "...", "status": "..."}
            qr_data = response.get("qr")
            
            if qr_data:
                return {
                    "qr_url": qr_data.get("url"),
                    "qr_base64": qr_data.get("base64"),
                    "payment_url": response.get("redirect"),
                    "status": response.get("status")
                }
            
            # Если QR нет в ответе, используем payment_url как fallback
            payment_url = response.get("redirect")
            if payment_url:
                return {
                    "qr_url": None,
                    "qr_base64": None,
                    "payment_url": payment_url,
                    "status": response.get("status")
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting QR code for {transaction_id}: {e}")
            return None

    @staticmethod
    def validate_callback(headers: Dict[str, str], body: str) -> tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Валидирует входящий callback от Platega.
        
        :param headers: Заголовки запроса
        :param body: Тело запроса (JSON строка)
        :return: (is_valid, payload, error_message)
        """
        if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET_KEY:
            return False, None, "Platega credentials not configured"
        
        try:
            callback_handler = PlategaCallback(
                merchant_id=PLATEGA_MERCHANT_ID,
                secret=PLATEGA_SECRET_KEY
            )
            
            is_valid = callback_handler.validate_raw(headers, body)
            
            if not is_valid:
                error = callback_handler.get_validation_error()
                logger.warning(f"Invalid callback: {error}")
                return False, None, error
            
            payload = callback_handler.get_payload()
            logger.info(f"Callback validated: Order {callback_handler.get_order_id()}, Status {callback_handler.get_status()}")
            
            return True, payload, None
            
        except Exception as e:
            logger.exception(f"Error validating callback: {e}")
            return False, None, str(e)


# Глобальный экземпляр сервиса
platega_service = PlategaService()
