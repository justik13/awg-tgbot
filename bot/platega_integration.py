"""
Platega payment integration for VPN bot.
Uses plategaio (async SDK) for non-blocking operations.
"""
import sys
import httpx
import os
from typing import Optional, Dict, Any
from datetime import timedelta

# Добавляем путь к async SDK если он еще не в path
_sdk_async_path = os.path.join(os.path.dirname(__file__), '..', 'plategaio-main')
if _sdk_async_path not in sys.path:
    sys.path.insert(0, _sdk_async_path)

try:
    from plategaio import PlategaAsyncClient, CreateTransactionRequest, PaymentDetails
    PLATEGAIO_AVAILABLE = True
except ImportError as e:
    PLATEGAIO_AVAILABLE = False
    logger.error(f"Failed to import plategaio: {e}")

from aiogram import Bot, types
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import logger
from database import save_payment, upsert_payment_precheck, update_payment_status, get_payment_status

# Payment method constants from Platega
PLATEGA_METHOD_SBP_QR = 2
PLATEGA_METHOD_CARDS_RUB = 10

# Platega payment statuses
PLATEGA_STATUS_PENDING = "PENDING"
PLATEGA_STATUS_CONFIRMED = "CONFIRMED"
PLATEGA_STATUS_CANCELED = "CANCELED"
PLATEGA_STATUS_CHARGEBACKED = "CHARGEBACKED"


class PlategaPaymentService:
    """Service for handling Platega payments."""
    
    def __init__(self, merchant_id: str, secret: str, base_url: str = "https://app.platega.io"):
        self.merchant_id = merchant_id
        self.secret = secret
        self.base_url = base_url
        
        if not PLATEGAIO_AVAILABLE:
            logger.error("plategaio library not available - payments will not work")
    
    async def create_payment(
        self,
        amount: float,
        currency: str,
        description: str,
        payload: str,
        return_url: Optional[str] = None,
        failed_url: Optional[str] = None,
        payment_method: int = PLATEGA_METHOD_SBP_QR,
    ) -> Dict[str, Any]:
        """Create a new Platega payment transaction."""
        headers = {
            "X-MerchantId": self.merchant_id,
            "X-Secret": self.secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        payload_json = {
            "paymentMethod": payment_method,
            "paymentDetails": {"amount": amount, "currency": currency},
            "description": description,
        }
        if return_url: payload_json["returnUrl"] = return_url
        if failed_url: payload_json["failedUrl"] = failed_url
        if payload: payload_json["payload"] = payload
            
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            response = await client.post(f"{self.base_url}/transaction/process", json=payload_json)
            response.raise_for_status()
            data = response.json()
            
            return {
                "transaction_id": data.get("transactionId"),
                "payment_url": str(data.get("redirect") or "").replace("app.platega.io", "pay.platega.io"),
                "status": data.get("status"),
                "expires_in": data.get("expiresIn"),
            }

    async def check_payment_status(self, transaction_id: str) -> Dict[str, Any]:
        """Check the status of a Platega payment.
        
        Args:
            transaction_id: Transaction ID from Platega
            
        Returns:
            dict with id, status, amount, currency, payment_method, redirect
            
        Raises:
            RuntimeError: If plategaio is not available or API call fails
        """
        if not PLATEGAIO_AVAILABLE:
            raise RuntimeError("plategaio library not available")
        
        headers = {
            "X-MerchantId": self.merchant_id,
            "X-Secret": self.secret,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            response = await client.get(f"{self.base_url}/transaction/{transaction_id}")
            response.raise_for_status()
            data = response.json()
            
            return {
                "id": data.get("id") or data.get("transactionId"),
                "status": data.get("status"),
                "amount": data.get("paymentDetails", {}).get("amount"),
                "currency": data.get("paymentDetails", {}).get("currency"),
                "payment_method": data.get("paymentMethod"),
                "redirect": str(data.get("redirect") or "").replace("app.platega.io", "pay.platega.io"),
            }
    
    @staticmethod
    def is_success_status(status: str) -> bool:
        """Check if payment status indicates successful payment."""
        return status == PLATEGA_STATUS_CONFIRMED
    
    @staticmethod
    def is_pending_status(status: str) -> bool:
        """Check if payment is still pending."""
        return status == PLATEGA_STATUS_PENDING
    
    @staticmethod
    def is_failed_status(status: str) -> bool:
        """Check if payment failed or was canceled."""
        return status in (PLATEGA_STATUS_CANCELED, PLATEGA_STATUS_CHARGEBACKED)


def get_platega_payment_keyboard(transaction_id: str, tariff_label: str) -> types.InlineKeyboardMarkup:
    """Generate keyboard with Platega payment button."""
    builder = InlineKeyboardBuilder()
    builder.button(
        text=f"💳 Оплатить {tariff_label} через СБП",
        callback_data=f"platega_pay:{transaction_id}"
    )
    builder.button(
        text="🔄 Проверить статус оплаты",
        callback_data=f"platega_check:{transaction_id}"
    )
    builder.button(
        text="⬅️ Назад к тарифам",
        callback_data="show_buy_menu"
    )
    return builder.as_markup()


def get_payment_method_selection_kb(tariff_payload: str, stars_price: int, rub_price: float) -> types.InlineKeyboardMarkup:
    """Generate keyboard for selecting payment method (Stars vs Platega)."""
    builder = InlineKeyboardBuilder()
    
    # Stars button
    builder.button(
        text=f"⭐ Telegram Stars ({stars_price}⭐)",
        callback_data=f"pay_stars:{tariff_payload}"
    )
    
    # Platega SBP button
    builder.button(
        text=f"💳 СБП ({rub_price}₽)",
        callback_data=f"pay_platega:{tariff_payload}"
    )
    
    builder.button(
        text="⬅️ Назад в профиль",
        callback_data="open_profile"
    )
    
    builder.adjust(1)  # One button per row
    return builder.as_markup()
