"""
Platega payment integration for VPN bot.
Uses official platega-sdk-python (sync SDK wrapped in asyncio.to_thread for non-blocking operations).
"""
import uuid
import asyncio
from typing import Optional, Dict, Any
from datetime import timedelta

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
    """Service for handling Platega payments using official SDK."""
    
    def __init__(self, merchant_id: str, secret: str, base_url: str = "https://app.platega.io"):
        self.merchant_id = merchant_id
        self.secret = secret
        self.base_url = base_url
    
    def _create_sync_payment(
        self,
        amount: float,
        currency: str,
        description: str,
        payload: str,
        return_url: Optional[str] = None,
        failed_url: Optional[str] = None,
        payment_method: int = PLATEGA_METHOD_SBP_QR,
    ) -> Dict[str, Any]:
        """Synchronous payment creation using official SDK."""
        from platega import Platega
        
        client = Platega(
            merchant_id=self.merchant_id,
            secret=self.secret,
        )
        # Override API URL if needed
        if self.base_url != Platega.API_URL:
            client.api_url = self.base_url
        
        response = client.create_payment(
            amount=amount,
            currency=currency,
            payment_method=payment_method,
            description=description,
            return_url=return_url,
            failed_url=failed_url,
            payload=payload,
        )
        
        return {
            "transaction_id": response.get("transactionId"),
            "redirect_url": response.get("redirect"),
            "status": response.get("status"),
            "expires_in": response.get("expiresIn"),
        }
    
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
        """Create a new Platega payment transaction (async wrapper)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._create_sync_payment,
            amount,
            currency,
            description,
            payload,
            return_url,
            failed_url,
            payment_method,
        )
    
    def _check_sync_status(self, transaction_id: str) -> Dict[str, Any]:
        """Synchronous status check using official SDK."""
        from platega import Platega
        
        client = Platega(
            merchant_id=self.merchant_id,
            secret=self.secret,
        )
        if self.base_url != client.API_URL:
            client.api_url = self.base_url
        
        response = client.get_payment_status(transaction_id)
        
        return {
            "id": response.get("id"),
            "status": response.get("status"),
            "amount": response.get("paymentDetails", {}).get("amount"),
            "currency": response.get("paymentDetails", {}).get("currency"),
            "payment_method": response.get("paymentMethod"),
        }
    
    async def check_payment_status(self, transaction_id: str) -> Dict[str, Any]:
        """Check the status of a Platega payment (async wrapper)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._check_sync_status,
            transaction_id,
        )
    
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
