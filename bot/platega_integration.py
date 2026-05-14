"""
Platega payment integration for VPN bot.
Uses plategaio (async SDK) for non-blocking operations.
"""
import uuid
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
    """Service for handling Platega payments."""
    
    def __init__(self, merchant_id: str, secret: str, base_url: str = "https://app.platega.io"):
        self.merchant_id = merchant_id
        self.secret = secret
        self.base_url = base_url
    
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
        from plategaio import PlategaAsyncClient, CreateTransactionRequest, PaymentDetails
        
        async with PlategaAsyncClient(
            merchant_id=self.merchant_id,
            secret=self.secret,
            base_url=self.base_url,
        ) as client:
            tx_request = CreateTransactionRequest(
                paymentMethod=payment_method,
                id=uuid.uuid4(),
                paymentDetails=PaymentDetails(amount=amount, currency=currency),
                description=description,
                return_url=return_url,
                failed_url=failed_url,
                payload=payload,
            )
            response = await client.create_transaction(tx_request)
            
            return {
                "transaction_id": response.transaction_id,
                "redirect_url": response.redirect,
                "status": response.status,
                "expires_in": response.expires_in,
            }
    
    async def check_payment_status(self, transaction_id: str) -> Dict[str, Any]:
        """Check the status of a Platega payment."""
        from plategaio import PlategaAsyncClient
        
        async with PlategaAsyncClient(
            merchant_id=self.merchant_id,
            secret=self.secret,
            base_url=self.base_url,
        ) as client:
            status_response = await client.get_transaction_status(transaction_id)
            
            return {
                "id": status_response.id,
                "status": status_response.status,
                "amount": status_response.payment_details.get("amount"),
                "currency": status_response.payment_details.get("currency"),
                "payment_method": status_response.payment_method,
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
