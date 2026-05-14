"""
Platega Webhook Handler for VPN Bot
Handles callback notifications from Platega payment system.
"""
import asyncio
import logging
from aiohttp import web
from typing import Dict, Any

from config import PLATEGA_MERCHANT_ID, PLATEGA_SECRET, logger
from database import update_payment_status, get_payment_status, finalize_payment_and_job
from platega_integration import PlategaPaymentService

logger = logging.getLogger(__name__)


class PlategaWebhookHandler:
    """HTTP handler for Platega payment callbacks."""
    
    def __init__(self, merchant_id: str, secret: str):
        self.merchant_id = merchant_id
        self.secret = secret
        self.app = web.Application()
        self.app.router.add_post('/webhook/platega', self.handle_callback)
    
    async def handle_callback(self, request: web.Request) -> web.Response:
        """Handle incoming Platega webhook callback."""
        try:
            # Get headers
            received_merchant_id = request.headers.get('X-MerchantId', '')
            received_secret = request.headers.get('X-Secret', '')
            
            # Validate credentials
            if not received_merchant_id or received_merchant_id != self.merchant_id:
                logger.warning("Invalid X-MerchantId header in webhook")
                return web.Response(text='Invalid', status=401)
            
            if not received_secret or received_secret != self.secret:
                logger.warning("Invalid X-Secret header in webhook")
                return web.Response(text='Invalid', status=401)
            
            # Parse body
            try:
                payload = await request.json()
            except Exception as e:
                logger.warning(f"Invalid JSON in webhook body: {e}")
                return web.Response(text='Invalid JSON', status=400)
            
            # Validate required fields
            required_fields = ['id', 'amount', 'currency', 'status', 'paymentMethod']
            for field in required_fields:
                if field not in payload:
                    logger.warning(f"Missing required field in webhook: {field}")
                    return web.Response(text=f'Missing field: {field}', status=400)
            
            transaction_id = payload.get('id')
            status = payload.get('status')
            custom_payload = payload.get('payload', '')  # This contains our order/user info
            
            logger.info(
                "Received Platega webhook: transaction_id=%s status=%s payload=%s",
                transaction_id, status, custom_payload
            )
            
            # Check if payment is successful
            if status == 'CONFIRMED':
                # Update payment status in database
                await self._process_successful_payment(transaction_id, custom_payload, payload)
                return web.Response(text='OK', status=200)
            elif status in ('CANCELED', 'CHARGEBACKED'):
                # Mark payment as failed
                await self._process_failed_payment(transaction_id, status)
                return web.Response(text='OK', status=200)
            else:
                # PENDING or unknown status - just acknowledge
                logger.info("Webhook received for pending/unknown status: %s", status)
                return web.Response(text='OK', status=200)
                
        except Exception as e:
            logger.exception(f"Error processing Platega webhook: {e}")
            return web.Response(text='Internal Error', status=500)
    
    async def _process_successful_payment(
        self, 
        transaction_id: str, 
        custom_payload: str,
        full_payload: Dict[str, Any]
    ) -> None:
        """Process successful payment and activate subscription."""
        try:
            # Parse custom payload to get user info
            # Format expected: "user_id:tariff_code" or just tariff_code
            parts = custom_payload.split(':') if ':' in custom_payload else [custom_payload]
            
            if len(parts) >= 2:
                try:
                    user_id = int(parts[0])
                    tariff_code = parts[1]
                except (ValueError, IndexError):
                    logger.warning(f"Could not parse custom payload: {custom_payload}")
                    return
            else:
                # Try to get from database by transaction_id
                payment_info = await get_payment_status(transaction_id)
                if not payment_info:
                    logger.warning(f"Payment not found in DB: {transaction_id}")
                    return
                user_id = payment_info.get('user_id')
                tariff_code = payment_info.get('payload', '')
            
            if not user_id:
                logger.warning(f"No user_id found for transaction {transaction_id}")
                return
            
            # Update payment status to CONFIRMED
            await update_payment_status(transaction_id, 'confirmed')
            
            # Finalize payment and trigger provisioning
            await finalize_payment_and_job(transaction_id)
            
            logger.info(
                "Successfully processed payment: transaction_id=%s user_id=%s tariff=%s",
                transaction_id, user_id, tariff_code
            )
            
        except Exception as e:
            logger.exception(f"Error finalizing successful payment {transaction_id}: {e}")
    
    async def _process_failed_payment(self, transaction_id: str, status: str) -> None:
        """Process failed/canceled payment."""
        try:
            db_status = 'canceled' if status == 'CANCELED' else 'chargebacked'
            await update_payment_status(transaction_id, db_status)
            logger.info("Marked payment as %s: transaction_id=%s", db_status, transaction_id)
        except Exception as e:
            logger.exception(f"Error updating failed payment {transaction_id}: {e}")
    
    def run(self, host: str = '0.0.0.0', port: int = 8080) -> None:
        """Run the webhook server."""
        web.run_app(self.app, host=host, port=port)


async def start_webhook_server(host: str = '0.0.0.0', port: int = 8080) -> web.AppRunner:
    """Start the Platega webhook server as part of the main bot application."""
    if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET:
        logger.info("Platega not configured, skipping webhook server startup")
        # Return a dummy runner that does nothing
        class DummyRunner:
            async def setup(self): pass
            async def cleanup(self): pass
        return DummyRunner()
    
    handler = PlategaWebhookHandler(PLATEGA_MERCHANT_ID, PLATEGA_SECRET)
    runner = web.AppRunner(handler.app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Platega webhook server started on http://{host}:{port}/webhook/platega")
    return runner


__all__ = ['start_webhook_server', 'PlategaWebhookHandler']
