"""
Platega Callback Handler
"""

import json
from typing import Dict, Any, Optional

from .exceptions import PlategaValidationError


class PlategaCallback:
    """
    Handler for incoming Platega payment callbacks
    
    Example (Flask):
        @app.route('/callback', methods=['POST'])
        def callback():
            callback = PlategaCallback('merchant_id', 'secret')
            if callback.validate(request):
                if callback.is_success():
                    order_id = callback.get_order_id()
                    # Process payment
                return 'OK', 200
            return 'Invalid', 401
    
    Example (Django):
        def callback_view(request):
            callback = PlategaCallback('merchant_id', 'secret')
            if callback.validate_django(request):
                if callback.is_success():
                    order_id = callback.get_order_id()
                    # Process payment
                return HttpResponse('OK')
            return HttpResponse('Invalid', status=401)
    """
    
    # Status constants
    STATUS_CONFIRMED = 'CONFIRMED'
    STATUS_CANCELED = 'CANCELED'
    STATUS_CHARGEBACKED = 'CHARGEBACKED'
    
    def __init__(self, merchant_id: str, secret: str):
        """
        Initialize callback handler
        
        Args:
            merchant_id: Your Merchant ID for validation
            secret: Your API Secret for validation
        """
        self.merchant_id = merchant_id
        self.secret = secret
        self.payload: Optional[Dict[str, Any]] = None
        self.validation_error: Optional[str] = None
        self._validated = False
        self._is_valid = False
    
    def validate(self, request) -> bool:
        """
        Validate callback from Flask request object
        
        Args:
            request: Flask request object
        
        Returns:
            bool: True if callback is valid
        """
        try:
            # Check method
            if request.method != 'POST':
                self.validation_error = 'Invalid request method'
                return False
            
            # Get headers
            received_merchant_id = request.headers.get('X-MerchantId', '')
            received_secret = request.headers.get('X-Secret', '')
            
            return self._validate_common(
                received_merchant_id,
                received_secret,
                request.get_data(as_text=True)
            )
            
        except Exception as e:
            self.validation_error = str(e)
            return False
    
    def validate_django(self, request) -> bool:
        """
        Validate callback from Django request object
        
        Args:
            request: Django HttpRequest object
        
        Returns:
            bool: True if callback is valid
        """
        try:
            # Check method
            if request.method != 'POST':
                self.validation_error = 'Invalid request method'
                return False
            
            # Get headers (Django uses META with HTTP_ prefix)
            received_merchant_id = request.META.get('HTTP_X_MERCHANTID', '')
            received_secret = request.META.get('HTTP_X_SECRET', '')
            
            return self._validate_common(
                received_merchant_id,
                received_secret,
                request.body.decode('utf-8')
            )
            
        except Exception as e:
            self.validation_error = str(e)
            return False
    
    def validate_raw(
        self,
        headers: Dict[str, str],
        body: str
    ) -> bool:
        """
        Validate callback from raw headers and body
        
        Args:
            headers: Request headers dict
            body: Raw request body string
        
        Returns:
            bool: True if callback is valid
        """
        # Normalize header names (case-insensitive)
        normalized_headers = {k.lower(): v for k, v in headers.items()}
        
        received_merchant_id = (
            normalized_headers.get('x-merchantid', '') or
            normalized_headers.get('x-merchant-id', '')
        )
        received_secret = normalized_headers.get('x-secret', '')
        
        return self._validate_common(received_merchant_id, received_secret, body)
    
    def _validate_common(
        self,
        received_merchant_id: str,
        received_secret: str,
        body: str
    ) -> bool:
        """Common validation logic"""
        self._validated = True
        
        # Validate credentials
        if not received_merchant_id or received_merchant_id != self.merchant_id:
            self.validation_error = 'Invalid or missing X-MerchantId header'
            self._is_valid = False
            return False
        
        if not received_secret or received_secret != self.secret:
            self.validation_error = 'Invalid or missing X-Secret header'
            self._is_valid = False
            return False
        
        # Parse body
        if not body:
            self.validation_error = 'Empty request body'
            self._is_valid = False
            return False
        
        try:
            self.payload = json.loads(body)
        except json.JSONDecodeError:
            self.validation_error = 'Invalid JSON in request body'
            self._is_valid = False
            return False
        
        # Validate required fields
        required_fields = ['id', 'amount', 'currency', 'status', 'paymentMethod']
        for field in required_fields:
            if field not in self.payload:
                self.validation_error = f'Missing required field: {field}'
                self._is_valid = False
                return False
        
        self._is_valid = True
        return True
    
    def is_valid(self) -> bool:
        """Check if callback was validated and is valid"""
        return self._is_valid
    
    def get_validation_error(self) -> Optional[str]:
        """Get validation error message"""
        return self.validation_error
    
    def get_payload(self) -> Optional[Dict[str, Any]]:
        """Get parsed callback payload"""
        return self.payload
    
    def get_transaction_id(self) -> Optional[str]:
        """Get transaction ID from callback"""
        if self.payload:
            return self.payload.get('id')
        return None
    
    def get_status(self) -> Optional[str]:
        """Get payment status from callback"""
        if self.payload:
            return self.payload.get('status')
        return None
    
    def get_amount(self) -> Optional[float]:
        """Get payment amount from callback"""
        if self.payload:
            return float(self.payload.get('amount', 0))
        return None
    
    def get_currency(self) -> Optional[str]:
        """Get payment currency from callback"""
        if self.payload:
            return self.payload.get('currency')
        return None
    
    def get_payment_method(self) -> Optional[int]:
        """Get payment method ID from callback"""
        if self.payload:
            return int(self.payload.get('paymentMethod', 0))
        return None
    
    def get_custom_payload(self) -> Optional[str]:
        """Get custom payload data (order ID, etc.)"""
        if self.payload:
            return self.payload.get('payload')
        return None
    
    def get_order_id(self) -> Optional[str]:
        """Alias for get_custom_payload() - get order ID from payload"""
        return self.get_custom_payload()
    
    def is_success(self) -> bool:
        """Check if payment was successful (CONFIRMED)"""
        return self.get_status() == self.STATUS_CONFIRMED
    
    def is_canceled(self) -> bool:
        """Check if payment was canceled"""
        return self.get_status() == self.STATUS_CANCELED
    
    def is_chargeback(self) -> bool:
        """Check if payment has chargeback"""
        return self.get_status() == self.STATUS_CHARGEBACKED
