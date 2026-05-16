"""
Platega API Client
"""

import json
from typing import Dict, Any, Optional, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from .exceptions import PlategaAPIError


class Platega:
    """
    Main class for interacting with Platega API
    
    Example:
        client = Platega('merchant_id', 'secret')
        result = client.create_payment(1000, 'RUB', Platega.METHOD_SBP_QR)
    """
    
    API_URL = 'https://app.platega.io'
    VERSION = '1.0.0'
    
    # Payment methods
    METHOD_SBP_QR = 2           # СБП QR
    METHOD_CARDS_RUB = 10       # Карты (RUB)
    METHOD_CARD_ACQUIRING = 11  # Карточный эквайринг
    METHOD_INTERNATIONAL = 12   # Международный эквайринг
    METHOD_CRYPTO = 13          # Криптовалюта
    
    # Payment statuses
    STATUS_PENDING = 'PENDING'
    STATUS_CONFIRMED = 'CONFIRMED'
    STATUS_CANCELED = 'CANCELED'
    STATUS_CHARGEBACKED = 'CHARGEBACKED'
    
    def __init__(self, merchant_id: str, secret: str, timeout: int = 30):
        """
        Initialize Platega client
        
        Args:
            merchant_id: Your Merchant ID (UUID)
            secret: Your API Secret key
            timeout: Request timeout in seconds
        """
        self.merchant_id = merchant_id
        self.secret = secret
        self.timeout = timeout
    
    def create_payment(
        self,
        amount: float,
        currency: str,
        payment_method: int,
        description: str = None,
        return_url: str = None,
        failed_url: str = None,
        payload: str = None
    ) -> Dict[str, Any]:
        """
        Create a new payment
        
        Args:
            amount: Payment amount
            currency: Currency code (e.g., 'RUB')
            payment_method: Payment method constant (e.g., METHOD_SBP_QR)
            description: Payment description
            return_url: Success redirect URL
            failed_url: Failed redirect URL
            payload: Custom payload data (e.g., order ID)
        
        Returns:
            dict: Response data with keys:
                - transactionId (str): Transaction UUID
                - redirect (str): Payment URL
                - status (str): Payment status
                - paymentMethod (str): Payment method name
                - expiresIn (str): Time until expiration
        
        Raises:
            PlategaAPIError: If API request fails
        """
        data = {
            'paymentMethod': int(payment_method),
            'paymentDetails': {
                'amount': float(amount),
                'currency': str(currency)
            }
        }
        
        if description:
            data['description'] = str(description)
        if return_url:
            data['return'] = str(return_url)
        if failed_url:
            data['failedUrl'] = str(failed_url)
        if payload:
            data['payload'] = str(payload)
        
        return self._request('POST', '/transaction/process', data)
    
    def get_payment_status(self, transaction_id: str) -> Dict[str, Any]:
        """
        Get payment status
        
        Args:
            transaction_id: Transaction UUID
        
        Returns:
            dict: Transaction data
        
        Raises:
            PlategaAPIError: If API request fails
        """
        return self._request('GET', f'/transaction/{transaction_id}')
    
    def get_qr_code(self, transaction_id: str) -> Dict[str, Any]:
        """
        Get QR code for payment
        
        Args:
            transaction_id: Transaction UUID
        
        Returns:
            dict: QR code data with keys:
                - qr (dict): QR code data containing url and/or base64
                - redirect (str): Payment URL
                - status (str): Payment status
        
        Raises:
            PlategaAPIError: If API request fails
        """
        return self._request('GET', f'/payment/qr/{transaction_id}')
    
    def get_rate(
        self,
        payment_method: int,
        currency_from: str,
        currency_to: str
    ) -> Dict[str, Any]:
        """
        Get exchange rate for payment method
        
        Args:
            payment_method: Payment method constant
            currency_from: Source currency (e.g., 'RUB')
            currency_to: Target currency (e.g., 'USDT')
        
        Returns:
            dict: Rate data
        
        Raises:
            PlategaAPIError: If API request fails
        """
        params = (
            f"merchantId={self.merchant_id}"
            f"&paymentMethod={payment_method}"
            f"&currencyFrom={currency_from}"
            f"&currencyTo={currency_to}"
        )
        return self._request('GET', f'/rates/payment_method_rate?{params}')
    
    @staticmethod
    def get_payment_methods() -> List[Dict[str, Any]]:
        """
        Get available payment methods
        
        Returns:
            list: List of payment methods with id, name, and description
        """
        return [
            {
                'id': Platega.METHOD_SBP_QR,
                'name': 'СБП QR',
                'description': 'СБП с QR-кодом (НСПК / QR)'
            },
            {
                'id': Platega.METHOD_CARDS_RUB,
                'name': 'Карты (RUB)',
                'description': 'Российские карты (МИР, Visa, Mastercard)'
            },
            {
                'id': Platega.METHOD_CARD_ACQUIRING,
                'name': 'Карточный эквайринг',
                'description': 'Общий карточный эквайринг'
            },
            {
                'id': Platega.METHOD_INTERNATIONAL,
                'name': 'Международный эквайринг',
                'description': 'Международные карточные платежи'
            },
            {
                'id': Platega.METHOD_CRYPTO,
                'name': 'Криптовалюта',
                'description': 'Общие криптовалютные платежи'
            }
        ]
    
    @staticmethod
    def is_success_status(status: str) -> bool:
        """Check if status indicates successful payment"""
        return status == Platega.STATUS_CONFIRMED
    
    @staticmethod
    def is_canceled_status(status: str) -> bool:
        """Check if status indicates canceled/failed payment"""
        return status == Platega.STATUS_CANCELED
    
    @staticmethod
    def is_chargeback_status(status: str) -> bool:
        """Check if status indicates chargeback"""
        return status == Platega.STATUS_CHARGEBACKED
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Send API request
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint
            data: Request data
        
        Returns:
            dict: Response data
        
        Raises:
            PlategaAPIError: If request fails
        """
        url = f"{self.API_URL}{endpoint}"
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-MerchantId': self.merchant_id,
            'X-Secret': self.secret,
            'User-Agent': f'Platega-Python-SDK/{self.VERSION}'
        }
        
        body = None
        if data is not None:
            body = json.dumps(data).encode('utf-8')
        
        request = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(request, timeout=self.timeout) as response:
                response_data = json.loads(response.read().decode('utf-8'))
                return response_data
                
        except HTTPError as e:
            try:
                error_data = json.loads(e.read().decode('utf-8'))
                message = error_data.get('message', str(e))
            except:
                error_data = {}
                message = str(e)
            
            raise PlategaAPIError(message, e.code, error_data)
            
        except URLError as e:
            raise PlategaAPIError(f"Connection error: {e.reason}")
            
        except json.JSONDecodeError as e:
            raise PlategaAPIError(f"Invalid JSON response: {e}")
