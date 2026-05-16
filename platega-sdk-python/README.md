# Platega Python SDK

Python SDK для интеграции с платёжной системой [Platega.io](https://platega.io).

## Установка

```bash
pip install platega
```

Или установка из исходников:

```bash
pip install -e .
```

## Требования

- Python 3.7+
- Без внешних зависимостей (только стандартная библиотека)

## Быстрый старт

### Создание платежа

```python
from platega import Platega

# Инициализация клиента
client = Platega('your-merchant-id', 'your-secret-key')

# Создание платежа
result = client.create_payment(
    amount=1000,
    currency='RUB',
    payment_method=Platega.METHOD_SBP_QR,
    description='Оплата заказа #123',
    return_url='https://yoursite.com/success',
    failed_url='https://yoursite.com/fail',
    payload='123'  # ID заказа
)

# Перенаправление на страницу оплаты
print(result['redirect'])
print(result['transactionId'])
```

### Проверка статуса платежа

```python
from platega import Platega

client = Platega('your-merchant-id', 'your-secret-key')

status = client.get_payment_status('transaction-uuid')
print(f"Статус: {status['status']}")
print(f"Сумма: {status['paymentDetails']['amount']}")
```

### Обработка callback (Flask)

```python
from flask import Flask, request
from platega import PlategaCallback

app = Flask(__name__)

@app.route('/callback', methods=['POST'])
def payment_callback():
    callback = PlategaCallback('your-merchant-id', 'your-secret-key')
    
    if not callback.validate(request):
        return callback.get_validation_error(), 401
    
    order_id = callback.get_order_id()
    amount = callback.get_amount()
    
    if callback.is_success():
        # Платёж успешен
        update_order_status(order_id, 'paid')
        
    elif callback.is_canceled():
        # Платёж отменён
        update_order_status(order_id, 'canceled')
        
    elif callback.is_chargeback():
        # Chargeback
        update_order_status(order_id, 'chargeback')
    
    return 'OK', 200
```

### Обработка callback (Django)

```python
from django.http import HttpResponse
from platega import PlategaCallback

def payment_callback(request):
    callback = PlategaCallback('your-merchant-id', 'your-secret-key')
    
    if not callback.validate_django(request):
        return HttpResponse(callback.get_validation_error(), status=401)
    
    if callback.is_success():
        order_id = callback.get_order_id()
        # Обработка успешного платежа
    
    return HttpResponse('OK')
```

## Способы оплаты

| Константа | Значение | Описание |
|-----------|----------|----------|
| `Platega.METHOD_SBP_QR` | 2 | СБП с QR-кодом |
| `Platega.METHOD_CARDS_RUB` | 10 | Российские карты |
| `Platega.METHOD_CARD_ACQUIRING` | 11 | Карточный эквайринг |
| `Platega.METHOD_INTERNATIONAL` | 12 | Международный эквайринг |
| `Platega.METHOD_CRYPTO` | 13 | Криптовалюта |

```python
# Получить список всех методов
methods = Platega.get_payment_methods()
```

## Статусы платежей

| Константа | Значение | Описание |
|-----------|----------|----------|
| `Platega.STATUS_PENDING` | PENDING | Ожидает оплаты |
| `Platega.STATUS_CONFIRMED` | CONFIRMED | Успешно оплачен |
| `Platega.STATUS_CANCELED` | CANCELED | Отменён |
| `Platega.STATUS_CHARGEBACKED` | CHARGEBACKED | Chargeback |

## Обработка ошибок

```python
from platega import Platega, PlategaAPIError

client = Platega('merchant_id', 'secret')

try:
    result = client.create_payment(1000, 'RUB', Platega.METHOD_SBP_QR)
except PlategaAPIError as e:
    print(f"Ошибка: {e}")
    print(f"HTTP код: {e.http_code}")
    print(f"Данные: {e.response_data}")
```

## Callback URL

Укажите этот URL в настройках личного кабинета Platega:

- **Flask**: `https://yoursite.com/callback`
- **Django**: `https://yoursite.com/payment/callback/`

## Лицензия

MIT License
