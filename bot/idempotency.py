"""
Idempotency and Click Guard Service.

Provides:
1. Idempotency keys for payment operations (prevents duplicate processing)
2. Request deduplication with TTL
3. Rate limiting per user/action type
4. Atomic guards for critical operations
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from config import logger


# ============================================================================
# IDEMPOTENCY SERVICE FOR PAYMENTS
# ============================================================================

@dataclass
class IdempotencyRecord:
    """Запись идемпотентности для операции."""
    
    key: str
    status: str  # 'pending', 'completed', 'failed'
    result: Any | None = None
    created_at: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 3600)  # 1 hour TTL
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


class IdempotencyService:
    """
    Сервис идемпотентности для платежных операций.
    
    Гарантирует, что одна и та же операция (по ключу) будет выполнена только один раз.
    Ключи генерируются из: user_id + operation_type + operation_params
    """
    
    def __init__(self, max_entries: int = 10000, default_ttl_seconds: int = 3600):
        self.max_entries = max_entries
        self.default_ttl_seconds = default_ttl_seconds
        self._store: OrderedDict[str, IdempotencyRecord] = OrderedDict()
    
    def _generate_key(self, user_id: int, operation: str, **params) -> str:
        """Генерирует уникальный ключ идемпотентности."""
        # Сортируем параметры для детерминизма
        param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        raw = f"{user_id}:{operation}:{param_str}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]
    
    def try_acquire(self, key: str) -> bool:
        """
        Пытается захватить ключ для выполнения операции.
        
        Returns:
            True если операция может быть выполнена (ключ новый или истек)
            False если операция уже выполняется или завершена
        """
        self._cleanup_expired()
        
        record = self._store.get(key)
        
        if record is None:
            # Новый ключ - разрешаем выполнение
            self._store[key] = IdempotencyRecord(key=key, status='pending')
            self._store.move_to_end(key)
            
            if len(self._store) > self.max_entries:
                self._store.popitem(last=False)
            
            return True
        
        if record.is_expired():
            # Истек - разрешаем повторное выполнение
            self._store[key] = IdempotencyRecord(key=key, status='pending')
            self._store.move_to_end(key)
            return True
        
        if record.status == 'completed':
            # Уже завершено - возвращаем сохраненный результат
            return False
        
        if record.status == 'pending':
            # Еще выполняется - блокируем
            return False
        
        return True
    
    def mark_completed(self, key: str, result: Any = None) -> None:
        """Отмечает операцию как завершенную с результатом."""
        record = self._store.get(key)
        if record:
            record.status = 'completed'
            record.result = result
    
    def mark_failed(self, key: str) -> None:
        """Отмечает операцию как неудачную (разрешает retry)."""
        record = self._store.get(key)
        if record:
            # При ошибке уменьшаем TTL чтобы быстрее разрешился retry
            record.status = 'failed'
            record.expires_at = time.time() + 300  # 5 минут для failed
    
    def get_result(self, key: str) -> Any | None:
        """Возвращает результат ранее выполненной операции."""
        record = self._store.get(key)
        if record and not record.is_expired():
            return record.result
        return None
    
    def _cleanup_expired(self) -> None:
        """Удаляет истекшие записи."""
        now = time.time()
        expired_keys = [k for k, v in self._store.items() if v.is_expired()]
        for key in expired_keys:
            del self._store[key]
    
    def remove(self, key: str) -> None:
        """Принудительно удаляет запись (для cleanup)."""
        self._store.pop(key, None)


# Global instance for payment idempotency
payment_idempotency = IdempotencyService(max_entries=5000, default_ttl_seconds=3600)


# ============================================================================
# CLICK GUARD WITH TTL (ENHANCED)
# ============================================================================

class ClickGuard:
    """
    Защита от повторных кликов с настраиваемым TTL.
    
    В отличие от middleware, этот сервис можно использовать явно в handlers
    для более гибкого контроля.
    """
    
    def __init__(self, ttl_seconds: float = 2.0, max_entries: int = 8192):
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._clicks: OrderedDict[str, float] = OrderedDict()
    
    def _make_key(self, user_id: int, callback_data: str) -> str:
        return f"{user_id}:{callback_data}"
    
    def is_duplicate(self, user_id: int, callback_data: str) -> bool:
        """Проверяет, был ли недавний клик с таким же callback."""
        key = self._make_key(user_id, callback_data)
        now = time.time()
        
        # Cleanup старых записей
        self._cleanup(now)
        
        last_click = self._clicks.get(key)
        
        if last_click is not None and (now - last_click) < self.ttl_seconds:
            return True
        
        self._clicks[key] = now
        self._clicks.move_to_end(key)
        
        if len(self._clicks) > self.max_entries:
            self._clicks.popitem(last=False)
        
        return False
    
    def _cleanup(self, now: float) -> None:
        cutoff = now - self.ttl_seconds
        expired = [k for k, v in self._clicks.items() if v < cutoff]
        for key in expired:
            del self._clicks[key]


# Global click guard instances
global_click_guard = ClickGuard(ttl_seconds=2.0, max_entries=8192)
payment_click_guard = ClickGuard(ttl_seconds=5.0, max_entries=4096)  # Longer for payments


# ============================================================================
# RATE LIMITER PER USER/ACTION
# ============================================================================

@dataclass
class RateLimitBucket:
    """Bucket для rate limiting."""
    
    hits: list[float] = field(default_factory=list)
    limited_until: float = 0.0


class ActionRateLimiter:
    """
    Rate limiter с разделением по типу действия.
    
    Позволяет задавать разные лимиты для разных типов операций
    (например, платежи vs навигация).
    """
    
    def __init__(
        self,
        default_ttl_seconds: float = 60.0,
        default_max_hits: int = 10,
        max_entries: int = 16384,
    ):
        self.default_ttl = default_ttl_seconds
        self.default_max_hits = default_max_hits
        self.max_entries = max_entries
        self._buckets: OrderedDict[str, RateLimitBucket] = OrderedDict()
        
        # Специфичные лимиты для разных action types
        self.action_limits: dict[str, tuple[float, int]] = {
            'payment': (60.0, 5),      # 5 платежей в минуту
            'config': (60.0, 10),      # 10 запросов конфига в минуту
            'navigation': (10.0, 20),  # 20 кликов навигации в 10 секунд
            'support': (300.0, 3),     # 3 сообщения в поддержку за 5 минут
        }
    
    def _make_key(self, user_id: int, action_type: str) -> str:
        return f"{user_id}:{action_type}"
    
    def is_limited(self, user_id: int, action_type: str = 'default') -> bool:
        """
        Проверяет, превышен ли лимит для пользователя по типу действия.
        
        Returns:
            True если пользователь ограничен (превысил лимит)
            False если действие разрешено
        """
        key = self._make_key(user_id, action_type)
        now = time.time()
        
        ttl, max_hits = self.action_limits.get(action_type, (self.default_ttl, self.default_max_hits))
        
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = RateLimitBucket()
            self._buckets[key] = bucket
        
        # Удаляем старые hits
        cutoff = now - ttl
        bucket.hits = [h for h in bucket.hits if h > cutoff]
        
        # Проверяем лимит
        if len(bucket.hits) >= max_hits:
            bucket.limited_until = now + ttl
            return True
        
        # Добавляем hit
        bucket.hits.append(now)
        
        # Cleanup
        if len(self._buckets) > self.max_entries:
            self._buckets.popitem(last=False)
        
        return False
    
    def get_remaining(self, user_id: int, action_type: str = 'default') -> int:
        """Возвращает количество оставшихся действий."""
        key = self._make_key(user_id, action_type)
        now = time.time()
        
        ttl, max_hits = self.action_limits.get(action_type, (self.default_ttl, self.default_max_hits))
        
        bucket = self._buckets.get(key)
        if bucket is None:
            return max_hits
        
        cutoff = now - ttl
        current_hits = len([h for h in bucket.hits if h > cutoff])
        
        return max(0, max_hits - current_hits)


# Global rate limiter instance
action_rate_limiter = ActionRateLimiter()


# ============================================================================
# DECORATORS FOR IDEMPOTENCY AND RATE LIMITING
# ============================================================================

def idempotent(operation_name: str = 'operation'):
    """
    Декоратор для обеспечения идемпотентности handler.
    
    Использование:
        @router.callback_query(...)
        @idempotent('payment:stars')
        async def handle_payment(cb: CallbackQuery, state: FSMContext):
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Извлекаем user_id и другие параметры из args/kwargs
            cb = kwargs.get('cb') or (args[0] if args else None)
            if not cb or not hasattr(cb, 'from_user'):
                return await func(*args, **kwargs)
            
            user_id = cb.from_user.id
            
            # Генерируем ключ из имени операции и данных callback
            callback_data = getattr(cb, 'data', '') or ''
            idem_key = payment_idempotency._generate_key(
                user_id, 
                operation_name,
                callback=callback_data
            )
            
            # Проверяем идемпотентность
            if not payment_idempotency.try_acquire(idem_key):
                logger.info(
                    "Idempotency guard: blocked duplicate %s for user=%s",
                    operation_name, user_id
                )
                await cb.answer(show_alert=False)
                return None
            
            try:
                result = await func(*args, **kwargs)
                payment_idempotency.mark_completed(idem_key, result)
                return result
            except Exception as e:
                payment_idempotency.mark_failed(idem_key)
                raise
        
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator


def rate_limited(action_type: str = 'default'):
    """
    Декоратор для rate limiting handler.
    
    Использование:
        @router.callback_query(...)
        @rate_limited('navigation')
        async def handle_navigation(cb: CallbackQuery):
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            cb = kwargs.get('cb') or (args[0] if args else None)
            if not cb or not hasattr(cb, 'from_user'):
                return await func(*args, **kwargs)
            
            user_id = cb.from_user.id
            
            if action_rate_limiter.is_limited(user_id, action_type):
                remaining = action_rate_limiter.get_remaining(user_id, action_type)
                logger.debug(
                    "Rate limit: user=%s action=%s limited (remaining=%d)",
                    user_id, action_type, remaining
                )
                await cb.answer(
                    f"⏳ Слишком быстро. Попробуйте через {remaining} сек",
                    show_alert=True
                )
                return None
            
            return await func(*args, **kwargs)
        
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator
