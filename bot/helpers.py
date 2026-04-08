
import base64
import html
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DATETIME_DISPLAY_FORMAT = "%d.%m.%Y %H:%M"


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def as_utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utc_naive_to_moscow(dt: datetime) -> datetime:
    return as_utc_aware(dt).astimezone(MOSCOW_TZ)


def parse_iso_utc_naive(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str)


def timestamp_to_moscow(ts: int | float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(MOSCOW_TZ)


def format_moscow_datetime(dt: datetime, fmt: str = DATETIME_DISPLAY_FORMAT) -> str:
    return utc_naive_to_moscow(dt).strftime(fmt)


def format_iso_to_moscow(dt_str: str, fmt: str = DATETIME_DISPLAY_FORMAT) -> str:
    return format_moscow_datetime(parse_iso_utc_naive(dt_str), fmt=fmt)


def format_timestamp_to_moscow(ts: int | float, fmt: str = DATETIME_DISPLAY_FORMAT) -> str:
    return timestamp_to_moscow(ts).strftime(fmt)



def subscription_is_active(dt_str: str | None) -> bool:
    if not dt_str or dt_str == "0":
        return False
    try:
        return parse_iso_utc_naive(dt_str) > utc_now_naive()
    except ValueError:
        return False


def get_status_text(dt_str: str | None) -> tuple[str, str]:
    if not dt_str or dt_str == "0":
        return "🔴 Не активен", "Доступ отсутствует"
    try:
        sub_dt = parse_iso_utc_naive(dt_str)
        until_text = format_moscow_datetime(sub_dt)
        if sub_dt > utc_now_naive():
            return "🟢 Активен", until_text
        return "🔴 Истек", until_text
    except ValueError:
        return "⚠️ Ошибка", "Некорректная дата"


def format_remaining_time(dt_str: str | None) -> str:
    if not dt_str or dt_str == "0":
        return "0 дн."
    try:
        delta = parse_iso_utc_naive(dt_str) - utc_now_naive()
        if delta.total_seconds() <= 0:
            return "0 дн."
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        if days > 0:
            return f"{days} дн. {hours} ч."
        if hours > 0:
            return f"{hours} ч. {minutes} мин."
        return f"{minutes} мин."
    except ValueError:
        return "неизвестно"


def parse_server_host_port(server_ip: str) -> tuple[str, int]:
    host, port_str = server_ip.rsplit(":", 1)
    return host, int(port_str)


def is_valid_awg_public_key(value: str) -> bool:
    if not value:
        return False
    try:
        raw = base64.b64decode(value, validate=True)
        return len(raw) == 32
    except Exception:
        return False


def format_tg_username(username: str | None) -> str:
    return f"@{username}" if username else "не указан"


def escape_html(value: str | None) -> str:
    return html.escape(value or "не указано")
