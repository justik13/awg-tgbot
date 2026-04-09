from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Literal

from awg_backend import issue_subscription
from config import logger
from content_settings import get_setting, get_text
from helpers import escape_html
from database import (
    create_referral_recurring_reward_once,
    create_referral_reward_once,
    ensure_referral_code,
    get_referral_attribution,
    get_referral_code,
    get_referral_inviter_identity,
    get_referral_summary,
    get_user_id_by_referral_code,
    has_user_received_service_access,
    has_referral_first_reward,
    payment_is_applied_for_user,
    set_referral_attribution,
    get_user_meta,
    write_audit_log,
)

CaptureReferralStartStatus = Literal[
    "saved",
    "already_attributed",
    "self_referral",
    "invalid_code",
    "blocked_existing_access",
    "referrals_disabled",
]


@dataclass(frozen=True)
class CaptureReferralStartResult:
    status: CaptureReferralStartStatus
    inviter_user_id: int | None = None
    referral_code: str | None = None


def _build_ref_code(user_id: int) -> str:
    digest = hashlib.sha256(f"awg-ref-{user_id}".encode("utf-8")).hexdigest()[:10]
    return digest.upper()


async def ensure_user_referral_code(user_id: int) -> str:
    code = await get_referral_code(user_id)
    if code:
        return code
    code = _build_ref_code(user_id)
    await ensure_referral_code(user_id, code)
    return code


async def capture_referral_start(invitee_user_id: int, start_arg: str) -> CaptureReferralStartResult:
    if int(await get_setting("REFERRAL_ENABLED", int) or 0) != 1:
        return CaptureReferralStartResult(status="referrals_disabled")
    if not start_arg.startswith("ref_"):
        return CaptureReferralStartResult(status="invalid_code")
    if await has_user_received_service_access(invitee_user_id):
        return CaptureReferralStartResult(status="blocked_existing_access")
    if await get_referral_attribution(invitee_user_id):
        return CaptureReferralStartResult(status="already_attributed")
    code = start_arg.removeprefix("ref_").strip().upper()
    inviter_user_id = await get_user_id_by_referral_code(code)
    if not inviter_user_id:
        return CaptureReferralStartResult(status="invalid_code")
    if inviter_user_id == invitee_user_id:
        return CaptureReferralStartResult(status="self_referral")
    saved = await set_referral_attribution(invitee_user_id, inviter_user_id, code)
    if saved:
        await write_audit_log(invitee_user_id, "referral_attribution_set", f"inviter={inviter_user_id}; code={code}")
        return CaptureReferralStartResult(status="saved", inviter_user_id=inviter_user_id, referral_code=code)
    return CaptureReferralStartResult(status="already_attributed", inviter_user_id=inviter_user_id, referral_code=code)


async def apply_referral_rewards_on_first_payment(invitee_user_id: int, payment_id: str) -> bool:
    if int(await get_setting("REFERRAL_ENABLED", int) or 0) != 1:
        return False
    attribution = await get_referral_attribution(invitee_user_id)
    if not attribution:
        return False
    inviter_user_id, code = attribution
    invitee_days = int(await get_setting("REFERRAL_INVITEE_BONUS_DAYS", int) or 5)
    inviter_days = int(await get_setting("REFERRAL_INVITER_BONUS_DAYS", int) or 3)
    created = await create_referral_reward_once(
        invitee_user_id=invitee_user_id,
        inviter_user_id=inviter_user_id,
        payment_id=payment_id,
        invitee_bonus_days=invitee_days,
        inviter_bonus_days=inviter_days,
    )
    if not created:
        return False
    await issue_subscription(invitee_user_id, invitee_days, silent=True, operation_id=f"ref-invitee-{payment_id}")
    await issue_subscription(inviter_user_id, inviter_days, silent=True, operation_id=f"ref-inviter-{payment_id}")
    await write_audit_log(
        invitee_user_id,
        "referral_rewards_applied",
        f"inviter={inviter_user_id}; code={code}; invitee_days={invitee_days}; inviter_days={inviter_days}",
    )
    logger.info("Referral rewards applied for payment=%s invitee=%s inviter=%s", payment_id, invitee_user_id, inviter_user_id)
    return True


def _format_tg_mention(username: str | None, user_id: int) -> str:
    if username:
        return f"@{username}"
    return f"id={user_id}"


async def notify_inviter_about_referral_reward(bot: Any, invitee_user_id: int) -> bool:
    if bot is None:
        return False
    attribution = await get_referral_attribution(invitee_user_id)
    if not attribution:
        return False
    inviter_user_id, _code = attribution
    inviter_days = int(await get_setting("REFERRAL_INVITER_BONUS_DAYS", int) or 3)
    invitee_username, _invitee_first_name = await get_user_meta(invitee_user_id)
    invitee_mention = _format_tg_mention(invitee_username, invitee_user_id)
    text = (
        "🎉 <b>Реферальный бонус начислен</b>\n\n"
        f"По покупке пользователя {invitee_mention} (ID: <code>{invitee_user_id}</code>) "
        f"вам начислено <b>+{inviter_days} дн.</b>"
    )
    try:
        await bot.send_message(inviter_user_id, text, parse_mode="HTML")
        return True
    except Exception as error:
        logger.warning("Не удалось отправить уведомление о реферальном бонусе inviter=%s: %s", inviter_user_id, error)
        return False


async def apply_referral_recurring_inviter_reward(
    invitee_user_id: int,
    payment_id: str,
    purchased_days: int,
) -> bool:
    if int(await get_setting("REFERRAL_ENABLED", int) or 0) != 1:
        return False
    attribution = await get_referral_attribution(invitee_user_id)
    if not attribution:
        return False
    recurring_min_purchase_days = int(await get_setting("REFERRAL_RECURRING_MIN_PURCHASE_DAYS", int) or 30)
    recurring_inviter_bonus_days = int(await get_setting("REFERRAL_RECURRING_INVITER_BONUS_DAYS", int) or 2)
    if purchased_days < recurring_min_purchase_days:
        return False
    if not await payment_is_applied_for_user(payment_id, invitee_user_id):
        return False
    if not await has_referral_first_reward(invitee_user_id):
        return False

    inviter_user_id, code = attribution
    created = await create_referral_recurring_reward_once(
        invitee_user_id=invitee_user_id,
        inviter_user_id=inviter_user_id,
        payment_id=payment_id,
        inviter_bonus_days=recurring_inviter_bonus_days,
    )
    if not created:
        return False

    await issue_subscription(
        inviter_user_id,
        recurring_inviter_bonus_days,
        silent=True,
        operation_id=f"ref-recurring-inviter-{payment_id}",
    )
    await write_audit_log(
        invitee_user_id,
        "referral_recurring_inviter_reward_applied",
        (
            f"inviter={inviter_user_id}; code={code}; payment_id={payment_id}; "
            f"inviter_days={recurring_inviter_bonus_days}; purchased_days={purchased_days}"
        ),
    )
    logger.info(
        "Referral recurring inviter reward applied for payment=%s invitee=%s inviter=%s days=%s",
        payment_id,
        invitee_user_id,
        inviter_user_id,
        recurring_inviter_bonus_days,
    )
    return True


def _format_inviter_display_name(username: str | None, first_name: str | None, inviter_user_id: int) -> str:
    if username:
        return f"@{escape_html(username.lstrip('@'))}"
    if first_name:
        return escape_html(first_name)
    return f"пользователь ID {escape_html(str(inviter_user_id))}"


async def build_referral_inviter_banner_text(result: CaptureReferralStartResult) -> str | None:
    if result.status != "saved" or not result.inviter_user_id:
        return None
    username, first_name = await get_referral_inviter_identity(result.inviter_user_id)
    inviter_name = _format_inviter_display_name(username, first_name, result.inviter_user_id)
    return await get_text(
        "referral_inviter_banner",
        inviter_display_name=inviter_name,
    )


async def notify_inviter_about_referral_recurring_reward(bot: Any, invitee_user_id: int, purchased_days: int) -> bool:
    if bot is None:
        return False
    attribution = await get_referral_attribution(invitee_user_id)
    if not attribution:
        return False
    inviter_user_id, _code = attribution
    recurring_bonus_days = int(await get_setting("REFERRAL_RECURRING_INVITER_BONUS_DAYS", int) or 2)
    invitee_username, invitee_first_name = await get_user_meta(invitee_user_id)
    invitee_name = _format_inviter_display_name(invitee_username, invitee_first_name, invitee_user_id)
    text = await get_text(
        "referral_recurring_reward_notification",
        invitee_name=invitee_name,
        invitee_user_id=invitee_user_id,
        purchased_days=purchased_days,
        recurring_bonus_days=recurring_bonus_days,
    )
    try:
        await bot.send_message(inviter_user_id, text, parse_mode="HTML")
        return True
    except Exception as error:
        logger.warning("Не удалось отправить recurring-уведомление inviter=%s: %s", inviter_user_id, error)
        return False


async def get_referral_screen_data(user_id: int, bot_username: str) -> dict[str, str | int]:
    code = await ensure_user_referral_code(user_id)
    summary = await get_referral_summary(user_id)
    return {
        "code": code,
        "link": f"https://t.me/{bot_username}?start=ref_{code}",
        "invited_count": summary["invited_count"],
        "rewarded_count_first_payment": summary["rewarded_count_first_payment"],
        "inviter_first_payment_bonus_days_total": summary["inviter_first_payment_bonus_days_total"],
        "inviter_recurring_bonus_days_total": summary["inviter_recurring_bonus_days_total"],
        "inviter_bonus_days_total": summary["inviter_bonus_days_total"],
        "friends_bonus_days_total": summary["friends_bonus_days_total"],
        "user_invitee_bonus_days_total": summary["user_invitee_bonus_days_total"],
        "overall_bonus_days_total": summary["overall_bonus_days_total"],
        # Backward compatibility for any legacy text overrides.
        "rewarded_count": summary["rewarded_count_first_payment"],
        "bonus_days": summary["inviter_bonus_days_total"] + summary["user_invitee_bonus_days_total"],
        "invitee_bonus_days_total": summary["user_invitee_bonus_days_total"],
    }
