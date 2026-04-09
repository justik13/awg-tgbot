import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("SERVER_PUBLIC_KEY", "test-public-key")
os.environ.setdefault("SERVER_IP", "1.1.1.1:51820")
os.environ.setdefault("ENCRYPTION_SECRET", "test-secret")

import database
import handlers_user
from database import (
    close_shared_db,
    create_promo_code,
    ensure_referral_code,
    ensure_user_exists,
    execute,
    get_referral_attribution,
    init_db,
    set_text_override,
    set_app_setting,
)
from referrals import (
    build_referral_inviter_banner_text,
    capture_referral_start,
    notify_inviter_about_referral_recurring_reward,
)


class DummyCallback:
    def __init__(self, user_id: int):
        self.from_user = SimpleNamespace(id=user_id, username="tester", first_name="Tester")
        self.bot = SimpleNamespace()
        self.message = SimpleNamespace()
        self._answers: list[tuple[str | None, bool]] = []

    async def answer(self, text: str | None = None, show_alert: bool = False):
        self._answers.append((text, show_alert))


class DummyBot:
    async def get_me(self):
        return SimpleNamespace(username="testbot")


class ReferralFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmp_dir.name) / "test.sqlite3")
        await close_shared_db()
        database.DB_PATH = self._db_path
        await init_db()
        await set_app_setting("REFERRAL_ENABLED", "1")
        await ensure_user_exists(1001, "inviter", "Inviter")
        await ensure_referral_code(1001, "INVITER100")

    async def asyncTearDown(self):
        await close_shared_db()
        self._tmp_dir.cleanup()

    async def test_capture_referral_start_saved_for_new_user(self):
        await ensure_user_exists(2001, "newbie", "New User")

        result = await capture_referral_start(2001, "ref_inviter100")

        self.assertEqual(result.status, "saved")
        self.assertEqual(result.inviter_user_id, 1001)
        self.assertEqual(await get_referral_attribution(2001), (1001, "INVITER100"))
        banner = await build_referral_inviter_banner_text(result)
        self.assertIn("Вас пригласил", banner or "")
        self.assertIn("@inviter", banner or "")

    async def test_capture_referral_start_blocked_for_manual_access(self):
        await ensure_user_exists(2002, "manual", "Manual")
        await execute("UPDATE users SET sub_until = '2035-01-01T00:00:00' WHERE user_id = ?", (2002,))

        result = await capture_referral_start(2002, "ref_INVITER100")

        self.assertEqual(result.status, "blocked_existing_access")
        self.assertIsNone(await get_referral_attribution(2002))

    async def test_capture_referral_start_not_blocked_by_promo_reservation_only(self):
        await ensure_user_exists(2003, "promo", "Promo")
        await create_promo_code("PROMO1", bonus_days=7, max_activations=10, created_by=1)
        promo_result = await database.activate_promo_code(2003, "PROMO1")
        self.assertEqual(promo_result["status"], "reserved")

        result = await capture_referral_start(2003, "ref_INVITER100")

        self.assertEqual(result.status, "saved")
        self.assertEqual(await get_referral_attribution(2003), (1001, "INVITER100"))

    async def test_capture_referral_start_self_referral_blocked(self):
        result = await capture_referral_start(1001, "ref_INVITER100")
        self.assertEqual(result.status, "self_referral")

    async def test_capture_referral_start_referrals_disabled(self):
        await ensure_user_exists(2005, "disabled", "Disabled")
        await set_app_setting("REFERRAL_ENABLED", "0")

        result = await capture_referral_start(2005, "ref_INVITER100")

        self.assertEqual(result.status, "referrals_disabled")

    async def test_notify_inviter_about_recurring_referral_reward(self):
        await ensure_user_exists(2006, "invitee", "Invitee")
        await database.set_referral_attribution(2006, 1001, "INVITER100")
        bot = AsyncMock()

        sent = await notify_inviter_about_referral_recurring_reward(bot, 2006, purchased_days=90)

        self.assertTrue(sent)
        bot.send_message.assert_awaited_once()
        args, kwargs = bot.send_message.await_args
        self.assertEqual(args[0], 1001)
        self.assertIn("повторную оплату", args[1].lower())
        self.assertIn("+2 дн.", args[1])
        self.assertEqual(kwargs["parse_mode"], "HTML")

    async def test_notify_inviter_about_recurring_referral_reward_uses_text_template(self):
        await ensure_user_exists(2010, "invitee2", "Invitee2")
        await database.set_referral_attribution(2010, 1001, "INVITER100")
        await set_text_override(
            "referral_recurring_reward_notification",
            "RECUR:{invitee_name}:{invitee_user_id}:{purchased_days}:{recurring_bonus_days}",
            updated_by=1,
        )
        bot = AsyncMock()

        sent = await notify_inviter_about_referral_recurring_reward(bot, 2010, purchased_days=45)

        self.assertTrue(sent)
        args, kwargs = bot.send_message.await_args
        self.assertEqual(args[1], "RECUR:@invitee2:2010:45:2")
        self.assertEqual(kwargs["parse_mode"], "HTML")

    async def test_referral_screen_callback_when_disabled(self):
        cb = DummyCallback(user_id=3001)
        with patch("handlers_user._send_or_edit_user_screen", new=AsyncMock()) as render_mock:
            await set_app_setting("REFERRAL_ENABLED", "0")
            await set_text_override("referral_unavailable", "REF_OFF", updated_by=1)
            await handlers_user.referrals_from_profile(cb)

        render_mock.assert_awaited_once()
        args, _kwargs = render_mock.await_args
        self.assertEqual(args[1], "REF_OFF")

    async def test_referral_screen_uses_dynamic_recurring_min_days(self):
        cb = DummyCallback(user_id=3002)
        cb.bot = DummyBot()
        await set_app_setting("REFERRAL_RECURRING_MIN_PURCHASE_DAYS", "45")
        fake_data = {
            "code": "INVITER100",
            "link": "https://t.me/testbot?start=ref_INVITER100",
            "invited_count": 1,
            "rewarded_count_first_payment": 1,
            "inviter_first_payment_bonus_days_total": 3,
            "inviter_recurring_bonus_days_total": 2,
            "inviter_bonus_days_total": 5,
            "friends_bonus_days_total": 5,
            "user_invitee_bonus_days_total": 0,
            "overall_bonus_days_total": 10,
            "rewarded_count": 1,
            "bonus_days": 5,
            "invitee_bonus_days_total": 0,
        }
        with (
            patch("handlers_user.get_referral_screen_data", new=AsyncMock(return_value=fake_data)),
            patch("handlers_user._send_or_edit_user_screen", new=AsyncMock()) as render_mock,
        ):
            await handlers_user.referrals_from_profile(cb)

        text = render_mock.await_args.args[1]
        self.assertIn("от 45 дней", text)

    async def test_start_banner_uses_text_template_flow(self):
        await set_text_override("referral_inviter_banner", "BANNER:{inviter_display_name}", updated_by=1)
        await ensure_user_exists(2009, "banner_user", "Banner User")
        result = await capture_referral_start(2009, "ref_INVITER100")

        banner = await build_referral_inviter_banner_text(result)

        self.assertEqual(result.status, "saved")
        self.assertEqual(banner, "BANNER:@inviter")

    async def test_existing_referral_screen_data_backward_compat(self):
        from referrals import get_referral_screen_data

        data = await get_referral_screen_data(1001, "mybot")

        self.assertIn("rewarded_count", data)
        self.assertIn("bonus_days", data)
        self.assertIn("invitee_bonus_days_total", data)


if __name__ == "__main__":
    unittest.main()
