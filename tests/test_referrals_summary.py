import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("SERVER_PUBLIC_KEY", "test-public-key")
os.environ.setdefault("SERVER_IP", "1.1.1.1:51820")
os.environ.setdefault("ENCRYPTION_SECRET", "test-secret")

import database
from database import (
    close_shared_db,
    create_referral_recurring_reward_once,
    create_referral_reward_once,
    get_referral_summary,
    init_db,
    set_referral_attribution,
)
from referrals import get_referral_screen_data


class ReferralSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmp_dir.name) / "test.sqlite3")
        await close_shared_db()
        database.DB_PATH = self._db_path
        await init_db()

    async def asyncTearDown(self):
        await close_shared_db()
        self._tmp_dir.cleanup()

    async def test_only_invited_without_payments(self):
        await set_referral_attribution(invitee_user_id=2001, inviter_user_id=1001, referral_code="AAA")
        await set_referral_attribution(invitee_user_id=2002, inviter_user_id=1001, referral_code="AAA")

        summary = await get_referral_summary(1001)

        self.assertEqual(summary["invited_count"], 2)
        self.assertEqual(summary["rewarded_count_first_payment"], 0)
        self.assertEqual(summary["inviter_first_payment_bonus_days_total"], 0)
        self.assertEqual(summary["inviter_recurring_bonus_days_total"], 0)
        self.assertEqual(summary["inviter_bonus_days_total"], 0)
        self.assertEqual(summary["friends_bonus_days_total"], 0)
        self.assertEqual(summary["user_invitee_bonus_days_total"], 0)
        self.assertEqual(summary["overall_bonus_days_total"], 0)

    async def test_first_payment_rewards_are_split(self):
        await set_referral_attribution(invitee_user_id=2001, inviter_user_id=1001, referral_code="AAA")
        await set_referral_attribution(invitee_user_id=2002, inviter_user_id=1001, referral_code="AAA")
        await create_referral_reward_once(2001, 1001, "pay-a", invitee_bonus_days=5, inviter_bonus_days=3)
        await create_referral_reward_once(2002, 1001, "pay-b", invitee_bonus_days=5, inviter_bonus_days=3)

        summary = await get_referral_summary(1001)

        self.assertEqual(summary["rewarded_count_first_payment"], 2)
        self.assertEqual(summary["inviter_first_payment_bonus_days_total"], 6)
        self.assertEqual(summary["inviter_recurring_bonus_days_total"], 0)
        self.assertEqual(summary["inviter_bonus_days_total"], 6)
        self.assertEqual(summary["friends_bonus_days_total"], 10)
        self.assertEqual(summary["user_invitee_bonus_days_total"], 0)
        self.assertEqual(summary["overall_bonus_days_total"], 16)

    async def test_recurring_rewards_counted_separately(self):
        await create_referral_reward_once(2001, 1001, "pay-a", invitee_bonus_days=5, inviter_bonus_days=3)
        await create_referral_recurring_reward_once(2001, 1001, "pay-a-r1", inviter_bonus_days=2)
        await create_referral_recurring_reward_once(2001, 1001, "pay-a-r2", inviter_bonus_days=2)

        summary = await get_referral_summary(1001)

        self.assertEqual(summary["inviter_first_payment_bonus_days_total"], 3)
        self.assertEqual(summary["inviter_recurring_bonus_days_total"], 4)
        self.assertEqual(summary["inviter_bonus_days_total"], 7)
        self.assertEqual(summary["friends_bonus_days_total"], 5)
        self.assertEqual(summary["user_invitee_bonus_days_total"], 0)
        self.assertEqual(summary["overall_bonus_days_total"], 12)

    async def test_invitee_bonus_is_isolated_for_user(self):
        await create_referral_reward_once(1005, 1001, "pay-self", invitee_bonus_days=5, inviter_bonus_days=3)

        summary = await get_referral_summary(1005)

        self.assertEqual(summary["invited_count"], 0)
        self.assertEqual(summary["inviter_bonus_days_total"], 0)
        self.assertEqual(summary["friends_bonus_days_total"], 0)
        self.assertEqual(summary["user_invitee_bonus_days_total"], 5)
        self.assertEqual(summary["overall_bonus_days_total"], 5)

    async def test_screen_data_splits_all_bonus_buckets(self):
        await set_referral_attribution(invitee_user_id=2001, inviter_user_id=1001, referral_code="AAA")
        await set_referral_attribution(invitee_user_id=2002, inviter_user_id=1001, referral_code="AAA")
        await create_referral_reward_once(2001, 1001, "pay-a", invitee_bonus_days=5, inviter_bonus_days=3)
        await create_referral_recurring_reward_once(2001, 1001, "pay-a-r1", inviter_bonus_days=2)
        await create_referral_reward_once(1001, 9999, "pay-user-as-invitee", invitee_bonus_days=4, inviter_bonus_days=1)

        data = await get_referral_screen_data(1001, "mytestbot")

        self.assertEqual(data["invited_count"], 2)
        self.assertEqual(data["rewarded_count_first_payment"], 1)
        self.assertEqual(data["inviter_first_payment_bonus_days_total"], 3)
        self.assertEqual(data["inviter_recurring_bonus_days_total"], 2)
        self.assertEqual(data["inviter_bonus_days_total"], 5)
        self.assertEqual(data["friends_bonus_days_total"], 5)
        self.assertEqual(data["user_invitee_bonus_days_total"], 4)
        self.assertEqual(data["overall_bonus_days_total"], 14)

    async def test_friends_bonus_is_not_user_invitee_bonus(self):
        await create_referral_reward_once(2101, 1001, "pay-friend", invitee_bonus_days=7, inviter_bonus_days=3)
        await create_referral_reward_once(1001, 9999, "pay-me", invitee_bonus_days=4, inviter_bonus_days=3)

        summary = await get_referral_summary(1001)

        self.assertEqual(summary["friends_bonus_days_total"], 7)
        self.assertEqual(summary["user_invitee_bonus_days_total"], 4)


if __name__ == "__main__":
    unittest.main()
