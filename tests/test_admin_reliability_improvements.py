import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("SERVER_PUBLIC_KEY", "test-public-key")
os.environ.setdefault("SERVER_IP", "1.1.1.1:51820")
os.environ.setdefault("ENCRYPTION_SECRET", "test-secret")

import database
from awg_backend import reissue_user_device
from config import ADMIN_ID, CONFIGS_PER_USER
from content_settings import validate_text_template
from database import (
    claim_next_broadcast_job,
    close_shared_db,
    count_problematic_activations,
    create_broadcast_job,
    ensure_user_exists,
    execute,
    get_broadcast_recipients,
    get_protected_public_keys,
    init_db,
    list_problematic_activations,
    set_referral_attribution,
)
from security_utils import encrypt_text
from texts import get_payment_result_text


class AdminReliabilityImprovementsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmp_dir.name) / "test.sqlite3")
        await close_shared_db()
        database.DB_PATH = self._db_path
        await init_db()

    async def asyncTearDown(self):
        await close_shared_db()
        self._tmp_dir.cleanup()

    async def _seed_key(self, user_id: int, public_key: str) -> None:
        await ensure_user_exists(user_id)
        await execute(
            """
            INSERT INTO keys (user_id, device_num, public_key, config, ip, created_at, psk_key, client_private_key, state)
            VALUES (?, 1, ?, 'cfg', ?, '2026-01-01T00:00:00', ?, ?, 'active')
            """,
            (
                user_id,
                public_key,
                f"10.8.0.{user_id}/32",
                encrypt_text("old-psk"),
                encrypt_text("old-private"),
            ),
        )

    async def test_reissue_protects_only_admin_peer(self):
        with (
            patch("awg_backend.remove_peer_from_awg", new=AsyncMock()),
            patch("awg_backend.add_peer_to_awg", new=AsyncMock()),
            patch("awg_backend.generate_keypair", new=AsyncMock(return_value=("new-private", "new-public-key"))),
            patch("awg_backend.generate_psk", new=AsyncMock(return_value="new-psk")),
        ):
            await self._seed_key(user_id=2, public_key="old-user-key")
            result_user = await reissue_user_device(2, 1)
            self.assertEqual(result_user["status"], "reissued")
            self.assertEqual(await get_protected_public_keys(), set())

        with (
            patch("awg_backend.remove_peer_from_awg", new=AsyncMock()),
            patch("awg_backend.add_peer_to_awg", new=AsyncMock()),
            patch("awg_backend.generate_keypair", new=AsyncMock(return_value=("new-private-a", "new-public-admin"))),
            patch("awg_backend.generate_psk", new=AsyncMock(return_value="new-psk-a")),
        ):
            await self._seed_key(user_id=ADMIN_ID, public_key="old-admin-key")
            result_admin = await reissue_user_device(ADMIN_ID, 1)
            self.assertEqual(result_admin["status"], "reissued")
            protected = await get_protected_public_keys()
            self.assertIn("new-public-admin", protected)

    async def test_problematic_activation_query_uses_latest_payment(self):
        await ensure_user_exists(1001)
        await ensure_user_exists(1002)
        await ensure_user_exists(1003)
        await execute(
            """
            INSERT INTO payments (
                telegram_payment_charge_id, user_id, payload, amount, created_at, status, last_provision_status, updated_at
            ) VALUES
                ('p-old-problem', 1001, 'sub_30', 100, '2026-01-01T00:00:00', 'needs_repair', 'needs_repair', '2026-01-01T00:01:00'),
                ('p-new-ok', 1001, 'sub_30', 100, '2026-01-02T00:00:00', 'applied', 'ready', '2026-01-02T00:01:00'),
                ('p-stuck', 1002, 'sub_30', 100, '2026-01-03T00:00:00', 'stuck_manual', 'stuck_manual', '2026-01-03T00:01:00'),
                ('p-ready-pending', 1003, 'sub_30', 100, '2026-01-04T00:00:00', 'applied', 'ready_config_pending', '2026-01-04T00:01:00')
            """
        )

        total = await count_problematic_activations()
        items = await list_problematic_activations(limit=10, offset=0)

        self.assertEqual(total, 2)
        self.assertEqual([item["user_id"] for item in items], [1002, 1003])
        self.assertEqual(items[0]["status"], "stuck_manual")

    async def test_broadcast_segments_recipient_selection(self):
        await ensure_user_exists(2001)
        await ensure_user_exists(2002)
        await ensure_user_exists(2003)
        await ensure_user_exists(2004)
        await ensure_user_exists(2005)
        await execute("UPDATE users SET sub_until = '2099-01-01T00:00:00' WHERE user_id = 2001")
        await execute("UPDATE users SET sub_until = '2099-01-01T00:00:00' WHERE user_id = 2005")
        await execute(
            """
            INSERT INTO keys (user_id, device_num, public_key, config, ip, created_at, state)
            VALUES
                (2001, 1, 'k1', 'cfg', '10.8.0.1/32', '2026-01-01T00:00:00', 'active'),
                (2005, 1, 'k5', 'cfg', '10.8.0.5/32', '2026-01-01T00:00:00', 'active')
            """
        )
        await execute(
            """
            INSERT INTO payments (telegram_payment_charge_id, user_id, payload, amount, created_at, status, last_provision_status, updated_at)
            VALUES
                ('pay-2001', 2001, 'sub_30', 100, '2026-01-01T00:00:00', 'applied', 'ready', '2026-01-01T00:01:00'),
                ('pay-2003', 2003, 'sub_30', 100, '2026-01-01T00:00:00', 'needs_repair', 'needs_repair', '2026-01-01T00:01:00')
            """
        )
        await set_referral_attribution(invitee_user_id=2004, inviter_user_id=2001, referral_code="abc")

        async def recipients_for(segment: str) -> list[int]:
            await create_broadcast_job(ADMIN_ID, text=f"segment {segment}", segment=segment)
            claimed = await claim_next_broadcast_job()
            self.assertIsNotNone(claimed)
            job_id, _, _, _, _ = claimed
            return await get_broadcast_recipients(job_id, 0, 100)

        self.assertEqual(await recipients_for("active_subscription"), [2001, 2005])
        self.assertEqual(await recipients_for("with_any_payment"), [2001, 2003])
        self.assertEqual(await recipients_for("problematic_activation"), [2003])
        self.assertEqual(await recipients_for("without_keys"), [2002, 2003, 2004])
        self.assertEqual(await recipients_for("with_referral_attribution"), [2004])

    async def test_text_override_validation_for_new_keys(self):
        ok_instruction, _ = await validate_text_template("instruction_body", "link: {download_url}")
        bad_instruction, bad_instruction_reason = await validate_text_template("instruction_body", "link: no placeholder")
        ok_next_step, _ = await validate_text_template("payment_next_step", "{configs_per_user}")
        bad_next_step, bad_next_step_reason = await validate_text_template("payment_next_step", "no placeholders")
        self.assertTrue(ok_instruction)
        self.assertFalse(bad_instruction)
        self.assertIn("download_url", bad_instruction_reason)
        self.assertTrue(ok_next_step)
        self.assertFalse(bad_next_step)
        self.assertIn("configs_per_user", bad_next_step_reason)

    async def test_post_payment_result_text_clarifies_device_choice(self):
        text = await get_payment_result_text("ready")
        self.assertIn("🔑 Подключение", text)
        self.assertIn("выберите нужное устройство", text.lower())
        self.assertIn(str(CONFIGS_PER_USER), text)


if __name__ == "__main__":
    unittest.main()
