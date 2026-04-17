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
    get_pending_broadcast,
    get_broadcast_recipients,
    get_protected_public_keys,
    init_db,
    list_problematic_activations,
    set_pending_broadcast,
    set_text_override,
    set_referral_attribution,
)
from security_utils import encrypt_text
from content_settings import get_text
from texts import get_payment_result_text
from keyboards import get_problem_activations_kb
from handlers_admin import _user_manage_kb, admin_noop, admin_retry_activation_from_problem, broadcast_confirm
from ui_constants import (
    CB_ADMIN_NOOP,
    CB_ADMIN_MANAGE_USER_PROBLEM_PREFIX,
    CB_ADMIN_OPEN_USER_CARD_PROBLEM_PREFIX,
    CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX,
    CB_ADMIN_USERS_PAGE_PREFIX,
)


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
                ('pay-2001-old-failed', 2001, 'sub_7', 100, '2025-12-31T00:00:00', 'failed', 'failed', '2025-12-31T00:01:00'),
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

    async def test_problematic_activation_keyboard_has_actions_for_every_visible_item(self):
        items = [
            {"user_id": 3001, "retry_enabled": True},
            {"user_id": 3002, "retry_enabled": False},
            {"user_id": 3003, "retry_enabled": True},
        ]
        kb = get_problem_activations_kb(page=1, total_pages=4, items=items)
        callbacks = [button.callback_data for row in kb.inline_keyboard for button in row]
        self.assertIn(f"{CB_ADMIN_OPEN_USER_CARD_PROBLEM_PREFIX}3001_1", callbacks)
        self.assertIn(f"{CB_ADMIN_OPEN_USER_CARD_PROBLEM_PREFIX}3002_1", callbacks)
        self.assertIn(f"{CB_ADMIN_OPEN_USER_CARD_PROBLEM_PREFIX}3003_1", callbacks)
        self.assertIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}3001_1", callbacks)
        self.assertIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}3003_1", callbacks)
        self.assertNotIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}3002_1", callbacks)

    async def test_problem_context_user_card_keeps_full_actions_and_context_nav(self):
        kb = _user_manage_kb(uid=4001, page=2, show_retry_activation=True, device_nums=[1, 2], source="problem_activations")
        callbacks = [button.callback_data for row in kb.inline_keyboard for button in row]
        self.assertIn("admin_add_days_4001_1_2", callbacks)
        self.assertIn("admin_revoke_4001_2", callbacks)
        self.assertIn("admin_delete_4001_2", callbacks)
        self.assertIn("admin_device_delete_4001_1_2", callbacks)
        self.assertIn("admin_device_reissue_4001_1_2", callbacks)
        self.assertIn(f"{CB_ADMIN_MANAGE_USER_PROBLEM_PREFIX}4001_2", callbacks)
        self.assertIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}4001_2", callbacks)
        self.assertIn("a:pm:pa:p:2", callbacks)

    async def test_normal_user_card_flow_not_regressed(self):
        kb = _user_manage_kb(uid=5001, page=1, show_retry_activation=True, device_nums=[1], source="users")
        callbacks = [button.callback_data for row in kb.inline_keyboard for button in row]
        self.assertIn("admin_add_days_5001_30_1", callbacks)
        self.assertIn("admin_retry_activation_5001_1", callbacks)
        self.assertIn(f"{CB_ADMIN_USERS_PAGE_PREFIX}1", callbacks)
        self.assertNotIn(f"{CB_ADMIN_MANAGE_USER_PROBLEM_PREFIX}5001_1", callbacks)
        self.assertNotIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}5001_1", callbacks)

    async def test_admin_noop_handler_is_silent(self):
        class DummyFromUser:
            id = ADMIN_ID

        class DummyCallback:
            from_user = DummyFromUser()

            def __init__(self):
                self.answer = AsyncMock()

        cb = DummyCallback()
        await admin_noop(cb)
        cb.answer.assert_awaited_once_with()

    async def test_page_indicator_uses_admin_noop_callback(self):
        kb = get_problem_activations_kb(page=0, total_pages=1, items=[{"user_id": 1, "retry_enabled": False}])
        callbacks = [button.callback_data for row in kb.inline_keyboard for button in row]
        self.assertIn(CB_ADMIN_NOOP, callbacks)

    async def _run_problem_retry_with_result(self, result_code: str):
        class DummyUser:
            id = ADMIN_ID

        class DummyMessage:
            def __init__(self):
                self.answer = AsyncMock()

        class DummyCb:
            def __init__(self):
                self.from_user = DummyUser()
                self.data = f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}777_3"
                self.message = DummyMessage()
                self.bot = object()
                self.answer = AsyncMock()

        cb = DummyCb()
        with (
            patch("handlers_admin.admin_command_limited", return_value=False),
            patch("handlers_admin.get_latest_user_payment_summary", new=AsyncMock(return_value={"payment_id": "pay-777"})),
            patch("handlers_admin.manual_retry_activation", new=AsyncMock(return_value={"result": result_code, "message": "ok"})),
            patch("handlers_admin.write_audit_log", new=AsyncMock()) as audit_mock,
        ):
            await admin_retry_activation_from_problem(cb)
        return audit_mock.await_args_list

    async def test_problem_retry_renders_result_with_problem_context_navigation(self):
        class DummyUser:
            id = ADMIN_ID

        class DummyMessage:
            def __init__(self):
                self.answer = AsyncMock()

        class DummyCb:
            def __init__(self):
                self.from_user = DummyUser()
                self.data = f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}777_3"
                self.message = DummyMessage()
                self.bot = object()
                self.answer = AsyncMock()

        cb = DummyCb()
        with (
            patch("handlers_admin.admin_command_limited", return_value=False),
            patch("handlers_admin.get_latest_user_payment_summary", new=AsyncMock(return_value={"payment_id": "pay-777"})),
            patch("handlers_admin.manual_retry_activation", new=AsyncMock(return_value={"result": "already_applied", "message": "noop"})),
            patch("handlers_admin.write_audit_log", new=AsyncMock()),
        ):
            await admin_retry_activation_from_problem(cb)

        cb.answer.assert_awaited_once_with("Повтор обработан")
        cb.message.answer.assert_awaited_once()
        msg_args = cb.message.answer.await_args.args
        msg_kwargs = cb.message.answer.await_args.kwargs
        self.assertEqual(msg_kwargs["parse_mode"], "HTML")
        self.assertIn("ℹ️ Повтор активации не требуется", msg_args[0])
        callbacks = [button.callback_data for row in msg_kwargs["reply_markup"].inline_keyboard for button in row]
        self.assertIn(f"{CB_ADMIN_MANAGE_USER_PROBLEM_PREFIX}777_3", callbacks)
        self.assertIn("a:pm:pa:p:3", callbacks)
        self.assertNotIn(f"{CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX}777_3", callbacks)

    async def test_problem_retry_audit_logging_succeeded(self):
        calls = await self._run_problem_retry_with_result("succeeded")
        self.assertTrue(any(call.args[1] == "manual_retry_succeeded" for call in calls))
        self.assertTrue(any("source=problem_activations" in str(call.args[2]) for call in calls))

    async def test_problem_retry_audit_logging_noop(self):
        calls = await self._run_problem_retry_with_result("already_applied")
        self.assertTrue(any(call.args[1] == "manual_retry_noop" for call in calls))
        self.assertTrue(any("source=problem_activations" in str(call.args[2]) for call in calls))

    async def test_problem_retry_audit_logging_failed(self):
        calls = await self._run_problem_retry_with_result("failed")
        self.assertTrue(any(call.args[1] == "manual_retry_failed" for call in calls))
        self.assertTrue(any("source=problem_activations" in str(call.args[2]) for call in calls))

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

    async def test_get_text_falls_back_to_default_template_on_invalid_override_formatting(self):
        await set_text_override("payment_next_step", "broken template {configs_per_user")

        rendered = await get_text("payment_next_step", configs_per_user=3)

        self.assertIn("3", rendered)
        self.assertNotIn("broken template", rendered)
        self.assertIn("до <b>3</b> устройств", rendered)

    async def test_pending_and_claimed_broadcast_keep_selected_segment(self):
        await set_pending_broadcast(ADMIN_ID, "hello operators", segment="problematic_activation")
        pending = await get_pending_broadcast(ADMIN_ID)
        self.assertEqual(pending, {"text": "hello operators", "segment": "problematic_activation"})

        job_id = await create_broadcast_job(ADMIN_ID, "hello operators", segment="problematic_activation")
        claimed = await claim_next_broadcast_job()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[0], job_id)
        self.assertEqual(claimed[4], "problematic_activation")

    async def test_broadcast_confirm_preserves_segment_and_reports_context(self):
        class DummyUser:
            id = ADMIN_ID

        class DummyMessage:
            def __init__(self):
                self.answer = AsyncMock()

        class DummyCb:
            def __init__(self):
                self.from_user = DummyUser()
                self.message = DummyMessage()
                self.answer = AsyncMock()

        cb = DummyCb()
        with (
            patch("handlers_admin._guard_admin_callback", new=AsyncMock(return_value=True)),
            patch(
                "handlers_admin.get_pending_broadcast",
                new=AsyncMock(return_value={"text": "queued text", "segment": "problematic_activation"}),
            ),
            patch("handlers_admin.get_broadcast_segment_user_count", new=AsyncMock(return_value=17)),
            patch("handlers_admin.create_broadcast_job", new=AsyncMock(return_value=9001)) as create_job_mock,
            patch("handlers_admin.clear_pending_broadcast", new=AsyncMock()) as clear_pending_mock,
            patch("handlers_admin.write_audit_log", new=AsyncMock()) as audit_mock,
        ):
            await broadcast_confirm(cb)

        create_job_mock.assert_awaited_once_with(ADMIN_ID, "queued text", segment="problematic_activation")
        self.assertNotEqual(create_job_mock.await_args.kwargs.get("segment"), "all")
        clear_pending_mock.assert_awaited_once_with(ADMIN_ID)
        cb.message.answer.assert_awaited_once()
        answer_text = cb.message.answer.await_args.args[0]
        self.assertIn("Проблемные активации", answer_text)
        self.assertIn("Текущая оценка получателей: <b>17</b>", answer_text)
        self.assertEqual(cb.message.answer.await_args.kwargs["parse_mode"], "HTML")
        cb.answer.assert_awaited_once_with("Поставлено в очередь")
        audit_mock.assert_awaited_once_with(ADMIN_ID, "broadcast_queued", "job_id=9001")

    async def test_post_payment_result_text_clarifies_device_choice(self):
        text = await get_payment_result_text("ready")
        self.assertIn("🔑 Подключение", text)
        self.assertIn("выберите нужное устройство", text.lower())
        self.assertIn(str(CONFIGS_PER_USER), text)


if __name__ == "__main__":
    unittest.main()
