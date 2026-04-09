import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("SERVER_PUBLIC_KEY", "test-public-key")
os.environ.setdefault("SERVER_IP", "1.1.1.1:51820")
os.environ.setdefault("ENCRYPTION_SECRET", "test-secret")

from content_settings import TEXT_DEFAULTS
from config import get_download_url
from keyboards import get_support_center_kb, get_support_subpage_back_kb
from ui_constants import CB_OPEN_SUPPORT, CB_SUPPORT_USEFUL


class SupportUsefulTests(unittest.TestCase):
    def test_support_center_has_useful_button(self):
        kb = get_support_center_kb()
        buttons = [button for row in kb.inline_keyboard for button in row]
        self.assertTrue(any(button.text == "📚 Полезное" and button.callback_data == CB_SUPPORT_USEFUL for button in buttons))

    def test_support_useful_text_uses_download_url_placeholder(self):
        self.assertIn("{download_url}", TEXT_DEFAULTS["support_useful"])

    def test_support_useful_renders_with_centralized_download_url(self):
        rendered = TEXT_DEFAULTS["support_useful"].format(download_url=get_download_url())
        self.assertIn(get_download_url(), rendered)
        self.assertIn("• Документация Amnezia", rendered)
        self.assertIn("• Зеркало документации", rendered)
        self.assertIn("• Скачать Amnezia", rendered)
        self.assertIn("https://docs.amnezia.org/ru/documentation/", rendered)
        self.assertIn("https://russia.iplist.opencck.org/ru/", rendered)
        self.assertIn("https://iplist.opencck.org/ru/", rendered)

    def test_support_subpage_back_keyboard(self):
        kb = get_support_subpage_back_kb()
        self.assertEqual(len(kb.inline_keyboard), 1)
        self.assertEqual(len(kb.inline_keyboard[0]), 1)
        button = kb.inline_keyboard[0][0]
        self.assertEqual(button.text, "⬅️ Назад в поддержку")
        self.assertEqual(button.callback_data, CB_OPEN_SUPPORT)

    def test_support_subpages_use_support_back_keyboard(self):
        handlers_source = (ROOT / "bot" / "handlers_user.py").read_text(encoding="utf-8")
        self.assertIn("support_payment_callback", handlers_source)
        self.assertIn("support_connection_callback", handlers_source)
        self.assertIn("support_terms_callback", handlers_source)
        self.assertIn("support_useful_callback", handlers_source)
        self.assertEqual(handlers_source.count("reply_markup=get_support_subpage_back_kb()"), 4)

    def test_support_connection_does_not_append_support_short_manually(self):
        handlers_source = (ROOT / "bot" / "handlers_user.py").read_text(encoding="utf-8")
        self.assertIn("await get_instruction_with_policy_text()", handlers_source)
        self.assertNotIn(
            'f"{await get_instruction_with_policy_text()}\\n\\n{await get_support_short_text()}"',
            handlers_source,
        )

    def test_help_windows_link_uses_centralized_download_url(self):
        handlers_source = (ROOT / "bot" / "handlers_user.py").read_text(encoding="utf-8")
        self.assertIn('kb.button(text="🪟 Windows", url=get_download_url())', handlers_source)
        self.assertNotIn("https://amnezia.org/downloads", handlers_source)


if __name__ == "__main__":
    unittest.main()
