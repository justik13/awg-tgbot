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
from keyboards import get_support_center_kb, get_support_subpage_back_kb
from ui_constants import CB_OPEN_SUPPORT, CB_SUPPORT_USEFUL


class SupportUsefulTests(unittest.TestCase):
    def test_support_center_has_useful_button(self):
        kb = get_support_center_kb()
        buttons = [button for row in kb.inline_keyboard for button in row]
        self.assertTrue(any(button.text == "📚 Полезное" and button.callback_data == CB_SUPPORT_USEFUL for button in buttons))

    def test_support_useful_text_matches_expected(self):
        self.assertEqual(
            TEXT_DEFAULTS["support_useful"],
            "📚 Полезное\n\n"
            "• Документация Amnezia\n"
            "Официальная документация по установке, подключению, настройке приложения и раздельному туннелированию:\n"
            "https://docs.amnezia.org/ru/documentation/\n\n"
            "• Зеркало документации\n"
            "Если основной сайт не открывается:\n"
            "https://m-docs-3w5hsuiikq-ez.a.run.app/ru/\n\n"
            "• Скачать Amnezia\n"
            "Официальная страница загрузки приложений:\n"
            "https://m-1-14-3w5hsuiikq-ez.a.run.app/ru/downloads\n\n"
            "• IP-списки для России\n"
            "Готовые списки российских IP-адресов для ручной настройки исключений / split tunneling:\n"
            "https://russia.iplist.opencck.org/ru/\n\n"
            "• Полные IP-списки\n"
            "Расширенные списки IP-адресов и сетей для более гибкой ручной маршрутизации:\n"
            "https://iplist.opencck.org/ru/\n\n"
            "Важно: это дополнительные материалы для ручной настройки. Базовое подключение к сервису работает и без них.",
        )

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


if __name__ == "__main__":
    unittest.main()
