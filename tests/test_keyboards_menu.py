import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / 'bot'))

from keyboards import get_main_menu
from ui_constants import BTN_BUY, BTN_GUIDE, BTN_PROMO


def test_main_menu_contains_promo_and_no_guide():
    kb = get_main_menu(user_id=100, admin_id=999)
    texts = [button.text for row in kb.keyboard for button in row]

    assert BTN_PROMO in texts
    assert BTN_GUIDE not in texts


def test_buy_button_wording_updated():
    assert BTN_BUY == '💳 Купить / Продлить'
