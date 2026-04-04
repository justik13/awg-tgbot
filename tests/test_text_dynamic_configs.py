import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

from content_settings import TEXT_DEFAULTS


def test_buy_menu_uses_dynamic_configs_per_user_placeholder():
    assert "{configs_per_user}" in TEXT_DEFAULTS["buy_menu"]
    assert "2 устройств" not in TEXT_DEFAULTS["buy_menu"]
