import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

from content_settings import validate_text_template


def test_validate_text_template_rejects_missing_buy_and_renew_placeholders():
    ok, err = asyncio.run(validate_text_template("buy_menu", "{configs_per_user}"))
    assert not ok
    assert "price_lines" in err

    ok, err = asyncio.run(validate_text_template("buy_menu", "{price_lines}"))
    assert not ok
    assert "configs_per_user" in err

    ok, err = asyncio.run(validate_text_template("renew_menu", "{remaining}"))
    assert not ok
    assert "price_lines" in err

    ok, err = asyncio.run(validate_text_template("renew_menu", "{price_lines}"))
    assert not ok
    assert "remaining" in err


def test_validate_text_template_accepts_valid_templates():
    assert asyncio.run(validate_text_template("buy_menu", "{configs_per_user}\n{price_lines}"))[0]
    assert asyncio.run(validate_text_template("renew_menu", "{remaining}\n{price_lines}"))[0]
    assert asyncio.run(validate_text_template("support_contact", "{support_username}"))[0]
