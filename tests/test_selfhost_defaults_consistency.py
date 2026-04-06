from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

from config_defaults import DEFAULT_ENV


def test_config_defaults_match_selfhost_profile():
    assert DEFAULT_ENV["EGRESS_DENYLIST_ENABLED"] == "1"
    assert DEFAULT_ENV["EGRESS_DENYLIST_MODE"] == "soft"
    assert DEFAULT_ENV["EGRESS_DENYLIST_REFRESH_MINUTES"] == "30"


def test_installer_selfhost_defaults_match_runtime_defaults():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert 'SELFHOST_EGRESS_DENYLIST_ENABLED_DEFAULT="1"' in script
    assert 'SELFHOST_EGRESS_DENYLIST_MODE_DEFAULT="soft"' in script
    assert 'SELFHOST_EGRESS_DENYLIST_REFRESH_MINUTES_DEFAULT="30"' in script


def test_default_db_path_points_to_runtime_dir():
    script = Path("awg-tgbot.sh").read_text(encoding="utf-8")
    assert DEFAULT_ENV["DB_PATH"] == "/opt/amnezia/bot/runtime/vpn_bot.db"
    assert 'RUNTIME_DIR="${INSTALL_DIR}/runtime"' in script
    assert 'DEFAULT_DB_PATH="${RUNTIME_DIR}/${DEFAULT_DB_BASENAME}"' in script
