from pathlib import Path
import subprocess


def test_autobackup_script_prunes_only_project_archives(tmp_path):
    install_dir = tmp_path / "install"
    scripts_dir = install_dir / "scripts"
    backups_dir = install_dir / "backups"
    scripts_dir.mkdir(parents=True)
    backups_dir.mkdir(parents=True)

    source_script = Path("scripts/awg-tgbot-autobackup.sh")
    test_script = scripts_dir / "awg-tgbot-autobackup.sh"
    test_script.write_text(source_script.read_text(encoding="utf-8"), encoding="utf-8")
    test_script.chmod(0o755)

    awg_script = install_dir / "awg-tgbot.sh"
    awg_script.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "mkdir -p \"$(dirname \"$0\")/backups\"\n"
        "touch \"$(dirname \"$0\")/backups/awg-tgbot-backup-20260103_000000.tar.gz\"\n",
        encoding="utf-8",
    )
    awg_script.chmod(0o755)

    (install_dir / ".env").write_text("AUTO_BACKUP_ENABLED=1\nAUTO_BACKUP_KEEP_COUNT=2\n", encoding="utf-8")

    for name in [
        "awg-tgbot-backup-20260101_000000.tar.gz",
        "awg-tgbot-backup-20260102_000000.tar.gz",
        "random-backup.tar.gz",
    ]:
        (backups_dir / name).write_text("x", encoding="utf-8")

    subprocess.run([str(test_script)], check=True)

    remaining = sorted(p.name for p in backups_dir.iterdir())
    assert "awg-tgbot-backup-20260103_000000.tar.gz" in remaining
    assert "awg-tgbot-backup-20260102_000000.tar.gz" in remaining
    assert "awg-tgbot-backup-20260101_000000.tar.gz" not in remaining
    assert "random-backup.tar.gz" in remaining
