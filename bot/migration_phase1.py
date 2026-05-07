"""
PHASE 1: Multi-node schema migration module.
This module handles safe, idempotent migration from single-server to multi-node architecture.

⚠️ PHASE 1 BOUNDARY
- Source of truth: `keys` table
- `devices` is MIGRATION-ONLY derived state
- DO NOT query `devices` in handlers/payments/referrals until Phase 2
- Enforcement: code review + integration tests

CRITICAL RULES:
- Never modify existing keys table or business logic
- Migration is transactional with full rollback on error
- Dry-run mode available for testing without DB writes
- Existing users continue using their configs without reconnect/regenerate
- Tables devices/nodes are migration/compatibility layer only until Phase 2
"""
import asyncio
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from config import DB_PATH, ENCRYPTION_SECRET, SERVER_IP, logger
from database import ensure_column, get_shared_db, open_db
from security_utils import encrypt_text


# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

SCHEMA_VERSION = "phase1_multi_node_schema"

CREATE_NODES_TABLE = """
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    s1 TEXT, s2 TEXT, s3 TEXT, s4 TEXT,
    h1 TEXT, h2 TEXT, h3 TEXT, h4 TEXT,
    country TEXT, flag_emoji TEXT,
    is_visible INTEGER DEFAULT 1,
    capacity INTEGER DEFAULT 50,
    active_configs INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    api_token TEXT,
    last_seen TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_DEVICES_TABLE = """
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    slot_number INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    public_key TEXT NOT NULL,
    private_key_enc TEXT NOT NULL,
    psk_enc TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT (datetime('now')),
    last_reissued_at TEXT,
    UNIQUE(user_id, slot_number),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(node_id) REFERENCES nodes(id)
);
"""

CREATE_SCHEMA_VERSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version TEXT PRIMARY KEY,
    applied_at TEXT DEFAULT (datetime('now')),
    description TEXT
);
"""

CREATE_INDEXES = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_devices_public_key_unique ON devices(public_key);",
    "CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_devices_node_id ON devices(node_id);",
    "CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);",
]

# =============================================================================
# BACKUP UTILITIES
# =============================================================================


async def create_backup(db_path: str) -> str:
    """Create timestamped backup of the database."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{timestamp}"
    
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, 
            shutil.copy2, 
            db_path, 
            backup_path
        )
        logger.info("Backup created: %s", backup_path)
        return backup_path
    except Exception as e:
        logger.error("Backup failed: %s", e)
        raise RuntimeError(f"Backup creation failed: {e}")


async def verify_backup(backup_path: str) -> bool:
    """Verify backup file exists and is readable."""
    try:
        if not Path(backup_path).exists():
            return False
        
        # Quick integrity check
        async with aiosqlite.connect(backup_path) as db:
            await db.execute("PRAGMA integrity_check")
        
        logger.info("Backup verified: %s", backup_path)
        return True
    except Exception as e:
        logger.error("Backup verification failed: %s", e)
        return False


# =============================================================================
# DRY-RUN MODE
# =============================================================================


async def run_migration_dry_run() -> dict[str, Any]:
    """
    Run migration analysis without writing to DB.
    Returns detailed report of what would happen.
    """
    report = {
        "users_scanned": 0,
        "configs_found": 0,
        "devices_to_create": 0,
        "conflicts": [],
        "missing_keys": [],
        "duplicate_slots": [],
        "duplicate_public_keys": [],
        "config_overflow_warnings": [],
        "migration_warnings": [],
        "legacy_node_info": {},
        "would_migrate": True,
    }
    
    try:
        db = await open_db()
        try:
            # Check if tables exist
            async with db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
                tables = {row[0] for row in await cursor.fetchall()}
            
            if "keys" not in tables:
                report["migration_warnings"].append("Table 'keys' does not exist - no legacy configs to migrate")
                return report
            
            # Count users
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                result = await cursor.fetchone()
                report["users_scanned"] = result[0] if result else 0
            
            # Analyze existing configs
            async with db.execute("""
                SELECT k.user_id, k.public_key, k.client_private_key, k.psk_key, k.created_at, u.sub_until
                FROM keys k
                LEFT JOIN users u ON k.user_id = u.user_id
                WHERE k.state = 'active' OR k.state IS NULL
                ORDER BY k.created_at ASC, k.id ASC
            """) as cursor:
                rows = await cursor.fetchall()
            
            report["configs_found"] = len(rows)
            
            # Track per-user configs
            user_configs: dict[int, list] = {}
            seen_public_keys: set[str] = set()
            
            for row in rows:
                user_id, public_key, private_key, psk_key, created_at, sub_until = row
                
                if user_id not in user_configs:
                    user_configs[user_id] = []
                user_configs[user_id].append(row)
                
                # Check for duplicate public keys
                if public_key in seen_public_keys:
                    report["duplicate_public_keys"].append({
                        "public_key": public_key[:20] + "...",
                        "user_id": user_id,
                    })
                    report["migration_warnings"].append(f"Duplicate public_key detected: {public_key[:20]}...")
                else:
                    seen_public_keys.add(public_key)
                
                # Check for missing keys
                if not private_key:
                    report["missing_keys"].append({
                        "user_id": user_id,
                        "public_key": public_key[:20] + "...",
                        "issue": "missing_private_key",
                    })
                if not psk_key:
                    report["missing_keys"].append({
                        "user_id": user_id,
                        "public_key": public_key[:20] + "...",
                        "issue": "missing_psk",
                    })
            
            # Check for config overflow
            from config import CONFIGS_PER_USER
            for user_id, configs in user_configs.items():
                if len(configs) > CONFIGS_PER_USER:
                    report["config_overflow_warnings"].append({
                        "user_id": user_id,
                        "config_count": len(configs),
                        "max_allowed": CONFIGS_PER_USER,
                    })
                    report["migration_warnings"].append(
                        f"User {user_id} has {len(configs)} configs but max is {CONFIGS_PER_USER}"
                    )
            
            # Calculate devices to create
            report["devices_to_create"] = sum(len(configs) for configs in user_configs.values())
            
            # Legacy node info
            report["legacy_node_info"] = {
                "server_ip": SERVER_IP or "NOT_SET",
                "will_create_node_1": True,
            }
            
        finally:
            await db.close()
            
    except Exception as e:
        logger.exception("Dry-run failed: %s", e)
        report["would_migrate"] = False
        report["migration_warnings"].append(f"Dry-run error: {e}")
    
    return report


def print_dry_run_report(report: dict[str, Any]) -> None:
    """Print formatted dry-run report."""
    print("\n" + "=" * 60)
    print("MIGRATION DRY-RUN REPORT")
    print("=" * 60)
    print(f"Users scanned:          {report['users_scanned']}")
    print(f"Configs found:          {report['configs_found']}")
    print(f"Devices to create:      {report['devices_to_create']}")
    print(f"Would migrate:          {report['would_migrate']}")
    
    if report["legacy_node_info"]:
        print(f"\nLegacy Node Info:")
        print(f"  Server IP:            {report['legacy_node_info'].get('server_ip', 'N/A')}")
        print(f"  Will create node_1:   {report['legacy_node_info'].get('will_create_node_1', False)}")
    
    if report["duplicate_public_keys"]:
        print(f"\n⚠️  DUPLICATE PUBLIC KEYS: {len(report['duplicate_public_keys'])}")
        for item in report["duplicate_public_keys"][:5]:
            print(f"   - {item['public_key']} (user_id={item['user_id']})")
    
    if report["missing_keys"]:
        print(f"\n⚠️  MISSING KEYS: {len(report['missing_keys'])}")
        for item in report["missing_keys"][:5]:
            print(f"   - user_id={item['user_id']}: {item['issue']}")
    
    if report["config_overflow_warnings"]:
        print(f"\n⚠️  CONFIG OVERFLOW: {len(report['config_overflow_warnings'])}")
        for item in report["config_overflow_warnings"][:5]:
            print(f"   - user_id={item['user_id']}: {item['config_count']} configs (max={item['max_allowed']})")
    
    if report["migration_warnings"]:
        print(f"\n⚠️  WARNINGS: {len(report['migration_warnings'])}")
        for warning in report["migration_warnings"][:10]:
            print(f"   - {warning}")
    
    print("\n" + "=" * 60)


# =============================================================================
# MIGRATION LOGIC
# =============================================================================


async def _schema_exists(db: aiosqlite.Connection, table_name: str) -> bool:
    """Check if table exists."""
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ) as cursor:
        return (await cursor.fetchone()) is not None


async def _ensure_schema(db: aiosqlite.Connection) -> None:
    """Create tables and indexes if they don't exist."""
    # Create nodes table
    await db.execute(CREATE_NODES_TABLE)
    
    # Create devices table
    await db.execute(CREATE_DEVICES_TABLE)
    
    # Create schema_versions table
    await db.execute(CREATE_SCHEMA_VERSIONS_TABLE)
    
    # Create indexes (one at a time for SQLite compatibility)
    for index_sql in CREATE_INDEXES:
        await db.execute(index_sql)
    
    # Add columns to users table (safe, idempotent)
    await ensure_column(db, "users", "subscription_expires_at", "TEXT")
    await ensure_column(db, "users", "max_devices", "INTEGER DEFAULT 3")
    
    logger.info("Schema tables and indexes ensured")


async def _bootstrap_legacy_node(db: aiosqlite.Connection) -> int:
    """
    Bootstrap node_1 as legacy single-server node.
    Returns node_id.
    """
    # Check if nodes table already has data
    async with db.execute("SELECT COUNT(*) FROM nodes") as cursor:
        count = (await cursor.fetchone())[0]
    
    if count > 0:
        logger.info("Nodes table already has %d entries, skipping bootstrap", count)
        # Return first node_id if exists
        async with db.execute("SELECT id FROM nodes ORDER BY id LIMIT 1") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 1
    
    # Parse SERVER_IP
    ip = "0.0.0.0"
    port = 51820
    status = "legacy_invalid"
    
    if SERVER_IP:
        try:
            if ":" in SERVER_IP:
                host_part, port_part = SERVER_IP.rsplit(":", 1)
                if port_part.isdigit() and 1 <= int(port_part) <= 65535:
                    ip = host_part
                    port = int(port_part)
                    status = "legacy"
                else:
                    logger.warning("Invalid port in SERVER_IP: %s", SERVER_IP)
            else:
                ip = SERVER_IP
                status = "legacy"
        except Exception as e:
            logger.warning("Failed to parse SERVER_IP: %s, error: %s", SERVER_IP, e)
    
    # Insert legacy node
    await db.execute("""
        INSERT INTO nodes (name, ip, port, status, is_visible, capacity, country, flag_emoji)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, ("node_1", ip, port, status, 1, 50, "Legacy", "🏠"))
    
    logger.info("Bootstrapped legacy node_1: ip=%s, port=%s, status=%s", ip, port, status)
    
    # Get the inserted node_id
    async with db.execute("SELECT last_insert_rowid()") as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 1


async def _is_already_encrypted(value: str | None) -> bool:
    """Check if value appears to be already encrypted."""
    if not value:
        return False
    return value.startswith("enc:")


async def _migrate_existing_configs(
    db: aiosqlite.Connection,
    legacy_node_id: int,
) -> dict[str, Any]:
    """
    Migrate existing configs from 'keys' table to 'devices' table.
    Uses SAVEPOINT for each user to ensure atomicity per user.
    Returns migration stats.
    """
    from config import CONFIGS_PER_USER
    
    stats = {
        "users_processed": 0,
        "devices_created": 0,
        "skipped_duplicates": 0,
        "errors": [],
    }
    
    # Fetch all active configs sorted deterministically
    async with db.execute("""
        SELECT k.user_id, k.public_key, k.client_private_key, k.psk_key, k.created_at, k.id
        FROM keys k
        WHERE (k.state = 'active' OR k.state IS NULL)
        ORDER BY k.created_at ASC, k.id ASC
    """) as cursor:
        all_configs = await cursor.fetchall()
    
    # Group by user
    user_configs: dict[int, list] = {}
    for row in all_configs:
        user_id = row[0]
        if user_id not in user_configs:
            user_configs[user_id] = []
        user_configs[user_id].append(row)
    
    # Track seen public keys to detect duplicates
    seen_public_keys: set[str] = set()
    
    for user_id, configs in user_configs.items():
        # Use SAVEPOINT for this user
        savepoint_name = f"user_{user_id}"
        
        try:
            await db.execute(f"SAVEPOINT {savepoint_name}")
            
            # Sort configs deterministically within user
            configs_sorted = sorted(configs, key=lambda x: (x[4] or "", x[5]))
            
            slot_number = 0
            for config in configs_sorted:
                _, public_key, private_key, psk_key, _, _ = config
                
                # Skip duplicate public keys
                if public_key in seen_public_keys:
                    logger.warning(
                        "Skipping duplicate public_key for user_id=%s: %s...",
                        user_id, public_key[:20]
                    )
                    stats["skipped_duplicates"] += 1
                    continue
                
                seen_public_keys.add(public_key)
                slot_number += 1
                
                # Handle encryption
                private_key_enc = private_key if _is_already_encrypted(private_key) else encrypt_text(private_key)
                psk_enc = psk_key if _is_already_encrypted(psk_key) else encrypt_text(psk_key)
                
                # Insert device record
                await db.execute("""
                    INSERT INTO devices (user_id, slot_number, node_id, public_key, private_key_enc, psk_enc, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'active')
                """, (user_id, slot_number, legacy_node_id, public_key, private_key_enc, psk_enc))
                
                stats["devices_created"] += 1
            
            # Release savepoint on success
            await db.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            stats["users_processed"] += 1
            
        except Exception as e:
            # Rollback to savepoint on error
            await db.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            error_msg = f"User {user_id}: {e}"
            stats["errors"].append(error_msg)
            logger.exception("Migration failed for user_id=%s: %s", user_id, e)
            # Continue with next user - don't abort entire migration
    
    return stats


async def _update_users_table(db: aiosqlite.Connection) -> None:
    """Update users table with max_devices and subscription_expires_at."""
    from config import CONFIGS_PER_USER
    
    # Update max_devices for all users
    await db.execute("""
        UPDATE users SET max_devices = ? WHERE max_devices IS NULL
    """, (CONFIGS_PER_USER,))
    
    # Copy sub_until to subscription_expires_at where applicable
    await db.execute("""
        UPDATE users 
        SET subscription_expires_at = sub_until 
        WHERE subscription_expires_at IS NULL AND sub_until != '0'
    """)
    
    logger.info("Updated users table with max_devices=%s", CONFIGS_PER_USER)


async def _record_schema_version(db: aiosqlite.Connection) -> None:
    """Record schema version in schema_versions table."""
    await db.execute("""
        INSERT OR IGNORE INTO schema_versions (version, description, applied_at)
        VALUES (?, ?, datetime('now'))
    """, (SCHEMA_VERSION, "Initial multi-node schema migration"))
    
    logger.info("Recorded schema version: %s", SCHEMA_VERSION)


async def _mark_legacy_node_ready(db: aiosqlite.Connection) -> None:
    """Mark legacy node as ready after successful migration."""
    await db.execute("""
        UPDATE nodes SET status = 'ready' 
        WHERE name = 'node_1' AND status IN ('pending', 'legacy', 'legacy_invalid')
    """)
    logger.info("Marked legacy node as ready")


# =============================================================================
# MAIN MIGRATION FUNCTION
# =============================================================================


async def run_migration(dry_run: bool = False) -> dict[str, Any]:
    """
    Execute Phase 1 migration.
    
    Args:
        dry_run: If True, only analyze without writing to DB
    
    Returns:
        Migration result dictionary with stats and status
    """
    result = {
        "success": False,
        "dry_run": dry_run,
        "backup_path": None,
        "stats": {},
        "errors": [],
        "warnings": [],
    }
    
    logger.info("Starting Phase 1 migration (dry_run=%s)", dry_run)
    
    # Phase 1: Backup
    if not dry_run:
        try:
            backup_path = await create_backup(DB_PATH)
            if not await verify_backup(backup_path):
                raise RuntimeError("Backup verification failed")
            result["backup_path"] = backup_path
        except Exception as e:
            error_msg = f"Backup phase failed: {e}"
            logger.error(error_msg)
            result["errors"].append(error_msg)
            return result
    
    # Phase 2: Schema and migration
    db = None
    try:
        db = await open_db()
        
        # Begin transaction BEFORE any schema changes
        await db.execute("BEGIN IMMEDIATE")
        
        try:
            # Ensure schema exists
            await _ensure_schema(db)
            
            if dry_run:
                # For dry-run, just rollback and return analysis
                await db.rollback()
                dry_run_report = await run_migration_dry_run()
                result["stats"] = dry_run_report
                result["success"] = True
                result["warnings"] = dry_run_report.get("migration_warnings", [])
                return result
            
            # Bootstrap legacy node
            legacy_node_id = await _bootstrap_legacy_node(db)
            result["stats"]["legacy_node_id"] = legacy_node_id
            
            # Migrate existing configs
            migration_stats = await _migrate_existing_configs(db, legacy_node_id)
            result["stats"]["migration"] = migration_stats
            
            if migration_stats["errors"]:
                result["warnings"].extend(migration_stats["errors"])
            
            # Update users table
            await _update_users_table(db)
            
            # Record schema version
            await _record_schema_version(db)
            
            # Mark legacy node ready
            await _mark_legacy_node_ready(db)
            
            # Commit transaction
            await db.commit()
            
            result["success"] = True
            logger.info("Migration completed successfully")
            
        except Exception as e:
            # Critical error - rollback entire transaction
            await db.rollback()
            error_msg = f"Migration transaction failed (rolled back): {e}"
            logger.exception(error_msg)
            result["errors"].append(error_msg)
            raise
    
    except Exception as e:
        result["errors"].append(str(e))
        result["success"] = False
    
    finally:
        if db:
            await db.close()
    
    return result


# =============================================================================
# CLI ENTRY POINT
# =============================================================================


async def main():
    """CLI entry point for migration."""
    import sys
    
    dry_run = "--dry-run" in sys.argv
    
    if dry_run:
        print("\n🔍 Running migration DRY-RUN (no changes will be made)\n")
        report = await run_migration_dry_run()
        print_dry_run_report(report)
    else:
        print("\n🚀 Starting Phase 1 migration...\n")
        result = await run_migration(dry_run=False)
        
        if result["success"]:
            print("\n✅ Migration completed successfully!")
            print(f"   Backup: {result['backup_path']}")
            print(f"   Stats: {result['stats']}")
        else:
            print("\n❌ Migration failed!")
            print(f"   Errors: {result['errors']}")
            if result["warnings"]:
                print(f"   Warnings: {result['warnings']}")
        
        return 0 if result["success"] else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
