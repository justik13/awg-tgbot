"""Менеджер резервного копирования и восстановления базы данных."""
import os
import shutil
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import config
from config import logger


BACKUP_DIR = Path(config.DB_PATH).parent / "backups"


def ensure_backup_dir() -> None:
    """Создать директорию для бэкапов, если она не существует."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def generate_backup_filename(prefix: str = "backup") -> str:
    """Сгенерировать имя файла для бэкапа с временной меткой."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.db"


def create_full_backup(description: Optional[str] = None) -> dict:
    """
    Создать полный бэкап базы данных.
    
    Returns:
        dict с информацией о бэкапе:
        - success: bool
        - backup_path: str (путь к файлу)
        - size_bytes: int (размер в байтах)
        - description: str (описание)
        - error: str (если произошла ошибка)
    """
    ensure_backup_dir()
    
    db_path = Path(config.DB_PATH)
    if not db_path.exists():
        return {
            "success": False,
            "error": "Файл базы данных не найден",
            "backup_path": None,
            "size_bytes": 0,
            "description": description,
        }
    
    backup_filename = generate_backup_filename("full_backup")
    backup_path = BACKUP_DIR / backup_filename
    
    try:
        # Копируем файл базы данных
        shutil.copy2(db_path, backup_path)
        
        # Создаем файл метаданных
        metadata = {
            "type": "full",
            "created_at": datetime.now().isoformat(),
            "source_db": str(db_path),
            "description": description or "",
            "size_bytes": backup_path.stat().st_size,
        }
        
        metadata_path = backup_path.with_suffix(".json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "backup_path": str(backup_path),
            "size_bytes": backup_path.stat().st_size,
            "description": description,
            "metadata_path": str(metadata_path),
        }
    except Exception as e:
        logger.exception("Ошибка создания полного бэкапа: %s", e)
        # Удаляем частично созданный файл
        if backup_path.exists():
            backup_path.unlink()
        return {
            "success": False,
            "error": str(e),
            "backup_path": None,
            "size_bytes": 0,
            "description": description,
        }


def create_users_backup(description: Optional[str] = None) -> dict:
    """
    Создать бэкап только таблиц пользователей и подписок.
    
    Экспортирует данные из таблиц:
    - users (пользователи)
    - keys (ключи доступа)
    - payments (платежи)
    - subscription_notifications (уведомления о подписках)
    - subscription_operations (операции с подписками)
    - referral_codes (реферальные коды)
    - referral_attributions (реферальные атрибуции)
    - referral_rewards (реферальные вознаграждения)
    - referral_recurring_rewards (повторяющиеся реферальные вознаграждения)
    - promo_codes (промокоды)
    - promo_activations (активации промокодов)
    - app_settings (настройки приложения)
    - text_overrides (переопределения текстов)
    
    Returns:
        dict с информацией о бэкапе
    """
    ensure_backup_dir()
    
    db_path = Path(config.DB_PATH)
    if not db_path.exists():
        return {
            "success": False,
            "error": "Файл базы данных не найден",
            "backup_path": None,
            "size_bytes": 0,
            "description": description,
        }
    
    backup_filename = generate_backup_filename("users_backup")
    backup_path = BACKUP_DIR / backup_filename
    
    try:
        # Подключаемся к исходной базе
        source_conn = sqlite3.connect(str(db_path))
        source_conn.row_factory = sqlite3.Row
        
        # Создаем новую базу для бэкапа
        backup_conn = sqlite3.connect(str(backup_path))
        backup_cursor = backup_conn.cursor()
        
        # Таблицы для экспорта - все пользовательские данные
        tables_to_backup = [
            "users",
            "keys", 
            "payments",
            "subscription_notifications",
            "subscription_operations",
            "referral_codes",
            "referral_attributions",
            "referral_rewards",
            "referral_recurring_rewards",
            "promo_codes",
            "promo_activations",
            "app_settings",
            "text_overrides",
            "runtime_metrics",
            "callback_guards",
        ]
        
        exported_tables = []
        
        for table_name in tables_to_backup:
            # Проверяем существование таблицы
            source_cursor = source_conn.cursor()
            source_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if not source_cursor.fetchone():
                logger.debug("Таблица %s не найдена, пропускаем", table_name)
                continue
            
            # Получаем схему таблицы
            source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            schema_row = source_cursor.fetchone()
            if schema_row and schema_row[0]:
                # Создаем таблицу в бэкапе
                backup_cursor.execute(schema_row[0])
                
                # Копируем данные
                source_cursor.execute(f"SELECT * FROM {table_name}")
                rows = source_cursor.fetchall()
                
                if rows:
                    # Получаем имена колонок
                    columns = [description[0] for description in source_cursor.description]
                    placeholders = ", ".join(["?" for _ in columns])
                    columns_str = ", ".join(columns)
                    
                    for row in rows:
                        values = tuple(row[col] for col in columns)
                        backup_cursor.execute(
                            f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                            values
                        )
                
                exported_tables.append(table_name)
                logger.info("Экспортирована таблица %s: %d записей", table_name, len(rows))
        
        backup_conn.commit()
        backup_conn.close()
        source_conn.close()
        
        # Создаем файл метаданных
        metadata = {
            "type": "users_only",
            "created_at": datetime.now().isoformat(),
            "source_db": str(db_path),
            "description": description or "",
            "tables": exported_tables,
            "size_bytes": backup_path.stat().st_size,
        }
        
        metadata_path = backup_path.with_suffix(".json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "backup_path": str(backup_path),
            "size_bytes": backup_path.stat().st_size,
            "description": description,
            "tables": exported_tables,
            "metadata_path": str(metadata_path),
        }
    except Exception as e:
        logger.exception("Ошибка создания бэкапа пользователей: %s", e)
        # Удаляем частично созданный файл
        if backup_path.exists():
            backup_path.unlink()
        return {
            "success": False,
            "error": str(e),
            "backup_path": None,
            "size_bytes": 0,
            "description": description,
        }


def list_backups() -> list[dict]:
    """
    Получить список всех доступных бэкапов.
    
    Returns:
        list[dict] с информацией о каждом бэкапе:
        - filename: str
        - path: str
        - size_bytes: int
        - created_at: str (ISO формат)
        - type: str (full/users_only)
        - description: str
    """
    ensure_backup_dir()
    
    backups = []
    
    for file_path in sorted(BACKUP_DIR.glob("*.db")):
        metadata_path = file_path.with_suffix(".json")
        metadata = {}
        
        if metadata_path.exists():
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception as e:
                logger.warning("Не удалось прочитать метаданные для %s: %s", file_path.name, e)
        
        backup_info = {
            "filename": file_path.name,
            "path": str(file_path),
            "size_bytes": file_path.stat().st_size,
            "created_at": metadata.get("created_at", ""),
            "type": metadata.get("type", "unknown"),
            "description": metadata.get("description", ""),
            "tables": metadata.get("tables", []),
        }
        
        backups.append(backup_info)
    
    # Сортируем по дате создания (новые первыми)
    backups.sort(key=lambda x: x["created_at"], reverse=True)
    
    return backups


def get_backup_info(backup_filename: str) -> Optional[dict]:
    """
    Получить информацию о конкретном бэкапе.
    
    Args:
        backup_filename: имя файла бэкапа
    
    Returns:
        dict с информацией о бэкапе или None если не найден
    """
    ensure_backup_dir()
    
    backup_path = BACKUP_DIR / backup_filename
    
    if not backup_path.exists():
        return None
    
    metadata_path = backup_path.with_suffix(".json")
    metadata = {}
    
    if metadata_path.exists():
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.warning("Не удалось прочитать метаданные для %s: %s", backup_filename, e)
    
    return {
        "filename": backup_filename,
        "path": str(backup_path),
        "size_bytes": backup_path.stat().st_size,
        "created_at": metadata.get("created_at", ""),
        "type": metadata.get("type", "unknown"),
        "description": metadata.get("description", ""),
        "tables": metadata.get("tables", []),
    }


def restore_from_backup(backup_filename: str, create_backup_before: bool = True) -> dict:
    """
    Восстановить базу данных из бэкапа.
    
    Args:
        backup_filename: имя файла бэкапа
        create_backup_before: создать ли бэкап текущего состояния перед восстановлением
    
    Returns:
        dict с результатом операции:
        - success: bool
        - restored_from: str (путь к файлу бэкапа)
        - current_backup: str (путь к бэкапу текущего состояния, если создавался)
        - error: str (если произошла ошибка)
        - restore_type: str (full/merge - полный или частичный)
    """
    ensure_backup_dir()
    
    backup_path = BACKUP_DIR / backup_filename
    
    if not backup_path.exists():
        return {
            "success": False,
            "error": f"Файл бэкапа не найден: {backup_filename}",
            "restored_from": None,
            "current_backup": None,
            "restore_type": None,
        }
    
    # Читаем метаданные бэкапа для определения типа
    metadata_path = backup_path.with_suffix(".json")
    backup_type = "unknown"
    tables_in_backup = []
    
    if metadata_path.exists():
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                backup_type = metadata.get("type", "unknown")
                tables_in_backup = metadata.get("tables", [])
        except Exception as e:
            logger.warning("Не удалось прочитать метаданные для %s: %s", backup_filename, e)
    
    db_path = Path(config.DB_PATH)
    
    # Создаем бэкап текущего состояния перед восстановлением
    current_backup_path = None
    if create_backup_before and db_path.exists():
        backup_result = create_full_backup("pre_restore_backup")
        if backup_result["success"]:
            current_backup_path = backup_result["backup_path"]
            logger.info("Создан бэкап текущего состояния: %s", current_backup_path)
        else:
            logger.warning("Не удалось создать бэкап текущего состояния: %s", backup_result.get("error"))
    
    try:
        # Если это users_backup (частичный бэкап), выполняем слияние данных
        if backup_type == "users_only" and tables_in_backup:
            return _restore_users_merge(backup_path, tables_in_backup, db_path, current_backup_path)
        
        # Для полного бэкапа или бэкапа без метаданных - простая замена
        # Сначала удаляем текущую базу (если существует)
        if db_path.exists():
            db_path.unlink()
        
        # Копируем бэкап
        shutil.copy2(backup_path, db_path)
        
        logger.info("База данных восстановлена из %s (полная замена)", backup_filename)
        
        return {
            "success": True,
            "restored_from": str(backup_path),
            "current_backup": current_backup_path,
            "restore_type": "full",
        }
    except Exception as e:
        logger.exception("Ошибка восстановления из бэкапа: %s", e)
        return {
            "success": False,
            "error": str(e),
            "restored_from": None,
            "current_backup": current_backup_path,
            "restore_type": None,
        }


def _restore_users_merge(backup_path: Path, tables_to_restore: list[str], db_path: Path, current_backup_path: Optional[str]) -> dict:
    """
    Выполнить слияние данных из users_backup с существующей базой данных.
    
    Эта функция предназначена для восстановления из частичного бэкапа (users_only)
    и корректно работает даже если структура БД отличается (старая версия).
    
    Args:
        backup_path: путь к файлу бэкапа
        tables_to_restore: список таблиц для восстановления
        db_path: путь к целевой базе данных
        current_backup_path: путь к бэкапу текущего состояния (если создан)
    
    Returns:
        dict с результатом операции
    """
    try:
        # Подключаемся к бэкапу
        backup_conn = sqlite3.connect(str(backup_path))
        backup_conn.row_factory = sqlite3.Row
        
        # Подключаемся к целевой базе
        target_conn = sqlite3.connect(str(db_path))
        target_cursor = target_conn.cursor()
        
        restored_tables = []
        skipped_tables = []
        total_rows_restored = 0
        
        for table_name in tables_to_restore:
            # Проверяем существование таблицы в бэкапе
            backup_cursor = backup_conn.cursor()
            backup_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if not backup_cursor.fetchone():
                logger.debug("Таблица %s не найдена в бэкапе, пропускаем", table_name)
                skipped_tables.append(table_name)
                continue
            
            # Проверяем существование таблицы в целевой базе
            target_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if not target_cursor.fetchone():
                logger.warning("Таблица %s не найдена в целевой базе, пропускаем", table_name)
                skipped_tables.append(table_name)
                continue
            
            # Получаем схему таблицы из бэкапа
            backup_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            schema_row = backup_cursor.fetchone()
            
            # Получаем информацию о колонках в целевой базе
            target_cursor.execute(f"PRAGMA table_info({table_name})")
            target_columns = {row[1]: row[2] for row in target_cursor.fetchall()}
            
            # Получаем информацию о колонках в бэкапе
            backup_cursor.execute(f"PRAGMA table_info({table_name})")
            backup_columns = {row[1]: row[2] for row in backup_cursor.fetchall()}
            
            # Определяем общие колонки
            common_columns = set(target_columns.keys()) & set(backup_columns.keys())
            
            if not common_columns:
                logger.warning("Нет общих колонок между бэкапом и целевой базой для таблицы %s", table_name)
                skipped_tables.append(table_name)
                continue
            
            # Получаем первичные ключи таблицы
            pk_columns = []
            for col_name, col_type in target_columns.items():
                target_cursor.execute(f"PRAGMA table_info({table_name})")
                rows = target_cursor.fetchall()
                for row in rows:
                    if row[5] == 1:  # pk column
                        pk_columns.append(row[1])
            
            # Если нет явного первичного ключа, используем первую колонку
            if not pk_columns and common_columns:
                pk_columns = [list(common_columns)[0]]
            
            # Получаем данные из бэкапа
            backup_cursor.execute(f"SELECT * FROM {table_name}")
            rows = backup_cursor.fetchall()
            
            if not rows:
                logger.debug("Таблица %s пуста в бэкапе", table_name)
                restored_tables.append(table_name)
                continue
            
            # Определяем колонки для вставки (только общие)
            columns_list = list(common_columns)
            columns_str = ", ".join(columns_list)
            placeholders = ", ".join(["?" for _ in columns_list])
            
            # Формируем условие для REPLACE INTO или INSERT OR REPLACE
            # Используем REPLACE INTO для обновления существующих записей
            inserted_count = 0
            conflict_count = 0
            
            for row in rows:
                try:
                    values = tuple(row[col] if col in backup_columns else None for col in columns_list)
                    
                    # Пробуем вставить или обновить запись
                    target_cursor.execute(
                        f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                        values
                    )
                    inserted_count += 1
                    
                    if target_cursor.rowcount > 1:
                        conflict_count += 1
                        
                except Exception as e:
                    logger.debug("Ошибка вставки записи в таблицу %s: %s", table_name, e)
                    continue
            
            target_conn.commit()
            
            restored_tables.append(table_name)
            total_rows_restored += inserted_count
            logger.info(
                "Восстановлена таблица %s: %d записей вставлено, %d обновлено (конфликтов)",
                table_name, inserted_count, conflict_count
            )
        
        backup_conn.close()
        target_conn.close()
        
        logger.info(
            "Слияние данных завершено: %d таблиц восстановлено, %d пропущено, всего %d записей",
            len(restored_tables), len(skipped_tables), total_rows_restored
        )
        
        return {
            "success": True,
            "restored_from": str(backup_path),
            "current_backup": current_backup_path,
            "restore_type": "merge",
            "restored_tables": restored_tables,
            "skipped_tables": skipped_tables,
            "total_rows_restored": total_rows_restored,
        }
        
    except Exception as e:
        logger.exception("Ошибка слияния данных из бэкапа: %s", e)
        return {
            "success": False,
            "error": str(e),
            "restored_from": None,
            "current_backup": current_backup_path,
            "restore_type": "merge",
        }


def delete_backup(backup_filename: str) -> dict:
    """
    Удалить бэкап.
    
    Args:
        backup_filename: имя файла бэкапа
    
    Returns:
        dict с результатом операции
    """
    ensure_backup_dir()
    
    backup_path = BACKUP_DIR / backup_filename
    metadata_path = backup_path.with_suffix(".json")
    
    if not backup_path.exists():
        return {
            "success": False,
            "error": f"Файл бэкапа не найден: {backup_filename}",
        }
    
    try:
        backup_path.unlink()
        if metadata_path.exists():
            metadata_path.unlink()
        
        logger.info("Бэкап удален: %s", backup_filename)
        
        return {
            "success": True,
            "deleted": backup_filename,
        }
    except Exception as e:
        logger.exception("Ошибка удаления бэкапа: %s", e)
        return {
            "success": False,
            "error": str(e),
        }


def format_backup_size(size_bytes: int) -> str:
    """Форматировать размер бэкапа в человекочитаемый вид."""
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} КБ"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} МБ"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} ГБ"
