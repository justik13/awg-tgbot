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
    - users
    - keys (ключи доступа)
    - payments (платежи)
    - subscriptions (подписки)
    
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
        
        # Таблицы для экспорта
        tables_to_backup = [
            "users",
            "keys", 
            "payments",
            "subscriptions",
            "promo_codes",
            "referrals",
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
    """
    ensure_backup_dir()
    
    backup_path = BACKUP_DIR / backup_filename
    
    if not backup_path.exists():
        return {
            "success": False,
            "error": f"Файл бэкапа не найден: {backup_filename}",
            "restored_from": None,
            "current_backup": None,
        }
    
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
        # Копируем бэкап на место текущей базы
        # Сначала удаляем текущую базу (если существует)
        if db_path.exists():
            db_path.unlink()
        
        # Копируем бэкап
        shutil.copy2(backup_path, db_path)
        
        logger.info("База данных восстановлена из %s", backup_filename)
        
        return {
            "success": True,
            "restored_from": str(backup_path),
            "current_backup": current_backup_path,
        }
    except Exception as e:
        logger.exception("Ошибка восстановления из бэкапа: %s", e)
        return {
            "success": False,
            "error": str(e),
            "restored_from": None,
            "current_backup": current_backup_path,
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
