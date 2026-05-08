"""
Конфигурация pytest для тестов проекта awg-tgbot.
"""
import os
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def setup_test_env(tmp_path, monkeypatch):
    """
    Настраивает переменные окружения для тестов.
    Использует временные файлы вместо реальных конфигов.
    """
    # Создаём временный файл БД
    db_file = tmp_path / "test_awg.db"
    
    # Устанавливаем необходимые переменные окружения
    monkeypatch.setenv("API_TOKEN", "test_token_12345")
    monkeypatch.setenv("ADMIN_ID", "123456789")
    monkeypatch.setenv("SERVER_PUBLIC_KEY", "testpubkey123456789012345678901234567890123=")
    monkeypatch.setenv("SERVER_IP", "203.0.113.1:51820")
    monkeypatch.setenv("ENCRYPTION_SECRET", "testsecret12345678901234567890")
    monkeypatch.setenv("BOT_TOKEN", "1234567890:AABBccDDeeFFggHHiiJJkkLLmmNNooppQQrrs")
    monkeypatch.setenv("DB_PATH", str(db_file))
    
    # Отключаем авто-детект для тестов
    monkeypatch.setenv("AUTO_DETECT_ON_IMPORT", "false")
    
    yield db_file
