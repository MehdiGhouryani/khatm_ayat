import pytest
import sqlite3
import asyncio
from unittest.mock import AsyncMock, MagicMock
from telegram import Update, User, Chat, Message
from telegram.ext import ContextTypes
from bot.database.db import init_db

@pytest.fixture
def db_connection():
    """Provide a temporary in-memory SQLite database."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with open("bot/database/schema.sql", "r") as f:
        conn.executescript(f.read())
    conn.commit()
    yield conn
    conn.close()

@pytest.fixture
def mock_update():
    """Mock Telegram Update object."""
    update = MagicMock(spec=Update)
    update.effective_user = MagicMock(spec=User)
    update.effective_user.id = 12345
    update.effective_user.first_name = "TestUser"
    update.effective_chat = MagicMock(spec=Chat)
    update.effective_chat.id = -100123456789
    update.effective_chat.type = "group"
    update.message = MagicMock(spec=Message)
    update.message.text = ""
    update.message.reply_text = AsyncMock()
    return update

@pytest.fixture
def mock_context():
    """Mock Telegram Context object."""
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.bot = AsyncMock()
    context.user_data = {}
    context.bot.get_chat_administrators = AsyncMock(return_value=[
        MagicMock(user=MagicMock(id=12345))
    ])
    return context

@pytest.fixture
def sample_group(db_connection):
    """Insert a sample group into the database."""
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO groups (group_id, is_active, is_topic_enabled) VALUES (?, ?, ?)",
        (-100123456789, 1, 1)
    )
    db_connection.commit()
    return -100123456789

@pytest.fixture
def sample_topic(db_connection, sample_group):
    """Insert a sample topic into the database."""
    cursor = db_connection.cursor()
    cursor.execute(
        """
        INSERT INTO topics (topic_id, group_id, name, khatm_type, current_verse, current_total)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (1001, sample_group, "Test Topic", "salavat", 1, 0)
    )
    db_connection.commit()
    return 1001

@pytest.fixture
def sample_user(db_connection, sample_group, sample_topic):
    """Insert a sample user into the database."""
    cursor = db_connection.cursor()
    cursor.execute(
        """
        INSERT INTO users (user_id, group_id, topic_id, username, first_name, total_salavat)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (12345, sample_group, sample_topic, "testuser", "TestUser", 100)
    )
    db_connection.commit()
    return 12345