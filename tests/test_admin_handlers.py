import pytest
import asyncio
from bot.handlers.admin_handlers import start, topic
from bot.utils.constants import KHATM_TYPES

@pytest.mark.asyncio
async def test_start_admin(mock_update, mock_context, db_connection):
    mock_update.message.text = "/start"
    await start(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with("ربات ثبت شد.")

    cursor = db_connection.cursor()
    cursor.execute("SELECT is_active FROM groups WHERE group_id = ?", (-100123456789,))
    assert cursor.fetchone()["is_active"] == 1

@pytest.mark.asyncio
async def test_start_non_admin(mock_update, mock_context):
    mock_context.bot.get_chat_administrators.return_value = []
    mock_update.message.text = "/start"
    await start(mock_update, mock_context)
    mock_update.message.reply_text.assert_called_with("لطفاً من را مدیر کنید.")

@pytest.mark.asyncio
async def test_topic(mock_update, mock_context, db_connection, sample_group):
    mock_update.message.text = "/topic Test Topic"
    mock_context.args = ["Test", "Topic"]
    mock_update.message.message_thread_id = 1001
    await topic(mock_update, mock_context)
    mock_update.message.reply_text.assert_called()

    cursor = db_connection.cursor()
    cursor.execute("SELECT name, khatm_type FROM topics WHERE topic_id = ? AND group_id = ?",
                   (1001, -100123456789))
    topic_data = cursor.fetchone()
    assert topic_data["name"] == "Test Topic"
    assert topic_data["khatm_type"] == "salavat"