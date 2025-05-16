import pytest
import asyncio
from bot.handlers.khatm_handlers import handle_khatm_message
from bot.utils.helpers import parse_number

@pytest.mark.asyncio
async def test_handle_khatm_message_valid_number(mock_update, mock_context, db_connection, sample_group, sample_topic):
    mock_update.message.text = "50"
    mock_update.message.message_thread_id = sample_topic
    await handle_khatm_message(mock_update, mock_context)
    mock_update.message.reply_text.assert_called()

    cursor = db_connection.cursor()
    cursor.execute("SELECT current_total FROM topics WHERE topic_id = ? AND group_id = ?",
                   (sample_topic, sample_group))
    assert cursor.fetchone()["current_total"] == 50

@pytest.mark.asyncio
async def test_handle_khatm_message_invalid_number(mock_update, mock_context):
    mock_update.message.text = "abc"
    await handle_khatm_message(mock_update, mock_context)
    mock_update.message.reply_text.assert_not_called()