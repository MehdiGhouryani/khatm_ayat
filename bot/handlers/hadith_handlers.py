import logging
import re
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest, Forbidden
from bot.database.db import get_db_connection
from bot.handlers.admin_handlers import is_admin
from config.settings import HADITH_CHANNEL

logger = logging.getLogger(__name__)

async def hadis_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /hadis_on")
            return

        if not HADITH_CHANNEL.startswith("@"):
            logger.error(f"Invalid HADITH_CHANNEL: {HADITH_CHANNEL}")
            await update.message.reply_text("کانال حدیث نامعتبر است. لطفاً تنظیمات را بررسی کنید.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled) VALUES (?, 1)",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Hadith enabled: group_id={group_id}")

        await update.message.reply_text(f"حدیث روزانه از {HADITH_CHANNEL} فعال شد.")
    except Exception as e:
        logger.error(f"Error in hadis_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def hadis_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /hadis_off")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled) VALUES (?, 0)",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Hadith disabled: group_id={group_id}")

        await update.message.reply_text("حدیث روزانه غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in hadis_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def send_daily_hadith(context: ContextTypes.DEFAULT_TYPE):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT group_id FROM hadith_settings WHERE hadith_enabled = 1")
            groups = [row["group_id"] for row in cursor.fetchall()]

        if not groups:
            logger.debug("No groups with enabled hadith")
            return

        messages = await context.bot.get_chat_history(chat_id=HADITH_CHANNEL, limit=1)
        if not messages:
            logger.warning(f"No messages found in channel {HADITH_CHANNEL}")
            return

        message = messages[0]
        if not message.text:
            logger.warning(f"Last message in {HADITH_CHANNEL} has no text")
            return

        text = clean_hadith_text(message.text)
        if not text:
            logger.warning(f"Cleaned hadith text is empty from {HADITH_CHANNEL}")
            return

        for group_id in groups:
            try:
                await context.bot.send_message(chat_id=group_id, text=text)
                logger.info(f"Hadith sent to group_id={group_id}: {text[:50]}...")
            except (BadRequest, Forbidden) as e:
                logger.error(f"Failed to send hadith to group_id={group_id}: {e}")

    except Exception as e:
        logger.error(f"Error in send_daily_hadith: {e}")

def clean_hadith_text(text: str) -> str:
    try:
        text = re.sub(r'@[A-Za-z0-9_]+', '', text)
        text = re.sub(r'(?:http[s]?://|t.me/)[^\s]+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    except Exception as e:
        logger.error(f"Error cleaning hadith text: {e}")
        return ""