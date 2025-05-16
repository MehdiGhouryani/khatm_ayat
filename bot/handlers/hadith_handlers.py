import logging
from telegram import Update
from telegram.ext import ContextTypes
from bot.database.db import get_db_connection
from bot.handlers.admin_handlers import is_admin
from config.settings import HADITH_CHANNEL

logger = logging.getLogger(__name__)

async def hadis_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /hadis_on command to enable daily hadith."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /hadis_on")
            await update.message.reply_text("فقط ادمین می‌تواند حدیث را فعال کند.")
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
    """Handle /hadis_off command to disable daily hadith."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /hadis_off")
            await update.message.reply_text("فقط ادمین می‌تواند حدیث را غیرفعال کند.")
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