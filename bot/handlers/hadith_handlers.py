import logging
import re
import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest, Forbidden, TimedOut
from bot.database.db import write_queue, fetch_all
from bot.handlers.admin_handlers import is_admin
from config.settings import HADITH_CHANNEL
from bot.utils.helpers import ignore_old_messages

logger = logging.getLogger(__name__)

@ignore_old_messages()
async def hadis_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable daily hadith for a group."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /hadis_on", update.effective_user.id)
            return

        if not HADITH_CHANNEL.startswith("@"):
            logger.error("Invalid HADITH_CHANNEL: %s", HADITH_CHANNEL)
            await update.message.reply_text("کانال حدیث نامعتبر است. لطفاً تنظیمات را بررسی کنید.")
            return

        # Validate channel
        try:
            await context.bot.get_chat(HADITH_CHANNEL)
        except (BadRequest, Forbidden) as e:
            logger.error("Cannot access HADITH_CHANNEL %s: %s", HADITH_CHANNEL, e)
            await update.message.reply_text("دسترسی به کانال حدیث ممکن نیست. لطفاً تنظیمات را بررسی کنید.")
            return

        group_id = update.effective_chat.id
        request = {
            "type": "hadis_on",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Hadith enabled queued: group_id=%s", group_id)

        await update.message.reply_text(f"حدیث روزانه از {HADITH_CHANNEL} فعال شد.")
    except Exception as e:
        logger.error("Error in hadis_on: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@ignore_old_messages()
async def hadis_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable daily hadith for a group."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /hadis_off", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        request = {
            "type": "hadis_off",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Hadith disabled queued: group_id=%s", group_id)

        await update.message.reply_text("حدیث روزانه غیرفعال شد.")
    except Exception as e:
        logger.error("Error in hadis_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def send_daily_hadith(context: ContextTypes.DEFAULT_TYPE):
    """Send daily hadith to groups with enabled hadith."""
    try:
        groups = await fetch_all("SELECT group_id FROM hadith_settings WHERE hadith_enabled = 1")
        groups = [row["group_id"] for row in groups]

        if not groups:
            logger.debug("No groups with enabled hadith")
            return

        try:
            messages = await context.bot.get_chat_history(chat_id=HADITH_CHANNEL, limit=1)
        except (BadRequest, Forbidden, TimedOut) as e:
            logger.error("Error fetching history from %s: %s", HADITH_CHANNEL, e)
            return

        if not messages:
            logger.warning("No messages found in channel %s", HADITH_CHANNEL)
            return

        message = messages[0]
        if not message.text:
            logger.warning("Last message in %s has no text", HADITH_CHANNEL)
            return

        text = clean_hadith_text(message.text)
        if not text:
            logger.warning("Cleaned hadith text is empty from %s", HADITH_CHANNEL)
            return

        for i, group_id in enumerate(groups):
            try:
                await context.bot.send_message(chat_id=group_id, text=text)
                logger.info("Hadith sent to group_id=%s: %s...", group_id, text[:50])
                await asyncio.sleep(0.1 * i)  # Stagger sending to avoid rate limits
            except (BadRequest, Forbidden, TimedOut) as e:
                logger.error("Failed to send hadith to group_id=%s: %s", group_id, e)

    except Exception as e:
        logger.error("Error in send_daily_hadith: %s", e)

def clean_hadith_text(text: str) -> str:
    """Clean hadith text by removing usernames and links."""
    try:
        text = re.sub(r'@[A-Za-z0-9_]+', '', text)
        text = re.sub(r'(?:http[s]?://|t.me/)[^\s]+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    except Exception as e:
        logger.error("Error cleaning hadith text: %s", e)
        return ""