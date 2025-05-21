import logging
import re
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest, Forbidden, TimedOut
from bot.database.db import fetch_one, fetch_all, execute, write_queue
from bot.utils.helpers import parse_number
from bot.handlers.admin_handlers import is_admin
import asyncio

logger = logging.getLogger(__name__)

async def reset_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset zekr/salavat current total and zekr_text."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_zekr", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند آمار ذکر و صلوات را ریست کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, is_active FROM topics 
            WHERE topic_id = ? AND group_id = ? AND khatm_type IN ('zekr', 'salavat')
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No zekr/salavat topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ذکر یا صلوات تنظیم نشده است.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr یا /khatm_salavat برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "reset_zekr",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Zekr/Salavat reset queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("آمار ذکر و صلوات ریست شد.")
    except Exception as e:
        logger.error("Error in reset_zekr: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_kol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset all khatm stats (current_total, zekr_text, current_verse_id)."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_kol", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند کل آمار ختم‌ها را ریست کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, is_active FROM topics 
            WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "reset_kol",
            "group_id": group_id,
            "topic_id": topic_id,
            "khatm_type": topic["khatm_type"]
        }
        await write_queue.put(request)
        logger.info("All khatm stats reset queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("کل آمار ختم‌ها ریست شد.")
    except Exception as e:
        logger.error("Error in reset_kol: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_max(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /max command to set maximum number."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /max", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند حداکثر تعداد را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Max command called without arguments")
            await update.message.reply_text("لطفاً عدد حداکثر را وارد کنید. مثال: /max 1000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning("Invalid max number: %s", context.args[0])
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "set_max",
            "group_id": group_id,
            "topic_id": topic_id,
            "max_number": number,
            "is_digit": context.args[0].isdigit()
        }
        await write_queue.put(request)
        logger.info("Max set queued: group_id=%s, topic_id=%s, max=%d", group_id, topic_id, number)

        await update.message.reply_text(f"حداکثر تعداد به {number} تنظیم شد.")
    except Exception as e:
        logger.error("Error in set_max: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def max_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /max_off command to disable maximum limit."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /max_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند محدودیت حداکثر را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "max_off",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Max disabled queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("محدودیت حداکثر غیرفعال شد.")
    except Exception as e:
        logger.error("Error in max_off: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_min(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /min command to set minimum number."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /min", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند حداقل تعداد را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Min command called without arguments")
            await update.message.reply_text("لطفاً عدد حداقل را وارد کنید. مثال: /min 10")
            return

        number = parse_number(context.args[0])
        if number is None or number < 0:
            logger.warning("Invalid min number: %s", context.args[0])
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "set_min",
            "group_id": group_id,
            "topic_id": topic_id,
            "min_number": number,
            "is_digit": context.args[0].isdigit()
        }
        await write_queue.put(request)
        logger.info("Min set queued: group_id=%s, topic_id=%s, min=%d", group_id, topic_id, number)

        await update.message.reply_text(f"حداقل تعداد به {number} تنظیم شد.")
    except Exception as e:
        logger.error("Error in set_min: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def min_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /min_off command to disable minimum limit."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /min_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند محدودیت حداقل را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "min_off",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Min disabled queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("محدودیت حداقل غیرفعال شد.")
    except Exception as e:
        logger.error("Error in min_off: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def sepas_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /sepas_on command to enable sepas texts."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /sepas_on", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند متن‌های سپاس را فعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "sepas_on",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Sepas enabled queued: group_id=%s", group_id)

        await update.message.reply_text("متن‌های سپاس فعال شدند.")
    except Exception as e:
        logger.error("Error in sepas_on: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def sepas_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /sepas_off command to disable sepas texts."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /sepas_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند متن‌های سپاس را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "sepas_off",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Sepas disabled queued: group_id=%s", group_id)

        await update.message.reply_text("متن‌های سپاس غیرفعال شدند.")
    except Exception as e:
        logger.error("Error in sepas_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def add_sepas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addsepas command to add custom sepas text."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /addsepas", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند متن سپاس اضافه کند.")
            return

        if not context.args:
            logger.warning("Addsepas command called without arguments")
            await update.message.reply_text("لطفاً متن سپاس را وارد کنید. مثال: /addsepas یا علی")
            return

        sepas_text = " ".join(context.args)
        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "add_sepas",
            "group_id": group_id,
            "sepas_text": sepas_text
        }
        await write_queue.put(request)
        logger.info("Sepas text added queued: group_id=%s, text=%s", group_id, sepas_text)

        await update.message.reply_text(f"متن سپاس '{sepas_text}' اضافه شد.")
    except Exception as e:
        logger.error("Error in add_sepas: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable daily reset for a group."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_daily", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ریست روزانه را فعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "reset_daily",
            "group_id": group_id,
            "action": "enable"
        }
        await write_queue.put(request)
        logger.info("Daily reset enabled queued: group_id=%s", group_id)

        await update.message.reply_text("ریست روزانه فعال شد. آمار هر روز صفر می‌شود.")
    except Exception as e:
        logger.error("Error in reset_daily: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable daily reset for a group."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ریست روزانه را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "reset_daily",
            "group_id": group_id,
            "action": "disable"
        }
        await write_queue.put(request)
        logger.info("Daily reset disabled queued: group_id=%s", group_id)

        await update.message.reply_text("ریست روزانه غیرفعال شد.")
    except Exception as e:
        logger.error("Error in reset_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_daily_groups(context: ContextTypes.DEFAULT_TYPE):
    """Reset contributions for groups with daily reset enabled."""
    try:
        groups = await fetch_all("SELECT group_id FROM groups WHERE reset_daily = 1")
        if not groups:
            logger.debug("No groups with daily reset enabled")
            return

        for group_row in groups:
            group_id = group_row["group_id"]
            try:
                group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
                if not group or not group["is_active"]:
                    logger.debug("Group not found or inactive during reset: group_id=%s", group_id)
                    continue

                topics = await fetch_all(
                    "SELECT topic_id, khatm_type FROM topics WHERE group_id = ?",
                    (group_id,)
                )
                for topic in topics:
                    topic_id = topic["topic_id"]
                    request = {
                        "type": "reset_daily_group",
                        "group_id": group_id,
                        "topic_id": topic_id,
                        "khatm_type": topic["khatm_type"]
                    }
                    await write_queue.put(request)
                    logger.debug("Queued daily reset: group_id=%s, topic_id=%s", group_id, topic_id)

                for attempt in range(2):
                    try:
                        await context.bot.send_message(
                            chat_id=group_id,
                            text="آمار روزانه گروه صفر شد."
                        )
                        logger.info("Daily reset completed and message sent: group_id=%s", group_id)
                        break
                    except (BadRequest, Forbidden, TimedOut) as e:
                        if attempt == 0 and isinstance(e, TimedOut):
                            await asyncio.sleep(2)
                        else:
                            logger.error("Failed to send reset message to group_id=%s: %s", group_id, e)

            except Exception as e:
                logger.error("Error resetting group_id=%s: %s", group_id, e)

    except Exception as e:
        logger.error("Error in reset_daily_groups: %s", e)

async def reset_periodic_topics(context: ContextTypes.DEFAULT_TYPE):
    """Reset topics that have reached their period number."""
    try:
        topics = await fetch_all(
            """
            SELECT group_id, topic_id, khatm_type, current_total, period_number
            FROM topics WHERE reset_on_period = 1 AND current_total >= period_number
            """
        )
        if not topics:
            logger.debug("No topics eligible for periodic reset")
            return

        for topic in topics:
            group_id = topic["group_id"]
            topic_id = topic["topic_id"]
            try:
                group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
                if not group or not group["is_active"]:
                    logger.debug("Group not found or inactive during periodic reset: group_id=%s", group_id)
                    continue

                request = {
                    "type": "reset_periodic_topic",
                    "group_id": group_id,
                    "topic_id": topic_id,
                    "khatm_type": topic["khatm_type"]
                }
                await write_queue.put(request)
                logger.debug("Queued periodic reset: group_id=%s, topic_id=%s", group_id, topic_id)

                for attempt in range(2):
                    try:
                        await context.bot.send_message(
                            chat_id=group_id,
                            message_thread_id=topic_id if topic_id != group_id else None,
                            text=f"دوره ختم {topic['khatm_type']} به پایان رسید و دوره جدید شروع شد."
                        )
                        logger.info("Periodic reset completed and message sent: group_id=%s, topic_id=%s", group_id, topic_id)
                        break
                    except (BadRequest, Forbidden, TimedOut) as e:
                        if attempt == 0 and isinstance(e, TimedOut):
                            await asyncio.sleep(2)
                        else:
                            logger.error("Failed to send reset message to group_id=%s, topic_id=%s: %s", group_id, topic_id, e)

            except Exception as e:
                logger.error("Error resetting group_id=%s, topic_id=%s: %s", group_id, topic_id, e)

    except Exception as e:
        logger.error("Error in reset_periodic_topics: %s", e)

async def reset_number_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_number_on command to enable period reset."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_number_on", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ریست خودکار دوره را فعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "reset_number_on",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Period reset enabled queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("ریست خودکار دوره فعال شد.")
    except Exception as e:
        logger.error("Error in reset_number_on: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_number_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_number_off command to disable period reset."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /reset_number_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ریست خودکار دوره را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "reset_number_off",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Period reset disabled queued: group_id=%s, topic_id=%s", group_id, topic_id)

        await update.message.reply_text("ریست خودکار دوره غیرفعال شد.")
    except Exception as e:
        logger.error("Error in reset_number_off: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /number command to set period number for khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /number", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند تعداد دوره را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Number command called without arguments")
            await update.message.reply_text("لطفاً تعداد دوره را وارد کنید. مثال: /number 1000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning("Invalid period number: %s", context.args[0])
            await update.message.reply_text("عدد نامعتبر است.")
            return

        reset_on_period = 1 if len(context.args) > 1 and context.args[1].lower() == "reset" else 0
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "set_number",
            "group_id": group_id,
            "topic_id": topic_id,
            "period_number": number,
            "reset_on_period": reset_on_period
        }
        await write_queue.put(request)
        logger.info("Period number set queued: topic_id=%s, group_id=%s, number=%d, reset=%d", 
                    topic_id, group_id, number, reset_on_period)

        reset_text = "و ریست می‌شود" if reset_on_period else ""
        await update.message.reply_text(f"دوره ختم به {number} تنظیم شد {reset_text}.")
    except Exception as e:
        logger.error("Error in set_number: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def number_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /number_off command to disable period number."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /number_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند دوره ختم را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "number_off",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Period number disabled queued: topic_id=%s, group_id=%s", topic_id, group_id)

        await update.message.reply_text("دوره ختم غیرفعال شد.")
    except Exception as e:
        logger.error("Error in number_off: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop_on command to set stop number for khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /stop_on", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند تعداد توقف را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Stop_on command called without arguments")
            await update.message.reply_text("لطفاً تعداد توقف را وارد کنید. مثال: /stop_on 5000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning("Invalid stop number: %s", context.args[0])
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "stop_on",
            "group_id": group_id,
            "topic_id": topic_id,
            "stop_number": number
        }
        await write_queue.put(request)
        logger.info("Stop number set queued: topic_id=%s, group_id=%s, number=%d", topic_id, group_id, number)

        await update.message.reply_text(f"ختم در تعداد {number} متوقف خواهد شد.")
    except Exception as e:
        logger.error("Error in stop_on: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop_on_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop_on_off command to disable stop number."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /stop_on_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند توقف ختم را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "stop_on_off",
            "group_id": group_id,
            "topic_id": topic_id
        }
        await write_queue.put(request)
        logger.info("Stop number disabled queued: topic_id=%s, group_id=%s", topic_id, group_id)

        await update.message.reply_text("توقف ختم غیرفعال شد.")
    except Exception as e:
        logger.error("Error in stop_on_off: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def time_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /time_off command to set inactive hours."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /time_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ساعات خاموشی را تنظیم کند.")
            return

        if len(context.args) < 2:
            logger.warning("Time_off command called with insufficient arguments")
            await update.message.reply_text("لطفاً ساعات شروع و پایان را وارد کنید. مثال: /time_off 22:00 06:00")
            return

        start_time, end_time = context.args[0], context.args[1]
        time_pattern = r"^\d{2}:\d{2}$"
        if not (re.match(time_pattern, start_time) and re.match(time_pattern, end_time)):
            logger.warning("Invalid time format: start=%s, end=%s", start_time, end_time)
            await update.message.reply_text("فرمت زمان نامعتبر است. مثال: 22:00")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "time_off",
            "group_id": group_id,
            "start_time": start_time,
            "end_time": end_time
        }
        await write_queue.put(request)
        logger.info("Time off set queued: group_id=%s, start=%s, end=%s", group_id, start_time, end_time)

        await update.message.reply_text(f"ربات از {start_time} تا {end_time} غیرفعال خواهد بود.")
    except Exception as e:
        logger.error("Error in time_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def time_off_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /time_off_disable command to disable inactive hours."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /time_off_disable", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ساعات خاموشی را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "time_off_disable",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Time off disabled queued: group_id=%s", group_id)

        await update.message.reply_text("ساعات خاموشی غیرفعال شد.")
    except Exception as e:
        logger.error("Error in time_off_disable: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def lock_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /lock_on command to enable lock mode."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /lock_on", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند حالت قفل را فعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "lock_on",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Lock enabled queued: group_id=%s", group_id)

        await update.message.reply_text("قفل فعال شد. فقط پیام‌های عددی پذیرفته می‌شوند و سایر پیام‌ها حذف خواهند شد.")
    except Exception as e:
        logger.error("Error in lock_on: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def lock_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /lock_off command to disable lock mode."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /lock_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند حالت قفل را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "lock_off",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Lock disabled queued: group_id=%s", group_id)

        await update.message.reply_text("حالت قفل غیرفعال شد.")
    except Exception as e:
        logger.error("Error in lock_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def delete_after(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete_after command to set message deletion time."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /delete_after", update.effective_user.id)
            return

        if not context.args:
            logger.warning("Delete_after command called without arguments")
            await update.message.reply_text("لطفاً تعداد دقیقه را وارد کنید. مثال: `delete_after 5`")
            return

        minutes = parse_number(context.args[0])
        if minutes is None or minutes < 1 or minutes > 1440:
            logger.warning("Invalid delete_after minutes: %s", context.args[0])
            await update.message.reply_text("تعداد دقیقه باید بین ۱ تا ۱۴۴۰ باشد.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "delete_after",
            "group_id": group_id,
            "minutes": minutes
        }
        await write_queue.put(request)
        logger.info("Delete after set queued: group_id=%s, minutes=%d", group_id, minutes)

        await update.message.reply_text(f"پیام‌های غیرادمین پس از {minutes} دقیقه حذف می‌شوند.")
    except Exception as e:
        logger.error("Error in delete_after: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def delete_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete_off command to disable message deletion."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /delete_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند حذف خودکار پیام‌ها را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "delete_off",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Delete after disabled queued: group_id=%s", group_id)

        await update.message.reply_text("حذف خودکار پیام‌ها غیرفعال شد.")
    except Exception as e:
        logger.error("Error in delete_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def handle_new_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new messages for scheduled deletion."""
    try:
        if not update.message or not update.effective_chat:
            logger.debug("Invalid message or chat in handle_new_message")
            return

        group_id = update.effective_chat.id
        result = await fetch_one("SELECT delete_after FROM groups WHERE group_id = ?", (group_id,))
        if not result or result["delete_after"] == 0:
            return

        if await is_admin(update, context):
            logger.debug("Message from admin skipped: user_id=%s", update.effective_user.id)
            return

        minutes = result["delete_after"]
        context.job_queue.run_once(
            delete_message,
            minutes * 60,
            data={
                "chat_id": group_id,
                "message_id": update.message.message_id,
                "message_thread_id": update.message.message_thread_id
            },
            name=f"delete_message_{group_id}_{update.message.message_id}"
        )
        logger.debug("Scheduled deletion: group_id=%s, message_id=%s, after=%d minutes", group_id, update.message.message_id, minutes)

    except Exception as e:
        logger.error("Error in handle_new_message: %s, group_id=%s", e, group_id)

async def delete_message(context: ContextTypes.DEFAULT_TYPE):
    """Delete a scheduled message."""
    try:
        job = context.job
        chat_id = job.data["chat_id"]
        message_id = job.data["message_id"]
        message_thread_id = job.data.get("message_thread_id")

        await context.bot.delete_message(
            chat_id=chat_id,
            message_id=message_id,
            message_thread_id=message_thread_id
        )
        logger.info("Message deleted: chat_id=%s, message_id=%s", chat_id, message_id)

    except (BadRequest, Forbidden) as e:
        logger.debug("Failed to delete message: chat_id=%s, message_id=%s, error=%s", chat_id, message_id, e)
    except Exception as e:
        logger.error("Error in delete_message: %s, chat_id=%s, message_id=%s", e, chat_id, message_id)

async def jam_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /jam_on command to enable showing total in messages."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /jam_on", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند نمایش جمع کل را فعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "jam_on",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Show total enabled queued: group_id=%s", group_id)

        await update.message.reply_text("نمایش جمع کل در پیام‌ها فعال شد.")
    except Exception as e:
        logger.error("Error in jam_on: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def jam_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /jam_off command to disable showing total in messages."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /jam_off", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند نمایش جمع کل را غیرفعال کند.")
            return

        group_id = update.effective_chat.id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        request = {
            "type": "jam_off",
            "group_id": group_id
        }
        await write_queue.put(request)
        logger.info("Show total disabled queued: group_id=%s", group_id)

        await update.message.reply_text("نمایش جمع کل در پیام‌ها غیرفعال شد.")
    except Exception as e:
        logger.error("Error in jam_off: %s, group_id=%s", e, group_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_completion_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /set_completion_message command to set custom completion message."""
    try:
        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted /set_completion_message", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند پیام تبریک را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Set_completion_message command called without arguments")
            await update.message.reply_text("لطفاً پیام تبریک را وارد کنید. مثال: /set_completion_message تبریک! ختم با موفقیت تکمیل شد.")
            return

        message = " ".join(context.args)
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT is_active FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        request = {
            "type": "set_completion_message",
            "group_id": group_id,
            "topic_id": topic_id,
            "message": message
        }
        await write_queue.put(request)
        logger.info("Completion message set queued: topic_id=%s, group_id=%s, message=%s", topic_id, group_id, message)

        await update.message.reply_text(f"پیام تبریک تنظیم شد: {message}")
    except Exception as e:
        logger.error("Error in set_completion_message: %s, group_id=%s, topic_id=%s", e, group_id, topic_id)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")