import logging
import re
from telegram import Update
from telegram.ext import ContextTypes
from bot.database.db import get_db_connection
from bot.utils.helpers import parse_number
from bot.handlers.admin_handlers import is_admin

logger = logging.getLogger(__name__)

async def reset_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_zekr command to reset zekr or salavat stats."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_zekr")
            await update.message.reply_text("فقط ادمین می‌تواند ذکر را ریست کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE topics 
                SET current_total = 0, zekr_text = ''
                WHERE group_id = ? AND topic_id = ? AND khatm_type IN ('zekr', 'salavat')
                """,
                (group_id, topic_id)
            )
            cursor.execute(
                """
                UPDATE users 
                SET total_zekr = 0, total_salavat = 0
                WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            conn.commit()
            logger.info(f"Zekr/Salavat reset: group_id={group_id}, topic_id={topic_id}")

        await update.message.reply_text("آمار ذکر و صلوات ریست شد.")
    except Exception as e:
        logger.error(f"Error in reset_zekr: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_kol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_kol command to reset all khatm stats."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_kol")
            await update.message.reply_text("فقط ادمین می‌تواند کل آمار را ریست کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE topics 
                SET current_total = 0, zekr_text = ''
                WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            cursor.execute(
                """
                UPDATE users 
                SET total_salavat = 0, total_zekr = 0, total_ayat = 0
                WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            cursor.execute(
                """
                UPDATE topics 
                SET current_verse_id = (
                    SELECT start_verse_id FROM khatm_ranges 
                    WHERE group_id = ? AND topic_id = ?
                )
                WHERE group_id = ? AND topic_id = ? AND khatm_type = 'ghoran'
                """,
                (group_id, topic_id, group_id, topic_id)
            )
            conn.commit()
            logger.info(f"All khatm stats reset: group_id={group_id}, topic_id={topic_id}")

        await update.message.reply_text("کل آمار ختم‌ها ریست شد.")
    except Exception as e:
        logger.error(f"Error in reset_kol: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")


async def set_max(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /max command to set maximum number."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /max")
            await update.message.reply_text("فقط ادمین می‌تواند حداکثر را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Max command called without arguments")
            await update.message.reply_text("لطفاً عدد حداکثر را وارد کنید. مثال: /max 1000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning(f"Invalid max number: {context.args[0]}")
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET max_number = ? WHERE group_id = ?",
                (number, group_id)
            )
            if context.args[0].isdigit():
                cursor.execute(
                    "UPDATE topics SET max_ayat = ? WHERE topic_id = ? AND group_id = ?",
                    (number, topic_id, group_id)
                )
            conn.commit()
            logger.info(f"Max set: group_id={group_id}, max={number}")

        await update.message.reply_text(f"حداکثر تعداد به {number} تنظیم شد.")
    except Exception as e:
        logger.error(f"Error in set_max: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def max_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /max_off command to disable maximum limit."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /max_off")
            await update.message.reply_text("فقط ادمین می‌تواند حداکثر را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET max_number = 1000000 WHERE group_id = ?",
                (group_id,)
            )
            cursor.execute(
                "UPDATE topics SET max_ayat = 100 WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Max disabled: group_id={group_id}")

        await update.message.reply_text("محدودیت حداکثر غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in max_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_min(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /min command to set minimum number."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /min")
            await update.message.reply_text("فقط ادمین می‌تواند حداقل را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Min command called without arguments")
            await update.message.reply_text("لطفاً عدد حداقل را وارد کنید. مثال: /min 10")
            return

        number = parse_number(context.args[0])
        if number is None or number < 0:
            logger.warning(f"Invalid min number: {context.args[0]}")
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET min_number = ? WHERE group_id = ?",
                (number, group_id)
            )
            if context.args[0].isdigit():
                cursor.execute(
                    "UPDATE topics SET min_ayat = ? WHERE topic_id = ? AND group_id = ?",
                    (number, topic_id, group_id)
                )
            conn.commit()
            logger.info(f"Min set: group_id={group_id}, min={number}")

        await update.message.reply_text(f"حداقل تعداد به {number} تنظیم شد.")
    except Exception as e:
        logger.error(f"Error in set_min: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def min_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /min_off command to disable minimum limit."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /min_off")
            await update.message.reply_text("فقط ادمین می‌تواند حداقل را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET min_number = 0 WHERE group_id = ?",
                (group_id,)
            )
            cursor.execute(
                "UPDATE topics SET min_ayat = 1 WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Min disabled: group_id={group_id}")

        await update.message.reply_text("محدودیت حداقل غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in min_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def sepas_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /sepas_on command to enable sepas texts."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /sepas_on")
            await update.message.reply_text("فقط ادمین می‌تواند متن سپاس را فعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET sepas_enabled = 1 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Sepas enabled: group_id={group_id}")

        await update.message.reply_text("متن‌های سپاس فعال شدند.")
    except Exception as e:
        logger.error(f"Error in sepas_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def sepas_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /sepas_off command to disable sepas texts."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /sepas_off")
            await update.message.reply_text("فقط ادمین می‌تواند متن سپاس را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET sepas_enabled = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Sepas disabled: group_id={group_id}")

        await update.message.reply_text("متن‌های سپاس غیرفعال شدند.")
    except Exception as e:
        logger.error(f"Error in sepas_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def add_sepas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addsepas command to add custom sepas text."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /addsepas")
            await update.message.reply_text("فقط ادمین می‌تواند متن سپاس اضافه کند.")
            return

        if not context.args:
            logger.warning("Addsepas command called without arguments")
            await update.message.reply_text("لطفاً متن سپاس را وارد کنید. مثال: /addsepas یا علی")
            return

        sepas_text = " ".join(context.args)
        group_id = update.effective_chat.id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO sepas_texts (group_id, text, is_default) VALUES (?, ?, 0)",
                (group_id, sepas_text)
            )
            conn.commit()
            logger.info(f"Sepas text added: group_id={group_id}, text={sepas_text}")

        await update.message.reply_text(f"متن سپاس '{sepas_text}' اضافه شد.")
    except Exception as e:
        logger.error(f"Error in add_sepas: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_daily command to enable daily reset."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_daily")
            await update.message.reply_text("فقط ادمین می‌تواند ریست روزانه را فعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET reset_daily = 1 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Daily reset enabled: group_id={group_id}")

        await update.message.reply_text("ریست روزانه فعال شد. آمار هر روز صفر می‌شود.")
    except Exception as e:
        logger.error(f"Error in reset_daily: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_off command to disable daily reset."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_off")
            await update.message.reply_text("فقط ادمین می‌تواند ریست روزانه را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET reset_daily = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Daily reset disabled: group_id={group_id}")

        await update.message.reply_text("ریست روزانه غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in reset_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_number_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_number_on command to enable period reset."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_number_on")
            await update.message.reply_text("فقط ادمین می‌تواند ریست دوره را فعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET reset_on_period = 1 WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            conn.commit()
            logger.info(f"Period reset enabled: group_id={group_id}, topic_id={topic_id}")

        await update.message.reply_text("ریست خودکار دوره فعال شد.")
    except Exception as e:
        logger.error(f"Error in reset_number_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def reset_number_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset_number_off command to disable period reset."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /reset_number_off")
            await update.message.reply_text("فقط ادمین می‌تواند ریست دوره را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET reset_on_period = 0 WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            conn.commit()
            logger.info(f"Period reset disabled: group_id={group_id}, topic_id={topic_id}")

        await update.message.reply_text("ریست خودکار دوره غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in reset_number_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /number command to set period number for khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /number")
            await update.message.reply_text("فقط ادمین می‌تواند تعداد دوره را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Number command called without arguments")
            await update.message.reply_text("لطفاً تعداد دوره را وارد کنید. مثال: /number 1000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning(f"Invalid period number: {context.args[0]}")
            await update.message.reply_text("عدد نامعتبر است.")
            return

        reset_on_period = 1 if len(context.args) > 1 and context.args[1].lower() == "reset" else 0
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET period_number = ?, reset_on_period = ? WHERE topic_id = ? AND group_id = ?",
                (number, reset_on_period, topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Period number set: topic_id={topic_id}, number={number}, reset={reset_on_period}")

        reset_text = "و ریست می‌شود" if reset_on_period else ""
        await update.message.reply_text(f"دوره ختم به {number} تنظیم شد {reset_text}.")
    except Exception as e:
        logger.error(f"Error in set_number: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def number_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /number_off command to disable period number."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /number_off")
            await update.message.reply_text("فقط ادمین می‌تواند تعداد دوره را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET period_number = 0, reset_on_period = 0 WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Period number disabled: topic_id={topic_id}")

        await update.message.reply_text("دوره ختم غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in number_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop_on command to stop khatm at a specific number."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /stop_on")
            await update.message.reply_text("فقط ادمین می‌تواند توقف ختم را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Stop_on command called without arguments")
            await update.message.reply_text("لطفاً تعداد توقف را وارد کنید. مثال: /stop_on 5000")
            return

        number = parse_number(context.args[0])
        if number is None or number <= 0:
            logger.warning(f"Invalid stop number: {context.args[0]}")
            await update.message.reply_text("عدد نامعتبر است.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET stop_number = ? WHERE topic_id = ? AND group_id = ?",
                (number, topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Stop number set: topic_id={topic_id}, number={number}")

        await update.message.reply_text(f"ختم در تعداد {number} متوقف خواهد شد.")
    except Exception as e:
        logger.error(f"Error in stop_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop_on_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop_on_off command to disable stop number."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /stop_on_off")
            await update.message.reply_text("فقط ادمین می‌تواند توقف ختم را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET stop_number = 0 WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Stop number disabled: topic_id={topic_id}")

        await update.message.reply_text("توقف ختم غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in stop_on_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def time_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /time_off command to set inactive hours."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /time_off")
            await update.message.reply_text("فقط ادمین می‌تواند ساعات خاموشی را تنظیم کند.")
            return

        if len(context.args) < 2:
            logger.warning("Time_off command called with insufficient arguments")
            await update.message.reply_text("لطفاً ساعات شروع و پایان را وارد کنید. مثال: /time_off 22:00 06:00")
            return

        start_time, end_time = context.args[0], context.args[1]
        time_pattern = r"^\d{2}:\d{2}$"
        if not (re.match(time_pattern, start_time) and re.match(time_pattern, end_time)):
            logger.warning(f"Invalid time format: start={start_time}, end={end_time}")
            await update.message.reply_text("فرمت زمان نامعتبر است. مثال: 22:00")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET time_off_start = ?, time_off_end = ? WHERE group_id = ?",
                (start_time, end_time, group_id)
            )
            conn.commit()
            logger.info(f"Time off set: group_id={group_id}, start={start_time}, end={end_time}")

        await update.message.reply_text(f"ربات از {start_time} تا {end_time} غیرفعال خواهد بود.")
    except Exception as e:
        logger.error(f"Error in time_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def time_off_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /time_off_disable command to disable inactive hours."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /time_off_disable")
            await update.message.reply_text("فقط ادمین می‌تواند ساعات خاموشی را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET time_off_start = '', time_off_end = '' WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Time off disabled: group_id={group_id}")

        await update.message.reply_text("ساعات خاموشی غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in time_off_disable: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def lock_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /lock_on command to enable lock mode."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /lock_on")
            await update.message.reply_text("فقط ادمین می‌تواند قفل را فعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET lock_enabled = 1 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Lock enabled: group_id={group_id}")

        await update.message.reply_text("حالت قفل فعال شد. فقط اعداد یا آیات پذیرفته می‌شوند.")
    except Exception as e:
        logger.error(f"Error in lock_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def lock_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /lock_off command to disable lock mode."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /lock_off")
            await update.message.reply_text("فقط ادمین می‌تواند قفل را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET lock_enabled = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Lock disabled: group_id={group_id}")

        await update.message.reply_text("حالت قفل غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in lock_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def delete_after(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete_after command to set message deletion time."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /delete_after")
            await update.message.reply_text("فقط ادمین می‌تواند زمان حذف را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Delete_after command called without arguments")
            await update.message.reply_text("لطفاً تعداد دقیقه را وارد کنید. مثال: /delete_after 5")
            return

        minutes = parse_number(context.args[0])
        if minutes is None or minutes <= 0:
            logger.warning(f"Invalid delete_after minutes: {context.args[0]}")
            await update.message.reply_text("تعداد دقیقه نامعتبر است.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET delete_after = ? WHERE group_id = ?",
                (minutes, group_id)
            )
            conn.commit()
            logger.info(f"Delete after set: group_id={group_id}, minutes={minutes}")

        await update.message.reply_text(f"پیام‌ها پس از {minutes} دقیقه حذف می‌شوند.")
    except Exception as e:
        logger.error(f"Error in delete_after: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def delete_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete_off command to disable message deletion."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /delete_off")
            await update.message.reply_text("فقط ادمین می‌تواند حذف پیام را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET delete_after = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Delete after disabled: group_id={group_id}")

        await update.message.reply_text("حذف خودکار پیام‌ها غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in delete_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def jam_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /jam_on command to enable showing total in messages."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /jam_on")
            await update.message.reply_text("فقط ادمین می‌تواند نمایش جمع را فعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET show_total = 1 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Show total enabled: group_id={group_id}")

        await update.message.reply_text("نمایش جمع کل در پیام‌ها فعال شد.")
    except Exception as e:
        logger.error(f"Error in jam_on: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def jam_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /jam_off command to disable showing total in messages."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /jam_off")
            await update.message.reply_text("فقط ادمین می‌تواند نمایش جمع را غیرفعال کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET show_total = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Show total disabled: group_id={group_id}")

        await update.message.reply_text("نمایش جمع کل در پیام‌ها غیرفعال شد.")
    except Exception as e:
        logger.error(f"Error in jam_off: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_completion_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /set_completion_message command to set custom completion message."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /set_completion_message")
            await update.message.reply_text("فقط ادمین می‌تواند پیام تبریک را تنظیم کند.")
            return

        if not context.args:
            logger.warning("Set_completion_message command called without arguments")
            await update.message.reply_text("لطفاً پیام تبریک را وارد کنید. مثال: /set_completion_message تبریک! ختم با موفقیت تکمیل شد.")
            return

        message = " ".join(context.args)
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET completion_message = ? WHERE topic_id = ? AND group_id = ?",
                (message, topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Completion message set: topic_id={topic_id}, message={message}")

        await update.message.reply_text(f"پیام تبریک تنظیم شد: {message}")
    except Exception as e:
        logger.error(f"Error in set_completion_message: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")