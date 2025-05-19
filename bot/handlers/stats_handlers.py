import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut
from bot.database.db import fetch_one, fetch_all
from bot.utils.quran import QuranManager
from bot.utils.helpers import format_user_link
import asyncio
logger = logging.getLogger(__name__)

async def show_total_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_kol command to show total khatm stats for salavat, zekr, or ghoran."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        # Check group and topic status
        group = await fetch_one(
            """
            SELECT is_active, max_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, zekr_text, completion_count, stop_number
            FROM topics WHERE topic_id = ? AND group_id = ?
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

        khatm_type = topic["khatm_type"]
        current_total = topic["current_total"]
        completion_count = topic["completion_count"]
        stop_number = topic["stop_number"] or "ندارد"

        if khatm_type == "ghoran":
            range_result = await fetch_one(
                """
                SELECT start_verse_id, end_verse_id 
                FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            if not range_result:
                logger.debug("No khatm range defined: topic_id=%s, group_id=%s", topic_id, group_id)
                await update.message.reply_text("محدوده ختم تعریف نشده است. از /set_range یا 'تنظیم محدوده' استفاده کنید.")
                return
            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]

            quran = await QuranManager.get_instance()
            start_verse = quran.get_verse_by_id(start_verse_id)
            end_verse = quran.get_verse_by_id(end_verse_id)
            if not start_verse or not end_verse:
                logger.error("Invalid verse IDs: start_verse_id=%d, end_verse_id=%d", start_verse_id, end_verse_id)
                await update.message.reply_text("خطا در دسترسی به آیات. لطفاً دوباره تلاش کنید.")
                return

            verses = await fetch_all(
                """
                SELECT verse_id 
                FROM contributions 
                WHERE group_id = ? AND topic_id = ? AND khatm_type = 'ghoran'
                """,
                (group_id, topic_id)
            )
            total_verses = len(verses)
            verse_texts = [
                quran.get_verse_by_id(v["verse_id"])["text"]
                for v in verses
                if quran.get_verse_by_id(v["verse_id"])
            ]

            message = (
                f"آمار ختم قرآن:\n"
                f"محدوده: از {start_verse['surah_name']} آیه {start_verse['ayah_number']} تا {end_verse['surah_name']} آیه {end_verse['ayah_number']}\n"
                f"تعداد آیات خوانده‌شده: {total_verses}\n"
                f"دفعات تکمیل: {completion_count}\n"
                f"آیات: {'، '.join(verse_texts[:5])}" + ("..." if len(verse_texts) > 5 else "")
            )
        else:
            zekr_text = topic["zekr_text"] or khatm_type
            message = (
                f"آمار ختم {khatm_type}:\n"
                f"متن: {zekr_text}\n"
                f"تعداد فعلی: {current_total}\n"
                f"هدف: {stop_number}\n"
                f"دفعات تکمیل: {completion_count}"
            )

        try:
            await update.message.reply_text(message)
        except TimedOut:
            logger.warning("Timed out sending total stats message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(message)

    except Exception as e:
        logger.error("Error in show_total_stats: group_id=%s, topic_id=%s, error=%s",
                    group_id, topic_id, e, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                          group_id, topic_id)

async def show_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_list command to show user rankings for all khatm types."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        # Check group and topic status
        group = await fetch_one(
            """
            SELECT is_active 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type 
            FROM topics WHERE topic_id = ? AND group_id = ?
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

        khatm_type = topic["khatm_type"]
        if khatm_type == "ghoran":
            rankings = await fetch_all(
                """
                SELECT u.user_id, u.username, u.first_name, u.total_ayat as contribution_count
                FROM users u
                WHERE u.group_id = ? AND u.topic_id = ? AND u.total_ayat > 0
                ORDER BY u.total_ayat DESC
                LIMIT 10
                """,
                (group_id, topic_id)
            )
            unit = "آیه"
        else:
            field = "total_salavat" if khatm_type == "salavat" else "total_zekr"
            rankings = await fetch_all(
                f"""
                SELECT u.user_id, u.username, u.first_name, u.{field} as contribution_count
                FROM users u
                WHERE u.group_id = ? AND u.topic_id = ? AND u.{field} > 0
                ORDER BY u.{field} DESC
                LIMIT 10
                """,
                (group_id, topic_id)
            )
            unit = "صلوات" if khatm_type == "salavat" else "ذکر"

        if not rankings:
            logger.debug("No contributions found: group_id=%s, topic_id=%s", group_id, topic_id)
            await update.message.reply_text("هیچ مشارکتی ثبت نشده است.")
            return

        ranking_text = f"رتبه‌بندی مشارکت‌کنندگان ({khatm_type}):\n"
        for i, row in enumerate(rankings, 1):
            user_link = format_user_link(row["user_id"], row["username"], row["first_name"])
            ranking_text += f"{i}. {user_link}: {row['contribution_count']} {unit}\n"

        try:
            await update.message.reply_text(ranking_text, parse_mode="Markdown")
        except TimedOut:
            logger.warning("Timed out sending ranking message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(ranking_text, parse_mode="Markdown")

    except Exception as e:
        logger.error("Error in show_ranking: group_id=%s, topic_id=%s, error=%s",
                    group_id, topic_id, e, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",group_id, topic_id)