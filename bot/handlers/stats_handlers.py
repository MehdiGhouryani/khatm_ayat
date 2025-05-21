import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, TelegramError
from bot.database.db import fetch_one, fetch_all, DatabaseError
from bot.utils.quran import QuranManager, QuranError
from bot.utils.helpers import format_user_link
import asyncio
import traceback

logger = logging.getLogger(__name__)

async def show_total_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_kol command to show total khatm stats for salavat, zekr, or ghoran."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.info(f"Processing total stats request - group_id={group_id}, topic_id={topic_id}, chat_type={update.effective_chat.type}")

        # Check group and topic status
        try:
            group = await fetch_one(
                """
                SELECT is_active, max_display_verses 
                FROM groups WHERE group_id = ?
                """,
                (group_id,)
            )
        except DatabaseError as e:
            logger.error(f"Database error while fetching group info: {e}", 
                        extra={"group_id": group_id, "error": str(e)})
            raise

        if not group:
            logger.warning("Group not found", 
                         extra={"group_id": group_id})
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return
        
        if not group["is_active"]:
            logger.warning("Group is inactive", 
                         extra={"group_id": group_id})
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        try:
            # First check if any active topic exists
            active_topic = await fetch_one(
                """
                SELECT COUNT(*) as count
                FROM topics 
                WHERE topic_id = ? AND group_id = ? AND is_active = 1
                """,
                (topic_id, group_id)
            )
            
            if not active_topic or active_topic["count"] == 0:
                logger.warning("No active topic found", 
                             extra={"group_id": group_id, "topic_id": topic_id})
                await update.message.reply_text("هیچ ختم فعالی در این تاپیک وجود ندارد.")
                return

            topic = await fetch_one(
                """
                SELECT khatm_type, current_total, current_verse_id, zekr_text, completion_count, stop_number, is_active
                FROM topics WHERE topic_id = ? AND group_id = ?
                """,
                (topic_id, group_id)
            )
        except DatabaseError as e:
            logger.error(f"Database error while fetching topic info: {e}",
                        extra={"group_id": group_id, "topic_id": topic_id, "error": str(e)})
            raise

        if not topic:
            logger.warning("Topic not found", 
                         extra={"group_id": group_id, "topic_id": topic_id})
            await update.message.reply_text("تاپیک ختم تنظیم نشده است.\nبرای تنظیم از دستور `topic` استفاده کنید.",parse_mode="Markdown")
            return

        if not topic["is_active"]:
            logger.info("Inactive topic accessed",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": topic["khatm_type"]})
            await update.message.reply_text(
                "این تاپیک ختم غیرفعال است.\n"
                "برای فعال‌سازی از یکی از دستورات زیر استفاده کنید:\n"
                "`ختم ذکر`\n"
                "`ختم صلوات`\n"
                "`ختم قرآن`"
            )
            return

        khatm_type = topic["khatm_type"]
        current_total = topic["current_total"]
        completion_count = topic["completion_count"]
        stop_number = topic["stop_number"] or "ندارد"

        if khatm_type == "ghoran":
            try:
                range_result = await fetch_one(
                    """
                    SELECT start_verse_id, end_verse_id 
                    FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                    """,
                    (group_id, topic_id)
                )
            except DatabaseError as e:
                logger.error(f"Database error while fetching khatm range: {e}",
                           extra={"group_id": group_id, "topic_id": topic_id, "error": str(e)})
                raise

            if not range_result:
                logger.warning("No khatm range defined",
                             extra={"group_id": group_id, "topic_id": topic_id})
                await update.message.reply_text(
                    "محدوده ختم تعریف نشده است.\n"
                    "برای تنظیم از دستور `تنظیم محدوده` استفاده کنید."
                )
                return

            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]
            logger.debug(f"Processing Quran khatm range", 
                        extra={"start_verse": start_verse_id, "end_verse": end_verse_id})

            try:
                quran = await QuranManager.get_instance()
                start_verse = quran.get_verse_by_id(start_verse_id)
                end_verse = quran.get_verse_by_id(end_verse_id)
            except QuranError as e:
                logger.error(f"Error accessing Quran data: {e}",
                           extra={"start_verse": start_verse_id, "end_verse": end_verse_id})
                await update.message.reply_text("خطا در دسترسی به اطلاعات قرآن. لطفاً با پشتیبانی تماس بگیرید.")
                return

            if not start_verse or not end_verse:
                logger.error("Invalid verse IDs",
                           extra={"start_verse": start_verse_id, "end_verse": end_verse_id})
                await update.message.reply_text("خطا در دسترسی به آیات. لطفاً دوباره تلاش کنید.")
                return

            try:
                verses = await fetch_all(
                    """
                    SELECT verse_id 
                    FROM contributions 
                    WHERE group_id = ? AND topic_id = ?
                    """,
                    (group_id, topic_id)
                )
            except DatabaseError as e:
                logger.error(f"Database error while fetching contributions: {e}",
                           extra={"group_id": group_id, "topic_id": topic_id, "error": str(e)})
                raise

            total_verses = len(verses)
            verse_texts = []
            for v in verses:
                try:
                    verse = quran.get_verse_by_id(v["verse_id"])
                    if verse:
                        verse_texts.append(verse["text"])
                except QuranError as e:
                    logger.warning(f"Error fetching verse text: {e}",
                                 extra={"verse_id": v["verse_id"]})
                    continue

            message = (
                f"آمار ختم قرآن:\n"
                f"محدوده: از {start_verse['surah_name']} آیه {start_verse['ayah_number']} تا {end_verse['surah_name']} آیه {end_verse['ayah_number']}\n"
                f"آیه فعلی: {topic['current_verse_id']}\n"
                f"دفعات تکمیل: {completion_count}"
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
            logger.info("Successfully sent stats message",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})
        except TimedOut:
            logger.warning("Timed out sending stats message, retrying",
                         extra={"group_id": group_id, "topic_id": topic_id})
            await asyncio.sleep(2)
            await update.message.reply_text(message)

    except DatabaseError as e:
        logger.error("Database operation failed",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطا در دسترسی به پایگاه داده. لطفاً دوباره تلاش کنید.")
    except TelegramError as e:
        logger.error("Telegram API error",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطا در ارتباط با تلگرام. لطفاً دوباره تلاش کنید.")
    except Exception as e:
        logger.critical("Unexpected error",
                       extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای غیرمنتظره رخ داد. لطفاً با پشتیبانی تماس بگیرید.")

async def show_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_list command to show user rankings for all khatm types."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.info(f"Processing ranking request", 
                   extra={"group_id": group_id, "topic_id": topic_id})

        try:
            group = await fetch_one(
                """
                SELECT is_active 
                FROM groups WHERE group_id = ?
                """,
                (group_id,)
            )
        except DatabaseError as e:
            logger.error(f"Database error while fetching group info: {e}",
                        extra={"group_id": group_id, "error": str(e)})
            raise

        if not group or not group["is_active"]:
            logger.warning("Group not found or inactive",
                         extra={"group_id": group_id, "group_exists": bool(group)})
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        try:
            topic = await fetch_one(
                """
                SELECT khatm_type, is_active
                FROM topics WHERE topic_id = ? AND group_id = ?
                """,
                (topic_id, group_id)
            )
        except DatabaseError as e:
            logger.error(f"Database error while fetching topic info: {e}",
                        extra={"group_id": group_id, "topic_id": topic_id, "error": str(e)})
            raise

        if not topic:
            logger.warning("Topic not found",
                         extra={"group_id": group_id, "topic_id": topic_id})
            await update.message.reply_text("تاپیک ختم تنظیم نشده است.\nبرای تنظیم از دستور `topic` استفاده کنید.",parse_mode="Markdown")
            return

        if not topic["is_active"]:
            logger.info("Inactive topic accessed",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": topic["khatm_type"]})
            await update.message.reply_text("لطفاً از `khatm_zekr`، `khatm_salavat` یا `khatm_ghoran` برای فعال‌سازی ختم استفاده کنید.",parse_mode="Markdown")
            return

        khatm_type = topic["khatm_type"]
        try:
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
        except DatabaseError as e:
            logger.error(f"Database error while fetching rankings: {e}",
                        extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type, "error": str(e)})
            raise

        if not rankings:
            logger.info("No contributions found",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})
            await update.message.reply_text("هیچ مشارکتی ثبت نشده است.")
            return

        ranking_text = f"رتبه‌بندی مشارکت‌کنندگان ({khatm_type}):\n"
        for i, row in enumerate(rankings, 1):
            user_link = format_user_link(row["user_id"], row["username"], row["first_name"])
            ranking_text += f"{i}. {user_link}: {row['contribution_count']} {unit}\n"

        try:
            await update.message.reply_text(ranking_text, parse_mode="Markdown")
            logger.info("Successfully sent ranking message",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})
        except TimedOut:
            logger.warning("Timed out sending ranking message, retrying",
                         extra={"group_id": group_id, "topic_id": topic_id})
            await asyncio.sleep(2)
            await update.message.reply_text(ranking_text, parse_mode="Markdown")

    except DatabaseError as e:
        logger.error("Database operation failed",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطا در دسترسی به پایگاه داده. لطفاً دوباره تلاش کنید.")
    except TelegramError as e:
        logger.error("Telegram API error",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطا در ارتباط با تلگرام. لطفاً دوباره تلاش کنید.")
    except Exception as e:
        logger.critical("Unexpected error",
                       extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای غیرمنتظره رخ داد. لطفاً با پشتیبانی تماس بگیرید.")