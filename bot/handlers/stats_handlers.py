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

        group = await fetch_one(
            """
            SELECT is_active, max_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.warning("Group not found or inactive", 
                         extra={"group_id": group_id})
            await update.message.reply_text("گروه فعال نیست. از /start استفاده کنید.")
            return

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
            await update.message.reply_text("<b>ختم فعالی وجود ندارد</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖", parse_mode='HTML')
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, current_verse_id, zekr_text, completion_count, stop_number, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.warning("Topic not found", 
                         extra={"group_id": group_id, "topic_id": topic_id})
            await update.message.reply_text("<b>تاپیک ختم تنظیم نشده</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n<b>اقدام</b>: از /topic استفاده کنید", parse_mode='HTML')
            return

        if not topic["is_active"]:
            logger.info("Inactive topic accessed",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": topic["khatm_type"]})
            await update.message.reply_text("<b>تاپیک غیرفعال است</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n<b>اقدام</b>: از /khatm_zekr، /khatm_salavat یا /khatm_ghoran استفاده کنید", parse_mode='HTML')
            return

        khatm_type = topic["khatm_type"]
        khatm_type_persian = {"salavat": "صلوات", "zekr": "ذکر", "ghoran": "قرآن"}.get(khatm_type, khatm_type)
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
                logger.warning("No khatm range defined",
                             extra={"group_id": group_id, "topic_id": topic_id})
                await update.message.reply_text("<b>محدوده ختم تعریف نشده</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n<b>اقدام</b>: از /set_range استفاده کنید", parse_mode='HTML')
                return

            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]
            quran = await QuranManager.get_instance()
            start_verse = quran.get_verse_by_id(start_verse_id)
            end_verse = quran.get_verse_by_id(end_verse_id)

            if not start_verse or not end_verse:
                logger.error("Invalid verse IDs",
                           extra={"start_verse": start_verse_id, "end_verse": end_verse_id})
                await update.message.reply_text("خطا در دسترسی به آیات. دوباره تلاش کنید.")
                return

            verses = await fetch_all(
                """
                SELECT verse_id 
                FROM contributions 
                WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            total_verses = len(verses)
            message = (
                f"<b>آمار ختم {khatm_type_persian}</b>🌱\n"
                f"➖➖➖➖➖➖➖➖➖➖➖\n"
                f"<b>محدوده</b>: از {start_verse['surah_name']} آیه {start_verse['ayah_number']} تا {end_verse['surah_name']} آیه {end_verse['ayah_number']}\n"
                f"<b>آیه فعلی</b>: {topic['current_verse_id']}\n"
                f"<b>تعداد آیات خوانده‌شده</b>: {total_verses}\n"
                f"<b>دفعات تکمیل</b>: {completion_count}"
            )
        else:
            if khatm_type == "zekr":
                actual_zekr_text = topic["zekr_text"] or "تعیین نشده"
                message = (
                    f"<b>آمار ختم {khatm_type_persian}</b>🌱\n"
                    f"➖➖➖➖➖➖➖➖➖➖➖\n"
                    f"<b>متن</b>: {actual_zekr_text}\n"
                    f"<b>تعداد فعلی</b>: {current_total}\n"
                    f"<b>هدف</b>: {stop_number}\n"
                    f"<b>دفعات تکمیل</b>: {completion_count}"
                )
            elif khatm_type == "salavat":
                message = (
                    f"<b>آمار ختم {khatm_type_persian}</b>🌱\n"
                    f"➖➖➖➖➖➖➖➖➖➖➖\n"
                    f"<b>تعداد فعلی</b>: {current_total}\n"
                    f"<b>هدف</b>: {stop_number}\n"
                    f"<b>دفعات تکمیل</b>: {completion_count}"
                )
            else:
                message = "<b>نوع ختم ناشناخته</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖"

        await update.message.reply_text(message, parse_mode='HTML')
        logger.info("Successfully sent stats message",
                   extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})

    except DatabaseError as e:
        logger.error("Database operation failed",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای پایگاه داده. دوباره تلاش کنید.")
    except TelegramError as e:
        logger.error("Telegram API error",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای تلگرام. دوباره تلاش کنید.")
    except Exception as e:
        logger.critical("Unexpected error",
                       extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای غیرمنتظره. با پشتیبانی تماس بگیرید.")


async def show_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_list command to show user rankings for all khatm types."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.info(f"Processing ranking request", 
                   extra={"group_id": group_id, "topic_id": topic_id})

        group = await fetch_one(
            """
            SELECT is_active 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.warning("Group not found or inactive",
                         extra={"group_id": group_id, "group_exists": bool(group)})
            await update.message.reply_text("گروه فعال نیست. از /start استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.warning("Topic not found",
                         extra={"group_id": group_id, "topic_id": topic_id})
            await update.message.reply_text("<b>تاپیک ختم تنظیم نشده</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n<b>اقدام</b>: از /topic استفاده کنید", parse_mode='HTML')
            return

        if not topic["is_active"]:
            logger.info("Inactive topic accessed",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": topic["khatm_type"]})
            await update.message.reply_text("<b>تاپیک غیرفعال است</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n<b>اقدام</b>: از /khatm_zekr، /khatm_salavat یا /khatm_ghoran استفاده کنید", parse_mode='HTML')
            return

        khatm_type = topic["khatm_type"]
        khatm_type_persian = {"salavat": "صلوات", "zekr": "ذکر", "ghoran": "قرآن"}.get(khatm_type, khatm_type)

        if khatm_type == "ghoran":
            rankings = await fetch_all(
                """
                SELECT u.user_id, u.username, u.first_name, u.total_ayat as contribution_count
                FROM users u
                WHERE u.group_id = ? AND u.topic_id = ? AND u.total_ayat > 0
                ORDER BY u.total_ayat DESC
                LIMIT 30
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
                LIMIT 30
                """,
                (group_id, topic_id)
            )
            unit = "صلوات" if khatm_type == "salavat" else "ذکر"

        if not rankings:
            logger.info("No contributions found",
                       extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})
            await update.message.reply_text("<b>مشارکتی ثبت نشده</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖", parse_mode='HTML')
            return

        ranking_text = f"<b>رتبه‌بندی مشارکت‌کنندگان ({khatm_type_persian})</b>🌱\n➖➖➖➖➖➖➖➖➖➖➖\n"
        for i, row in enumerate(rankings, 1):
            user_link = format_user_link(row["user_id"], row["username"], row["first_name"])
            ranking_text += f"{i}. {user_link}: {row['contribution_count']} {unit}\n"

        await update.message.reply_text(ranking_text, parse_mode='HTML')
        logger.info("Successfully sent ranking message",
                   extra={"group_id": group_id, "topic_id": topic_id, "khatm_type": khatm_type})

    except DatabaseError as e:
        logger.error("Database operation failed",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای پایگاه داده. دوباره تلاش کنید.")
    except TelegramError as e:
        logger.error("Telegram API error",
                    extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای تلگرام. دوباره تلاش کنید.")
    except Exception as e:
        logger.critical("Unexpected error",
                       extra={"group_id": group_id, "topic_id": topic_id, "error": str(e), "traceback": traceback.format_exc()})
        await update.message.reply_text("خطای غیرمنتظره. با پشتیبانی تماس بگیرید.")