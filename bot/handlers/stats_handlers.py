import logging
from telegram import Update
from telegram.ext import ContextTypes
from bot.database.db import fetch_one, fetch_all
from bot.utils.constants import quran
from bot.utils.helpers import format_user_link

logger = logging.getLogger(__name__)

async def show_total_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_kol command to show total khatm stats."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        range_result = await fetch_one(
            "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
            (group_id, topic_id)
        )
        if not range_result:
            await update.message.reply_text("محدوده ختم تعریف نشده است.")
            return
        start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]

        start_verse = quran.get_verse_by_id(start_verse_id)
        end_verse = quran.get_verse_by_id(end_verse_id)

        verses = await fetch_all(
            "SELECT verse_id FROM contributions WHERE group_id = ? AND topic_id = ?",
            (group_id, topic_id)
        )
        total_verses = len(verses)
        verse_texts = [quran.get_verse_by_id(v["verse_id"])["text"] for v in verses if quran.get_verse_by_id(v["verse_id"])]

        await update.message.reply_text(
            f"آمار ختم:\n"
            f"محدوده: از {start_verse['surah_name']} آیه {start_verse['ayah_number']} تا {end_verse['surah_name']} آیه {end_verse['ayah_number']}\n"
            f"تعداد آیات خوانده‌شده: {total_verses}\n"
            f"آیات: {'، '.join(verse_texts[:5])}" + ("..." if len(verse_texts) > 5 else "")
        )
    except Exception as e:
        logger.error("Error in show_total_stats: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def show_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_list command to show user rankings."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        rankings = await fetch_all(
            """
            SELECT u.user_id, u.username, u.first_name, COUNT(c.verse_id) as verse_count
            FROM contributions c
            JOIN users u ON c.user_id = u.user_id
            WHERE c.group_id = ? AND c.topic_id = ?
            GROUP BY c.user_id
            ORDER BY verse_count DESC
            LIMIT 10
            """,
            (group_id, topic_id)
        )

        if not rankings:
            await update.message.reply_text("هیچ مشارکتی ثبت نشده است.")
            return

        ranking_text = "رتبه‌بندی مشارکت‌کنندگان:\n"
        for i, row in enumerate(rankings, 1):
            user_link = format_user_link(row["user_id"], row["username"], row["first_name"])
            ranking_text += f"{i}. {user_link}: {row['verse_count']} آیه\n"

        await update.message.reply_text(ranking_text, parse_mode="Markdown")
    except Exception as e:
        logger.error("Error in show_ranking: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")