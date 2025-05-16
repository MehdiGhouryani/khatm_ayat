import logging
from telegram import Update
from telegram.ext import ContextTypes
from bot.database.db import get_db_connection
from bot.utils.quran import QuranManager
from bot.utils.helpers import format_user_link

logger = logging.getLogger(__name__)

async def show_total_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_kol command to show total khatm stats."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            range_result = cursor.fetchone()
            if not range_result:
                await update.message.reply_text("محدوده ختم تعریف نشده است.")
                return
            start_verse_id, end_verse_id = range_result

            start_verse = QuranManager.get_verse_by_id(start_verse_id)
            end_verse = QuranManager.get_verse_by_id(end_verse_id)

            cursor.execute(
                "SELECT verse_id FROM contributions WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            verses = cursor.fetchall()
            total_verses = len(verses)
            verse_texts = [QuranManager.get_verse_by_id(v[0])['text'] for v in verses if QuranManager.get_verse_by_id(v[0])]

            await update.message.reply_text(
                f"آمار ختم:\n"
                f"محدوده: از {start_verse['surah_name']} آیه {start_verse['ayah_number']} تا {end_verse['surah_name']} آیه {end_verse['ayah_number']}\n"
                f"تعداد آیات خوانده‌شده: {total_verses}\n"
                f"آیات: {'، '.join(verse_texts[:5])}" + ("..." if len(verse_texts) > 5 else "")
            )
    except Exception as e:
        logger.error(f"Error in show_total_stats: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def show_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /amar_list command to show user rankings."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
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
            rankings = cursor.fetchall()

            if not rankings:
                await update.message.reply_text("هیچ مشارکتی ثبت نشده است.")
                return

            ranking_text = "رتبه‌بندی مشارکت‌کنندگان:\n"
            for i, (user_id, username, first_name, verse_count) in enumerate(rankings, 1):
                user_link = format_user_link(user_id, username, first_name)
                ranking_text += f"{i}. {user_link}: {verse_count} آیه\n"

            await update.message.reply_text(ranking_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in show_ranking: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")