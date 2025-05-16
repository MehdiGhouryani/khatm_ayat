import logging
import re
from telegram import Update
from telegram.ext import ContextTypes, filters
from bot.database.db import get_db_connection
from bot.services.khatm_service import process_khatm_number
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas
from bot.handlers.admin_handlers import is_admin
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

quran = QuranManager()

async def handle_khatm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming text messages for khatm contributions."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Message received in non-group chat")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        text = update.message.text.strip()

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_active, lock_enabled, min_number, max_number FROM groups WHERE group_id = ?",
                (group_id,)
            )
            group = cursor.fetchone()
            if not group or not group["is_active"]:
                logger.debug(f"Bot is not active for group_id={group_id}")
                return

            if group["lock_enabled"]:
                if not re.match(r"^-?\d+$", text.replace(" ", "")) and "سوره" not in text and "آیه" not in text:
                    logger.debug(f"Non-numeric or non-verse message in locked mode: {text}")
                    return

            cursor.execute(
                "SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, period_number, stop_number, completion_message FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if not topic:
                logger.debug(f"No topic found for topic_id={topic_id}, group_id={group_id}")
                return

            user_id = update.effective_user.id
            username = update.effective_user.username or update.effective_user.first_name
            first_name = update.effective_user.first_name

            cursor.execute(
                """
                INSERT OR IGNORE INTO users (user_id, group_id, topic_id, username, first_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, group_id, topic_id, username, first_name)
            )

            if topic["khatm_type"] == "ghoran":
                # Parse verse input (e.g., "سوره 1 آیه 1" or "1")
                try:
                    if "سوره" in text and "آیه" in text:
                        parts = text.split()
                        surah_number = int(parts[parts.index("سوره") + 1])
                        ayah_number = int(parts[parts.index("آیه") + 1])
                    else:
                        verse_id = int(text)
                        verse = quran.get_verse_by_id(verse_id)
                        if not verse:
                            logger.debug(f"Invalid verse ID: {text}")
                            await update.message.reply_text("آیه نامعتبر است.")
                            return
                        surah_number, ayah_number = verse['surah_number'], verse['ayah_number']
                except (ValueError, IndexError):
                    logger.debug(f"Invalid verse format: {text}")
                    await update.message.reply_text("لطفاً آیه را به شکل صحیح وارد کنید (مثل 'سوره 1 آیه 1' یا '1').")
                    return

                # Validate verse
                verse = quran.get_verse(surah_number, ayah_number)
                if not verse:
                    logger.debug(f"Verse not found: surah={surah_number}, ayah={ayah_number}")
                    await update.message.reply_text("آیه یافت نشد.")
                    return

                # Check verse range
                cursor.execute(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                range_result = cursor.fetchone()
                if not range_result:
                    logger.debug(f"No khatm range defined for topic_id={topic_id}")
                    await update.message.reply_text("محدوده ختم تعریف نشده است. از /set_range استفاده کنید.")
                    return
                start_verse_id, end_verse_id = range_result
                if not (start_verse_id <= verse['id'] <= end_verse_id):
                    logger.debug(f"Verse out of range: verse_id={verse['id']}, range={start_verse_id}-{end_verse_id}")
                    await update.message.reply_text("آیه خارج از محدوده ختم است.")
                    return

                # Check min/max ayat
                number = 1  # Each contribution is one verse
                if number < topic["min_ayat"] or number > topic["max_ayat"]:
                    logger.warning(f"Number out of range for Quran: {number}, min={topic['min_ayat']}, max={topic['max_ayat']}")
                    await update.message.reply_text(f"تعداد آیات باید بین {topic['min_ayat']} و {topic['max_ayat']} باشد.")
                    return

                # Register contribution
                cursor.execute(
                    """
                    INSERT INTO contributions (group_id, topic_id, user_id, amount, verse_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (group_id, topic_id, user_id, number, verse['id'])
                )
                cursor.execute(
                    "UPDATE users SET total_ayat = total_ayat + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                    (number, user_id, group_id, topic_id)
                )

                # Update topic total
                previous_total, new_total, completed = process_khatm_number(
                    group_id, topic_id, number, topic["khatm_type"], topic["current_total"], conn
                )

                # Check completion based on stop_number
                if topic["stop_number"] > 0 and new_total >= topic["stop_number"]:
                    completed = True
                    cursor.execute(
                        "UPDATE topics SET is_active = 0 WHERE topic_id = ? AND group_id = ?",
                        (topic_id, group_id)
                    )
                    conn.commit()

                # Format message
                sepas_text = get_random_sepas(group_id, conn)
                message = format_khatm_message(
                    topic["khatm_type"],
                    previous_total,
                    number,
                    new_total,
                    sepas_text,
                    group_id,
                    verse_id=verse['id']
                )

                await update.message.reply_text(message)
                logger.info(f"Quran khatm processed: group_id={group_id}, topic_id={topic_id}, verse_id={verse['id']}")

                if completed:
                    completion_message = topic["completion_message"] or "تبریک! ختم قرآن کامل شد. برای ختم جدید، محدوده را با /set_range تنظیم کنید."
                    await update.message.reply_text(completion_message)

            else:  # salavat or zekr
                number = parse_number(text)
                if number is None:
                    logger.debug(f"Invalid number format: {text}")
                    return

                if number == 0:
                    logger.debug(f"Zero number received: {text}")
                    await update.message.reply_text("عدد صفر مجاز نیست.")
                    return

                if number > 0 and (number < group["min_number"] or number > group["max_number"]):
                    logger.warning(f"Number out of range: {number}, min={group['min_number']}, max={group['max_number']}")
                    await update.message.reply_text(f"عدد باید بین {group['min_number']} و {group['max_number']} باشد.")
                    return

                previous_total, new_total, completed = process_khatm_number(
                    group_id, topic_id, number, topic["khatm_type"], topic["current_total"], conn
                )

                if number < 0:
                    if new_total < 0:
                        await update.message.reply_text("مجموع نمی‌تواند منفی شود.")
                        return
                    sepas_text = get_random_sepas(group_id, conn)
                    message = format_khatm_message(
                        topic["khatm_type"],
                        previous_total + abs(number),
                        f"_{abs(number)}_",
                        new_total,
                        sepas_text,
                        group_id,
                        topic["zekr_text"]
                    )
                else:
                    sepas_text = get_random_sepas(group_id, conn)
                    message = format_khatm_message(
                        topic["khatm_type"],
                        previous_total,
                        number,
                        new_total,
                        sepas_text,
                        group_id,
                        topic["zekr_text"]
                    )

                if topic["khatm_type"] == "zekr":
                    cursor.execute(
                        "UPDATE users SET total_zekr = total_zekr + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                        (number, user_id, group_id, topic_id)
                    )
                else:  # salavat
                    cursor.execute(
                        "UPDATE users SET total_salavat = total_salavat + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                        (number, user_id, group_id, topic_id)
                    )
                conn.commit()

                await update.message.reply_text(message, parse_mode="Markdown")
                logger.info(f"Khatm processed: group_id={group_id}, topic_id={topic_id}, number={number}")

                if completed:
                    completion_message = topic["completion_message"] or "تبریک! ختم کامل شد. ختم بعدی آغاز می‌شود."
                    await update.message.reply_text(completion_message)

                if topic["stop_number"] > 0 and new_total >= topic["stop_number"]:
                    cursor.execute(
                        "UPDATE topics SET is_active = 0 WHERE topic_id = ? AND group_id = ?",
                        (topic_id, group_id)
                    )
                    conn.commit()
                    completion_message = topic["completion_message"] or f"ختم در تعداد {topic['stop_number']} متوقف شد."
                    await update.message.reply_text(completion_message)

        logger.debug(f"Message processed successfully: group_id={group_id}, topic_id={topic_id}")
    except Exception as e:
        logger.error(f"Error processing khatm message: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")