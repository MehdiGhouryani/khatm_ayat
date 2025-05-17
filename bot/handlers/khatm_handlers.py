import logging
import re
from telegram import Update
from telegram.ext import ContextTypes
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
        logger.debug(f"Processing message: group_id={group_id}, topic_id={topic_id}, text={text}")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_active, lock_enabled, min_number, max_number, max_display_verses FROM groups WHERE group_id = ?",
                (group_id,)
            )
            group = cursor.fetchone()
            if not group:
                logger.warning(f"No group found for group_id={group_id}. Group may not be initialized.")
                await update.message.reply_text("گروه در ربات ثبت نشده است. لطفاً از دستور /start استفاده کنید.")
                return
            if not group["is_active"]:
                logger.debug(f"Group is not active: group_id={group_id}")
                await update.message.reply_text("ربات در این گروه فعال نیست. از /start استفاده کنید.")
                return

            cursor.execute(
                "SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, period_number, stop_number, completion_message, current_verse_id, is_active, completion_count FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if not topic:
                logger.debug(f"No topic found for topic_id={topic_id}, group_id={group_id}")
                await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا /khatm_ghoran استفاده کنید.")
                return
            if not topic["is_active"]:
                logger.debug(f"Topic is not active: topic_id={topic_id}")
                await update.message.reply_text("این تاپیک ختم غیرفعال است.")
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

            # Parse input as a number
            number = parse_number(text)
            if number is None:
                logger.debug(f"Non-numeric input ignored: text={text}, khatm_type={topic['khatm_type']}")
                if group["lock_enabled"]:
                    return
                if topic["khatm_type"] == "ghoran":
                    return  # Ignore non-numeric inputs for Quran
                await update.message.reply_text("لطفاً یک عدد معتبر وارد کنید.")
                return

            if number <= 0:
                logger.debug(f"Invalid number: {number}")
                await update.message.reply_text("عدد باید مثبت باشد.")
                return

            if topic["khatm_type"] == "ghoran":
                logger.debug(f"Processing Quran khatm: number={number}")
                # Limit number to min(max_ayat, max_display_verses)
                max_allowed = min(topic["max_ayat"], group["max_display_verses"])
                if number > max_allowed:
                    logger.info(f"Number exceeds max_allowed: requested={number}, max_allowed={max_allowed}")
                    number = max_allowed
                if number < topic["min_ayat"]:
                    logger.warning(f"Number below minimum for Quran: {number}, min={topic['min_ayat']}")
                    await update.message.reply_text(f"تعداد آیات باید حداقل {topic['min_ayat']} باشد.")
                    return

                # Get verse range
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

                # Calculate verse range to assign
                current_verse_id = topic["current_verse_id"]
                start_assign_id = current_verse_id  # Start from current_verse_id (includes Bismillah)
                end_assign_id = min(start_assign_id + number - 1, end_verse_id)

                if start_assign_id > end_verse_id:
                    logger.debug(f"No more verses available in range: {start_verse_id}-{end_verse_id}")
                    await update.message.reply_text("ختم محدوده به پایان رسیده است. لطفاً محدوده جدید را با /set_range تنظیم کنید.")
                    return

                assigned_number = end_assign_id - start_assign_id + 1
                verses = quran.get_verses_in_range(start_assign_id, end_assign_id)
                if not verses:
                    logger.error(f"No verses found for range: {start_assign_id}-{end_assign_id}")
                    await update.message.reply_text("خطا در تخصیص آیات. لطفاً دوباره تلاش کنید.")
                    return

                # Register contributions
                for verse in verses:
                    cursor.execute(
                        """
                        INSERT INTO contributions (group_id, topic_id, user_id, amount, verse_id)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (group_id, topic_id, user_id, 1, verse['id'])
                    )
                cursor.execute(
                    "UPDATE users SET total_ayat = total_ayat + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                    (assigned_number, user_id, group_id, topic_id)
                )
                cursor.execute(
                    "UPDATE topics SET current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    (end_assign_id, topic_id, group_id)
                )

                # Update topic total
                previous_total, new_total, completed = process_khatm_number(
                    group_id, topic_id, assigned_number, topic["khatm_type"], topic["current_total"], conn
                )

                # Check completion
                if end_assign_id >= end_verse_id:
                    completed = True
                    cursor.execute(
                        "UPDATE topics SET is_active = 0, completion_count = completion_count + 1 WHERE topic_id = ? AND group_id = ?",
                        (topic_id, group_id)
                    )

                conn.commit()

                # Format message
                sepas_text = get_random_sepas(group_id, conn)
                message = format_khatm_message(
                    topic["khatm_type"],
                    previous_total,
                    assigned_number,
                    new_total,
                    sepas_text,
                    group_id,
                    verses=verses,
                    max_display_verses=group["max_display_verses"],
                    completion_count=topic["completion_count"]
                )

                await update.message.reply_text(message)
                logger.info(f"Quran khatm processed: group_id={group_id}, topic_id={topic_id}, verses={start_assign_id}-{end_assign_id}")

                if completed:
                    completion_message = topic["completion_message"] or "تبریک! ختم قرآن کامل شد. برای ختم جدید، محدوده را با /set_range تنظیم کنید."
                    await update.message.reply_text(completion_message)

            else:  # salavat or zekr
                logger.debug(f"Processing {topic['khatm_type']} khatm: number={number}")
                # Validate number against min_number and max_number
                if number < group["min_number"] or number > group["max_number"]:
                    logger.warning(f"Number out of range for {topic['khatm_type']}: {number}, min={group['min_number']}, max={group['max_number']}")
                    await update.message.reply_text(f"عدد باید بین {group['min_number']} و {group['max_number']} باشد.")
                    return

                previous_total, new_total, completed = process_khatm_number(
                    group_id, topic_id, number, topic["khatm_type"], topic["current_total"], conn
                )

                sepas_text = get_random_sepas(group_id, conn)
                message = format_khatm_message(
                    topic["khatm_type"],
                    previous_total,
                    number,
                    new_total,
                    sepas_text,
                    group_id,
                    topic["zekr_text"],
                    completion_count=topic["completion_count"]
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
                cursor.execute(
                    """
                    INSERT INTO contributions (group_id, topic_id, user_id, amount)
                    VALUES (?, ?, ?, ?)
                    """,
                    (group_id, topic_id, user_id, number)
                )
                conn.commit()

                await update.message.reply_text(message, parse_mode="Markdown")
                logger.info(f"{topic['khatm_type'].capitalize()} khatm processed: group_id={group_id}, topic_id={topic_id}, number={number}")

                if completed:
                    completion_message = topic["completion_message"] or f"تبریک! ختم {topic['khatm_type']} کامل شد."
                    await update.message.reply_text(completion_message)

                if topic["stop_number"] > 0 and new_total >= topic["stop_number"]:
                    cursor.execute(
                        "UPDATE topics SET is_active = 0, completion_count = completion_count + 1 WHERE topic_id = ? AND group_id = ?",
                        (topic_id, group_id)
                    )
                    conn.commit()
                    completion_message = topic["completion_message"] or f"ختم در تعداد {topic['stop_number']} متوقف شد."
                    await update.message.reply_text(completion_message)

    except Exception as e:
        logger.error(f"Error processing khatm message: {e}", exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")