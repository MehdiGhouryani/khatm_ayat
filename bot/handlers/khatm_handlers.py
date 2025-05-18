# khatm_handlers.py
import logging
import re
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from bot.database.db import get_db_connection
from bot.services.khatm_service import process_khatm_number
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas
from bot.handlers.admin_handlers import is_admin
from bot.utils.quran import QuranManager
from bot.handlers.admin_handlers import TEXT_COMMANDS

logger = logging.getLogger(__name__)

quran = QuranManager()



async def handle_khatm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Message received in non-group chat")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        text = raw_text.lower()

        # Step 1: Check if the message is a 'start' command or its aliases
        start_command_info = TEXT_COMMANDS.get("start")
        if start_command_info and (text == "start" or raw_text in start_command_info["aliases"]):
            logger.debug(f"Processing 'start' command: group_id={group_id}, text={raw_text}")
            if start_command_info["admin_only"] and not await is_admin(update, context):
                await update.message.reply_text("فقط ادمین می‌تواند این دستور را اجرا کند.")
                return
            args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
            context.args = args
            await start_command_info["handler"](update, context)
            return

        # Step 2: Proceed with group and topic checks for other messages
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_active, lock_enabled, min_number, max_number, max_display_verses FROM groups WHERE group_id = ?",
                (group_id,)
            )
            group = cursor.fetchone()
            if not group:
                await update.message.reply_text("گروه در ربات ثبت نشده است. لطفاً از دستور /start یا 'شروع' استفاده کنید.")
                return
            if not group["is_active"]:
                await update.message.reply_text("ربات در این گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
                return

            cursor.execute(
                "SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, period_number, stop_number, completion_message, current_verse_id, is_active, completion_count FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if not topic:
                await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
                return
            if not topic["is_active"]:
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

            # Step 3: Handle commands when lock is enabled
            if group["lock_enabled"]:
                command_found = False
                for command, info in TEXT_COMMANDS.items():
                    if command == "start":  # Skip start, already handled
                        continue
                    if text == command or raw_text in info["aliases"]:
                        command_found = True
                        if info["admin_only"] and not await is_admin(update, context):
                            try:
                                await context.bot.delete_message(
                                    chat_id=group_id,
                                    message_id=update.message.message_id,
                                    message_thread_id=topic_id if update.message.message_thread_id else None
                                )
                            except BadRequest:
                                pass
                            return
                        args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
                        context.args = args
                        await info["handler"](update, context)
                        return

                number = parse_number(raw_text)
                if number is None:
                    try:
                        await context.bot.delete_message(
                            chat_id=group_id,
                            message_id=update.message.message_id,
                            message_thread_id=topic_id if update.message.message_thread_id else None
                        )
                    except BadRequest:
                        pass
                    return

                if number < 0 and await is_admin(update, context):
                    context.args = [str(abs(number))]
                    await subtract_khatm(update, context)
                    return

            # Step 4: Skip processing if awaiting zekr or salavat input
            if context.user_data.get("awaiting_zekr") or context.user_data.get("awaiting_salavat"):
                return

            # Step 5: Handle commands when lock is disabled
            if not group["lock_enabled"]:
                command_found = False
                for command, info in TEXT_COMMANDS.items():
                    if command == "start":  # Skip start, already handled
                        continue
                    if text == command or raw_text in info["aliases"]:
                        command_found = True
                        if info["admin_only"] and not await is_admin(update, context):
                            await update.message.reply_text("فقط ادمین می‌تواند این دستور را اجرا کند.")
                            return
                        args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
                        context.args = args
                        await info["handler"](update, context)
                        return

                number = parse_number(raw_text)
                if number is None:
                    if topic["khatm_type"] == "ghoran":
                        return
                    await update.message.reply_text("لطفاً یک عدد معتبر یا دستور معتبر (مثل 'راهنما' یا /help) وارد کنید.")
                    return

                if number < 0 and await is_admin(update, context):
                    context.args = [str(abs(number))]
                    await subtract_khatm(update, context)
                    return

            # Step 6: Process number input
            number = parse_number(raw_text)
            if number is None:
                if topic["khatm_type"] == "ghoran":
                    return
                await update.message.reply_text("لطفاً یک عدد معتبر یا دستور معتبر (مثل 'راهنما' یا /help) وارد کنید.")
                return

            if number <= 0:
                await update.message.reply_text("عدد باید مثبت باشد.")
                return

            # Step 7: Handle Quran khatm
            if topic["khatm_type"] == "ghoran":
                max_allowed = min(topic["max_ayat"], group["max_display_verses"])
                if number > max_allowed:
                    number = max_allowed
                if number < topic["min_ayat"]:
                    await update.message.reply_text(f"تعداد آیات باید حداقل {topic['min_ayat']} باشد.")
                    return

                cursor.execute(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                range_result = cursor.fetchone()
                if not range_result:
                    await update.message.reply_text("محدوده ختم تعریف نشده است. از /set_range یا 'تنظیم محدوده' استفاده کنید.")
                    return
                start_verse_id, end_verse_id = range_result

                current_verse_id = topic["current_verse_id"]
                start_assign_id = current_verse_id
                end_assign_id = min(start_assign_id + number - 1, end_verse_id)

                if start_assign_id > end_verse_id:
                    await update.message.reply_text("ختم محدوده به پایان رسیده است. لطفاً محدوده جدید را با /set_range یا 'تنظیم محدوده' تنظیم کنید.")
                    return

                assigned_number = end_assign_id - start_assign_id + 1
                verses = quran.get_verses_in_range(start_assign_id, end_assign_id)
                if not verses:
                    await update.message.reply_text("خطا در تخصیص آیات. لطفاً دوباره تلاش کنید.")
                    return

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
                    (end_assign_id + 1, topic_id, group_id)
                )

                previous_total, new_total, completed = process_khatm_number(
                    group_id, topic_id, assigned_number, topic["khatm_type"], topic["current_total"], conn
                )

                if end_assign_id >= end_verse_id:
                    completed = True
                    cursor.execute(
                        "UPDATE topics SET is_active = 0, completion_count = completion_count + 1 WHERE topic_id = ? AND group_id = ?",
                        (topic_id, group_id)
                    )

                conn.commit()

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

                if completed:
                    completion_message = topic["completion_message"] or "تبریک! ختم قرآن کامل شد. برای ختم جدید، محدوده را با /set_range یا 'تنظیم محدوده' تنظیم کنید."
                    await update.message.reply_text(completion_message)

            # Step 8: Handle salavat or zekr khatm
            else:
                if number < group["min_number"] or number > group["max_number"]:
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
                else:
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
        logger.error(f"Error in handle_khatm_message: {e}", exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")


async def subtract_khatm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle subtraction of khatm contributions by admin (e.g., -100 or subtract 100)."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Subtract command received in non-group chat")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        logger.debug(f"Processing subtract command: group_id={group_id}, topic_id={topic_id}, text={raw_text}")

        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted subtract command: {raw_text}")
            return

        number = None
        if context.args:
            number = parse_number(context.args[0])
        if number is None:
            number = parse_number(raw_text)
        if number is None or number == 0:
            logger.debug(f"Invalid number for subtract: {raw_text}")
            await update.message.reply_text("لطفاً یک عدد معتبر وارد کنید (مثال: -100 یا subtract 100).")
            return

        number = -abs(number)
        max_subtract_ayat = 20 
        if topic and topic["khatm_type"] == "ghoran":
            number = max(number, -max_subtract_ayat) 

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_active, max_display_verses FROM groups WHERE group_id = ?",
                (group_id,)
            )
            group = cursor.fetchone()
            if not group or not group["is_active"]:
                logger.debug(f"Group not found or inactive: group_id={group_id}")
                await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
                return

            cursor.execute(
                "SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, current_verse_id, completion_count FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if not topic:
                logger.debug(f"No topic found for topic_id={topic_id}")
                await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
                return
            if not topic["is_active"]:
                logger.debug(f"Topic is not active: topic_id={topic_id}")
                await update.message.reply_text("این تاپیک ختم غیرفعال است.")
                return

            user_id = update.effective_user.id
            username = update.effective_user.username or update.effective_user.first_name
            first_name = update.effective_user.first_name

            if topic["current_total"] + number < 0:
                logger.warning(f"Cannot subtract {number}: current_total={topic['current_total']} would become negative")
                await update.message.reply_text("مقدار کسر بیشتر از مجموع فعلی است.")
                return

            cursor.execute(
                "SELECT total_salavat, total_zekr, total_ayat FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                (user_id, group_id, topic_id)
            )
            user = cursor.fetchone()
            user_total = (
                user["total_salavat"] if topic["khatm_type"] == "salavat" else
                user["total_zekr"] if topic["khatm_type"] == "zekr" else
                user["total_ayat"]
            )
            if user_total + number < 0:
                logger.warning(f"Cannot subtract {number}: user_total={user_total} would become negative")
                await update.message.reply_text("مقدار کسر بیشتر از مشارکت کاربر است.")
                return

            verses = None
            new_verse_id = None
            if topic["khatm_type"] == "ghoran":
                cursor.execute(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                range_result = cursor.fetchone()
                if not range_result:
                    logger.debug(f"No khatm range defined for topic_id={topic_id}")
                    await update.message.reply_text("محدوده ختم تعریف نشده است.")
                    return
                start_verse_id, end_verse_id = range_result

                verses_to_remove = min(abs(number), max_subtract_ayat)
                number = -verses_to_remove 
                current_verse_id = topic["current_verse_id"]
                new_verse_id = max(start_verse_id, current_verse_id - verses_to_remove)
                cursor.execute(
                    """
                    DELETE FROM contributions
                    WHERE group_id = ? AND topic_id = ? AND verse_id >= ? AND verse_id < ?
                    """,
                    (group_id, topic_id, new_verse_id, current_verse_id)
                )

                cursor.execute(
                    "UPDATE topics SET current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    (new_verse_id, topic_id, group_id)
                )

                cursor.execute(
                    "UPDATE users SET total_ayat = total_ayat + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                    (number, user_id, group_id, topic_id)
                )

                verses = quran.get_verses_in_range(new_verse_id, current_verse_id - 1) if number < 0 else []

            else: 
                if topic["khatm_type"] == "zekr":
                    cursor.execute(
                        "UPDATE users SET total_zekr = total_zekr + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                        (number, user_id, group_id, topic_id)
                    )
                else: 
                    cursor.execute(
                        "UPDATE users SET total_salavat = total_salavat + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                        (number, user_id, group_id, topic_id)
                    )
            cursor.execute(
                """
                INSERT INTO contributions (group_id, topic_id, user_id, amount, verse_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (group_id, topic_id, user_id, number, new_verse_id if topic["khatm_type"] == "ghoran" else None)
            )

            previous_total, new_total, completed = process_khatm_number(
                group_id, topic_id, number, topic["khatm_type"], topic["current_total"], conn
            )

            conn.commit()

            sepas_text = get_random_sepas(group_id, conn)
            message = format_khatm_message(
                topic["khatm_type"],
                previous_total,
                number,
                new_total,
                sepas_text,
                group_id,
                topic["zekr_text"] if topic["khatm_type"] != "ghoran" else None,
                verses=verses if topic["khatm_type"] == "ghoran" else None,
                max_display_verses=group["max_display_verses"],
                completion_count=topic["completion_count"]
            )

            await update.message.reply_text(message, parse_mode="Markdown")
            logger.info(f"Subtracted {number} from {topic['khatm_type']}: group_id={group_id}, topic_id={topic_id}, user_id={user_id}")

            if topic["khatm_type"] == "ghoran" and abs(number) > 0:
                verse_info = quran.get_verse_info(new_verse_id)
                surah_name = verse_info.get("surah_name", "نامشخص")
                verse_number = verse_info.get("verse_number", new_verse_id)
                await update.message.reply_text(
                    f"20 آیه به عقب برگشتیم، اکنون در آیه {verse_number} سوره {surah_name} هستیم.",
                    parse_mode="Markdown"
                )

    except Exception as e:
        logger.error(f"Error in subtract_khatm: {e}", exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")




async def start_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        args = context.args

        if not await is_admin(update, context):
            await update.message.reply_text("فقط ادمین می‌تواند این دستور را اجرا کند.")
            return

        if not args:
            await update.message.reply_text("لطفاً عدد شروع را وارد کنید (مثال: start from 1234 یا start from 1234 surah 2 ayah 10 برای قرآن).")
            return

        number = parse_number(args[0])
        if number is None or number < 0:
            await update.message.reply_text("لطفاً یک عدد معتبر و مثبت وارد کنید.")
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_active FROM groups WHERE group_id = ?",
                (group_id,)
            )
            group = cursor.fetchone()
            if not group or not group["is_active"]:
                await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
                return

            cursor.execute(
                "SELECT khatm_type, is_active, zekr_text, completion_count FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if not topic:
                await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
                return
            if not topic["is_active"]:
                await update.message.reply_text("این تاپیک ختم غیرفعال است.")
                return

            if topic["khatm_type"] == "ghoran":
                if len(args) < 5 or args[1].lower() != "surah" or args[3].lower() != "ayah":
                    await update.message.reply_text("برای قرآن، لطفاً سوره و آیه را مشخص کنید (مثال: start from 1234 surah 2 ayah 10).")
                    return
                surah = parse_number(args[2])
                ayah = parse_number(args[4])
                if not (1 <= surah <= 114):
                    await update.message.reply_text("شماره سوره باید بین ۱ تا ۱۱۴ باشد.")
                    return
                verse = quran.get_verse(surah, ayah)
                if not verse:
                    await update.message.reply_text(f"آیه {ayah} سوره {surah} وجود ندارد.")
                    return
                cursor.execute(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                range_result = cursor.fetchone()
                if not range_result:
                    await update.message.reply_text("محدوده ختم تعریف نشده است. از /set_range استفاده کنید.")
                    return
                start_verse_id, end_verse_id = range_result
                if not (start_verse_id <= verse['id'] <= end_verse_id):
                    await update.message.reply_text("آیه انتخاب‌شده خارج از محدوده ختم است.")
                    return
                cursor.execute(
                    "UPDATE topics SET current_total = ?, current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    (number, verse['id'], topic_id, group_id)
                )
            else:
                cursor.execute(
                    "UPDATE topics SET current_total = ? WHERE topic_id = ? AND group_id = ?",
                    (number, topic_id, group_id)
                )

            conn.commit()

            if topic["khatm_type"] == "ghoran":
                verse_info = quran.get_verse_info(verse['id'])
                surah_name = verse_info.get("surah_name", "نامشخص")
                verse_number = verse_info.get("verse_number", verse['id'])
                await update.message.reply_text(
                    f"ختم قرآن از آیه {verse_number} سوره {surah_name} با مجموع {number} شروع شد.",
                    parse_mode="Markdown"
                )
            else:
                khatm_name = "صلوات" if topic["khatm_type"] == "salavat" else topic["zekr_text"]
                await update.message.reply_text(
                    f"ختم {khatm_name} از {number} شروع شد.",
                    parse_mode="Markdown"
                )

    except Exception as e:
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")