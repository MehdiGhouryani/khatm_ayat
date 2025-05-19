import logging
import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, BadRequest
from bot.database.db import fetch_one, fetch_all, write_queue
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas
from bot.handlers.admin_handlers import is_admin, TEXT_COMMANDS
from bot.utils.constants import quran

logger = logging.getLogger(__name__)

async def handle_khatm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle khatm-related messages for salavat, zekr, or Quran contributions."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Message received in non-group chat: user_id=%s", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        text = raw_text.lower()

        # Step 1: Check if the message is a 'start' command or its aliases
        start_command_info = TEXT_COMMANDS.get("start")
        if start_command_info and (text == "start" or raw_text in start_command_info["aliases"]):
            logger.debug("Processing 'start' command: group_id=%s, text=%s", group_id, raw_text)
            if start_command_info["admin_only"] and not await is_admin(update, context):
                await update.message.reply_text("فقط ادمین می‌تواند این دستور را اجرا کند.")
                return
            args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
            context.args = args
            await start_command_info["handler"](update, context)
            return

        # Step 2: Check group and topic status
        group = await fetch_one(
            """
            SELECT is_active, lock_enabled, min_number, max_number, max_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group:
            await update.message.reply_text("گروه در ربات ثبت نشده است. لطفاً از دستور /start یا 'شروع' استفاده کنید.")
            return
        if not group["is_active"]:
            await update.message.reply_text("ربات در این گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, period_number, 
                   stop_number, completion_message, current_verse_id, is_active, completion_count 
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
            return

        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name
        first_name = update.effective_user.first_name

        # Insert or update user information via write_queue
        await write_queue.put({
            "type": "update_user",
            "group_id": group_id,
            "topic_id": topic_id,
            "user_id": user_id,
            "username": username,
            "first_name": first_name
        })

        # Step 3: Handle commands when lock is enabled
        if group["lock_enabled"]:
            command_found = False
            for command, info in TEXT_COMMANDS.items():
                if command == "start":
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
                        except BadRequest as e:
                            logger.debug("Failed to delete message: %s", e)
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
                except BadRequest as e:
                    logger.debug("Failed to delete message: %s", e)
                return

        # Step 4: Handle awaiting states for zekr or salavat
        if context.user_data.get("awaiting_zekr") or context.user_data.get("awaiting_salavat"):
            if await is_admin(update, context):
                for command, info in TEXT_COMMANDS.items():
                    if text == command or raw_text in info["aliases"]:
                        args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
                        context.args = args
                        await info["handler"](update, context)
                        return

            if context.user_data.get("awaiting_zekr"):
                from bot.handlers.admin_handlers import set_zekr_text
                await set_zekr_text(update, context)
                return
            elif context.user_data.get("awaiting_salavat"):
                from bot.handlers.admin_handlers import set_salavat_count
                await set_salavat_count(update, context)
                return

        # Step 5: Handle commands when lock is disabled
        if not group["lock_enabled"]:
            for command, info in TEXT_COMMANDS.items():
                if command == "start":
                    continue
                if text == command or raw_text in info["aliases"]:
                    if info["admin_only"] and not await is_admin(update, context):
                        return
                    args = raw_text.split()[1:] if len(raw_text.split()) > 1 else []
                    context.args = args
                    await info["handler"](update, context)
                    return

        # Step 6: Process number input
        number = parse_number(raw_text)
        if number is None:
            if topic["khatm_type"] == "ghoran":
                return
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

            range_result = await fetch_one(
                """
                SELECT start_verse_id, end_verse_id 
                FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            if not range_result:
                await update.message.reply_text("محدوده ختم تعریف نشده است. از /set_range یا 'تنظیم محدوده' استفاده کنید.")
                return
            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]

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
                request = {
                    "type": "contribution",
                    "group_id": group_id,
                    "topic_id": topic_id,
                    "user_id": user_id,
                    "amount": 1,
                    "verse_id": verse["id"],
                    "khatm_type": "ghoran",
                    "current_verse_id": end_assign_id + 1,
                    "completed": end_assign_id >= end_verse_id,
                }
                await write_queue.put(request)
                logger.debug("Queued contribution: group_id=%s, topic_id=%s, verse_id=%d",
                            group_id, topic_id, verse["id"])

            previous_total = topic["current_total"]
            new_total = previous_total + assigned_number
            completed = end_assign_id >= end_verse_id

            sepas_text = await get_random_sepas(group_id)
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

            try:
                await update.message.reply_text(message)
            except TimedOut:
                logger.warning("Timed out sending message for group_id=%s, topic_id=%s, retrying once",
                              group_id, topic_id)
                await asyncio.sleep(2)
                await update.message.reply_text(message)

            if completed:
                completion_message = topic["completion_message"] or "تبریک! ختم قرآن کامل شد. برای ختم جدید، محدوده را با /set_range یا 'تنظیم محدوده' تنظیم کنید."
                try:
                    await update.message.reply_text(completion_message)
                except TimedOut:
                    logger.warning("Timed out sending completion message for group_id=%s, topic_id=%s",
                                  group_id, topic_id)
                    await asyncio.sleep(2)
                    await update.message.reply_text(completion_message)

        # Step 8: Handle salavat or zekr khatm
        else:
            if number < group["min_number"] or number > group["max_number"]:
                await update.message.reply_text(f"عدد باید بین {group['min_number']} و {group['max_number']} باشد.")
                return

            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": number,
                "khatm_type": topic["khatm_type"],
                "completed": topic["stop_number"] > 0 and topic["current_total"] + number >= topic["stop_number"],
            }
            await write_queue.put(request)
            logger.debug("Queued contribution: group_id=%s, topic_id=%s, amount=%d",
                        group_id, topic_id, number)

            previous_total = topic["current_total"]
            new_total = previous_total + number
            completed = request["completed"]

            sepas_text = await get_random_sepas(group_id)
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

            try:
                await update.message.reply_text(message, parse_mode="Markdown")
            except TimedOut:
                logger.warning("Timed out sending message for group_id=%s, topic_id=%s, retrying once",
                              group_id, topic_id)
                await asyncio.sleep(2)
                await update.message.reply_text(message, parse_mode="Markdown")

            if completed:
                completion_message = topic["completion_message"] or f"تبریک! ختم {topic['khatm_type']} کامل شد."
                try:
                    await update.message.reply_text(completion_message)
                except TimedOut:
                    logger.warning("Timed out sending completion message for group_id=%s, topic_id=%s",
                                  group_id, topic_id)
                    await asyncio.sleep(2)
                    await update.message.reply_text(completion_message)

    except TimedOut:
        logger.error("Timed out error in handle_khatm_message: group_id=%s, topic_id=%s, user_id=%s",
                    group_id, topic_id, update.effective_user.id, exc_info=True)
        return
    except Exception as e:
        logger.error("Error in handle_khatm_message: %s, group_id=%s, topic_id=%s, user_id=%s",
                    e, group_id, topic_id, update.effective_user.id, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                          group_id, topic_id)

async def subtract_khatm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle subtraction of khatm contributions by admin."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Subtract command in non-group chat: user_id=%s", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        logger.debug("Processing subtract command: group_id=%s, topic_id=%s, text=%s",
                   group_id, topic_id, raw_text)

        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted subtract command: %s",
                         update.effective_user.id, raw_text)
            await update.message.reply_text("فقط ادمین می‌تواند مشارکت را کاهش دهد.")
            return

        number = None
        if context.args:
            number = parse_number(context.args[0])
        if number is None:
            number = parse_number(raw_text)
        if number is None or number == 0:
            logger.debug("Invalid number for subtract: %s, group_id=%s", raw_text, group_id)
            await update.message.reply_text("لطفاً یک عدد معتبر وارد کنید (مثال: -100 یا /subtract 100).")
            return

        number = -abs(number)  # Ensure number is negative

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
            SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, 
                   current_verse_id, completion_count, is_active
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

        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name
        first_name = update.effective_user.first_name

        user = await fetch_one(
            """
            SELECT total_salavat, total_zekr, total_ayat 
            FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?
            """,
            (user_id, group_id, topic_id)
        )
        user_total = (
            user["total_salavat"] if topic["khatm_type"] == "salavat" else
            user["total_zekr"] if topic["khatm_type"] == "zekr" else
            user["total_ayat"]
        )
        if user_total + number < 0:
            logger.warning("Cannot subtract %d: user_total=%d would become negative, user_id=%s",
                         number, user_total, user_id)
            await update.message.reply_text("مقدار کسر بیشتر از مشارکت کاربر است.")
            return

        verses = None
        new_verse_id = None
        if topic["khatm_type"] == "ghoran":
            max_subtract_ayat = 20
            number = max(number, -max_subtract_ayat)
            range_result = await fetch_one(
                """
                SELECT start_verse_id, end_verse_id 
                FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            if not range_result:
                logger.debug("No khatm range defined: topic_id=%s, group_id=%s", topic_id, group_id)
                await update.message.reply_text("محدوده ختم تعریف نشده است.")
                return
            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]

            verses_to_remove = min(abs(number), max_subtract_ayat)
            number = -verses_to_remove
            current_verse_id = topic["current_verse_id"]
            new_verse_id = max(start_verse_id, current_verse_id - verses_to_remove)

            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": number,
                "verse_id": new_verse_id,
                "khatm_type": "ghoran",
                "current_verse_id": new_verse_id,
                "completed": False,
            }
            await write_queue.put(request)
            logger.debug("Queued subtract contribution: group_id=%s, topic_id=%s, amount=%d",
                        group_id, topic_id, number)

            verses = quran.get_verses_in_range(new_verse_id, current_verse_id - 1) if number < 0 else []

        else:
            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": number,
                "khatm_type": topic["khatm_type"],
                "completed": False,
            }
            await write_queue.put(request)
            logger.debug("Queued subtract contribution: group_id=%s, topic_id=%s, amount=%d",
                        group_id, topic_id, number)

        previous_total = topic["current_total"]
        new_total = previous_total + number

        sepas_text = await get_random_sepas(group_id)
        message = format_khatm_message(
            topic["khatm_type"],
            previous_total,
            number,
            new_total,
            sepas_text,
            group_id,
            topic["zekr_text"] if topic["khatm_type"] in ["zekr", "salavat"] else None,
            verses=verses,
            max_display_verses=group["max_display_verses"],
            completion_count=topic["completion_count"]
        )

        try:
            await update.message.reply_text(message, parse_mode="Markdown")
        except TimedOut:
            logger.warning("Timed out sending subtract message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(message, parse_mode="Markdown")

    except TimedOut:
        logger.error("Timed out error in subtract_khatm: group_id=%s, topic_id=%s, user_id=%s",
                    group_id, topic_id, update.effective_user.id, exc_info=True)
        return
    except Exception as e:
        logger.error("Error in subtract_khatm: %s, group_id=%s, topic_id=%s, user_id=%s",
                    e, group_id, topic_id, update.effective_user.id, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                          group_id, topic_id)

async def start_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the starting number for a khatm (admin only) using write_queue."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Start_from command in non-group chat: user_id=%s", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.debug("Processing start_from command: group_id=%s, topic_id=%s", group_id, topic_id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted start_from command", update.effective_user.id)
            await update.message.reply_text("فقط ادمین می‌تواند ختم را از عدد دلخواه شروع کند.")
            return

        if not context.args:
            logger.debug("No number provided for start_from: group_id=%s", group_id)
            await update.message.reply_text("لطفاً یک عدد معتبر وارد کنید (مثال: /start_from 1000).")
            return

        number = parse_number(context.args[0])
        if number is None or number < 0:
            logger.debug("Invalid number for start_from: %s, group_id=%s", context.args[0], group_id)
            await update.message.reply_text("لطفاً یک عدد معتبر و غیرمنفی وارد کنید (مثال: /start_from 1000).")
            return

        group = await fetch_one(
            """
            SELECT is_active FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, completion_count, is_active
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

        if topic["khatm_type"] == "ghoran":
            logger.debug("Start_from not supported for Quran khatm: topic_id=%s", topic_id)
            await update.message.reply_text("دستور /start_from برای ختم قرآن پشتیبانی نمی‌شود. از /set_range استفاده کنید.")
            return

        request = {
            "type": "start_from",
            "group_id": group_id,
            "topic_id": topic_id,
            "number": number,
            "khatm_type": topic["khatm_type"]
        }
        await write_queue.put(request)
        logger.info("Khatm start_from queued: topic_id=%s, group_id=%s, number=%d", topic_id, group_id, number)

        try:
            await update.message.reply_text(
                f"ختم {topic['khatm_type']} از عدد {number} شروع شد.",
                parse_mode="Markdown"
            )
        except TimedOut:
            logger.warning("Timed out sending start_from message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(
                f"ختم {topic['khatm_type']} از عدد {number} شروع شد.",
                parse_mode="Markdown"
            )

    except TimedOut:
        logger.error("Timed out error in start_from: group_id=%s, topic_id=%s, user_id=%s",
                    group_id, topic_id, update.effective_user.id, exc_info=True)
        return
    except Exception as e:
        logger.error("Error in start_from: %s, group_id=%s, topic_id=%s, user_id=%s",
                    e, group_id, topic_id, update.effective_user.id, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                          group_id, topic_id)

async def khatm_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current khatm status."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        topic = await fetch_one(
            """
            SELECT khatm_type, is_active, current_total, zekr_text, stop_number
            FROM topics
            WHERE group_id = ? AND topic_id = ?
            """,
            (group_id, topic_id)
        )

        if not topic:
            await update.message.reply_text("هیچ ختمی برای این گروه/تاپیک تعریف نشده است.")
            return

        khatm_type = topic["khatm_type"]
        is_active = topic["is_active"]
        current_total = topic["current_total"]
        zekr_text = topic["zekr_text"] or "ندارد"
        stop_number = topic["stop_number"] or "ندارد"

        status = (
            f"وضعیت ختم:\n"
            f"نوع: {khatm_type}\n"
            f"فعال: {'بله' if is_active else 'خیر'}\n"
            f"مقدار فعلی: {current_total}\n"
            f"متن ذکر: {zekr_text}\n"
            f"تعداد هدف: {stop_number}"
        )
        try:
            await update.message.reply_text(status)
        except TimedOut:
            logger.warning("Timed out sending khatm_status message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(status)

    except TimedOut:
        logger.error("Timed out error in khatm_status: group_id=%s, topic_id=%s, user_id=%s",
                    group_id, topic_id, update.effective_user.id, exc_info=True)
        return
    except Exception as e:
        logger.error("Error in khatm_status: group_id=%s, topic_id=%s, error=%s",
                    group_id, topic_id, e, exc_info=True)
        try:
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",group_id, topic_id)