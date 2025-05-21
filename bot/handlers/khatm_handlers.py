import logging
import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, BadRequest
from bot.database.db import fetch_one, fetch_all, write_queue
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas
from bot.handlers.admin_handlers import is_admin, TEXT_COMMANDS
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)
async def handle_khatm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle khatm-related messages for salavat, zekr, or Quran contributions."""
    try:
        logger.info("Starting handle_khatm_message: user_id=%s, chat_id=%s, message_id=%s", 
                   update.effective_user.id, update.effective_chat.id, update.message.message_id)

        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.warning("Message received in non-group chat: user_id=%s, chat_type=%s", 
                         update.effective_user.id, update.effective_chat.type if update.effective_chat else None)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        text = raw_text.lower()

        logger.info("Processing message: group_id=%s, topic_id=%s, text=%s, user=%s", 
                   group_id, topic_id, raw_text, update.effective_user.username or update.effective_user.first_name)

        # Step 1: Check if the message is a command (English or Persian)
        command_found = False
        for command, info in TEXT_COMMANDS.items():
            # Check exact match for commands that don't take arguments
            if info.get("takes_args", False):
                # Commands that accept arguments (e.g., /topic, /start_from)
                if (text == command or 
                    raw_text in info["aliases"] or 
                    text.startswith(command + " ") or 
                    any(raw_text.startswith(alias + " ") for alias in info["aliases"])):
                    
                    logger.info("Command found (with args): command=%s, text=%s, aliases=%s, user=%s", 
                              command, text, info["aliases"], update.effective_user.username or update.effective_user.first_name)
                    
                    if info["admin_only"] and not await is_admin(update, context):
                        logger.warning("Non-admin user attempted admin command: user_id=%s, command=%s, username=%s", 
                                     update.effective_user.id, command, update.effective_user.username or update.effective_user.first_name)
                        return
                    
                    # Extract arguments properly
                    if text.startswith(command + " "):
                        args = text[len(command)+1:].split()
                    elif any(raw_text.startswith(alias + " ") for alias in info["aliases"]):
                        matching_alias = next(alias for alias in info["aliases"] if raw_text.startswith(alias + " "))
                        args = raw_text[len(matching_alias)+1:].split()
                    else:
                        args = []
                    
                    context.args = args
                    logger.info("Executing command handler: command=%s, args=%s, user=%s", 
                              command, args, update.effective_user.username or update.effective_user.first_name)
                    await info["handler"](update, context)
                    command_found = True
                    return
            else:
                # Commands that must be exact (e.g., /start, شروع)
                if text == command or raw_text in info["aliases"]:
                    logger.info("Exact command found: command=%s, text=%s, aliases=%s, user=%s", 
                              command, text, info["aliases"], update.effective_user.username or update.effective_user.first_name)
                    
                    if info["admin_only"] and not await is_admin(update, context):
                        logger.warning("Non-admin user attempted admin command: user_id=%s, command=%s, username=%s", 
                                     update.effective_user.id, command, update.effective_user.username or update.effective_user.first_name)
                        return
                    
                    context.args = []
                    logger.info("Executing command handler: command=%s, args=%s, user=%s", 
                              command, [], update.effective_user.username or update.effective_user.first_name)
                    await info["handler"](update, context)
                    command_found = True
                    return

        # If no command found, continue with normal message processing
        if not command_found:
            logger.debug("No command found, processing as normal message: text=%s", raw_text)
            # Rest of the function remains unchanged...
            # Step 2: Check group and topic status
            group = await fetch_one(
                """
                SELECT is_active, lock_enabled, min_number, max_number, max_display_verses 
                FROM groups WHERE group_id = ?
                """,
                (group_id,)
            )
            if not group:
                logger.warning("Group not found: group_id=%s, user=%s", 
                             group_id, update.effective_user.username or update.effective_user.first_name)
                await update.message.reply_text("گروه در ربات ثبت نشده است. لطفاً از دستور /start یا 'شروع' استفاده کنید.")
                return
            if not group["is_active"]:
                logger.warning("Group not active: group_id=%s, user=%s", 
                             group_id, update.effective_user.username or update.effective_user.first_name)
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
                logger.warning("Topic not found: group_id=%s, topic_id=%s, user=%s", 
                             group_id, topic_id, update.effective_user.username or update.effective_user.first_name)
                await update.message.reply_text("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
                return
            if not topic["is_active"]:
                logger.warning("Topic not active: group_id=%s, topic_id=%s, user=%s", 
                             group_id, topic_id, update.effective_user.username or update.effective_user.first_name)
                await update.message.reply_text("این تاپیک ختم غیرفعال است. لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید.")
                return

            user_id = update.effective_user.id
            username = update.effective_user.username or update.effective_user.first_name
            first_name = update.effective_user.first_name

            logger.info("Processing message for active topic: group_id=%s, topic_id=%s, khatm_type=%s, user=%s, current_total=%d", 
                       group_id, topic_id, topic["khatm_type"], username, topic["current_total"])

            # Step 3: Handle awaiting states for zekr or salavat
            if context.user_data.get("awaiting_zekr") or context.user_data.get("awaiting_salavat"):
                logger.info("Found awaiting state: awaiting_zekr=%s, awaiting_salavat=%s, user=%s", 
                          bool(context.user_data.get("awaiting_zekr")), 
                          bool(context.user_data.get("awaiting_salavat")),
                          username)
                
                if await is_admin(update, context):
                    if context.user_data.get("awaiting_zekr"):
                        logger.info("Processing awaiting_zekr state for user=%s", username)
                        from bot.handlers.admin_handlers import set_zekr_text
                        await set_zekr_text(update, context)
                        return
                    elif context.user_data.get("awaiting_salavat"):
                        logger.info("Processing awaiting_salavat state for user=%s", username)
                        from bot.handlers.admin_handlers import set_salavat_count
                        await set_salavat_count(update, context)
                        return
                else:
                    logger.warning("Non-admin user attempted to set zekr/salavat: user=%s", username)

            # Step 4: Process number input for contributions
            number = parse_number(raw_text)
            if number is None:
                logger.debug("Message is not a number: text=%s, user=%s", raw_text, username)
                if topic["khatm_type"] == "ghoran":
                    return
                return

            logger.info("Parsed number from message: number=%d, user=%s", number, username)

            # Step 5: Validate number range
            if number < group["min_number"] or number > group["max_number"]:
                logger.warning("Number out of range: number=%d, min=%d, max=%d, user=%s", 
                             number, group["min_number"], group["max_number"], username)
                await update.message.reply_text(f"عدد باید بین {group['min_number']} و {group['max_number']} باشد.")
                return

            # Step 5.5: Ensure user exists in users table
            user_exists = await fetch_one(
                "SELECT 1 FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                (user_id, group_id, topic_id)
            )
            if not user_exists:
                await fetch_one(
                    "INSERT INTO users (user_id, group_id, topic_id, username, first_name, total_salavat, total_zekr, total_ayat) VALUES (?, ?, ?, ?, ?, 0, 0, 0)",
                    (user_id, group_id, topic_id, username, first_name)
                )

            # Step 6: Process contribution
            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": number,
                "khatm_type": topic["khatm_type"],
                "completed": topic["stop_number"] > 0 and topic["current_total"] + number >= topic["stop_number"],
            }

            # Add verse information for Quran khatm
            if topic["khatm_type"] == "ghoran":
                if not topic["current_verse_id"]:
                    logger.error("No current_verse_id found for Quran khatm: group_id=%s, topic_id=%s, user=%s", 
                               group_id, topic_id, username)
                    await update.message.reply_text("❌ اطلاعات آیات موجود نیست. لطفاً ابتدا محدوده ختم قرآن را تنظیم کنید.")
                    return

                # Get verse range
                range_result = await fetch_one(
                    """
                    SELECT start_verse_id, end_verse_id 
                    FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                    """,
                    (group_id, topic_id)
                )
                if not range_result:
                    logger.error("No verse range found for Quran khatm: group_id=%s, topic_id=%s, user=%s", 
                               group_id, topic_id, username)
                    await update.message.reply_text("❌ محدوده آیات تنظیم نشده است. لطفاً از دستور /set_range استفاده کنید.")
                    return

                # Calculate new verse ID
                current_verse_id = topic["current_verse_id"]
                new_verse_id = min(current_verse_id + number, range_result["end_verse_id"])
                
                request.update({
                    "verse_id": new_verse_id,
                    "current_verse_id": new_verse_id,
                    "start_verse_id": range_result["start_verse_id"],
                    "end_verse_id": range_result["end_verse_id"]
                })

                logger.info("Quran khatm contribution: current_verse_id=%d, new_verse_id=%d, user=%s", 
                          current_verse_id, new_verse_id, username)

            await write_queue.put(request)
            logger.info("Queued contribution: group_id=%s, topic_id=%s, amount=%d, khatm_type=%s, user=%s",
                       group_id, topic_id, number, topic["khatm_type"], username)

            previous_total = topic["current_total"]
            new_total = previous_total + number
            completed = request["completed"]

            sepas_text = await get_random_sepas(group_id)
            
            # Get verse information for Quran khatm
            verses = None
            if topic["khatm_type"] == "ghoran":
                quran = await QuranManager.get_instance()
                current_verse_id = topic["current_verse_id"]
                verses = []
                for i in range(min(number, 10)):  # Get up to 10 verses
                    verse = quran.get_verse_by_id(current_verse_id + i)
                    if verse:
                        verses.append(verse)
            
            message = format_khatm_message(
                topic["khatm_type"],
                previous_total,
                number,
                new_total,
                sepas_text,
                group_id,
                topic["zekr_text"],
                verses=verses,
                max_display_verses=group["max_display_verses"],
                completion_count=topic["completion_count"]
            )

            try:
                await update.message.reply_text(message, parse_mode="Markdown")
                logger.info("Sent contribution confirmation message: group_id=%s, topic_id=%s, user=%s", 
                          group_id, topic_id, username)
            except TimedOut:
                logger.warning("Timed out sending message for group_id=%s, topic_id=%s, user=%s, retrying once",
                             group_id, topic_id, username)
                await asyncio.sleep(2)
                await update.message.reply_text(message, parse_mode="Markdown")
                logger.info("Sent contribution confirmation message after retry: group_id=%s, topic_id=%s, user=%s", 
                          group_id, topic_id, username)

    except TimedOut:
        logger.error(
            "Timed out error in handle_khatm_message: group_id=%s, topic_id=%s, user_id=%s, username=%s",
            group_id, topic_id, update.effective_user.id, update.effective_user.username or update.effective_user.first_name,
            exc_info=True
        )
        return
    except Exception as e:
        logger.error(
            "Error in handle_khatm_message: %s, group_id=%s, topic_id=%s, user_id=%s, username=%s",
            e, group_id, topic_id, update.effective_user.id, update.effective_user.username or update.effective_user.first_name,
            exc_info=True
        )
        try:
            await update.message.reply_text(
                "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
            )
        except TimedOut:
            logger.warning(
                "Timed out sending error message for group_id=%s, topic_id=%s, user=%s",
                group_id, topic_id, update.effective_user.username or update.effective_user.first_name
            )


            
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
            await update.message.reply_text("❌ فقط ادمین می‌تواند مشارکت را کاهش دهد.")
            return

        # Parse number from command arguments or message text
        number = None
        if context.args:
            number = parse_number(context.args[0])
        if number is None:
            # Try to parse from raw text (handles both -50 and /subtract 50 formats)
            number = parse_number(raw_text.replace("/subtract", "").strip())
        
        if number is None:
            logger.debug("Invalid number for subtract: %s, group_id=%s", raw_text, group_id)
            await update.message.reply_text(
                "📝 لطفاً یک عدد معتبر وارد کنید.\n"
                "مثال: subtract 50\n"
                "یا: -50"
            )
            return

        # Ensure number is positive for subtraction
        number = abs(number)

        group = await fetch_one(
            """
            SELECT is_active, max_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text("❌ گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
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
            await update.message.reply_text("❌ تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text(
                "❌ این تاپیک ختم غیرفعال است.\n"
                "لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید."
            )
            return

        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name
        first_name = update.effective_user.first_name

        # Get user's current contribution
        user = await fetch_one(
            """
            SELECT total_salavat, total_zekr, total_ayat 
            FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?
            """,
            (user_id, group_id, topic_id)
        )

        # Get the appropriate total based on khatm type
        user_total = (
            user["total_salavat"] if topic["khatm_type"] == "salavat" else
            user["total_zekr"] if topic["khatm_type"] == "zekr" else
            user["total_ayat"] if topic["khatm_type"] == "ghoran" else 0
        ) if user else 0

        # Validate subtraction amount
        if user_total < number:
            logger.warning(
                "Cannot subtract %d: user_total=%d would become negative, user_id=%s",
                number, user_total, user_id
            )
            await update.message.reply_text(
                f"❌ مقدار کسر ({number}) نمی‌تواند از مشارکت فعلی ({user_total}) بیشتر باشد."
            )
            return

        verses = None
        new_verse_id = None
        if topic["khatm_type"] == "ghoran":
            max_subtract_ayat = min(20, user_total)  # Limit to user's total or 20, whichever is smaller
            number = min(number, max_subtract_ayat)
            
            range_result = await fetch_one(
                """
                SELECT start_verse_id, end_verse_id 
                FROM khatm_ranges WHERE group_id = ? AND topic_id = ?
                """,
                (group_id, topic_id)
            )
            if not range_result:
                logger.debug("No khatm range defined: topic_id=%s, group_id=%s", topic_id, group_id)
                await update.message.reply_text("❌ محدوده ختم تعریف نشده است.")
                return

            start_verse_id, end_verse_id = range_result["start_verse_id"], range_result["end_verse_id"]
            current_verse_id = topic["current_verse_id"]
            new_verse_id = max(start_verse_id, current_verse_id - number)

            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": -number,  # Negative amount for subtraction
                "verse_id": new_verse_id,
                "khatm_type": "ghoran",
                "current_verse_id": new_verse_id,
                "completed": False,
            }
        else:
            request = {
                "type": "contribution",
                "group_id": group_id,
                "topic_id": topic_id,
                "user_id": user_id,
                "amount": -number,  # Negative amount for subtraction
                "khatm_type": topic["khatm_type"],
                "completed": False,
            }

        await write_queue.put(request)
        logger.debug(
            "Queued subtract contribution: group_id=%s, topic_id=%s, amount=%d",
            group_id, topic_id, -number
        )

        previous_total = topic["current_total"]
        new_total = previous_total - number

        sepas_text = await get_random_sepas(group_id)
        message = format_khatm_message(
            topic["khatm_type"],
            previous_total,
            -number,  # Negative number for subtraction
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
            logger.warning(
                "Timed out sending subtract message for group_id=%s, topic_id=%s, retrying once",
                group_id, topic_id
            )
            await asyncio.sleep(2)
            await update.message.reply_text(message, parse_mode="Markdown")

    except TimedOut:
        logger.error(
            "Timed out error in subtract_khatm: group_id=%s, topic_id=%s, user_id=%s",
            group_id, topic_id, update.effective_user.id, exc_info=True
        )
        return
    except Exception as e:
        logger.error(
            "Error in subtract_khatm: %s, group_id=%s, topic_id=%s, user_id=%s",
            e, group_id, topic_id, update.effective_user.id, exc_info=True
        )
        try:
            await update.message.reply_text(
                "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
            )
        except TimedOut:
            logger.warning(
                "Timed out sending error message for group_id=%s, topic_id=%s",
                group_id, topic_id
            )

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
            return

        # Validate input
        if not context.args:
            logger.debug("No number provided for start_from: group_id=%s", group_id)
            await update.message.reply_text(
                "📝 لطفاً یک عدد معتبر وارد کنید.\n"
                "مثال: start_from 1000\n"
                "یا: شروع از 1000"
            )
            return

        number = parse_number(context.args[0])
        if number is None:
            logger.debug("Invalid number format for start_from: %s, group_id=%s", context.args[0], group_id)
            await update.message.reply_text("❌ لطفاً یک عدد معتبر وارد کنید.")
            return

        if number < 0:
            logger.debug("Negative number provided for start_from: %d, group_id=%s", number, group_id)
            await update.message.reply_text("❌ عدد نمی‌تواند منفی باشد.")
            return

        # Check group status
        group = await fetch_one(
            """
            SELECT is_active, max_number 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group:
            logger.debug("Group not found: group_id=%s", group_id)
            await update.message.reply_text("❌ گروه در ربات ثبت نشده است. از /start یا 'شروع' استفاده کنید.")
            return

        if not group["is_active"]:
            logger.debug("Group is inactive: group_id=%s", group_id)
            await update.message.reply_text("❌ گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return

        # Check topic status
        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, stop_number, completion_count, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("❌ تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")
            return

        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text(
                "❌ این تاپیک ختم غیرفعال است.\n"
                "لطفاً از /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای فعال‌سازی ختم استفاده کنید."
            )
            return

        # Validate number against stop_number if set
        if topic["stop_number"] and number > topic["stop_number"]:
            logger.debug("Number exceeds stop_number: number=%d, stop_number=%d", number, topic["stop_number"])
            await update.message.reply_text(
                f"❌ عدد نمی‌تواند از تعداد هدف ({topic['stop_number']}) بیشتر باشد."
            )
            return

        # Validate number against max_number if set
        if group["max_number"] and number > group["max_number"]:
            logger.debug("Number exceeds max_number: number=%d, max_number=%d", number, group["max_number"])
            await update.message.reply_text(
                f"❌ عدد نمی‌تواند از حداکثر مجاز ({group['max_number']}) بیشتر باشد."
            )
            return

        if topic["khatm_type"] == "ghoran":
            logger.debug("Start_from not supported for Quran khatm: topic_id=%s", topic_id)
            await update.message.reply_text(
                "❌ دستور /start_from برای ختم قرآن پشتیبانی نمی‌شود.\n"
                "از /set_range یا 'تنظیم محدوده' استفاده کنید."
            )
            return

        # Queue the start_from request
        request = {
            "type": "start_from",
            "group_id": group_id,
            "topic_id": topic_id,
            "number": number,
            "khatm_type": topic["khatm_type"],
            "completion_count": topic["completion_count"]
        }
        await write_queue.put(request)
        logger.info(
            "Khatm start_from queued: topic_id=%s, group_id=%s, number=%d, type=%s",
            topic_id, group_id, number, topic["khatm_type"]
        )

        # Send confirmation message
        khatm_type_display = {
            "salavat": "صلوات",
            "zekr": "ذکر",
            "ghoran": "قرآن"
        }.get(topic["khatm_type"], topic["khatm_type"])

        message = (
            f"✅ ختم {khatm_type_display} از عدد {number} شروع شد.\n"
            f"تعداد قبلی: {topic['current_total']}\n"
            f"تعداد جدید: {number}"
        )

        try:
            await update.message.reply_text(message, parse_mode="Markdown")
        except TimedOut:
            logger.warning(
                "Timed out sending start_from message for group_id=%s, topic_id=%s, retrying once",
                group_id, topic_id
            )
            await asyncio.sleep(2)
            await update.message.reply_text(message, parse_mode="Markdown")

    except TimedOut:
        logger.error(
            "Timed out error in start_from: group_id=%s, topic_id=%s, user_id=%s",
            group_id, topic_id, update.effective_user.id, exc_info=True
        )
        return
    except Exception as e:
        logger.error(
            "Error in start_from: %s, group_id=%s, topic_id=%s, user_id=%s",
            e, group_id, topic_id, update.effective_user.id, exc_info=True
        )
        try:
            await update.message.reply_text(
                "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
            )
        except TimedOut:
            logger.warning(
                "Timed out sending error message for group_id=%s, topic_id=%s",
                group_id, topic_id
            )

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