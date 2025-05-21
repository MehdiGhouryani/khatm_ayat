import logging
import asyncio
from telegram import Update,constants
from telegram.ext import ContextTypes
from telegram.error import TimedOut, BadRequest
from bot.database.db import fetch_one, fetch_all, write_queue
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas
from bot.handlers.admin_handlers import is_admin, TEXT_COMMANDS
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

# Add debug log for tracking function entry/exit
def log_function_call(func):
    async def wrapper(*args, **kwargs):
        logger.debug(f"Entering function: {func.__name__}")
        try:
            result = await func(*args, **kwargs)
            logger.debug(f"Exiting function: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in function {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

@log_function_call
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
                await update.message.reply_text(
                    "گروه ثبت نشده.\n"
                    "از start یا 'شروع' استفاده کنید."
                )
                return

            if not group["is_active"]:
                logger.warning("Group not active: group_id=%s, user=%s", 
                              group_id, update.effective_user.username or update.effective_user.first_name)
                await update.message.reply_text(
                    "گروه فعال نیست.\n"
                    "از start یا 'شروع' استفاده کنید."
                )
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
                return

            if not topic["is_active"]:
                logger.warning("Topic not active: group_id=%s, topic_id=%s, user=%s", 
                              group_id, topic_id, update.effective_user.username or update.effective_user.first_name)

                return

            user_id = update.effective_user.id
            username = update.effective_user.username or update.effective_user.first_name
            first_name = update.effective_user.first_name

            logger.info("Processing message for active topic: group_id=%s, topic_id=%s, khatm_type=%s, user=%s, current_total=%d", 
                       group_id, topic_id, topic["khatm_type"], username, topic["current_total"])

            # Step 3: Handle awaiting states for zekr or salavat
            if context.user_data.get("awaiting_zekr"):
                logger.info("Found awaiting_zekr state: user=%s", username)
                
                if await is_admin(update, context):
                    logger.info("Processing awaiting_zekr state for user=%s", username)
                    from bot.handlers.admin_handlers import set_zekr_text
                    await set_zekr_text(update, context)
                    return
                else:
                    logger.warning("Non-admin user attempted to set zekr: user=%s", username)
                    # Clear the flag if a non-admin attempts to use it, to prevent it from blocking normal number inputs
                    context.user_data.pop("awaiting_zekr", None)
            
            # Step 4: Process number input for contributions
            number = parse_number(raw_text)
            if number is None:
                logger.debug("Message is not a number: text=%s, user=%s", raw_text, username)
                if topic["khatm_type"] == "ghoran":
                    logger.info("Informed user about numeric input for Quran khatm: group_id=%s, user=%s", group_id, username)
                    return
                return

            logger.info("Parsed number from message: number=%d, user=%s", number, username)

            # Step 5: Validate number range
            is_admin_user = await is_admin(update, context)
            
            # Allow negative numbers for admins
            if number < 0 and is_admin_user:
                # Admin can use negative numbers, proceed with the contribution
                pass
            # For non-admins or positive numbers, apply normal validations
            elif topic["khatm_type"] not in ["salavat", "zekr"]:
                if number < group["min_number"] or number > group["max_number"]:
                    logger.warning("Number out of range (non-salavat/non-zekr): number=%d, min=%d, max=%d, user=%s",
                                 number, group["min_number"], group["max_number"], username)
                    await update.message.reply_text(f"عدد باید بین {group['min_number']} و {group['max_number']} باشد.")
                    return
            elif number < group["min_number"]: # For salavat and zekr, only check min_number if it's greater than 0
                if group["min_number"] > 0: # Only enforce min_number if it's set to a positive value
                    logger.warning("Number less than min_number (salavat/zekr): number=%d, min=%d, user=%s",
                                    number, group["min_number"], username)
                    await update.message.reply_text(f"عدد باید حداقل {group['min_number']} باشد.")
                    return
            # Check max_number for salavat and zekr only if it's set and user is not admin
            elif topic["khatm_type"] in ["salavat", "zekr"] and not is_admin_user:
                if number > group["max_number"] and group["max_number"] > 0:
                    logger.warning("Number exceeds max_number (salavat/zekr): number=%d, max=%d, user=%s",
                                 number, group["max_number"], username)
                    await update.message.reply_text(f"عدد نمی‌تواند بیشتر از {group['max_number']} باشد.")
                    return

            # Step 5.5: Ensure user exists in users table
            user_exists = await fetch_one(
                "SELECT 1 FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?",
                (user_id, group_id, topic_id)
            )
            logger.debug("Checking user existence: user_id=%s, group_id=%s, topic_id=%s, exists=%s",
                        user_id, group_id, topic_id, bool(user_exists))
            
            if not user_exists:
                logger.info("Creating new user record: user_id=%s, username=%s, group_id=%s, topic_id=%s",
                          user_id, username, group_id, topic_id)
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
            }
            logger.debug("Initial contribution request: %s", request)

            current_topic_total_before_contribution = topic["current_total"] # Store for display

            if topic["khatm_type"] == "ghoran":
                logger.debug("Processing Quran contribution details: current_db_verse_id=%s", topic["current_verse_id"])
                if not topic["current_verse_id"]:
                    logger.error("No current_verse_id for Quran khatm: group_id=%s, topic_id=%s", group_id, topic_id)
                    await update.message.reply_text("❌ اطلاعات آیات موجود نیست. لطفاً ابتدا محدوده ختم قرآن را تنظیم کنید.")
                    return

                range_result = await fetch_one(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                if not range_result:
                    logger.error("No verse range for Quran khatm: group_id=%s, topic_id=%s", group_id, topic_id)
                    await update.message.reply_text("❌ محدوده آیات تنظیم نشده. از `set_range` استفاده کنید.", parse_mode=constants.ParseMode.MARKDOWN)
                    return

                current_db_verse_id = topic["current_verse_id"] # Verse ID before this contribution
                
                # Number of verses to actually display and advance the main khatm by
                if number < 0:
                    # For negative numbers, use the actual number for display and advancement
                    displayed_amount = number
                else:
                    displayed_amount = min(number, group["max_display_verses"])
                request["displayed_amount"] = displayed_amount # For db handler

                # Potential new verse_id after this contribution (based on displayed amount for topic progress)
                new_topic_verse_id = current_db_verse_id + displayed_amount
                
                # Don't allow verse_id to go below start_verse_id
                if new_topic_verse_id < range_result["start_verse_id"]:
                    new_topic_verse_id = range_result["start_verse_id"]

                is_quran_khatm_completed = (new_topic_verse_id >= range_result["end_verse_id"])
                request["completed"] = is_quran_khatm_completed
                
                # The verse_id to store in topics table for the *next* contribution (topic progress)
                topic_verse_id_for_db_update = min(new_topic_verse_id, range_result["end_verse_id"])
                
                request.update({
                    "verse_id": topic_verse_id_for_db_update, # ID of the last verse effectively read for topic progress
                    "current_verse_id": topic_verse_id_for_db_update, # This is what will be stored in topics.current_verse_id
                    "start_verse_id": range_result["start_verse_id"], # For reference in queue processor if needed
                    "end_verse_id": range_result["end_verse_id"]   # For reference
                })
                logger.info("Quran khatm request update: to_store_topic_current_verse_id=%d, completed=%s, displayed_amount=%d, user_amount=%d",
                            topic_verse_id_for_db_update, is_quran_khatm_completed, displayed_amount, number)
            else: # For salavat and zekr
                if number < 0:
                    # For negative contributions, we don't check completion
                    request["completed"] = False
                else:
                    request["completed"] = topic["stop_number"] > 0 and (current_topic_total_before_contribution + number >= topic["stop_number"])
                request["displayed_amount"] = number # For non-Quran, displayed_amount is same as amount

            await write_queue.put(request)
            logger.info("Queued contribution: %s", request)

            # Prepare data for the confirmation message
            sepas_text = await get_random_sepas(group_id)
            
            verses_for_display = []
            if topic["khatm_type"] == "ghoran":
                quran = await QuranManager.get_instance()
                # For display, we show verses starting from current_db_verse_id (before this contribution)
                # The number of verses to show is min(user_input_number, max_display_verses_setting)
                current_verse_id_for_display_fetch = current_db_verse_id # This is topic["current_verse_id"] before update
                
                if number < 0:
                    # For negative numbers, we don't display any verses
                    num_verses_to_fetch_for_display = 0
                else:
                    num_verses_to_fetch_for_display = min(displayed_amount, group["max_display_verses"])

                logger.debug(f"Verse display pre-fetch: topic_id={topic_id}, group_id={group_id}, current_verse_id_for_display_fetch={current_verse_id_for_display_fetch}, num_verses_to_fetch_for_display={num_verses_to_fetch_for_display}, user_input_number={displayed_amount}, group_max_display={group['max_display_verses']}")

                for i in range(num_verses_to_fetch_for_display):
                    verse = quran.get_verse_by_id(current_verse_id_for_display_fetch + i)
                    if verse:
                        verses_for_display.append(verse)
                    else:
                        logger.warning("Verse not found for display: id %d. Stopping verse fetch.", current_verse_id_for_display_fetch + i)
                        break
                logger.debug("Retrieved %d verses for display list", len(verses_for_display))
            
            # topic["current_total"] is the value BEFORE this contribution.
            # topic["completion_count"] is also the value BEFORE this contribution.
            
            # For Quran, new_total for display purposes should reflect topic advancement (by displayed_amount)
            # For Salavat/Zekr, new_total is based on the full number.
            new_total_for_display = current_topic_total_before_contribution
            if topic["khatm_type"] == "ghoran":
                new_total_for_display += displayed_amount
            else:
                new_total_for_display += number

            message = format_khatm_message(
                khatm_type=topic["khatm_type"],
                previous_total=current_topic_total_before_contribution,
                amount=number, # User's actual input number (e.g., 60)
                new_total=new_total_for_display, # New total reflecting topic progress
                sepas_text=sepas_text,
                group_id=group_id,
                zekr_text=topic["zekr_text"],
                verses=verses_for_display,
                max_display_verses=group["max_display_verses"], # Pass the setting to formatter
                completion_count=topic["completion_count"] # Pass current completion_count
            )
            logger.debug("Formatted khatm message for user")

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


            
@log_function_call
async def subtract_khatm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle subtraction of khatm contributions by admin."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Subtract command in non-group chat: user_id=%s", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        raw_text = update.message.text.strip()
        logger.debug("Processing subtract command: group_id=%s, topic_id=%s, text=%s, user_id=%s",
                   group_id, topic_id, raw_text, update.effective_user.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user %s attempted subtract command: %s",
                         update.effective_user.id, raw_text)
            await update.message.reply_text("❌ فقط ادمین می‌تواند مشارکت را کاهش دهد.")
            return

        # Parse number from command arguments or message text
        number = None
        if context.args:
            number = parse_number(context.args[0])
            logger.debug("Attempting to parse number from args: args=%s, result=%s", context.args[0], number)
        if number is None:
            # Try to parse from raw text (handles both -50 and /subtract 50 formats)
            number = parse_number(raw_text.replace("/subtract", "").strip())
            logger.debug("Attempting to parse number from raw text: text=%s, result=%s", 
                        raw_text.replace("/subtract", "").strip(), number)
        
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
        logger.debug("Normalized subtraction amount: %d", number)

        group = await fetch_one(
            """
            SELECT is_active, max_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        logger.debug("Retrieved group info: group_id=%s, active=%s", 
                    group_id, group["is_active"] if group else None)

        if not group or not group["is_active"]:
            logger.debug("Group not found or inactive: group_id=%s", group_id)
            await update.message.reply_text(" از `start` یا 'شروع' استفاده کنید.",parse_mode=constants.ParseMode.MARKDOWN)
            return

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, 
                   current_verse_id, completion_count, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        logger.debug("Retrieved topic info: topic_id=%s, type=%s, active=%s", 
                    topic_id, topic["khatm_type"] if topic else None, 
                    topic["is_active"] if topic else None)

        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text("❌ تاپیک ختم تنظیم نشده است. از `topic` یا 'تاپیک' استفاده کنید.",parse_mode=constants.ParseMode.MARKDOWN)
            return
        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text(
                "برای فعال‌سازی، لطفاً از دستورات `khatm_zekr`، `khatm_salavat` یا `khatm_ghoran` استفاده کنید.",
                parse_mode=constants.ParseMode.MARKDOWN
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
        logger.debug("Retrieved user contribution: user_id=%s, salavat=%s, zekr=%s, ayat=%s",
                    user_id, user["total_salavat"] if user else None,
                    user["total_zekr"] if user else None,
                    user["total_ayat"] if user else None)

        # Get the appropriate total based on khatm type
        user_total = (
            user["total_salavat"] if topic["khatm_type"] == "salavat" else
            user["total_zekr"] if topic["khatm_type"] == "zekr" else
            user["total_ayat"] if topic["khatm_type"] == "ghoran" else 0
        ) if user else 0
        logger.debug("Calculated user total for khatm_type %s: %d", topic["khatm_type"], user_total)

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

@log_function_call
async def start_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the starting number for a khatm (admin only) using write_queue."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.debug("Start_from command in non-group chat: user_id=%s", update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.debug("Processing start_from command: group_id=%s, topic_id=%s, user_id=%s", 
                    group_id, topic_id, update.effective_user.id)

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
        logger.debug("Parsed start_from number: input=%s, result=%s", context.args[0], number)
        
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
            SELECT is_active 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        logger.debug("Retrieved group info: group_id=%s, active=%s",
                    group_id, group["is_active"] if group else None)

        if not group:
            logger.debug("Group not found: group_id=%s", group_id)
            await update.message.reply_text(
                "گروه ثبت نشده.\n"
                "از start یا 'شروع' استفاده کنید."
            )
            return

        if not group["is_active"]:
            logger.debug("Group is inactive: group_id=%s", group_id)
            await update.message.reply_text(
                "گروه فعال نیست.\n"
                "از start یا 'شروع' استفاده کنید."
            )
            return

        # Check topic status
        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, stop_number, completion_count, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )
        logger.debug("Retrieved topic info: topic_id=%s, type=%s, current_total=%s, stop_number=%s",
                    topic_id, topic["khatm_type"] if topic else None,
                    topic["current_total"] if topic else None,
                    topic["stop_number"] if topic else None)

        if not topic:
            logger.debug("No topic found: topic_id=%s, group_id=%s", topic_id, group_id)
            await update.message.reply_text(
                "تاپیک ختم تنظیم نشده.\n"
                "از topic یا 'تاپیک' استفاده کنید."
            )
            return

        if not topic["is_active"]:
            logger.debug("Topic is not active: topic_id=%s", topic_id)
            await update.message.reply_text(
                "تاپیک ختم غیرفعال است.\n"
                "از khatm_zekr، khatm_salavat یا khatm_ghoran استفاده کنید."
            )
            return

        # تنها موردی که چک می‌شود، بیشتر بودن از stop_number است (اگر تعیین شده باشد)
        if topic["stop_number"] and number > topic["stop_number"]:
            logger.debug("Number exceeds stop_number: number=%d, stop_number=%d", 
                        number, topic["stop_number"])
            await update.message.reply_text(
                f"❌ عدد نمی‌تواند از تعداد هدف ({topic['stop_number']}) بیشتر باشد."
            )
            return

        # بررسی max_number حذف شده است - هیچ محدودیتی برای شروع از هر عددی وجود ندارد
        # حتی بررسی stop_number می‌تواند حذف شود اگر مطلقاً هیچ محدودیتی نمی‌خواهید

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
        logger.debug("Preparing start_from request: %s", request)
        
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
            f"تعداد جدید: {number}"
        )
        logger.debug("Prepared confirmation message for start_from")

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

@log_function_call
async def khatm_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current khatm status."""
    try:
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.debug("Processing khatm_status request: group_id=%s, topic_id=%s, user_id=%s",
                    group_id, topic_id, update.effective_user.id)

        topic = await fetch_one(
            """
            SELECT khatm_type, is_active, current_total, zekr_text, stop_number
            FROM topics
            WHERE group_id = ? AND topic_id = ?
            """,
            (group_id, topic_id)
        )
        logger.debug("Retrieved topic info: topic_id=%s, type=%s, active=%s, current_total=%s",
                    topic_id, topic["khatm_type"] if topic else None,
                    topic["is_active"] if topic else None,
                    topic["current_total"] if topic else None)

        if not topic:
            logger.debug("No topic found for khatm_status: group_id=%s, topic_id=%s",
                        group_id, topic_id)
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
        logger.debug("Prepared status message: active=%s, current_total=%s, stop_number=%s",
                    is_active, current_total, stop_number)

        try:
            await update.message.reply_text(status)
            logger.info("Sent khatm status message: group_id=%s, topic_id=%s, type=%s",
                       group_id, topic_id, khatm_type)
        except TimedOut:
            logger.warning("Timed out sending khatm_status message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await update.message.reply_text(status)
            logger.info("Sent khatm status message after retry: group_id=%s, topic_id=%s",
                       group_id, topic_id)

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
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                         group_id, topic_id)