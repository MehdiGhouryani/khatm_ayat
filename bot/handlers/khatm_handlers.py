import asyncio
import datetime
import logging
import time
from datetime import timezone
from pytz import timezone
from telegram import Update, constants, ReplyParameters, InlineKeyboardButton, InlineKeyboardMarkup
from typing import List, Optional
from telegram.ext import ContextTypes
from telegram.error import TimedOut
from bot.database.db import fetch_one, write_queue, fetch_all, execute
from bot.utils.helpers import parse_number, format_khatm_message, get_random_sepas, reply_text_and_schedule_deletion, ignore_old_messages
from bot.utils.quran import QuranManager
from bot.handlers.admin_handlers import is_admin, TEXT_COMMANDS,process_doa_setup,process_doa_removal
from telegram.constants import ParseMode
logger = logging.getLogger(__name__)

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



@ignore_old_messages()
@log_function_call
async def handle_khatm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle khatm-related messages for salavat, zekr, or Quran contributions."""
    try:
        group_id = update.effective_chat.id if update.effective_chat else None
        topic_id = update.effective_message.message_thread_id if update.effective_message else None
    # --------------------------------------------
        if await process_doa_removal(update, context):
            return
        if await process_doa_setup(update, context):
            return

        is_admin_user = await is_admin(update, context)


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
        # and if the user is an admin. If so, execute and return.
        is_admin_user = await is_admin(update, context) # Check admin status once

        command_found_and_executed = False
        for command, info in TEXT_COMMANDS.items():
            is_command_match = False
            args = []

            if info.get("takes_args", False):
                if (text == command or 
                    raw_text in info["aliases"] or 
                    text.startswith(command + " ") or 
                    any(raw_text.startswith(alias + " ") for alias in info["aliases"])):
                    is_command_match = True
                    if text.startswith(command + " "):
                        args = text[len(command)+1:].split()
                    elif any(raw_text.startswith(alias + " ") for alias in info["aliases"]):
                        matching_alias = next(alias for alias in info["aliases"] if raw_text.startswith(alias + " "))
                        args = raw_text[len(matching_alias)+1:].split()
            else: # Commands that must be exact
                if text == command or raw_text in info["aliases"]:
                    is_command_match = True
            
            if is_command_match:
                logger.info("Command matched: command=%s, text='%s', user=%s, is_admin=%s", 
                            command, raw_text, update.effective_user.id, is_admin_user)
                if info["admin_only"] and not is_admin_user:
                    logger.warning("Non-admin user %s attempted admin command '%s'. Ignoring.", 
                                   update.effective_user.id, command)
                    return 

                context.args = args
                logger.info("Executing command handler: command=%s, args=%s, user=%s", 
                            command, args, update.effective_user.id)
                try:
                    await info["handler"](update, context)
                    command_found_and_executed = True 
                except Exception as e_handler:
                    logger.error(f"Error executing handler for command {command}: {e_handler}", exc_info=True)
                    try:
                        await update.message.reply_text("خطایی در اجرای دستور رخ داد.")
                    except:
                        pass 
                return 

        # Step 2: Check time-off for non-admins
        if not is_admin_user: 
            group_settings = await fetch_one(
                "SELECT time_off_start, time_off_end FROM groups WHERE group_id = ?", 
                (group_id,)
            )

            if group_settings and group_settings["time_off_start"] and group_settings["time_off_end"]:
                try:
                    tz = timezone('Asia/Tehran')
                    now_dt_tehran = datetime.datetime.now(tz) 
                    
                    start_time_str = group_settings["time_off_start"] 
                    end_time_str = group_settings["time_off_end"]     

                    start_hour, start_minute = map(int, start_time_str.split(':'))
                    end_hour, end_minute = map(int, end_time_str.split(':'))

                    time_off_start_naive = datetime.time(start_hour, start_minute)
                    time_off_end_naive = datetime.time(end_hour, end_minute)
                    
                    logger.debug(
                        f"Checking time_off for non-admin in group {group_id}: \\n"
                        f"  Current datetime (Tehran): {now_dt_tehran}\\n"
                        f"  DB start_time_str: {start_time_str}, DB end_time_str: {end_time_str}\\n"
                        f"  Parsed naive start_time: {time_off_start_naive}, Parsed naive end_time: {time_off_end_naive}"
                    )

                    is_currently_off = False
                    if time_off_start_naive <= time_off_end_naive: 
                        if time_off_start_naive <= now_dt_tehran.time() < time_off_end_naive:
                            is_currently_off = True
                    else:
                        if now_dt_tehran.time() >= time_off_start_naive or now_dt_tehran.time() < time_off_end_naive:
                            is_currently_off = True
                            logger.debug(f"  Time_off spans midnight and current time {now_dt_tehran.time()} is within {time_off_start_naive} or {time_off_end_naive} }}.")
                        else:
                            logger.debug(f"  Time_off spans midnight but current time {now_dt_tehran.time()} is NOT within {time_off_start_naive} }} or before {time_off_end_naive} .")

                    if is_currently_off:
                        logger.info(f"Group {group_id} is currently in time_off_period: {start_time_str} - {end_time_str}}}. Ignoring non-admin message from user {update.effective_user.id}.")
                        return 
                except ValueError as ve:
                    logger.error(f"Error parsing time_off times for group_id {group_id} }}: {ve}. Start: {group_settings['time_off_start']}, End: {group_settings['time_off_end']}")
                except Exception as e:
                    logger.error(f"Unexpected error during time_off check for group_id {group_id} }}: {e}", exc_info=True)

        # Step 3: Fetch group settings
        group = await fetch_one(
            """
            SELECT is_active, lock_enabled, min_number, max_number, max_display_verses, min_display_verses 
            FROM groups WHERE group_id = ?
            """,
            (group_id,)
        )
        if not group:
            logger.warning("Group not found: group_id=%s, user=%s", 
                          group_id, update.effective_user.username or update.effective_user.first_name)
            await update.message.reply_text(
                "گروه ثبت‌نشده است.\n"
                "از /start یا 'شروع' استفاده کنید."
            )
            return

        if not group["is_active"]:
            logger.warning("Group not active: group_id=%s, user=%s", 
                          group_id, update.effective_user_id.username or update.effective_user.first_name)
            await update.message.reply_text(
                "گروه فعال نیست.\n"
                "از /start یا 'شروع' استفاده کنید."
            )
            return

        # Step 4: Fetch topic details

        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, min_ayat, max_ayat, period_number, 
                   stop_number, completion_message, current_verse_id, is_active, 
                   completion_count, is_completed, min_number, max_number 
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

        if topic["is_completed"]:
            logger.warning("Topic is already completed: group_id=%s, topic_id=%s, user=%s", 
                          group_id, topic_id, update.effective_user.username or update.effective_user.first_name)
            await reply_text_and_schedule_deletion(update, context, "❌ این ختم تکمیل شده است. لطفاً نوع ختم جدید را از دکمه‌ها انتخاب کنید.")
            return

        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name
        first_name = update.effective_user.first_name

        logger.info("Processing message for active topic: group_id=%s, topic_id=%s, khatm_type=%s, user=%s, current_total=%d", 
                   group_id, topic_id, topic["khatm_type"], username, topic["current_total"])


        if group["lock_enabled"] and not is_admin_user:  

            if parse_number(raw_text) is None:
                logger.info(f"Lock mode ON for group {group_id}. Non-numeric message '{raw_text}' from non-admin user {update.effective_user.username or update.effective_user.first_name} will be deleted.")
                try:
                    await update.message.delete()
                    
                except Exception as e_del:
                    logger.error(f"Failed to delete non-numeric message in lock mode for group {group_id}: {e_del}")
                return 
        # Step 5: Handle awaiting states for zekr


        # Step 6: Process number input for contributions
        number = parse_number(raw_text)
        if number is None:
            logger.debug("Message is not a number: text=%s, user=%s", raw_text, username)
            if topic["khatm_type"] == "ghoran":
                logger.info("Informed user about numeric input for Quran khatm: group_id=%s, user=%s", group_id, username)
                return
            return
        amount = number
        logger.info("Parsed number from message: number=%d, user=%s", number, username)

# Step 7: Validate number range
        is_admin_user = await is_admin(update, context)
        
        in_topic_context = bool(update.message.message_thread_id)

        if number < 0 and is_admin_user:
            pass
        elif topic["khatm_type"] == "ghoran": #
            min_verses = group.get("min_display_verses", 1) #
            if number < min_verses: #
                logger.warning("Number of verses below minimum (Quran): number=%d, min=%d, user=%s",
                              number, min_verses, username)
                await update.message.reply_text(f"تعداد آیات باید حداقل {min_verses} باشد.")
                return
            
        elif topic["khatm_type"] == "salavat":
            min_limit_to_apply = 0
            max_limit_to_apply = float('inf')
            limit_source_description = "گروه"

            if in_topic_context and topic: 
                min_limit_to_apply = topic.get("min_ayat", 1) #
                max_limit_to_apply = topic.get("max_ayat", 100) #
                limit_source_description = f"تاپیک (min_ayat: {min_limit_to_apply}, max_ayat: {max_limit_to_apply})"
                logger.info(f"Using TOPIC limits for salavat/zekr in topic {topic_id}: min={min_limit_to_apply}, max={max_limit_to_apply}")
            elif group: 
                min_limit_to_apply = group.get("min_number", 0) #
                max_limit_to_apply = group.get("max_number", 100000000000) #
                limit_source_description = f"گروه (min_number: {min_limit_to_apply}, max_number: {max_limit_to_apply})"
                logger.info(f"Using GROUP limits for salavat/zekr in group {group_id}: min={min_limit_to_apply}, max={max_limit_to_apply}")
            else:
                logger.error("Could not determine limits: group or topic info missing.")
                await update.message.reply_text("خطا در تعیین محدودیت‌ها.")
                return

            if min_limit_to_apply > 0 and number < min_limit_to_apply: #
                logger.warning(f"Number {number} from user {username} is less than {limit_source_description} min_limit {min_limit_to_apply}")
                await update.message.reply_text(f"عدد باید حداقل {min_limit_to_apply} باشد.")
                return

            if not is_admin_user: #
                if max_limit_to_apply > 0 and max_limit_to_apply != float('inf') and number > max_limit_to_apply: #
                    logger.warning(f"Number {number} from user {username} exceeds {limit_source_description} max_limit {max_limit_to_apply} for non-admin.")
                    await update.message.reply_text(f"عدد نمی‌تواند بیشتر از {max_limit_to_apply} باشد.")
                    return
        

        elif topic["khatm_type"] == "zekr":
            min_limit = topic["min_number"] if topic["min_number"] is not None else group.get("min_number", 0)
            max_limit = topic["max_number"] if topic["max_number"] and topic["max_number"] > 0 else group.get("max_number", 1000000000)

            if not (min_limit <= amount <= max_limit):
                logger.warning("Contribution amount %s out of range (%s-%s): group_id=%s, user_id=%s",
                               amount, min_limit, max_limit, group_id, user_id)
                msg = f"عدد ارسالی باید بین {min_limit} و {max_limit} باشد."
                await reply_text_and_schedule_deletion(update, context, msg)
                return
            
            logger.info("Handling zekr contribution, fetching zekr list: group_id=%s, topic_id=%s", group_id, topic_id)
            zekrs = await fetch_all(
                "SELECT id, zekr_text FROM topic_zekrs WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )

            if not zekrs:
                logger.warning("Zekr contribution received, but no zekr items are defined: group_id=%s, topic_id=%s",
                               group_id, topic_id)
                await reply_text_and_schedule_deletion(update, context, "ختم ذکر فعال است اما هیچ متنی برای آن تعریف نشده. لطفاً ادمین را مطلع کنید.")
                return

            user_msg_id = update.message.message_id
            if 'pending_zekr' not in context.chat_data:
                context.chat_data['pending_zekr'] = {}
            
            context.chat_data['pending_zekr'][user_msg_id] = {
                "user_id": user_id,
                "amount": amount,
                "timestamp": time.time(),
                "group_id": group_id,
                "topic_id": topic_id,
                "username": username,
                "first_name": first_name
            }
            logger.info("Stored pending zekr: msg_id=%s, user_id=%s, amount=%s", user_msg_id, user_id, amount)

            keyboard = []
            row = []
            for zekr in zekrs:
                if zekr and zekr.get('zekr_text'):
                    callback_data = f"zekr_sel_{user_msg_id}_{zekr['id']}"
                    row.append(InlineKeyboardButton(zekr['zekr_text'], callback_data=callback_data))
                
                # وقتی دو تا شد، اضافه کن به کیبورد و ردیف را خالی کن
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            
            # اگر دکمه‌ای باقی مانده (تعداد فرد)، آن را هم اضافه کن
            if row:
                keyboard.append(row)

            # دکمه لغو در سطر آخر
            keyboard.append([InlineKeyboardButton("❌ لغو", callback_data=f"zekr_cancel_{user_msg_id}")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
            	"ذکر شما برای کدام مورد ثبت شود؟",
            	reply_markup=reply_markup,
            	reply_parameters=ReplyParameters(message_id=user_msg_id)
            )
            return
        # ---------------------------------------------------------------------
        # بخش جدید: مدیریت ادعیه و زیارات (نمایش دکمه‌های دو ستونه)
        # ---------------------------------------------------------------------
        elif topic["khatm_type"] == "doa":
            # 1. خواندن لیست آیتم‌ها از دیتابیس
            items = await fetch_all(
                "SELECT id, title, category FROM doa_items WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            
            if not items:
                await reply_text_and_schedule_deletion(update, context, "❌ هنوز هیچ دعا یا زیارتی برای این تاپیک تعریف نشده است.")
                return

            # 2. ذخیره موقت اطلاعات (عدد ارسالی کاربر)
            user_msg_id = update.message.message_id
            if 'pending_doa' not in context.chat_data:
                context.chat_data['pending_doa'] = {}
                
            context.chat_data['pending_doa'][user_msg_id] = {
                "user_id": user_id,
                "amount": amount, # عددی که کاربر فرستاده
                "username": username,
                "first_name": first_name
            }

            # 3. ساخت کیبورد دو ستونه (زیارت: چپ | دعا: راست)
            ziyarats = [x for x in items if x['category'] == 'ziyarat']
            duas = [x for x in items if x['category'] == 'doa']
            
            keyboard = []
            max_len = max(len(ziyarats), len(duas))
            
            for i in range(max_len):
                row = []
                
                # --- ستون چپ: زیارت ---
                if i < len(ziyarats):
                    z = ziyarats[i]
                    # فرمت کال‌بک: doa_sel_شناسه پیام_شناسه آیتم
                    row.append(InlineKeyboardButton(f"🕌 {z['title']}", callback_data=f"doa_sel_{user_msg_id}_{z['id']}"))
                elif i < len(duas): 
                    # اگر زیارت تمام شده ولی دعا مانده، برای حفظ نظم ظاهری
                    pass 

                # --- ستون راست: دعا ---
                if i < len(duas):
                    d = duas[i]
                    row.append(InlineKeyboardButton(f"🤲 {d['title']}", callback_data=f"doa_sel_{user_msg_id}_{d['id']}"))
                
                keyboard.append(row)

            # دکمه لغو در پایین
            keyboard.append([InlineKeyboardButton("❌ لغو", callback_data=f"doa_cancel_{user_msg_id}")])
            
            # ارسال پیام پرسشی به کاربر
            await update.message.reply_text(
                f"شما عدد **{amount}** را وارد کردید.\nاین تعداد برای کدام مورد ثبت شود؟ 👇",
                reply_markup=InlineKeyboardMarkup(keyboard),
                reply_to_message_id=user_msg_id,
                parse_mode=ParseMode.MARKDOWN
            )
            return # خروج از تابع (تا پیام تایید پیش‌فرض ارسال نشود)
        # ---------------------------------------------------------------------

        elif topic["khatm_type"] not in ["ghoran", "salavat", "zekr"]: #
            group_min_number = group.get("min_number", 0) #
            group_max_number = group.get("max_number", 100000000000) #
            if (group_min_number > 0 and number < group_min_number) or \
               (group_max_number > 0 and group_max_number != float('inf') and number > group_max_number): #
                logger.warning(f"Number {number} from user {username} for khatm_type {topic['khatm_type']} is out of group range (min: {group_min_number}, max: {group_max_number})")
                await update.message.reply_text(f"عدد باید بین {group_min_number} و {group_max_number} باشد.")
                return
            
            
        # Step 8: Ensure user exists in users table
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

        # Step 9: Process contribution
        request = {
            "type": "contribution",
            "group_id": group_id,
            "topic_id": topic_id,
            "user_id": user_id,
            "amount": number,
            "khatm_type": topic["khatm_type"],
            "username": username,
        }
        logger.debug("Initial contribution request: %s", request)

        current_topic_total_before_contribution = topic["current_total"] or 0

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

            current_db_verse_id = topic["current_verse_id"]  # Verse ID before this contribution
            
            # Check if already at or beyond end_verse_id
            if current_db_verse_id >= range_result["end_verse_id"]:
                logger.warning("Quran khatm already at or beyond end verse: group_id=%s, topic_id=%s, current_verse_id=%d, end_verse_id=%d",
                              group_id, topic_id, current_db_verse_id, range_result["end_verse_id"])
                await reply_text_and_schedule_deletion(update, context, "❌ ختم قرآن تکمیل شده است. لطفاً نوع ختم جدید را از دکمه‌ها انتخاب کنید.")
                return

            # Number of verses to actually display and advance the main khatm by
            if number < 0:
                # For negative numbers, use the actual number for display and advancement
                displayed_amount = number
            else:
                displayed_amount = min(number, group["max_display_verses"])
            request["displayed_amount"] = displayed_amount  # For db handler

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
                "verse_id": topic_verse_id_for_db_update,  # ID of the last verse effectively read for topic progress
                "current_verse_id": topic_verse_id_for_db_update,  # This is what will be stored in topics.current_verse_id
                "start_verse_id": range_result["start_verse_id"],  # For reference in queue processor if needed
                "end_verse_id": range_result["end_verse_id"]  # For reference
            })

            # Check if khatm is completed and not already marked as completed
            if is_quran_khatm_completed:
                topic_completed = await fetch_one(
                    "SELECT is_completed FROM topics WHERE group_id = ? AND topic_id = ?",
                    (group_id, topic_id)
                )
                if topic_completed and topic_completed["is_completed"] == 0:
                    request["send_completion"] = True
                    request["bot"] = context.bot
                    request["chat_id"] = group_id
                    request["thread_id"] = topic_id if topic_id != group_id else None
                    request["current_total"] = current_topic_total_before_contribution + displayed_amount
                    request["khatm_type_display"] = "قرآن"

            logger.info("Quran khatm request update: to_store_topic_current_verse_id=%d, completed=%s, displayed_amount=%d, user_amount=%d",
                        topic_verse_id_for_db_update, is_quran_khatm_completed, displayed_amount, number)
        else: 
            if number < 0:
                request["completed"] = False
            else:
                request["completed"] = topic["stop_number"] > 0 and (current_topic_total_before_contribution + number >= topic["stop_number"])
            
                if request["completed"]:
                    topic_completed = await fetch_one(
                        "SELECT is_completed FROM topics WHERE group_id = ? AND topic_id = ?",
                        (group_id, topic_id)
                    )
                    if topic_completed and topic_completed["is_completed"] == 0:
                        request["send_completion"] = True
                        request["bot"] = context.bot
                        request["chat_id"] = group_id
                        request["thread_id"] = topic_id if topic_id != group_id else None
                        request["current_total"] = current_topic_total_before_contribution + number
                        request["khatm_type_display"] = "صلوات" if topic["khatm_type"] == "salavat" else "ذکر"
            request["displayed_amount"] = number

        await write_queue.put(request)
        logger.info("Queued contribution: %s", request)
        # --- شروع کدهای جدید برای ادعیه ---
        if topic["khatm_type"] == "doa":
            # 1. خواندن اطلاعات دعا (لینک و نام)
            doa_info = await fetch_one(
                "SELECT title, link FROM topic_doas WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            # اگر پیدا نشد، از نام تاپیک استفاده کن
            title = doa_info['title'] if doa_info else (topic['name'] or "دعا")
            link = doa_info['link'] if doa_info else "https://t.me/"
            
            # 2. ساخت لینک
            link_text = f"🔗 <a href='{link}'>برای مشاهده متن {title} اینجا کلیک کنید</a>"
            sepas = await get_random_sepas(group_id)
            new_total = (topic["current_total"] or 0) + number
            
            # 3. متن پیام نهایی
            response_text = (
                f"✅ <b>{number}</b> بار <b>{title}</b> ثبت شد!\n"
                f"📊  کل: <b>{new_total:,}</b>\n"
                "➖➖➖➖➖➖➖➖\n"
                f"{link_text}\n"
                "➖➖➖➖➖➖➖➖\n"
                f"🌱 <i>{sepas}</i>"
            )
            
            # 4. ارسال و خروج (تا بقیه کدهای پایین اجرا نشوند)
            await reply_text_and_schedule_deletion(
                update, 
                context, 
                response_text, 
                parse_mode=ParseMode.HTML
            )
            return
    # --- پایان کدهای جدید ---

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
        
        new_total_for_display = current_topic_total_before_contribution
        if topic["khatm_type"] == "ghoran":
            new_total_for_display += displayed_amount
        else:
            new_total_for_display += number
        formatted_data = await format_khatm_message(
            khatm_type=topic["khatm_type"],
            previous_total=current_topic_total_before_contribution,
            amount=number,
            new_total=new_total_for_display,
            sepas_text=sepas_text,
            group_id=group_id,
            zekr_text=None,
            verses=verses_for_display,
            max_display_verses=group["max_display_verses"],
            completion_count=topic["completion_count"]
        )
        logger.debug("Formatted khatm message for user - expecting tuple now")

        messages_to_send: List[str]
        persian_audio_reply_params: Optional[ReplyParameters] = None

        if isinstance(formatted_data, tuple) and len(formatted_data) == 2:
            messages_to_send, persian_audio_reply_params = formatted_data
        elif isinstance(formatted_data, list):
            messages_to_send = formatted_data
        else:
            logger.error(f"Unexpected output from format_khatm_message: {type(formatted_data)}. Expected Tuple or List.")
            messages_to_send = ["خطا در پردازش پیام ختم. لطفاً به ادمین اطلاع دهید."]
            
        try:
            for idx, msg_part in enumerate(messages_to_send):
                current_reply_params_for_this_part: Optional[ReplyParameters] = None
                
                if idx == 0 and topic["khatm_type"] == "ghoran" and persian_audio_reply_params:
                    current_reply_params_for_this_part = persian_audio_reply_params

                await reply_text_and_schedule_deletion(
                    update,
                    context,
                    msg_part,
                    reply_parameters=current_reply_params_for_this_part,
                    parse_mode=ParseMode.HTML
                )
                if idx < len(messages_to_send) - 1:
                    await asyncio.sleep(0.5)
            
            logger.info("Sent contribution confirmation message: group_id=%s, topic_id=%s, user=%s", 
                      group_id, topic_id, username)
        except TimedOut:
            logger.warning(
                "Timed out sending message for group_id=%s, topic_id=%s, retrying once",
                group_id, topic_id
            )
            await asyncio.sleep(2)
            
            first_msg_text = ""
            reply_params_for_retry: Optional[ReplyParameters] = None

            if messages_to_send: # Ensure messages_to_send is not empty
                first_msg_text = messages_to_send[0]
                if topic["khatm_type"] == "ghoran" and persian_audio_reply_params:
                     reply_params_for_retry = persian_audio_reply_params
            
            if first_msg_text: # Ensure there is a message to send
                await reply_text_and_schedule_deletion(
                    update, 
                    context, 
                    first_msg_text, 
                    reply_parameters=reply_params_for_retry,
                    parse_mode=ParseMode.HTML
                )
            
            if len(messages_to_send) > 1:
                for idx_retry, msg_part_retry in enumerate(messages_to_send[1:], 1):
                    try:
                        await reply_text_and_schedule_deletion(
                            update, 
                            context, 
                            msg_part_retry, 
                            parse_mode=ParseMode.HTML
                        )
                        await asyncio.sleep(0.5)
                    except TimedOut:
                        logger.warning("Timed out sending message part %d during retry for group_id=%s, topic_id=%s",
                                     idx_retry, group_id, topic_id) # Original idx was based on the full list, this is now based on the remainder
            logger.info("Attempted to send contribution confirmation message after initial timeout: group_id=%s, topic_id=%s, user=%s", 
                      group_id, topic_id, username)

    except TimedOut:
        logger.error(
            "Outer Timed out error in handle_khatm_message: group_id=%s, topic_id=%s, user_id=%s, username=%s",
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
            await reply_text_and_schedule_deletion(update, context, 
                "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
            )
        except TimedOut:
            logger.warning(
                "Timed out sending error message for group_id=%s, topic_id=%s",
                group_id, topic_id
            )    






@ignore_old_messages()
@log_function_call
async def subtract_khatm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle subtraction of khatm contributions by admin."""
    try:
        # 1. بررسی‌های اولیه: آیا در گروه هستیم؟
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        
        # 2. بررسی دسترسی ادمین
        if not await is_admin(update, context):
            await update.message.reply_text("❌ فقط ادمین می‌تواند مشارکت را کاهش دهد.")
            return

        # 3. دریافت عدد از پیام (مثلاً -1 یا subtract 1)
        raw_text = update.message.text.strip()
        number = None
        
        # تلاش برای خواندن عدد از آرگومان‌ها
        if context.args:
            number = parse_number(context.args[0])
        
        # اگر عدد پیدا نشد، تلاش برای خواندن از کل متن
        if number is None:
            clean_text = raw_text.replace("/subtract", "").replace("کاهش", "").strip()
            number = parse_number(clean_text)
        
        if number is None:
            await update.message.reply_text("📝 لطفاً یک عدد معتبر وارد کنید (مثال: -1).")
            return

        # برای محاسبات، قدر مطلق (مثبت) عدد را می‌گیریم
        number = abs(number)

        # 4. دریافت اطلاعات تاپیک از دیتابیس
        topic = await fetch_one(
            """
            SELECT khatm_type, current_total, zekr_text, min_ayat, max_ayat, 
                   current_verse_id, completion_count, is_active
            FROM topics WHERE topic_id = ? AND group_id = ?
            """,
            (topic_id, group_id)
        )

        if not topic or not topic["is_active"]:
            await update.message.reply_text("❌ تاپیک فعال یافت نشد.")
            return

        # =====================================================================
        # 🔴 بخش ویژه برای ادعیه و زیارات (DOA)
        # اینجا مسیر را کاملاً جدا می‌کنیم تا قاطی نکند
        # =====================================================================
        if topic["khatm_type"] == "doa":
            # الف) دریافت لیست دعاها
            items = await fetch_all(
                "SELECT id, title, category FROM doa_items WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            
            if not items:
                await update.message.reply_text("❌ هیچ دعایی در این تاپیک تعریف نشده است.")
                return

            # ب) ذخیره اطلاعات در حافظه (amount را منفی ذخیره می‌کنیم)
            user_msg_id = update.message.message_id
            if 'pending_doa' not in context.chat_data:
                context.chat_data['pending_doa'] = {}
                
            context.chat_data['pending_doa'][user_msg_id] = {
                "user_id": update.effective_user.id,
                "amount": -number,  # <--- نکته کلیدی: عدد منفی ذخیره می‌شود
                "username": update.effective_user.username,
                "first_name": update.effective_user.first_name,
                "is_subtraction": True # برای اطمینان بیشتر
            }

            # ج) ساخت دکمه‌های شیشه‌ای (دو ستونه: زیارت | دعا)
            ziyarats = [i for i in items if i['category'] == 'ziyarat']
            duas = [i for i in items if i['category'] == 'doa']
            
            keyboard = []
            max_len = max(len(ziyarats), len(duas))
            
            for i in range(max_len):
                row = []
                # ستون چپ: زیارت
                if i < len(ziyarats):
                    item = ziyarats[i]
                    cb_data = f"doa_sel_{user_msg_id}_{item['id']}"
                    row.append(InlineKeyboardButton(f"🕌 {item['title']}", callback_data=cb_data))
                else:
                    # دکمه خالی برای حفظ چیدمان
                    row.append(InlineKeyboardButton(" ", callback_data="noop"))
                
                # ستون راست: دعا
                if i < len(duas):
                    item = duas[i]
                    cb_data = f"doa_sel_{user_msg_id}_{item['id']}"
                    row.append(InlineKeyboardButton(f"🤲 {item['title']}", callback_data=cb_data))
                else:
                    row.append(InlineKeyboardButton(" ", callback_data="noop"))
                
                keyboard.append(row)

            # دکمه لغو
            keyboard.append([InlineKeyboardButton("❌ لغو", callback_data=f"doa_cancel_{user_msg_id}")])
            
            # د) ارسال پیام و خروج
            await update.message.reply_text(
                f"🔻 درخواست کسر **{number}** عدد.\n"
                "لطفاً انتخاب کنید از کدام مورد کم شود:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return  # <--- خروج اجباری: اجازه نمی‌دهیم کدهای پایین اجرا شوند
        
        # =====================================================================
        # پایان بخش ویژه ادعیه
        # =====================================================================


        # 5. منطق عادی برای سایر ختم‌ها (صلوات، ذکر، قرآن)
        # (این بخش فقط زمانی اجرا می‌شود که نوع ختم "doa" نباشد)
        
        user_id = update.effective_user.id
        
        # بررسی موجودی کاربر
        user = await fetch_one(
            """
            SELECT total_salavat, total_zekr, total_ayat 
            FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?
            """,
            (user_id, group_id, topic_id)
        )
        
        user_total = 0
        if user:
            if topic["khatm_type"] == "salavat": user_total = user["total_salavat"]
            elif topic["khatm_type"] == "zekr": user_total = user["total_zekr"]
            elif topic["khatm_type"] == "ghoran": user_total = user["total_ayat"]

        # اگر موجودی کمتر از مقدار کسر بود، خطا بده
        if user_total < number:
            await update.message.reply_text(f"❌ موجودی شما ({user_total}) کمتر از مقدار کسر ({number}) است.")
            return

        # ثبت تراکنش کسر در دیتابیس
        request = {
            "type": "contribution",
            "group_id": group_id,
            "topic_id": topic_id,
            "user_id": user_id,
            "amount": -number, # کسر مقدار
            "khatm_type": topic["khatm_type"],
            "completed": False,
        }
        
        if topic["khatm_type"] == "ghoran":
             request["verse_id"] = topic["current_verse_id"]
             request["current_verse_id"] = topic["current_verse_id"]

        await write_queue.put(request)

        # نمایش پیام نتیجه
        new_total = topic["current_total"] - number
        sepas_text = await get_random_sepas(group_id)
        
        # دریافت تنظیمات گروه برای نمایش
        group = await fetch_one("SELECT max_display_verses FROM groups WHERE group_id = ?", (group_id,))
        max_display = group["max_display_verses"] if group else 10

        msg = await format_khatm_message(
            topic["khatm_type"],
            topic["current_total"],
            -number,
            new_total,
            sepas_text,
            group_id,
            None,  # zekr_text دیگر در جدول topics وجود ندارد
            max_display_verses=max_display,
            completion_count=topic["completion_count"]
        )

        await reply_text_and_schedule_deletion(update, context, msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Error in subtract_khatm: {e}", exc_info=True)
        # خطا را به کاربر نشان نده که گیج شود، فقط لاگ کن و یک پیام ساده بده
        try:
            await update.message.reply_text("❌ خطا در انجام عملیات.")
        except:
            pass







@ignore_old_messages()
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
            await reply_text_and_schedule_deletion(update, context, message, parse_mode=ParseMode.HTML)
        except TimedOut:
            logger.warning(
                "Timed out sending start_from message for group_id=%s, topic_id=%s, retrying once",
                group_id, topic_id
            )
            await asyncio.sleep(2)
            await reply_text_and_schedule_deletion(update, context, message, parse_mode=ParseMode.HTML)

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
            await reply_text_and_schedule_deletion(update, context, 
                "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
            )
        except TimedOut:
            logger.warning(
                "Timed out sending error message for group_id=%s, topic_id=%s",
                group_id, topic_id
            )

@ignore_old_messages()
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
            await reply_text_and_schedule_deletion(update, context, "هیچ ختمی برای این گروه/تاپیک تعریف نشده است.")
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
            f"تعداد هدف: {stop_number}"
        )
        logger.debug("Prepared status message: active=%s, current_total=%s, stop_number=%s",
                    is_active, current_total, stop_number)

        try:
            await reply_text_and_schedule_deletion(update, context, status)
            logger.info("Sent khatm status message: group_id=%s, topic_id=%s, type=%s",
                       group_id, topic_id, khatm_type)
        except TimedOut:
            logger.warning("Timed out sending khatm_status message for group_id=%s, topic_id=%s, retrying once",
                          group_id, topic_id)
            await asyncio.sleep(2)
            await reply_text_and_schedule_deletion(update, context, status)
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
            await reply_text_and_schedule_deletion(update, context, "خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        except TimedOut:
            logger.warning("Timed out sending error message for group_id=%s, topic_id=%s",
                         group_id, topic_id)





@log_function_call
async def handle_zekr_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the callback query for selecting a zekr type."""
    try:
        query = update.callback_query
        await query.answer()
        
        data = query.data
        user_id = query.from_user.id
        
        # فرمت دیتا: zekr_sel_{user_msg_id}_{zekr_id} یا zekr_cancel_{user_msg_id}
        parts = data.split("_")
        if len(parts) < 3:
            logger.warning("Invalid callback data format: %s", data)
            return

        action = parts[1] # sel یا cancel
        user_msg_id = int(parts[2])
        
        logger.info("Processing zekr selection: action=%s, user_msg_id=%s, user_id=%s", action, user_msg_id, user_id)

        # بازیابی اطلاعات موقت
        pending_data = context.chat_data.get('pending_zekr', {}).get(user_msg_id)

        if not pending_data:
            try:
                await query.edit_message_text("❌ این درخواست منقضی شده است. لطفاً دوباره عدد را ارسال کنید.")
            except Exception:
                await query.message.delete()
            return

        # کنترل دسترسی: فقط شخصی که عدد را فرستاده می‌تواند انتخاب کند
        if user_id != pending_data["user_id"]:
            await query.answer("⛔ این دکمه مربوط به درخواست شما نیست.", show_alert=True)
            return

        if action == "cancel":
            # حذف اطلاعات موقت و پیام
            if 'pending_zekr' in context.chat_data and user_msg_id in context.chat_data['pending_zekr']:
                del context.chat_data['pending_zekr'][user_msg_id]
            await query.message.delete()
            return

        if action == "sel":
            zekr_id = int(parts[3])
            amount = pending_data["amount"]
            group_id = pending_data["group_id"]
            topic_id = pending_data["topic_id"]
            username = pending_data["username"]
            first_name = pending_data["first_name"]

            # ایجاد درخواست برای دیتابیس
            request = {
                "type": "submit_zekr_contribution",  # نوع جدید درخواست برای db.py
                "user_id": user_id,
                "group_id": group_id,
                "topic_id": topic_id,
                "zekr_id": zekr_id,
                "amount": amount,
                "username": username,
                "first_name": first_name,
                # اطلاعات برای ارسال پیام تایید در db.py
                "bot": context.bot,
                "chat_id": group_id,
                "thread_id": topic_id if topic_id != group_id else None
            }

            await write_queue.put(request)
            logger.info("Queued zekr contribution: user_id=%s, zekr_id=%s, amount=%s", user_id, zekr_id, amount)

            # پاکسازی
            if 'pending_zekr' in context.chat_data and user_msg_id in context.chat_data['pending_zekr']:
                del context.chat_data['pending_zekr'][user_msg_id]
            
            # حذف پیام دکمه‌ها
            await query.message.delete()

    except Exception as e:
        logger.error("Error in handle_zekr_selection: %s", e, exc_info=True)
        if query and query.message:
            try:
                await query.edit_message_text("خطایی رخ داد.")
            except Exception:
                pass




# -------------------------------------------------------------------------
# هندلر پردازش کلیک روی دکمه‌های ادعیه و زیارات
# -------------------------------------------------------------------------

@log_function_call
async def handle_doa_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data 
    parts = data.split('_')
    
    if len(parts) < 3:
        return

    action = parts[1] 
    msg_id = int(parts[2])
    
    # --- حالت لغو ---
    if action == 'cancel':
        if 'pending_doa' in context.chat_data:
            context.chat_data['pending_doa'].pop(msg_id, None)
        await query.message.delete()
        return

    # --- حالت انتخاب آیتم ---
    if len(parts) < 4:
        return
    item_id = int(parts[3])
    
    pending_data = context.chat_data.get('pending_doa', {}).get(msg_id)
    
    if not pending_data:
        try:
            await query.message.edit_text("❌ زمان انتخاب منقضی شده یا اطلاعات یافت نشد.")
        except:
            await query.message.delete()
        return

    if query.from_user.id != pending_data['user_id']:
        await query.answer("⛔️ این دکمه مربوط به پیام شما نیست!", show_alert=True)
        return

    amount = pending_data['amount']
    group_id = query.message.chat.id
    topic_id = query.message.message_thread_id if query.message.is_topic_message else group_id
    
    # 1. آپدیت دیتابیس
    await execute(
        "UPDATE doa_items SET current_total = current_total + ? WHERE id = ?",
        (amount, item_id)
    )
    await execute(
        "UPDATE topics SET current_total = current_total + ? WHERE group_id = ? AND topic_id = ?",
        (amount, group_id, topic_id)
    )
    
    # 2. دریافت اطلاعات جدید
    item_info = await fetch_one("SELECT title, link, current_total FROM doa_items WHERE id = ?", (item_id,))
    total_topic = await fetch_one("SELECT current_total FROM topics WHERE group_id = ? AND topic_id = ?", (group_id, topic_id))
    
    if not item_info:
        await query.message.edit_text("❌ آیتم مورد نظر یافت نشد.")
        return

    title = item_info['title']
    link = item_info['link']
    new_item_total = item_info['current_total']
    new_topic_total = total_topic['current_total'] if total_topic else 0
    
    sepas = await get_random_sepas(group_id)
    
    # --- 3. ساخت متن پیام طبق سلیقه کارفرما ---
    
    # ساخت بخش لینک
    link_section = ""
    if link:
        link_section = f"<a href='{link}'>مشاهده متن {title}</a>\n➖➖➖➖➖➖➖➖\n"
    
    # قالب نهایی (کل متن بولد شده)
    response_text = (
        f"<b>{amount} بار {title} ثبت شد!\n"
        f"آمار {title} : {new_item_total:,}\n"
        f"امار کل ختم ها : {new_topic_total:,}\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"{link_section}"
        f"{sepas} 🌱</b>"
    )
    
    # حذف دکمه‌ها
    await query.message.delete()
    
    # ارسال پیام
    await context.bot.send_message(
        chat_id=group_id,
        text=response_text,
        message_thread_id=topic_id if topic_id != group_id else None,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    
    context.chat_data['pending_doa'].pop(msg_id, None)