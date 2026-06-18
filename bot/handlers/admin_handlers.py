import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from bot.database.db import fetch_one, fetch_all, execute, write_queue
from bot.utils.constants import KHATM_TYPES, DEFAULT_MAX_NUMBER
from bot.utils.helpers import parse_number, ignore_old_messages
import re
from telegram import constants
from bot.utils.quran import QuranManager
import time
from bot.utils.constants import SUPER_ADMIN_IDS
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
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing help command: user_id=%s, chat_id=%s", 
                   update.effective_user.id, update.effective_chat.id)
        help_text = """
**دستورات عمومی ربات**

فعال‌سازی و توقف ربات:
`start` - فعال کردن ربات
`stop` - غیرفعال کردن ربات

ریست آمار:
`reset zekr` - ریست آمار صلوات و ذکر
`reset kol` - ریست تمام آمار و اعداد
`start from 1234` - شروع ختم از عدد دلخواه

تنظیم تعداد و شروع ختم:
`number 14000` - تنظیم تعداد هدف صلوات/ذکر
`khatm salavat` - شروع ختم صلوات
`khatm ghoran` - شروع ختم قرآن
`set range` - تنظیم محدوده ختم قرآن 
`set completion message` - تنظیم پیام پایان ختم

مدیریت ختم ذکر:
`khatm zekr` - فعال‌سازی ختم ذکر (آماده‌سازی برای افزودن ذکرها)
`add_zekr سبحان الله` - افزودن متن ذکر جدید به ختم
`remove_zekr` - حذف یک ذکر از لیست
`list_zekrs` - نمایش لیست ذکرها و آمار فعلی

تصحیح مشارکت:
`-100` - کاهش صلوات یا ذکر اشتباه واردشده

ریست خودکار:
`reset on` - فعال کردن ریست خودکار 24 ساعته
`reset off` - غیرفعال کردن ریست خودکار 24 ساعته
`reset number on` - فعال کردن ریست خودکار پس از هر دوره
`reset number off` - غیرفعال کردن ریست خودکار پس از هر دوره

پیام‌های سپاس:
`sepas on` - فعال کردن پیام‌های سپاس زیر پیام‌های ربات
`sepas off` - غیرفعال کردن پیام‌های سپاس
`add sepas یا علی` - افزودن متن سپاس دلخواه

آمار و رتبه‌بندی:
`amar kol` - نمایش آمار کل ختم فعال
`amar list` - نمایش رتبه‌بندی ذاکرها

تنظیم محدودیت‌های ارسال (مخصوص ذکر و صلوات):
`max 1000` - تنظیم حداکثر تعداد مجاز
`min 10` - تنظیم حداقل تعداد مجاز

تنظیم تعداد آیات قابل قبول برای هر مشارکت:
`min_ayat 1` - حداقل تعداد آیات برای هر فرد
`max_ayat 20` - حداکثر تعداد آیات نمایشی در هر پیام
حدیث روزانه:
`hadis on` - فعال کردن حدیث روزانه
`hadis off` - غیرفعال کردن حدیث روزانه

نمایش جمع مشارکت‌ها:
`jam on` - نمایش جمع اعداد مشارکت
`jam off` - مخفی کردن جمع اعداد مشارکت

توقف خودکار:
`stop on 5000` - توقف ختم در تعداد دلخواه
`stop on off` - غیرفعال کردن توقف خودکار

توقف ساعتی:
`time off 23-08` - تنظیم ساعات خاموشی ربات
`time_off_disable` - غیرفعال کردن ساعات خاموشی

حذف خودکار پیام‌ها:
`delete on 01` - حذف پیام‌های ربات پس از X دقیقه
`delete off` - غیرفعال کردن حذف خودکار

قفل پیام‌ها:
`lock on` - قفل کردن تمام پیام‌ها به جز عدد
`lock off` - غیرفعال کردن قفل پیام‌ها

تگ کردن اعضا:
`tag` - تگ کردن تمام اعضای فعال گروه
`cancel_tag` - لغو عملیات تگ کردن

----------------------------------------
نام‌گذاری تاپیک:
`topic اصلی` - تنظیم نام تاپیک 

"""
        await update.message.reply_text(help_text, parse_mode=constants.ParseMode.MARKDOWN)
        logger.info("Help message sent successfully: user_id=%s", update.effective_user.id)
    except Exception as e:
        logger.error("Error in help command: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")





@log_function_call
async def set_max_verses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing set_max_verses: user_id=%s, chat_id=%s", 
                   update.effective_user.id, update.effective_chat.id)
        
        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted set_max_verses: user_id=%s", 
                         update.effective_user.id)
            return
            
        if not context.args:
            logger.debug("No arguments provided for set_max_verses")
            await update.message.reply_text("لطفاً تعداد حداکثر آیات را وارد کنید. مثال: /set_max_verses 10")
            return
            
        group_id = update.effective_chat.id
        max_verses = int(context.args[0])
        logger.debug("Attempting to set max verses: group_id=%s, max_verses=%d", 
                    group_id, max_verses)
        
        if max_verses <= 0 or max_verses > 100:
            logger.warning("Invalid max verses value: %d", max_verses)
            await update.message.reply_text("تعداد باید بین 1 تا 100 باشد.")
            return
            
        await execute(
            "UPDATE groups SET max_display_verses = ? WHERE group_id = ?",
            (max_verses, group_id)
        )
        logger.info("Successfully set max verses: group_id=%s, max_verses=%d", 
                   group_id, max_verses)
        
        await update.message.reply_text(f"حداکثر تعداد آیات نمایش به {max_verses} تنظیم شد.")
    except Exception as e:
        logger.error("Error in set_max_verses: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@ignore_old_messages()
@log_function_call
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing start command: user_id=%s, chat_id=%s", 
                   update.effective_user.id, update.effective_chat.id)
        
        if update.effective_chat.type not in ["group", "supergroup"]:
            logger.warning("Start command used outside group: chat_type=%s", 
                         update.effective_chat.type)
            await update.message.reply_text("این دستور فقط در گروه‌ها قابل استفاده است.")
            return
            
        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted start command: user_id=%s", 
                         update.effective_user.id)
            await update.message.reply_text("لطفاً من را مدیر کنید.")
            return
            
        group_id = update.effective_chat.id
        logger.debug("Checking existing group: group_id=%s", group_id)
        
        group = await fetch_one("SELECT * FROM groups WHERE group_id = ?", (group_id,))
        if not group:
            logger.info("Creating new group: group_id=%s", group_id)
            await execute(
                "INSERT INTO groups (group_id, is_active, max_display_verses, min_display_verses, max_number) VALUES (?, 1, 10, 1, ?)",
                (group_id, DEFAULT_MAX_NUMBER)
            )
        else:
            logger.info("Activating existing group: group_id=%s", group_id)
            await execute(
                "UPDATE groups SET is_active = 1 WHERE group_id = ?",
                (group_id,)
            )
            
        await execute(
            "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
            (group_id, group_id, "اصلی", "salavat")
        )
        logger.debug("Created/updated default topic: group_id=%s", group_id)
        
        is_topic_enabled = bool(update.message.message_thread_id)
        logger.info("Group topic status: group_id=%s, is_topic_enabled=%s", 
                   group_id, is_topic_enabled)
        
        if is_topic_enabled:
            await update.message.reply_text(
                "گروه تاپیک‌دار است.\n"
                "از topic یا 'تاپیک' برای تنظیم استفاده کنید."
            )
        else:
            await update.message.reply_text(
                "برای شروع ختم، از یکی از دستورات زیر استفاده کنید:\n"
                "- `khatm zekr` (ختم ذکر)\n"
                "- `khatm salavat` (ختم صلوات)\n"
                "- `khatm ghoran` (ختم قرآن)"
                ,parse_mode=constants.ParseMode.MARKDOWN
            )
    except Exception as e:
        logger.error("Error in start command: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@ignore_old_messages()
@log_function_call
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing stop command: user_id=%s, chat_id=%s", 
                   update.effective_user.id, update.effective_chat.id)
        
        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted stop command: user_id=%s", 
                         update.effective_user.id)
            return
            
        group_id = update.effective_chat.id
        logger.debug("Deactivating group: group_id=%s", group_id)
        
        await execute(
            "UPDATE groups SET is_active = 0 WHERE group_id = ?",
            (group_id,)
        )
        logger.info("Successfully deactivated group: group_id=%s", group_id)
        
        await update.message.reply_text("ربات خاموش شد.")
    except Exception as e:
        logger.error("Error in stop command: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@ignore_old_messages()
@log_function_call
async def topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing topic command: user_id=%s, chat_id=%s", 
                   update.effective_user.id, update.effective_chat.id)
        
        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted topic command: user_id=%s", 
                         update.effective_user.id)
            return
            
        if not context.args:
            logger.debug("No topic name provided")
            await update.message.reply_text(
                "لطفاً نام تاپیک را وارد کنید.\n"
                "مثال: topic ختم صلوات\n"
            )
            return
            
        group_id = update.effective_chat.id
        is_topic_enabled = bool(update.message.message_thread_id)
        logger.debug("Topic status check: group_id=%s, is_topic_enabled=%s", 
                    group_id, is_topic_enabled)

        if not is_topic_enabled:
            logger.warning("Topics not enabled for group: group_id=%s", group_id)
            await update.message.reply_text(
                "❌ این گروه از تاپیک‌ها پشتیبانی نمی‌کند.\n"
            )
            return

        topic_id = update.message.message_thread_id or group_id
        topic_name = " ".join(context.args)
        logger.debug("Processing topic update: group_id=%s, topic_id=%s, name=%s", 
                    group_id, topic_id, topic_name)

        # Check if topic already exists
        existing_topic = await fetch_one(
            "SELECT name, khatm_type FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )
        logger.debug("Existing topic check: exists=%s, type=%s", 
                    bool(existing_topic), 
                    existing_topic["khatm_type"] if existing_topic else None)

        if existing_topic:
            await execute(
                "UPDATE topics SET name = ? WHERE topic_id = ? AND group_id = ?",
                (topic_name, topic_id, group_id)
            )
            logger.info("Updated existing topic: group_id=%s, topic_id=%s, new_name=%s", 
                       group_id, topic_id, topic_name)
            message = f"✅ نام تاپیک به '{topic_name}' تغییر کرد."
            if existing_topic["khatm_type"]:
                khatm_type_fa = {
                    "salavat": "صلوات",
                    "zekr": "ذکر",
                    "ghoran": "قرآن"
                }.get(existing_topic["khatm_type"], existing_topic["khatm_type"])
                message += f"\nنوع ختم فعلی: {khatm_type_fa}"
        else:
            await execute(
                "INSERT INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                (topic_id, group_id, topic_name, "salavat")
            )
            logger.info("Created new topic: group_id=%s, topic_id=%s, name=%s", 
                       group_id, topic_id, topic_name)
            message = f"✅ تاپیک '{topic_name}' ایجاد شد."

        await execute(
            "UPDATE groups SET is_topic_enabled = 1 WHERE group_id = ?",
            (group_id,)
        )
        logger.debug("Updated group topic status: group_id=%s", group_id)

        keyboard = [
            [
                InlineKeyboardButton("صلوات 🙏", callback_data="khatm_salavat"),
                InlineKeyboardButton("قرآن 📖", callback_data="khatm_ghoran"),
                InlineKeyboardButton("ذکر 📿", callback_data="khatm_zekr"),
            ],
            [
            InlineKeyboardButton("ادعیه و زیارت 🤲", callback_data="khatm_doa") # <--- دکمه جدید اینجاست
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"{message}\n\n"
            f"در تاپیک {topic_name}، چه نوع ختمی انجام شود؟",
            reply_markup=reply_markup
        )
        logger.info("Sent topic selection message: group_id=%s, topic_id=%s", 
                   group_id, topic_id)

    except Exception as e:
        logger.error("Error in topic command: %s", e, exc_info=True)
        await update.message.reply_text(
            "❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید."
        )




@ignore_old_messages()
@log_function_call
async def khatm_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        logger.info("Processing khatm selection: user_id=%s, chat_id=%s, data=%s",
                   update.effective_user.id, update.effective_chat.id, query.data)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted khatm selection: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = query.message.message_thread_id or group_id
        khatm_type = query.data.replace("khatm_", "")
        logger.debug("Khatm selection details: group_id=%s, topic_id=%s, type=%s",
                    group_id, topic_id, khatm_type)

        # 1. Validate khatm type (شامل 'doa' شد)
        # ---------------------------------------------------
        if khatm_type not in ["salavat", "ghoran", "zekr", "doa"]:
        # ---------------------------------------------------
            logger.warning("Invalid khatm type selected: %s", khatm_type)
            await query.message.edit_text("❌ نوع ختم نامعتبر است.")
            return

        # Check if group is active
        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.warning("Inactive group for khatm selection: group_id=%s", group_id)
            await query.message.edit_text(
                " ابتدا با دستور `start` گروه را فعال کنید.",
                parse_mode=constants.ParseMode.MARKDOWN
            )
            return

        # Deactivate current khatm logic (ریست کردن ختم قبلی)
        await execute(
            "UPDATE topics SET is_active = 0 WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )
        
        # بروزرسانی نوع ختم در دیتابیس
        await execute(
            "UPDATE topics SET khatm_type = ?, is_active = 1, is_completed = 0, current_total = 0 WHERE topic_id = ? AND group_id = ?",
            (khatm_type, topic_id, group_id)
        )
        logger.info("Reset topic and set type: group_id=%s, topic_id=%s, type=%s", group_id, topic_id, khatm_type)
        
        message = f" ختم {khatm_type} فعال شد."

        # --- لاجیک بر اساس نوع ختم ---

        if khatm_type == "ghoran":
            quran = await QuranManager.get_instance()
            start_verse = quran.get_verse(1, 1)
            end_verse = quran.get_verse(114, 6)
            
            if start_verse and end_verse:
                await execute(
                    "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                    (group_id, topic_id, start_verse['id'], end_verse['id'])
                )
                await execute(
                    "UPDATE topics SET current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    (start_verse['id'], topic_id, group_id)
                )
                message = "📖 ختم قرآن فعال شد."
            else:
                 message = "❌ خطا در دریافت اطلاعات قرآن."

        elif khatm_type == "zekr":
            context.user_data["awaiting_zekr"] = {
                "topic_id": topic_id,
                "group_id": group_id,
                "timestamp": time.time()
            }
            message = "📿 ختم ذکر انتخاب شد.\nلطفاً **متن ذکر** مورد نظر خود را ارسال کنید."

        # 2. اضافه شدن بخش ادعیه (Doa)
        # ---------------------------------------------------
        elif khatm_type == "doa":
            # به جای اینکه مستقیم نام بخواهیم، اول نوع را می‌پرسیم
            keyboard = [
                [
                    InlineKeyboardButton("🕌 زیارت (ستون چپ)", callback_data="set_cat_ziyarat"),
                    InlineKeyboardButton("🤲 دعا (ستون راست)", callback_data="set_cat_doa")
                ]
            ]
            
            # متن پیام را عوض می‌کنیم و دکمه‌ها را می‌فرستیم
            await query.message.edit_text(
                "🤲 شما ختم «ادعیه و زیارت» را انتخاب کردید.\n\n"
                "لطفاً ابتدا مشخص کنید موردی که می‌خواهید اضافه کنید در کدام دسته قرار می‌گیرد؟",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=constants.ParseMode.MARKDOWN
            )
            return # اینجا خارج می‌شویم تا منتظر کلیک دکمه بمانیم
        # ---------------------------------------------------
        elif khatm_type == "salavat":
            default_stop_number = 100_000_000_000
            await execute(
                "UPDATE topics SET stop_number = ? WHERE topic_id = ? AND group_id = ?",
                (default_stop_number, topic_id, group_id)
            )
            message = "🙏 ختم صلوات فعال شد."

        await query.message.edit_text(message, parse_mode=constants.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error("Error in khatm_selection: %s", e, exc_info=True)
        if query and query.message:
            await query.message.edit_text("❌ خطایی رخ داد.")



@ignore_old_messages()
async def start_khatm_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start a new zekr khatm and clear any existing zekr items for this topic."""
    try:
        logger.info("Starting start_khatm_zekr: user_id=%s, chat_id=%s", 
                    update.effective_user.id, update.effective_chat.id)

        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.warning("start_khatm_zekr called in non-group chat: user_id=%s", update.effective_user.id)
            return ConversationHandler.END

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted start_khatm_zekr: user_id=%s", update.effective_user.id)
            return ConversationHandler.END

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.info("Processing start_khatm_zekr: group_id=%s, topic_id=%s", group_id, topic_id)

        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            logger.warning("Group not active for start_khatm_zekr: group_id=%s", group_id)
            await update.message.reply_text("گروه فعال نیست. از `start` یا 'شروع' استفاده کنید.", parse_mode=constants.ParseMode.MARKDOWN)
            return ConversationHandler.END

        context.user_data.clear()
        logger.debug("Cleared user_data context for start_khatm_zekr")
        
        old_khatm_type = await deactivate_current_khatm(group_id, topic_id)
        logger.info("Deactivated old khatm: group_id=%s, topic_id=%s, old_type=%s", 
                    group_id, topic_id, old_khatm_type)

        await execute(
            "DELETE FROM topic_zekrs WHERE group_id = ? AND topic_id = ?",
            (group_id, topic_id)
        )
        logger.info("Cleared old zekr items for new khatm: group_id=%s, topic_id=%s", group_id, topic_id)

        await execute(
            """
            INSERT OR REPLACE INTO topics
            (topic_id, group_id, name, khatm_type, is_active, current_total, is_completed)
            VALUES (?, ?, ?, ?, 1, 0, 0)
            """,
            (topic_id, group_id, "اصلی", "zekr")
        )
        logger.info("Directly started/replaced zekr khatm: group_id=%s, topic_id=%s", group_id, topic_id)

        message = (
            "**📿 ختم ذکر فعال شد** 🌱\n"
            "➖➖➖➖➖➖➖➖➖➖➖\n"
            "اکنون با دستور `add_zekr` یا 'اضافه ذکر' متن ذکرهای مورد نظر خود را اضافه کنید.\n"
            "**مثال:** `add_zekr سبحان الله`"
        )

        await update.message.reply_text(message, parse_mode=constants.ParseMode.MARKDOWN)
        logger.info("Sent zekr start message, prompting for /add_zekr")
        return ConversationHandler.END

    except Exception as e:
        logger.error("Error in start_khatm_zekr: group_id=%s, topic_id=%s, error=%s",
                     group_id, topic_id, e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        context.user_data.clear()
        return ConversationHandler.END


@ignore_old_messages()
@log_function_call
async def set_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing set_range command: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not update.message or not update.message.text:
            logger.warning("Invalid message format for set_range")
            await update.message.reply_text("لطفاً محدوده را وارد کنید.")
            return

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted set_range: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        text = update.message.text.strip()
        logger.debug("Processing range text: %s", text)

        pattern = r'(?:سوره|surah)?\s*(\d+)\s*(?:آیه|ایه|ayah)?\s*(\d+)\s*(?:تا|to|-)\s*(?:سوره|surah)?\s*(\d+)\s*(?:آیه|ایه|ayah)?\s*(\d+)|(\d+):(\d+)\s*(?:تا|to|-)\s*(\d+):(\d+)'
        match = re.search(pattern, text, re.IGNORECASE)

        if not match:
            logger.warning("Invalid range format: text=%s", text)
            await update.message.reply_text(
                "فرمت نامعتبر است. مثال: `/set_range سوره 1 آیه 1 تا سوره 2 آیه 10`"
            )
            return

        if match.group(1):
            start_surah = parse_number(match.group(1))
            start_ayah = parse_number(match.group(2))
            end_surah = parse_number(match.group(3))
            end_ayah = parse_number(match.group(4))
        else:
            start_surah = parse_number(match.group(5))
            start_ayah = parse_number(match.group(6))
            end_surah = parse_number(match.group(7))
            end_ayah = parse_number(match.group(8))

        logger.debug("Parsed range values: start_surah=%s, start_ayah=%s, end_surah=%s, end_ayah=%s",
                    start_surah, start_ayah, end_surah, end_ayah)

        if not (1 <= start_surah <= 114 and 1 <= end_surah <= 114):
            logger.warning("Invalid surah numbers: start=%d, end=%d", start_surah, end_surah)
            await update.message.reply_text("شماره سوره باید بین ۱ تا ۱۱۴ باشد.")
            return

        quran = await QuranManager.get_instance()
        start_verse = quran.get_verse(start_surah, start_ayah)
        end_verse = quran.get_verse(end_surah, end_ayah)
        logger.debug("Retrieved verse information: start_verse=%s, end_verse=%s",
                    bool(start_verse), bool(end_verse))

        if not start_verse or not end_verse:
            logger.error("Invalid verse numbers: start=%d:%d, end=%d:%d",
                        start_surah, start_ayah, end_surah, end_ayah)
            await update.message.reply_text(f"آیه نامعتبر است: {start_surah}:{start_ayah} یا {end_surah}:{end_ayah} وجود ندارد.")
            return

        if start_verse['id'] > end_verse['id']:
            logger.warning("Invalid verse order: start_id=%d, end_id=%d",
                         start_verse['id'], end_verse['id'])
            await update.message.reply_text("آیه شروع باید قبل از آیه پایان باشد.")
            return

        await execute(
            "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
            (group_id, topic_id, start_verse['id'], end_verse['id'])
        )
        await execute(
            "UPDATE topics SET khatm_type = ?, current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
            ("ghoran", start_verse['id'], topic_id, group_id)
        )
        logger.info("Successfully set verse range: group_id=%s, topic_id=%s, start_id=%d, end_id=%d",
                   group_id, topic_id, start_verse['id'], end_verse['id'])

        await update.message.reply_text(
            f"محدوده ختم تنظیم شد: از {start_verse['surah_name']} آیه {start_ayah} تا {end_verse['surah_name']} آیه {end_ayah}"
        )

    except Exception as e:
        logger.error("Error in set_range command: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@log_function_call
async def deactivate_current_khatm(group_id: int, topic_id: int) -> str:
    """Deactivate the current khatm and return its type."""
    try:
        logger.debug("Attempting to deactivate khatm: group_id=%s, topic_id=%s", 
                    group_id, topic_id)
        
        topic = await fetch_one(
            "SELECT khatm_type FROM topics WHERE group_id = ? AND topic_id = ? AND is_active = 1",
            (group_id, topic_id)
        )
        logger.debug("Found active topic: exists=%s, type=%s", 
                    bool(topic), topic["khatm_type"] if topic else None)
        
        if topic:
            await execute(
                "UPDATE topics SET is_active = 0 WHERE group_id = ? AND topic_id = ?",
                (group_id, topic_id)
            )
            logger.info("Deactivated khatm: group_id=%s, topic_id=%s, type=%s",
                       group_id, topic_id, topic["khatm_type"])
            return topic["khatm_type"]
        return ""
    except Exception as e:
        logger.error("Error deactivating khatm: group_id=%s, topic_id=%s, error=%s",
                    group_id, topic_id, e, exc_info=True)
        return ""
    
    

@ignore_old_messages()
@log_function_call
async def start_khatm_salavat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing start_khatm_salavat: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)
        
        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted start_khatm_salavat: user_id=%s",
                         update.effective_user.id)
            return
            
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.debug("Starting salavat khatm: group_id=%s, topic_id=%s",
                    group_id, topic_id)
        
        old_khatm_type = await deactivate_current_khatm(group_id, topic_id)
        logger.info("Deactivated previous khatm: group_id=%s, topic_id=%s, old_type=%s",
                   group_id, topic_id, old_khatm_type)
        
        default_stop_number = 100_000_000_000
        
        # Directly insert/replace the new salavat khatm
        await execute(
            """
            INSERT OR REPLACE INTO topics
            (topic_id, group_id, name, khatm_type, is_active, current_total, stop_number, is_completed)
            VALUES (?, ?, ?, ?, 1, 0, ?, 0)
            """,
            (topic_id, group_id, "اصلی", "salavat", default_stop_number)
        )
        logger.info("Directly started/replaced salavat khatm: group_id=%s, topic_id=%s, stop_number=%d", 
                   group_id, topic_id, default_stop_number)
        
        message = "🙏 ختم صلوات فعال شد."

        await update.message.reply_text(message)
        logger.info("Salavat khatm started with default target: group_id=%s, topic_id=%s, stop_number=%d",
                   group_id, topic_id, default_stop_number)
        return ConversationHandler.END
    except Exception as e:
        logger.error("Error in start_khatm_salavat: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return ConversationHandler.END

@ignore_old_messages()
@log_function_call
async def start_khatm_ghoran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing start_khatm_ghoran: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not update.message or not update.message.text:
            logger.warning("Invalid message format in start_khatm_ghoran")
            await update.message.reply_text("لطفاً دستور را درست وارد کنید.")
            return

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted start_khatm_ghoran: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        logger.debug("Starting Quran khatm: group_id=%s, topic_id=%s",
                    group_id, topic_id)

        # Check if group is active
        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        logger.debug("Group status check: group_id=%s, active=%s",
                    group_id, group["is_active"] if group else None)

        if not group:
            logger.error("Group not found in database: group_id=%s", group_id)
            await update.message.reply_text("❌ گروه در سیستم ثبت نشده است.")
            return
        if not group["is_active"]:
            logger.warning("Group is inactive: group_id=%s", group_id)
            await update.message.reply_text("❌ گروه غیرفعال است.")
            return

        # Deactivate any existing khatm
        try:
            await execute(
                "UPDATE topics SET is_active = 0 WHERE group_id = ? AND is_active = 1",
                (group_id,)
            )
            logger.debug("Deactivated existing khatm for group_id=%s", group_id)
        except Exception as e:
            logger.error("Failed to deactivate existing khatm: %s", e, exc_info=True)
            raise

        # Get verse information for start and end
        try:
            quran = await QuranManager.get_instance()
            start_verse = quran.get_verse(1, 1)  # Surah 1 Ayah 1
            end_verse = quran.get_verse(114, 6)  # Surah 114 Ayah 6

            if not start_verse or not end_verse:
                logger.error("Failed to get verse information: start_verse=%s, end_verse=%s", 
                           start_verse, end_verse)
                await update.message.reply_text("❌ خطا در دریافت اطلاعات آیات")
                return

            logger.debug("Retrieved verse information: start_verse_id=%d, end_verse_id=%d", 
                        start_verse['id'], end_verse['id'])
        except Exception as e:
            logger.error("Error getting verse information: %s", e, exc_info=True)
            await update.message.reply_text("❌ خطا در دریافت اطلاعات آیات")
            return

        # Queue the start_khatm_ghoran request
        try:
            request = {
                "type": "start_khatm_ghoran",
                "group_id": group_id,
                "topic_id": topic_id,
                "topic_name": "اصلی",
                "khatm_type": "ghoran",
                "start_verse_id": start_verse['id'],
                "end_verse_id": end_verse['id']
            }
            await write_queue.put(request)
            logger.info("Queued start_khatm_ghoran request: group_id=%s, topic_id=%s, start_verse_id=%d, end_verse_id=%d", 
                       group_id, topic_id, start_verse['id'], end_verse['id'])
        except Exception as e:
            logger.error("Failed to queue start_khatm_ghoran request: %s", e, exc_info=True)
            raise

        # Send confirmation message
        message = "✅ ختم قرآن فعال شد."
        await update.message.reply_text(message)
        logger.info("Successfully started Quran khatm: group_id=%s, topic_id=%s",
                   group_id, topic_id)

    except Exception as e:
        logger.error("Critical error in start_khatm_ghoran: group_id=%s, topic_id=%s, error=%s",
                    group_id, topic_id, e, exc_info=True)
        await update.message.reply_text("❌ خطایی رخ داد. لطفاً دوباره تلاش کنید.")

@ignore_old_messages()
@log_function_call
async def set_khatm_target_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Processing set_khatm_target_number: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted set_khatm_target_number: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        
        if not context.args:
            logger.warning("No number provided for set_khatm_target_number: group_id=%s", group_id)
            await update.message.reply_text(
                "📝 لطفاً یک عدد معتبر برای تعداد هدف وارد کنید.\\n"
                "مثال: number 14000"
            )
            return

        count = parse_number(context.args[0])
        logger.debug("Parsed target number: input=%s, result=%s",
                    context.args[0], count)

        if count is None or count <= 0:
            logger.warning("Invalid target number: %s", count)
            await update.message.reply_text("لطفاً یک عدد معتبر و مثبت وارد کنید (مثال: 14000).")
            return

        topic = await fetch_one(
            "SELECT khatm_type, is_active FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )

        if not topic or not topic["is_active"]:
            logger.warning("No active topic found or topic inactive: group_id=%s, topic_id=%s", group_id, topic_id)
            await update.message.reply_text("هیچ ختم فعالی برای تنظیم تعداد وجود ندارد. ابتدا یک ختم را شروع کنید.")
            return

        if topic["khatm_type"] not in ["salavat", "zekr"]:
            logger.warning("Cannot set target number for khatm type %s: group_id=%s, topic_id=%s",
                         topic["khatm_type"], group_id, topic_id)
            await update.message.reply_text(f"نمی‌توان تعداد هدف را برای ختم از نوع '{topic['khatm_type']}' تنظیم کرد. این دستور فقط برای صلوات و ذکر است.")
            return
            
        # Check current_total against new stop_number
        current_khatm_info = await fetch_one(
            "SELECT current_total FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )
        if current_khatm_info and current_khatm_info["current_total"] > count:
            logger.warning(
                "New target number %d is less than current total %d for topic_id=%s",
                count, current_khatm_info["current_total"], topic_id
            )
            await update.message.reply_text(
                f"❌ تعداد هدف جدید ({count}) نمی‌تواند کمتر از تعداد فعلی ختم ({current_khatm_info['current_total']}) باشد."
            )
            return

        await execute(
            "UPDATE topics SET stop_number = ? WHERE topic_id = ? AND group_id = ?",
            (count, topic_id, group_id)
        )
        logger.info("Set khatm target number: group_id=%s, topic_id=%s, khatm_type=%s, count=%d",
                   group_id, topic_id, topic["khatm_type"], count)

        khatm_type_fa = "صلوات" if topic["khatm_type"] == "salavat" else "ذکر"
        await update.message.reply_text(f"✅ تعداد هدف برای ختم {khatm_type_fa} به {count} تغییر یافت.")

    except Exception as e:
        logger.error("Error in set_khatm_target_number: %s", e, exc_info=True)
        await update.message.reply_text("خطایی در تنظیم تعداد هدف رخ داد. لطفاً دوباره تلاش کنید.")

@log_function_call
async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        logger.debug("Checking admin status: user_id=%s, chat_id=%s", user_id, chat_id)
        
        # Check if user is a super admin
        if user_id in SUPER_ADMIN_IDS:
            logger.debug("User is super admin: user_id=%s", user_id)
            return True
        
        # Check if user is a group admin
        admins = await context.bot.get_chat_administrators(chat_id)
        is_admin = any(admin.user.id == user_id for admin in admins)
        logger.debug("Group admin check result: user_id=%s, is_admin=%s", user_id, is_admin)
        
        return is_admin
    except Exception as e:
        logger.error("Error checking admin status: %s", e, exc_info=True)
        return False

@log_function_call
async def set_completion_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the completion message for a khatm by an admin."""
    try:
        logger.info("Processing set_completion_message: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted set_completion_message: user_id=%s",
                         update.effective_user.id)
            return

        if not context.args:
            logger.warning("No message provided for set_completion_message")
            await update.message.reply_text(
                "📝 لطفاً یک پیام برای اتمام ختم وارد کنید.\n"
                "مثال: set_completion_message ختم با موفقیت به پایان رسید!"
            )
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        
        # جمع‌آوری کل متن پیام از آرگومان‌ها
        message_text = " ".join(context.args)
        
        # بررسی طول پیام
        if len(message_text) > 500:
            logger.warning("Completion message too long: length=%d", len(message_text))
            await update.message.reply_text("پیام بیش از حد طولانی است. حداکثر ۵۰۰ کاراکتر مجاز است.")
            return

        # بررسی وجود تاپیک فعال
        topic = await fetch_one(
            "SELECT khatm_type, is_active FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )

        if not topic:
            logger.warning("No topic found: group_id=%s, topic_id=%s", group_id, topic_id)
            await update.message.reply_text("هیچ ختمی برای این گروه/تاپیک تنظیم نشده است.")
            return

        # ارسال درخواست به پردازشگر نوشتن
        request = {
            "type": "set_completion_message",
            "group_id": group_id,
            "topic_id": topic_id,
            "message": message_text
        }
        await write_queue.put(request)
        logger.info("Queued set_completion_message: group_id=%s, topic_id=%s", 
                   group_id, topic_id)

        await update.message.reply_text("✅ پیام اتمام ختم با موفقیت تنظیم شد.")
        logger.info("Successfully set completion message: group_id=%s, topic_id=%s",
                   group_id, topic_id)

    except Exception as e:
        logger.error("Error in set_completion_message: %s", e, exc_info=True)
        await update.message.reply_text("❌ خطایی رخ داد. لطفاً دوباره تلاش کنید.")




@ignore_old_messages()
@log_function_call
async def add_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new zekr text to the active zekr khatm."""
    try:
        logger.info("Processing add_zekr: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted add_zekr: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        topic = await fetch_one(
            "SELECT is_active, khatm_type FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )

        if not topic or not topic["is_active"] or topic["khatm_type"] != "zekr":
            logger.warning("No active zekr khatm found: group_id=%s, topic_id=%s",
                         group_id, topic_id)
            await update.message.reply_text("هیچ ختم ذکر فعالی برای افزودن متن وجود ندارد. ابتدا ختم ذکر را شروع کنید.")
            return

        if not context.args:
            logger.warning("No zekr text provided for add_zekr")
            await update.message.reply_text(
                "لطفاً متن ذکر را وارد کنید.\n"
                "مثال: add_zekr سبحان الله"
            )
            return

        zekr_text = " ".join(context.args).strip()
        if not zekr_text:
            logger.warning("Empty zekr text provided")
            await update.message.reply_text("متن ذکر نمی‌تواند خالی باشد.")
            return

        if len(zekr_text) > 100:
            logger.warning("Zekr text too long: length=%d", len(zekr_text))
            await update.message.reply_text("متن ذکر نباید بیشتر از ۱۰۰ کاراکتر باشد.")
            return

        await execute(
            "INSERT INTO topic_zekrs (group_id, topic_id, zekr_text) VALUES (?, ?, ?)",
            (group_id, topic_id, zekr_text)
        )
        logger.info("Added new zekr: group_id=%s, topic_id=%s, text=%s",
                   group_id, topic_id, zekr_text)

        await update.message.reply_text(f"✅ ذکر با موفقیت اضافه شد:\n{zekr_text}")

    except Exception as e:
        logger.error("Error in add_zekr: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")




@ignore_old_messages()
@log_function_call
async def list_zekrs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all current zekr texts and their totals for the active khatm."""
    try:
        logger.info("Processing list_zekrs: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted list_zekrs: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        topic = await fetch_one(
            "SELECT is_active, khatm_type FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )

        if not topic or not topic["is_active"] or topic["khatm_type"] != "zekr":
            logger.warning("No active zekr khatm found for list_zekrs: group_id=%s, topic_id=%s",
                         group_id, topic_id)
            await update.message.reply_text("هیچ ختم ذکر فعالی وجود ندارد.")
            return

        zekrs = await fetch_all(
            "SELECT zekr_text, current_total FROM topic_zekrs WHERE group_id = ? AND topic_id = ?",
            (group_id, topic_id)
        )

        if not zekrs:
            await update.message.reply_text(
                "هنوز هیچ ذکری برای این ختم اضافه نشده است.\n"
                "از دستور `add_zekr` برای اضافه کردن استفاده کنید."
            )
            return

        message = "📊 **لیست ذکرها و آمار فعلی:**\n"
        message += "➖➖➖➖➖➖➖➖➖➖➖\n"
        total_khatm = 0
        for zekr in zekrs:
            message += f"• **{zekr['zekr_text']}**: {zekr['current_total']:,}\n"
            total_khatm += zekr['current_total']
        
        message += f"\n**مجموع کل:** {total_khatm:,}"

        await update.message.reply_text(message, parse_mode=constants.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error("Error in list_zekrs: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")



@ignore_old_messages()
@log_function_call
async def remove_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show a list of zekrs with delete buttons for admins."""
    try:
        logger.info("Processing remove_zekr: user_id=%s, chat_id=%s",
                   update.effective_user.id, update.effective_chat.id)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted remove_zekr: user_id=%s",
                         update.effective_user.id)
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        topic = await fetch_one(
            "SELECT is_active, khatm_type FROM topics WHERE topic_id = ? AND group_id = ?",
            (topic_id, group_id)
        )

        if not topic or not topic["is_active"] or topic["khatm_type"] != "zekr":
            logger.warning("No active zekr khatm found for remove_zekr: group_id=%s, topic_id=%s",
                         group_id, topic_id)
            await update.message.reply_text("هیچ ختم ذکر فعالی وجود ندارد.")
            return

        zekrs = await fetch_all(
            "SELECT id, zekr_text FROM topic_zekrs WHERE group_id = ? AND topic_id = ?",
            (group_id, topic_id)
        )

        if not zekrs:
            await update.message.reply_text("هیچ ذکری برای حذف وجود ندارد.")
            return

        keyboard = []
        for zekr in zekrs:
            button_text = f"❌ {zekr['zekr_text']}"
            callback_data = f"del_zekr_{zekr['id']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("کدام ذکر حذف شود؟", reply_markup=reply_markup)

    except Exception as e:
        logger.error("Error in remove_zekr: %s", e, exc_info=True)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")


@log_function_call
async def handle_remove_zekr_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the callback query for deleting a zekr."""
    try:
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        group_id = query.message.chat.id
        logger.info("Processing handle_remove_zekr_click: user_id=%s, chat_id=%s, data=%s",
                   user_id, group_id, query.data)

        if not await is_admin(update, context):
            logger.warning("Non-admin user attempted zekr deletion callback")
            await query.answer("این دکمه مخصوص ادمین است.", show_alert=True)
            return

        if not query.data.startswith("del_zekr_"):
            logger.warning("Invalid callback data received: %s", query.data)
            return

        zekr_id = int(query.data.split("_")[-1])

        zekr = await fetch_one("SELECT zekr_text FROM topic_zekrs WHERE id = ?", (zekr_id,))
        
        if not zekr:
            logger.warning("Zekr not found or already deleted: id=%d", zekr_id)
            await query.edit_message_text("این ذکر قبلاً حذف شده است.")
            return

        zekr_text = zekr['zekr_text']
        await execute("DELETE FROM topic_zekrs WHERE id = ?", (zekr_id,))
        
        logger.info("Zekr deleted: id=%d, text=%s, by_admin=%s",
                    zekr_id, zekr_text, user_id)

        await query.edit_message_text(f"✅ ذکر «{zekr_text}» با موفقیت حذف شد.")

    except Exception as e:
        logger.error("Error in handle_remove_zekr_click: %s", e, exc_info=True)
        if query and query.message:
            await query.edit_message_text("خطایی در حذف ذکر رخ داد.")


        
async def set_completion_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set the completion_count for a specific topic."""
    if not update.message or not update.effective_chat:
        logger.error("No message or chat in update")
        return

    group_id = update.effective_chat.id
    topic_id = update.message.message_thread_id or group_id
    user_id = update.effective_user.id

    # Check if user is admin
    chat_member = await context.bot.get_chat_member(group_id, user_id)
    if chat_member.status not in ("administrator", "creator"):
        await update.message.reply_text("❌ فقط ادمین‌ها می‌توانند تعداد ختم‌های تکمیل‌شده را تنظیم کنند.")
        return

    # Parse command arguments
    try:
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("❌ لطفاً یک عدد معتبر وارد کنید. مثال: /set_completion_count 1")
            return
        new_count = int(context.args[0])
        if new_count < 0:
            await update.message.reply_text("❌ تعداد ختم‌ها نمی‌تواند منفی باشد.")
            return
    except ValueError:
        await update.message.reply_text("❌ لطفاً یک عدد معتبر وارد کنید. مثال: /set_completion_count 1")
        return

    # Check if topic exists
    topic = await fetch_one(
        "SELECT khatm_type FROM topics WHERE group_id = ? AND topic_id = ?",
        (group_id, topic_id)
    )
    if not topic:
        await update.message.reply_text("❌ تاپیک ختم یافت نشد.")
        return

    # Update completion_count
    try:
        await execute(  # جایگزینی execute_query با execute
            """
            UPDATE topics SET completion_count = ?
            WHERE group_id = ? AND topic_id = ?
            """,
            (new_count, group_id, topic_id)
        )
        await update.message.reply_text(f"✅ تعداد ختم‌های تکمیل‌شده به {new_count} تنظیم شد.")
        logger.info("Set completion_count to %d for group_id=%s, topic_id=%s by user=%s",
                    new_count, group_id, topic_id, user_id)
    except Exception as e:
        logger.error("Failed to set completion_count: %s", e, exc_info=True)
        await update.message.reply_text("❌ خطایی رخ داد. لطفاً دوباره تلاش کنید یا با ادمین تماس بگیرید.")




# --------------------------------------------------------------------------------
# بخش مدیریت ادعیه و زیارات (چند آیتمی)
# --------------------------------------------------------------------------------

# 1. شروع فرآیند افزودن (دستور /add_doa)
async def start_add_doa_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """نمایش منوی انتخاب نوع (زیارت یا دعا)"""
    # بررسی ادمین بودن (تابع is_admin در همین فایل موجود است)
    if not await is_admin(update, context):
        return

    keyboard = [
        [
            InlineKeyboardButton("🕌 زیارت (ستون چپ)", callback_data="set_cat_ziyarat"),
            InlineKeyboardButton("🤲 دعا (ستون راست)", callback_data="set_cat_doa")
        ]
    ]
    
    await update.message.reply_text(
        "📝 **افزودن مورد جدید به لیست**\n\n"
        "لطفاً نوع مورد را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=constants.ParseMode.MARKDOWN
    )


async def handle_doa_category_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """پردازش انتخاب نوع (زیارت/دعا) و درخواست نام"""
    query = update.callback_query
    await query.answer()
    
    # 1. تشخیص نوع بر اساس دکمه کلیک شده
    category = 'ziyarat' if 'ziyarat' in query.data else 'doa'
    
    # 2. پیدا کردن ID تاپیک
    topic_id = query.message.message_thread_id if query.message.is_topic_message else query.message.chat.id
    
    # 3. ذخیره اطلاعات حیاتی در حافظه
    context.user_data['doa_setup_step'] = 'waiting_for_doa_name' # تنظیم مرحله بعدی
    context.user_data['doa_setup_topic_id'] = topic_id
    context.user_data['doa_category'] = category # <--- مهم: ذخیره دسته‌بندی
    
    cat_text = "زیارت 🕌" if category == 'ziyarat' else "دعا 🤲"
    
    # 4. درخواست نام از کاربر
    await query.edit_message_text(
        f"✅ دسته‌بندی: **{cat_text}**\n\n"
        "✍️ حالا لطفاً **نام** آن را بنویسید:\n"
        "(مثال: زیارت عاشورا)",
        parse_mode=constants.ParseMode.MARKDOWN
    )




# 3. تابع اصلی پردازش متن‌های ادمین (نسخه اصلاح‌شده و بدون خطای دیتابیس)
@log_function_call
async def process_doa_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    بررسی می‌کند آیا ادمین در حال افزودن دعا است؟
    اگر بله، نام و لینک را می‌گیرد و در جدول doa_items ذخیره می‌کند.
    """
    step = context.user_data.get('doa_setup_step')
    if not step:
        return False # ادمین در حال انجام این کار نیست

    # بررسی ادمین
    if not await is_admin(update, context):
        return False

    # بررسی اینکه پیام در همان تاپیک مورد نظر باشد
    target_topic_id = context.user_data.get('doa_setup_topic_id')
    current_topic_id = update.message.message_thread_id if update.message.is_topic_message else update.effective_chat.id
    
    if update.effective_chat.is_forum and current_topic_id != target_topic_id:
        return False # پیام مربوط به تاپیک دیگری است

    text = update.message.text
    chat_id = update.effective_chat.id

    # --- مرحله 1: دریافت نام ---
    if step == 'waiting_for_doa_name':
        context.user_data['doa_title'] = text
        context.user_data['doa_setup_step'] = 'waiting_for_doa_link'
        
        await update.message.reply_text(
            f"👌 نام **{text}** ثبت شد.\n\n"
            "🔗 حالا **لینک متن** را ارسال کنید:\n"
            "(اگر لینک ندارید، کلمه `خالی` را بفرستید)",
            parse_mode=constants.ParseMode.MARKDOWN
        )
        return True

    # --- مرحله 2: دریافت لینک و ذخیره ---
    elif step == 'waiting_for_doa_link':
        link = text if text != 'خالی' else ""
        title = context.user_data.get('doa_title')
        category = context.user_data.get('doa_category')
        
        # ۱. ابتدا بررسی می‌کنیم که آیا این تاپیک در جدول اصلی topics وجود دارد یا خیر
        topic_exists = await fetch_one(
            "SELECT 1 FROM topics WHERE group_id = ? AND topic_id = ?",
            (chat_id, target_topic_id)
        )
        
        # ۲. اگر تاپیک هنوز در دیتابیس ثبت نشده، ابتدا آن را می‌سازیم تا خطا رخ ندهد
        if not topic_exists:
            # تلاش برای پیدا کردن نام واقعی تاپیک از پیام‌های تلگرام، در غیر این صورت نام پیش‌فرض
            topic_name = "ختم ادعیه و زیارات"
            if update.effective_message.reply_to_message and update.effective_message.reply_to_message.forum_topic_created:
                topic_name = update.effective_message.reply_to_message.forum_topic_created.name
                
            await execute(
                """
                INSERT INTO topics (group_id, topic_id, name, khatm_type)
                VALUES (?, ?, ?, 'doa')
                """,
                (chat_id, target_topic_id, topic_name)
            )
        
        # ۳. حالا با اطمینان آیتم دعا را در جدول doa_items درج می‌کنیم
        await execute(
            """
            INSERT INTO doa_items (group_id, topic_id, title, link, category, current_total)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (chat_id, target_topic_id, title, link, category)
        )
        
        # ۴. مطمئن می‌شویم نوع تاپیک روی 'doa' تنظیم و فعال است
        await execute(
            """
            UPDATE topics 
            SET khatm_type = 'doa', is_active = 1 
            WHERE group_id = ? AND topic_id = ?
            """,
            (chat_id, target_topic_id)
        )

        icon = "🕌" if category == 'ziyarat' else "🤲"
        await update.message.reply_text(
            f"🎉 {icon} **{title}** به لیست اضافه شد!\n\n"
            "مشارکت‌های بعدی کاربران در این تاپیک، منوی انتخاب را نمایش خواهد داد.",
            parse_mode=constants.ParseMode.MARKDOWN
        )
        
        # پایان کار و پاکسازی اطلاعات موقت همین بخش (به جای clear کل دیتا)
        for key in ['doa_setup_step', 'doa_setup_topic_id', 'doa_title', 'doa_category']:
            context.user_data.pop(key, None)
            
        return True

    return False




# -----------------------------------------------------------------------------
# بخش حذف آیتم‌ها
# -----------------------------------------------------------------------------

# 1. تابع شروع حذف (متصل به دستور /del_doa)
async def start_remove_doa_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """شروع پروسه حذف آیتم با دریافت نام"""
    if not await is_admin(update, context):
        return

    # ذخیره وضعیت: منتظر نام برای حذف
    topic_id = update.message.message_thread_id if update.message.is_topic_message else update.message.chat.id
    
    context.user_data['doa_setup_step'] = 'waiting_for_delete_name'
    context.user_data['doa_setup_topic_id'] = topic_id
    
    await update.message.reply_text(
        "🗑 **حذف دعا یا زیارت**\n\n"
        "لطفاً **نام دقیق** موردی که می‌خواهید حذف شود را بنویسید:\n"
        "(مثال: زیارت عاشورا)",
        parse_mode=constants.ParseMode.MARKDOWN
    )

# 2. تابع پردازش حذف (باید در main.py به عنوان MessageHandler اضافه شود)
@log_function_call
async def process_doa_removal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    بررسی می‌کند اگر ادمین در حالت حذف است، آیتم را پاک کند.
    """
    step = context.user_data.get('doa_setup_step')
    
    # فقط اگر در مرحله حذف باشیم اجرا شود
    if step != 'waiting_for_delete_name':
        return False

    if not await is_admin(update, context):
        return False

    # بررسی تاپیک صحیح
    target_topic_id = context.user_data.get('doa_setup_topic_id')
    current_topic_id = update.message.message_thread_id if update.message.is_topic_message else update.effective_chat.id
    
    if update.effective_chat.is_forum and current_topic_id != target_topic_id:
        return False

    text = update.message.text.strip() # حذف فاصله‌های اضافی
    chat_id = update.effective_chat.id

    # بررسی وجود آیتم قبل از حذف
    item = await fetch_one(
        "SELECT id FROM doa_items WHERE group_id = ? AND topic_id = ? AND title = ?",
        (chat_id, target_topic_id, text)
    )
    
    if not item:
        await update.message.reply_text(
            f"❌ موردی با نام **{text}** یافت نشد.\n"
            "لطفاً نام را دقیق وارد کنید یا برای لغو کلمه `لغو` را بفرستید.",
            parse_mode=constants.ParseMode.MARKDOWN
        )
        return True # پیام پردازش شد اما حذف نشد (منتظر تلاش بعدی)
    
    # حذف از دیتابیس
    await execute(
        "DELETE FROM doa_items WHERE id = ?",
        (item['id'],)
    )
    
    await update.message.reply_text(
        f"✅ مورد **{text}** با موفقیت حذف شد.",
        parse_mode=constants.ParseMode.MARKDOWN
    )
    
    # پایان و پاکسازی وضعیت
    context.user_data.clear()
    return True



TEXT_COMMANDS = {
    "lock on": {"handler": "lock_on", "admin_only": True, "aliases": ["قفل روشن"], "takes_args": False},
    "lock off": {"handler": "lock_off", "admin_only": True, "aliases": ["قفل خاموش"], "takes_args": False},
    "start": {"handler": "start", "admin_only": True, "aliases": ["شروع"], "takes_args": False},
    "stop": {"handler": "stop", "admin_only": True, "aliases": ["توقف"], "takes_args": False},
    "help": {"handler": "help_command", "admin_only": False, "aliases": ["راهنما"], "takes_args": False},
    "max": {"handler": "set_max", "admin_only": True, "aliases": ["حداکثر"], "takes_args": True},
    "max off": {"handler": "max_off", "admin_only": True, "aliases": ["حداکثر خاموش"], "takes_args": False},
    "max_ayat": {"handler": "max_ayat", "admin_only": True, "aliases": ["حداکثر آیات"], "takes_args": True},
    "min": {"handler": "set_min", "admin_only": True, "aliases": ["حداقل"], "takes_args": True},
    "min_ayat": {"handler": "min_ayat", "admin_only": True, "aliases": ["حداقل آیات"], "takes_args": True},
    "min off": {"handler": "min_off", "admin_only": True, "aliases": ["حداقل خاموش"], "takes_args": False},
    "sepas on": {"handler": "sepas_on", "admin_only": True, "aliases": ["سپاس روشن"], "takes_args": False},
    "sepas off": {"handler": "sepas_off", "admin_only": True, "aliases": ["سپاس خاموش"], "takes_args": False},
    "add sepas": {"handler": "add_sepas", "admin_only": True, "aliases": ["اضافه سپاس"], "takes_args": True},
    "reset daily": {"handler": "reset_daily", "admin_only": True, "aliases": ["ریست روزانه"], "takes_args": False},
    "reset off": {"handler": "reset_off", "admin_only": True, "aliases": ["ریست خاموش"], "takes_args": False},
    "reset zekr": {"handler": "reset_zekr", "admin_only": True, "aliases": ["ریست ذکر"], "takes_args": False},
    "reset kol": {"handler": "reset_kol", "admin_only": True, "aliases": ["ریست کل"], "takes_args": False},
    "time off": {"handler": "time_off", "admin_only": True, "aliases": ["خاموشی"], "takes_args": True},
    "time_off_disable": {"handler": "time_off_disable", "admin_only": True, "aliases": ["خاموشی غیرفعال"], "takes_args": False},
    "hadis on": {"handler": "hadis_on", "admin_only": True, "aliases": ["حدیث روزانه"], "takes_args": False},
    "hadis off": {"handler": "hadis_off", "admin_only": True, "aliases": ["حدیث خاموش"], "takes_args": False},
    "amar kol": {"handler": "show_total_stats", "admin_only": False, "aliases": ["آمار کل"], "takes_args": False},
    "amar list": {"handler": "show_ranking", "admin_only": False, "aliases": ["لیست آمار"], "takes_args": False},
    "stop on": {"handler": "stop_on", "admin_only": True, "aliases": ["توقف روشن"], "takes_args": True},
    "stop on off": {"handler": "stop_on_off", "admin_only": True, "aliases": ["توقف خاموش"], "takes_args": False},
    "number": {"handler": "set_khatm_target_number", "admin_only": True, "aliases": ["تعداد"], "takes_args": True},
    "number off": {"handler": "number_off", "admin_only": True, "aliases": ["تعداد خاموش"], "takes_args": False},
    "reset number on": {"handler": "reset_number_on", "admin_only": True, "aliases": ["ریست تعداد روشن"], "takes_args": False},
    "reset number off": {"handler": "reset_number_off", "admin_only": True, "aliases": ["ریست تعداد خاموش"], "takes_args": False},
    "jam on": {"handler": "jam_on", "admin_only": True, "aliases": ["جمع روشن"], "takes_args": False},
    "jam off": {"handler": "jam_off", "admin_only": True, "aliases": ["جمع خاموش"], "takes_args": False},
    "set completion message": {"handler": "set_completion_message", "admin_only": True, "aliases": ["پیام تکمیل"], "takes_args": True},
    "khatm zekr": {"handler": "start_khatm_zekr", "admin_only": True, "aliases": ["ختم ذکر"], "takes_args": False},    "khatm salavat": {"handler": "start_khatm_salavat", "admin_only": True, "aliases": ["ختم صلوات"], "takes_args": False},
    "khatm ghoran": {"handler": "start_khatm_ghoran", "admin_only": True, "aliases": ["ختم قرآن"], "takes_args": False},
    "set range": {"handler": "set_range", "admin_only": True, "aliases": ["تنظیم محدوده"], "takes_args": True},
    "topic": {"handler": "topic", "admin_only": True, "aliases": ["تاپیک"], "takes_args": True},
    "tag": {"handler": "tag_command", "admin_only": True, "aliases": ["تگ"], "takes_args": False},
    "cancel_tag": {"handler": "cancel_tag", "admin_only": True, "aliases": ["لغو تگ"], "takes_args": False},
    "subtract": {"handler": "subtract_khatm", "admin_only": True, "aliases": ["کاهش"], "takes_args": True},
    "start from": {"handler": "start_from", "admin_only": True, "aliases": ["شروع از"], "takes_args": True},
    "delete on": {"handler": "delete_after", "admin_only": True, "aliases": ["حذف روشن"], "takes_args": True},
    "delete off": {"handler": "delete_off", "admin_only": True, "aliases": ["حذف خاموش"], "takes_args": False},
    "status": {"handler": "khatm_status", "admin_only": False, "aliases": ["وضعیت"], "takes_args": False},
    "add zekr": {"handler": "add_zekr", "admin_only": True, "aliases": ["اضافه ذکر"], "takes_args": True},
    "remove zekr": {"handler": "remove_zekr", "admin_only": True, "aliases": ["حذف ذکر"], "takes_args": False},
    "list zekrs": {"handler": "list_zekrs", "admin_only": True, "aliases": ["لیست ذکرها"], "takes_args": False},
    "add doa": {"handler": start_add_doa_item, "admin_only": True, "aliases": ["افزودن دعا", "افزودن زیارت", "مدیریت دعا"], "takes_args": False},
    "del doa": {"handler": start_remove_doa_item, "admin_only": True, "aliases": ["حذف دعا", "حذف زیارت"], "takes_args": False},
}
