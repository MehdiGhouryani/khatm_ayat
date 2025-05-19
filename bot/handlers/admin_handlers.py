import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from bot.database.db import fetch_one, fetch_all, execute, write_queue
from bot.utils.constants import KHATM_TYPES
from bot.utils.helpers import parse_number
import re
from telegram import constants
from bot.utils.constants import quran
logger = logging.getLogger(__name__)


TEXT_COMMANDS = {
    "lock on": {"handler": "lock_on", "admin_only": True, "aliases": ["قفل روشن"]},
    "lock off": {"handler": "lock_off", "admin_only": True, "aliases": ["قفل خاموش"]},
    "start": {"handler": "start", "admin_only": True, "aliases": ["شروع"]},
    "stop": {"handler": "stop", "admin_only": True, "aliases": ["توقف"]},
    "help": {"handler": "help_command", "admin_only": False, "aliases": ["راهنما"]},
    "max": {"handler": "set_max", "admin_only": True, "aliases": ["حداکثر"]},
    "max off": {"handler": "max_off", "admin_only": True, "aliases": ["حداکثر خاموش"]},
    "min": {"handler": "set_min", "admin_only": True, "aliases": ["حداقل"]},
    "min off": {"handler": "min_off", "admin_only": True, "aliases": ["حداقل خاموش"]},
    "sepas on": {"handler": "sepas_on", "admin_only": True, "aliases": ["سپاس روشن"]},
    "sepas off": {"handler": "sepas_off", "admin_only": True, "aliases": ["سپاس خاموش"]},
    "add sepas": {"handler": "add_sepas", "admin_only": True, "aliases": ["اضافه سپاس"]},
    "reset daily": {"handler": "reset_daily", "admin_only": True, "aliases": ["ریست روزانه"]},
    "reset off": {"handler": "reset_off", "admin_only": True, "aliases": ["ریست خاموش"]},
    "reset zekr": {"handler": "reset_zekr", "admin_only": True, "aliases": ["ریست ذکر"]},
    "reset kol": {"handler": "reset_kol", "admin_only": True, "aliases": ["ریست کل"]},
    "time off": {"handler": "time_off", "admin_only": True, "aliases": ["خاموشی"]},
    "time off disable": {"handler": "time_off_disable", "admin_only": True, "aliases": ["خاموشی غیرفعال"]},
    "hadis on": {"handler": "hadis_on", "admin_only": True, "aliases": ["حدیث روشن"]},
    "hadis off": {"handler": "hadis_off", "admin_only": True, "aliases": ["حدیث خاموش"]},
    "amar kol": {"handler": "show_total_stats", "admin_only": False, "aliases": ["آمار کل"]},
    "amar list": {"handler": "show_ranking", "admin_only": False, "aliases": ["لیست آمار"]},
    "stop on": {"handler": "stop_on", "admin_only": True, "aliases": ["توقف روشن"]},
    "stop on off": {"handler": "stop_on_off", "admin_only": True, "aliases": ["توقف خاموش"]},
    "number": {"handler": "set_number", "admin_only": True, "aliases": ["تعداد"]},
    "number off": {"handler": "number_off", "admin_only": True, "aliases": ["تعداد خاموش"]},
    "reset number on": {"handler": "reset_number_on", "admin_only": True, "aliases": ["ریست تعداد روشن"]},
    "reset number off": {"handler": "reset_number_off", "admin_only": True, "aliases": ["ریست تعداد خاموش"]},
    "jam on": {"handler": "jam_on", "admin_only": True, "aliases": ["جمع روشن"]},
    "jam off": {"handler": "jam_off", "admin_only": True, "aliases": ["جمع خاموش"]},
    "set completion message": {"handler": "set_completion_message", "admin_only": True, "aliases": ["پیام تکمیل"]},
    "khatm zekr": {"handler": "start_khatm_zekr", "admin_only": True, "aliases": ["ختم ذکر"]},
    "khatm salavat": {"handler": "start_khatm_salavat", "admin_only": True, "aliases": ["ختم صلوات"]},
    "khatm ghoran": {"handler": "start_khatm_ghoran", "admin_only": True, "aliases": ["ختم قرآن"]},
    "set range": {"handler": "set_range", "admin_only": True, "aliases": ["تنظیم محدوده"]},
    "topic": {"handler": "topic", "admin_only": True, "aliases": ["تاپیک"]},
    "tag": {"handler": "tag_command", "admin_only": True, "aliases": ["تگ"]},
    "cancel_tag": {"handler": "cancel_tag", "admin_only": True, "aliases": ["لغو تگ"]},
    "subtract": {"handler": "subtract_khatm", "admin_only": True, "aliases": ["کاهش"]},
    "start from": {"handler": "start_from", "admin_only": True, "aliases": ["شروع از"]}
}

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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
`khatm zekr` - شروع ختم ذکر
`khatm salavat` - شروع ختم صلوات
`khatm ghoran` - شروع ختم قرآن
`set range` - تنظیم محدوده ختم قرآن (مثال: surah 1 ayah 1 to 2:10)
`set completion message` - تنظیم پیام پایان ختم

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

تنظیم محدودیت‌های ارسال:
`max 1000` - تنظیم حداکثر تعداد مجاز
`max off` - غیرفعال کردن حداکثر تعداد
`min 10` - تنظیم حداقل تعداد مجاز
`min off` - غیرفعال کردن حداقل تعداد

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
`time off disable` - غیرفعال کردن ساعات خاموشی

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
**قابلیت‌های گروه‌های تاپیک‌دار**

نام‌گذاری تاپیک:
`topic اصلی` - تنظیم نام تاپیک (مثال: topic اصلی)

تنظیم نوع ختم در تاپیک:
`khatm salavat` - شروع ختم صلوات در تاپیک
`khatm ghoran` - شروع ختم قرآن در تاپیک
`khatm zekr` - شروع ختم ذکر در تاپیک

----------------------------------------
**دستورات مخصوص ختم قرآن**

تنظیم تعداد آیات:
`min 1` - حداقل تعداد آیات برای هر فرد
`max 20` - حداکثر تعداد آیات برای هر فرد
`max day 20` - حداکثر تعداد آیات روزانه هر فرد
"""
        await update.message.reply_text(help_text, parse_mode=constants.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error("Error in help command: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_max_verses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            return
        if not context.args:
            await update.message.reply_text("لطفاً تعداد حداکثر آیات را وارد کنید. مثال: /set_max_verses 10")
            return
        group_id = update.effective_chat.id
        max_verses = int(context.args[0])
        if max_verses <= 0 or max_verses > 100:
            await update.message.reply_text("تعداد باید بین 1 تا 100 باشد.")
            return
        await execute(
            "UPDATE groups SET max_display_verses = ? WHERE group_id = ?",
            (max_verses, group_id)
        )
        await update.message.reply_text(f"حداکثر تعداد آیات نمایش به {max_verses} تنظیم شد.")
    except Exception as e:
        logger.error("Error in set_max_verses: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("این دستور فقط در گروه‌ها قابل استفاده است.")
            return
        if not await is_admin(update, context):
            await update.message.reply_text("لطفاً من را مدیر کنید.")
            return
        group_id = update.effective_chat.id
        group = await fetch_one("SELECT * FROM groups WHERE group_id = ?", (group_id,))
        if not group:
            await execute(
                "INSERT INTO groups (group_id, is_active, max_display_verses) VALUES (?, 1, 10)",
                (group_id,)
            )
        else:
            await execute(
                "UPDATE groups SET is_active = 1 WHERE group_id = ?",
                (group_id,)
            )
        await execute(
            "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
            (group_id, group_id, "اصلی", "salavat")
        )
        is_topic_enabled = bool(update.message.message_thread_id)
        if is_topic_enabled:
            await update.message.reply_text("گروه تاپیک‌دار است. لطفاً تاپیک‌ها را با /topic تنظیم کنید.")
        else:
            message = (
                "گروه فاقد تاپیک است و حالت بدون تاپیک فعال شد.\n\n"
                "برای شروع ختم، لطفاً یکی از دستورات زیر را وارد کنید:\n"
                "• ختم ذکر: /khatm_zekr\n"
                "• ختم صلوات: /khatm_salavat\n"
                "• ختم قرآن: /khatm_ghoran"
            )
            await update.message.reply_text(message)
        await update.message.reply_text("ربات با موفقیت فعال شد و آماده به کار است.")
    except Exception as e:
        logger.error("Error in start command: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            return
        group_id = update.effective_chat.id
        await execute(
            "UPDATE groups SET is_active = 0 WHERE group_id = ?",
            (group_id,)
        )
        await update.message.reply_text("ربات خاموش شد.")
    except Exception as e:
        logger.error("Error in stop command: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            return
        if not context.args:
            await update.message.reply_text("لطفاً نام تاپیک را وارد کنید. مثال: /topic 1")
            return
        group_id = update.effective_chat.id
        is_topic_enabled = bool(update.message.message_thread_id)
        if not is_topic_enabled:
            await update.message.reply_text("این گروه از تاپیک‌ها پشتیبانی نمی‌کند.")
            return
        topic_id = update.message.message_thread_id or group_id
        topic_name = " ".join(context.args)
        await execute(
            "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
            (topic_id, group_id, topic_name, "salavat")
        )
        await execute(
            "UPDATE groups SET is_topic_enabled = 1 WHERE group_id = ?",
            (group_id,)
        )
        keyboard = [
            [
                InlineKeyboardButton("صلوات", callback_data="khatm_salavat"),
                InlineKeyboardButton("قرآن", callback_data="khatm_ghoran"),
                InlineKeyboardButton("ذکر", callback_data="khatm_zekr"),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"در تاپیک {topic_name}، چه نوع ختمی انجام شود؟",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error("Error in topic command: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def khatm_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        if not await is_admin(update, context):
            return
        group_id = update.effective_chat.id
        topic_id = query.message.message_thread_id or group_id
        khatm_type = query.data.replace("khatm_", "")
        await execute(
            "UPDATE topics SET khatm_type = ? WHERE topic_id = ? AND group_id = ?",
            (khatm_type, topic_id, group_id)
        )
        if khatm_type == "ghoran":
            start_verse = quran.get_verse(1, 1)
            end_verse = quran.get_verse(114, 6)
            await execute(
                "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                (group_id, topic_id, start_verse['id'], end_verse['id'])
            )
            await execute(
                "UPDATE topics SET current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                (start_verse['id'], topic_id, group_id)
            )
        if khatm_type == "zekr":
            await query.message.reply_text("ذکر مورد نظر خود را ارسال کنید.")
            context.user_data["awaiting_zekr"] = {"topic_id": topic_id, "group_id": group_id}
        elif khatm_type == "ghoran":
            await query.message.reply_text(
                "ختم قرآن فعال شد (پیش‌فرض: کل قرآن). برای تغییر محدوده، از /set_range استفاده کنید."
            )
        else:
            await query.message.reply_text(f"ختم فعال: {khatm_type.capitalize()}")
    except Exception as e:
        logger.error("Error in khatm_selection: %s", e)
        await query.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_zekr_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if "awaiting_zekr" not in context.user_data:
            return
        if not await is_admin(update, context):
            return
        zekr_data = context.user_data.pop("awaiting_zekr")
        zekr_text = update.message.text.strip()
        await execute(
            "UPDATE topics SET zekr_text = ? WHERE topic_id = ? AND group_id = ?",
            (zekr_text, zekr_data["topic_id"], zekr_data["group_id"])
        )
        await update.message.reply_text(f"ختم فعال: {zekr_text}")
    except Exception as e:
        logger.error("Error in set_zekr_text: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message or not update.message.text:
            return
        if not await is_admin(update, context):
            await update.message.reply_text("فقط ادمین می‌تواند محدوده ختم را تنظیم کند.")
            return
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        text = update.message.text.strip()
        pattern = r'(?:سوره|surah)?\s*(\d+)\s*(?:آیه|ایه|ayah)?\s*(\d+)\s*(?:تا|to|-)\s*(?:سوره|surah)?\s*(\d+)\s*(?:آیه|ایه|ayah)?\s*(\d+)|(\d+):(\d+)\s*(?:تا|to|-)\s*(\d+):(\d+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
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
        if not (1 <= start_surah <= 114 and 1 <= end_surah <= 114):
            await update.message.reply_text("شماره سوره باید بین ۱ تا ۱۱۴ باشد.")
            return
        start_verse = quran.get_verse(start_surah, start_ayah)
        end_verse = quran.get_verse(end_surah, end_ayah)
        if not start_verse or not end_verse:
            await update.message.reply_text(f"آیه نامعتبر است: {start_surah}:{start_ayah} یا {end_surah}:{end_ayah} وجود ندارد.")
            return
        if start_verse['id'] > end_verse['id']:
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
        await update.message.reply_text(
            f"محدوده ختم تنظیم شد: از {start_verse['surah_name']} آیه {start_ayah} تا {end_verse['surah_name']} آیه {end_ayah}"
        )
    except Exception as e:
        logger.error("Error in set_range command: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def deactivate_current_khatm(group_id: int, topic_id: int):
    try:
        current_khatm = await fetch_one(
            "SELECT khatm_type FROM topics WHERE group_id = ? AND topic_id = ? AND is_active = 1",
            (group_id, topic_id)
        )
        if current_khatm:
            request = {
                "type": "deactivate_khatm",
                "group_id": group_id,
                "topic_id": topic_id
            }
            await write_queue.put(request)
            return current_khatm["khatm_type"]
        return None
    except Exception as e:
        logger.error("Error deactivating current khatm: %s", e)
        raise

async def start_khatm_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            return
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        old_khatm_type = await deactivate_current_khatm(group_id, topic_id)
        request = {
            "type": "start_khatm_zekr",
            "group_id": group_id,
            "topic_id": topic_id,
            "topic_name": "اصلی",
            "khatm_type": "zekr"
        }
        await write_queue.put(request)
        message = "📿 ختم ذکر فعال شد. لطفاً متن ذکر را وارد کنید (مثال: سبحان الله)."
        if old_khatm_type:
            message = f"✅ ختم {old_khatm_type} غیرفعال شد.\n" + message
        await update.message.reply_text(message)
        context.user_data["awaiting_zekr"] = {"topic_id": topic_id, "group_id": group_id}
        return 1
    except Exception as e:
        logger.error("Error in start_khatm_zekr: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return ConversationHandler.END

async def start_khatm_salavat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            return
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        old_khatm_type = await deactivate_current_khatm(group_id, topic_id)
        request = {
            "type": "start_khatm_salavat",
            "group_id": group_id,
            "topic_id": topic_id,
            "topic_name": "اصلی",
            "khatm_type": "salavat"
        }
        await write_queue.put(request)
        message = "🙏 ختم صلوات فعال شد. لطفاً تعداد صلوات را وارد کنید (مثال: 14000)."
        if old_khatm_type:
            message = f"✅ ختم {old_khatm_type} غیرفعال شد.\n" + message
        await update.message.reply_text(message)
        context.user_data["awaiting_salavat"] = {"topic_id": topic_id, "group_id": group_id}
        return 2
    except Exception as e:
        logger.error("Error in start_khatm_salavat: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return ConversationHandler.END

async def start_khatm_ghoran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not await is_admin(update, context):
            await update.message.reply_text("فقط ادمین می‌تواند ختم قرآن را شروع کند.")
            return
        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        group = await fetch_one("SELECT is_active FROM groups WHERE group_id = ?", (group_id,))
        if not group or not group["is_active"]:
            await update.message.reply_text("گروه فعال نیست. از /start یا 'شروع' استفاده کنید.")
            return
        old_khatm_type = await deactivate_current_khatm(group_id, topic_id)
        start_verse = quran.get_verse(1, 1)
        end_verse = quran.get_verse(114, 6)
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
        message = (
            "📖 ختم قرآن با محدوده پیش‌فرض (کل قرآن) فعال شد.\n"
            "برای تنظیم محدوده دلخواه، از دستور /set_range استفاده کنید."
        )
        if old_khatm_type:
            message = f"✅ ختم {old_khatm_type} غیرفعال شد.\n" + message
        await update.message.reply_text(message)
        return ConversationHandler.END
    except Exception as e:
        logger.error("Error in start_khatm_ghoran: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return ConversationHandler.END

async def set_salavat_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if "awaiting_salavat" not in context.user_data:
            return
        if not await is_admin(update, context):
            return
        salavat_data = context.user_data.pop("awaiting_salavat")
        count = int(update.message.text)
        if count <= 0:
            await update.message.reply_text("تعداد باید مثبت باشد.")
            return 2
        await execute(
            "UPDATE topics SET stop_number = ? WHERE topic_id = ? AND group_id = ?",
            (count, salavat_data["topic_id"], salavat_data["group_id"])
        )
        await update.message.reply_text(f"✅ ختم {count} صلوات تنظیم شد. ختم صلوات آغاز شد!")
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ لطفاً یک عدد معتبر وارد کنید (مثال: 14000).")
        return 2
    except Exception as e:
        logger.error("Error in set_salavat_count: %s", e)
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return 2

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        admins = await context.bot.get_chat_administrators(chat_id)
        is_admin = any(admin.user.id == user_id for admin in admins)
        return is_admin
    except Exception as e:
        logger.error("Error checking admin status: %s", e)
        return False