import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, filters
from bot.database.db import get_db_connection
from bot.utils.constants import KHATM_TYPES
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

quran = QuranManager()

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command to show command guide."""
    try:
        help_text = """
راهنمای دستورات ربات:

**فعال‌سازی و توقف ربات:**
/start - فعال‌سازی ربات
/stop - توقف ربات

**ریست ختم:**
/reset_zekr - ریست ذکر یا صلوات
/reset_kol - ریست کل آمار ختم‌ها

**تنظیم ختم:**
/number 14000 - تنظیم دوره ختم (مثلاً 14000 صلوات)
/reset_number_on - ریست خودکار پس از تکمیل دوره
/reset_number_off - غیرفعال کردن ریست خودکار
/reset_on - ریست خودکار 24 ساعته
/reset_off - غیرفعال کردن ریست 24 ساعته

**متن سپاس:**
/sepas_on - فعال کردن متن سپاس
/sepas_off - غیرفعال کردن متن سپاس
/addsepas [متن] - افزودن متن سپاس (مثال: /addsepas یا علی)

**نمایش جمع کل:**
/jam_on - فعال کردن نمایش جمع کل در پیام‌ها
/jam_off - غیرفعال کردن نمایش جمع کل

**پیام تبریک:**
/set_completion_message [متن] - تنظیم پیام تبریک تکمیل ختم (مثال: /set_completion_message تبریک! ختم کامل شد)

**آمار و رتبه‌بندی:**
/amar_kol - نمایش آمار کل ختم‌ها
/amar_list - نمایش رتبه‌بندی ذاکرین

**محدودیت ارسال:**
/max 1000 - تنظیم حداکثر تعداد (مثلاً 1000)
/max_off - غیرفعال کردن حداکثر
/min 10 - تنظیم حداقل تعداد (مثلاً 10)
/min_off - غیرفعال کردن حداقل

**حدیث روزانه:**
/hadis_on - فعال کردن حدیث روزانه
/hadis_off - غیرفعال کردن حدیث روزانه

**توقف و پاک‌سازی:**
/stop_on 5000 - توقف ختم در تعداد مشخص (مثلاً 5000)
/stop_on_off - غیرفعال کردن توقف
/time_off 23-08 - توقف ساعتی (مثلاً 11 شب تا 8 صبح)
/delete_on 01 - پاک‌سازی پیام‌ها پس از 1 دقیقه
/delete_off - غیرفعال کردن پاک‌سازی

**قفل پیام‌ها:**
/lock_on - قفل پیام‌ها به اعداد یا آیات
/lock_off - غیرفعال کردن قفل

**تنظیمات تاپیک:**
/topic 1 - تنظیم نام تاپیک (مثلاً تاپیک 1)
/khatm_salavat - تنظیم ختم صلوات
/khatm_ghoran - تنظیم ختم قرآن
/khatm_zekr - تنظیم ختم ذکر

**ختم قرآن:**
/set_range - تنظیم محدوده ختم قرآن (مثال: /set_range سوره 1 آیه 1 تا سوره 2 آیه 10)
/min 1 - حداقل آیات برای هر نفر
/max 20 - حداکثر آیات برای هر نفر
-
"""
        await update.message.reply_text(help_text, parse_mode="Markdown")
        logger.info(f"Help command executed by user_id={update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in help command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command to activate the bot."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.warning("Start command received in non-group chat")
            return
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /start")
            await update.message.reply_text("لطفاً من را مدیر کنید.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM groups WHERE group_id = ?", (group_id,))
            group = cursor.fetchone()
            if not group:
                cursor.execute(
                    "INSERT INTO groups (group_id, is_active) VALUES (?, 1)",
                    (group_id,)
                )
            else:
                cursor.execute(
                    "UPDATE groups SET is_active = 1 WHERE group_id = ?",
                    (group_id,)
                )
            conn.commit()
            logger.info(f"Bot activated for group_id={group_id}")

        is_topic_enabled = bool(update.message.message_thread_id)
        if is_topic_enabled:
            await update.message.reply_text("گروه تاپیک‌دار است. لطفاً تاپیک‌ها را با /topic تنظیم کنید.")
        else:
            await update.message.reply_text("گروه بدون تاپیک است. حالت نو تاپیک فعال شد.")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM topics WHERE group_id = ? AND topic_id = ?",
                (group_id, group_id)
            )
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                    (group_id, group_id, "اصلی", "salavat")
                )
                conn.commit()
                logger.info(f"Default topic created for group_id={group_id}")

        await update.message.reply_text("ربات ثبت شد.")
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command to deactivate the bot."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /stop")
            await update.message.reply_text("فقط ادمین می‌تواند ربات را متوقف کند.")
            return

        group_id = update.effective_chat.id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE groups SET is_active = 0 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Bot deactivated for group_id={group_id}")

        await update.message.reply_text("ربات خاموش شد.")
    except Exception as e:
        logger.error(f"Error in stop command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /topic command to set topic name."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted /topic")
            await update.message.reply_text("فقط ادمین می‌تواند تاپیک تنظیم کند.")
            return

        if not context.args:
            logger.warning("Topic command called without arguments")
            await update.message.reply_text("لطفاً نام تاپیک را وارد کنید. مثال: /topic 1")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id
        topic_name = " ".join(context.args)

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                (topic_id, group_id, topic_name, "salavat")
            )
            cursor.execute(
                "UPDATE groups SET is_topic_enabled = 1 WHERE group_id = ?",
                (group_id,)
            )
            conn.commit()
            logger.info(f"Topic set: topic_id={topic_id}, name={topic_name}")

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
        logger.error(f"Error in topic command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def khatm_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle khatm type selection from inline buttons."""
    try:
        query = update.callback_query
        await query.answer()

        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted khatm_selection")
            await query.message.reply_text("فقط ادمین می‌تواند نوع ختم را تنظیم کند.")
            return

        group_id = update.effective_chat.id
        topic_id = query.message.message_thread_id or group_id
        khatm_type = query.data.replace("khatm_", "")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET khatm_type = ? WHERE topic_id = ? AND group_id = ?",
                (khatm_type, topic_id, group_id)
            )
            conn.commit()
            logger.info(f"Khatm type set: topic_id={topic_id}, type={khatm_type}")

        if khatm_type == "zekr":
            await query.message.reply_text("ذکر مورد نظر خود را ارسال کنید.")
            context.user_data["awaiting_zekr"] = {"topic_id": topic_id, "group_id": group_id}
        elif khatm_type == "ghoran":
            await query.message.reply_text(
                "ختم قرآن فعال شد. لطفاً محدوده آیات را با /set_range تنظیم کنید (مثال: /set_range سوره 1 آیه 1 تا سوره 2 آیه 10).\n"
                "بسم‌الله به‌عنوان آیه اول هر سوره (به جز سوره توبه) شمرده می‌شود."
            )
        else:
            await query.message.reply_text(f"ختم فعال: {khatm_type.capitalize()}")
    except Exception as e:
        logger.error(f"Error in khatm_selection: {e}")
        await query.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_zekr_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle zekr text input after selecting zekr khatm."""
    try:
        if "awaiting_zekr" not in context.user_data:
            logger.debug("No awaiting zekr data found")
            return

        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted set_zekr_text")
            await update.message.reply_text("فقط ادمین می‌تواند ذکر را تنظیم کند.")
            return

        zekr_data = context.user_data.pop("awaiting_zekr")
        zekr_text = update.message.text.strip()

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE topics SET zekr_text = ? WHERE topic_id = ? AND group_id = ?",
                (zekr_text, zekr_data["topic_id"], zekr_data["group_id"])
            )
            conn.commit()
            logger.info(f"Zekr text set: topic_id={zekr_data['topic_id']}, text={zekr_text}")

        await update.message.reply_text(f"ختم فعال: ذکر {zekr_text}")
    except Exception as e:
        logger.error(f"Error in set_zekr_text: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the verse range for a Quran khatm."""
    try:
        if not update.message or not update.message.text:
            logger.debug("No message text for set_range")
            return

        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted set_range")
            await update.message.reply_text("فقط ادمین‌ها می‌توانند محدوده ختم را تنظیم کنند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        # Parse command (e.g., "/set_range سوره 1 آیه 1 تا سوره 2 آیه 10")
        text = update.message.text.strip()
        try:
            parts = text.split()
            start_surah = int(parts[parts.index("سوره") + 1])
            start_ayah = int(parts[parts.index("آیه") + 1])
            end_surah = int(parts[parts.index("سوره", parts.index("تا")) + 1])
            end_ayah = int(parts[parts.index("آیه", parts.index("تا")) + 1])

            start_verse = quran.get_verse(start_surah, start_ayah)
            end_verse = quran.get_verse(end_surah, end_ayah)
            if not start_verse or not end_verse:
                logger.debug(f"Invalid verses: start={start_surah}:{start_ayah}, end={end_surah}:{end_ayah}")
                await update.message.reply_text("آیات نامعتبر هستند.")
                return

            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT sfOR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                    (group_id, topic_id, start_verse['id'], end_verse['id'])
                )
                conn.commit()
                logger.info(f"Khatm range set: group_id={group_id}, topic_id={topic_id}, range={start_verse['id']}-{end_verse['id']}")

            await update.message.reply_text(
                f"محدوده ختم تنظیم شد: از {start_verse['surah_name']} آیه {start_ayah} تا {end_verse['surah_name']} آیه {end_ayah}\n"
                "بسم‌الله به‌عنوان آیه اول هر سوره (به جز سوره توبه) شمرده می‌شود."
            )
        except (ValueError, IndexError):
            logger.debug(f"Invalid set_range format: {text}")
            await update.message.reply_text("لطفاً محدوده را به شکل صحیح وارد کنید (مثل '/set_range سوره 1 آیه 1 تا سوره 2 آیه 10').")
    except Exception as e:
        logger.error(f"Error in set_range command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if the user is an admin."""
    try:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        admins = await context.bot.get_chat_administrators(chat_id)
        is_admin = any(admin.user.id == user_id for admin in admins)
        logger.debug(f"Admin check: user_id={user_id}, is_admin={is_admin}")
        return is_admin
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False