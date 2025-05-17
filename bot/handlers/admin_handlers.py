import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, filters, ConversationHandler
from bot.database.db import get_db_connection
from bot.utils.constants import KHATM_TYPES
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

quran = QuranManager()

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command to show command guide."""
    try:
        help_text = """
<b>راهنمای دستورات ربات</b>

<b>1. فعال‌سازی و توقف ربات:</b>
- /start: فعال کردن ربات در گروه
- /stop: غیرفعال کردن ربات

<b>2. تنظیم نوع ختم:</b>
- /khatm_ghoran: شروع ختم قرآن (پیش‌فرض: کل قرآن)
- /khatm_salavat: شروع ختم صلوات (نیاز به تعیین تعداد)
- /khatm_zekr: شروع ختم ذکر (نیاز به تعیین متن ذکر)
- /set_range: تنظیم محدوده ختم قرآن (مثال: سوره 1 آیه 1 تا سوره 2 آیه 10)

<b>3. مدیریت ختم:</b>
- /number 14000: تنظیم تعداد برای ختم صلوات/ذکر (مثال: 14000 صلوات)
- /reset_number_on: ریست خودکار پس از تکمیل تعداد
- /reset_number_off: غیرفعال کردن ریست خودکار
- /reset_on: ریست خودکار آمار هر 24 ساعت
- /reset_off: غیرفعال کردن ریست 24 ساعته
- /stop_on 5000: توقف ختم در تعداد مشخص (مثال: 5000)
- /stop_on_off: غیرفعال کردن توقف

<b>4. محدودیت‌ها:</b>
- /max 1000: تنظیم حداکثر تعداد (مثال: 1000 صلوات یا آیه)
- /max_off: غیرفعال کردن حداکثر
- /min 10: تنظیم حداقل تعداد (مثال: 10 صلوات یا آیه)
- /min_off: غیرفعال کردن حداقل
- /set_max_verses 10: تنظیم حداکثر تعداد آیات نمایش‌داده‌شده (مثال: 10 آیه)
- /lock_on: قفل پیام‌ها (فقط اعداد یا آیات)
- /lock_off: غیرفعال کردن قفل

<b>5. پیام‌ها و متن‌ها:</b>
- /sepas_on: فعال کردن متن‌های سپاس
- /sepas_off: غیرفعال کردن متن‌های سپاس
- /addsepas [متن]: افزودن متن سپاس (مثال: /addsepas یا علی)
- /set_completion_message [متن]: تنظیم پیام تبریک (مثال: /set_completion_message تبریک! ختم کامل شد)
- /jam_on: نمایش جمع کل در پیام‌ها
- /jam_off: غیرفعال کردن نمایش جمع کل

<b>6. آمار و رتبه‌بندی:</b>
- /amar_kol: نمایش آمار کل ختم
- /amar_list: نمایش رتبه‌بندی مشارکت‌کنندگان

<b>7. ریست آمار:</b>
- /reset_zekr: ریست آمار صلوات و ذکر
- /reset_kol: ریست کل آمار (صلوات، ذکر، آیات)

<b>8. حدیث روزانه:</b>
- /hadis_on: فعال کردن حدیث روزانه
- /hadis_off: غیرفعال کردن حدیث روزانه

<b>9. پاک‌سازی و توقف:</b>
- /time_off 23-08: توقف ساعتی (مثال: 11 شب تا 8 صبح)
- /delete_on 01: پاک‌سازی پیام‌ها پس از 1 دقیقه
- /delete_off: غیرفعال کردن پاک‌سازی

<b>10. تنظیمات تاپیک:</b>
- /topic 1: تنظیم نام تاپیک (مثال: تاپیک 1)
"""
        await update.message.reply_text(help_text, parse_mode="HTML")
        logger.info(f"Help command executed by user_id={update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in help command: {e}")
        await update.message.reply_text("⚠️ خطایی رخ داد؛ لطفاً دوباره تلاش کنید.")

async def set_max_verses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the maximum number of verses to display."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted set_max_verses")
            await update.message.reply_text("فقط ادمین می‌تواند حداکثر آیات نمایش را تنظیم کند.")
            return

        if not context.args:
            logger.warning("set_max_verses command called without arguments")
            await update.message.reply_text("لطفاً تعداد حداکثر آیات را وارد کنید. مثال: /set_max_verses 10")
            return

        group_id = update.effective_chat.id
        try:
            max_verses = int(context.args[0])
            if max_verses <= 0:
                raise ValueError("تعداد باید مثبت باشد.")
            if max_verses > 100:
                raise ValueError("حداکثر تعداد آیات نمایش نمی‌تواند بیشتر از 100 باشد.")

            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE groups SET max_display_verses = ? WHERE group_id = ?",
                    (max_verses, group_id)
                )
                conn.commit()
                logger.info(f"Max display verses set: group_id={group_id}, max_verses={max_verses}")

            await update.message.reply_text(f"حداکثر تعداد آیات نمایش به {max_verses} تنظیم شد.")
        except ValueError as e:
            logger.warning(f"Invalid max_verses input: {context.args[0]}")
            await update.message.reply_text(f"خطا: {str(e)}. لطفاً یک عدد معتبر وارد کنید (مثال: /set_max_verses 10).")
    except Exception as e:
        logger.error(f"Error in set_max_verses command: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command to activate the bot."""
    try:
        if not update.effective_chat or update.effective_chat.type not in ["group", "supergroup"]:
            logger.warning("Start command received in non-group chat")
            await update.message.reply_text("این دستور فقط در گروه‌ها قابل استفاده است.")
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
                    "INSERT INTO groups (group_id, is_active, max_display_verses) VALUES (?, 1, 10)",
                    (group_id,)
                )
                logger.info(f"Group inserted: group_id={group_id}")
            else:
                cursor.execute(
                    "UPDATE groups SET is_active = 1 WHERE group_id = ?",
                    (group_id,)
                )
                logger.info(f"Group updated: group_id={group_id}")
            cursor.execute(
                "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                (group_id, group_id, "اصلی", "salavat")
            )
            logger.info(f"Default topic created/updated for group_id={group_id}")
            conn.commit()

        is_topic_enabled = bool(update.message.message_thread_id)
        if is_topic_enabled:
            await update.message.reply_text("گروه تاپیک‌دار است. لطفاً تاپیک‌ها را با /topic تنظیم کنید.")
        else:
            await update.message.reply_text("گروه بدون تاپیک است. حالت بدون تاپیک فعال شد. می‌توانید ختم را با دستورات /khatm_zekr، /khatm_salavat یا /khatm_ghoran تنظیم کنید.")

        await update.message.reply_text("ربات با موفقیت فعال شد.")
        logger.info(f"Bot activated for group_id={group_id}")
    except Exception as e:
        logger.error(f"Error in start command: {e}", exc_info=True)
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
        is_topic_enabled = bool(update.message.message_thread_id)
        if not is_topic_enabled:
            await update.message.reply_text("این گروه از تاپیک‌ها پشتیبانی نمی‌کند. لطفاً از دستورات /khatm_zekr، /khatm_salavat یا /khatm_ghoran برای تنظیم ختم استفاده کنید.")
            return

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
            if khatm_type == "ghoran":
                start_verse = quran.get_verse(1, 1)
                end_verse = quran.get_verse(114, 6)
                cursor.execute(
                    "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                    (group_id, topic_id, start_verse['id'], end_verse['id'])
                )
                cursor.execute(
                    "UPDATE topics SET current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    (start_verse['id'], topic_id, group_id)
                )
            conn.commit()
            logger.info(f"Khatm type set: topic_id={topic_id}, type={khatm_type}")

        if khatm_type == "zekr":
            await query.message.reply_text("ذکر مورد نظر خود را ارسال کنید.")
            context.user_data["awaiting_zekr"] = {"topic_id": topic_id, "group_id": group_id}
        elif khatm_type == "ghoran":
            await query.message.reply_text(
                "ختم قرآن فعال شد (پیش‌فرض: کل قرآن). برای تغییر محدوده، از /set_range استفاده کنید.\n"
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
                    "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                    (group_id, topic_id, start_verse['id'], end_verse['id'])
                )
                cursor.execute(
                    "UPDATE topics SET khatm_type = ?, current_verse_id = ? WHERE topic_id = ? AND group_id = ?",
                    ("ghoran", start_verse['id'], topic_id, group_id)
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

async def start_khatm_zekr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start a zekr khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted start_khatm_zekr")
            await update.message.reply_text("فقط ادمین می‌تواند ختم ذکر را تنظیم کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                (topic_id, group_id, "اصلی", "zekr")
            )
            conn.commit()
            logger.info(f"Zekr khatm started: topic_id={topic_id}, group_id={group_id}")

        await update.message.reply_text("📿 لطفاً متن ذکر را وارد کنید (مثال: سبحان الله).")
        context.user_data["awaiting_zekr"] = {"topic_id": topic_id, "group_id": group_id}
        return 1  # ZEKR_STATE
    except Exception as e:
        logger.error(f"Error in start_khatm_zekr: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def start_khatm_salavat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start a salavat khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted start_khatm_salavat")
            await update.message.reply_text("فقط ادمین می‌تواند ختم صلوات را تنظیم کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type) VALUES (?, ?, ?, ?)",
                (topic_id, group_id, "اصلی", "salavat")
            )
            conn.commit()
            logger.info(f"Salavat khatm started: topic_id={topic_id}, group_id={group_id}")

        await update.message.reply_text("🙏 لطفاً تعداد صلوات برای ختم را وارد کنید (مثال: 14000).")
        context.user_data["awaiting_salavat"] = {"topic_id": topic_id, "group_id": group_id}
        return 2  # SALAVAT_STATE
    except Exception as e:
        logger.error(f"Error in start_khatm_salavat: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def start_khatm_ghoran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start a Quran khatm."""
    try:
        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted start_khatm_ghoran")
            await update.message.reply_text("فقط ادمین می‌تواند ختم قرآن را تنظیم کند.")
            return

        group_id = update.effective_chat.id
        topic_id = update.message.message_thread_id or group_id

        start_verse = quran.get_verse(1, 1)
        end_verse = quran.get_verse(114, 6)

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO topics (topic_id, group_id, name, khatm_type, current_verse_id) VALUES (?, ?, ?, ?, ?)",
                (topic_id, group_id, "اصلی", "ghoran", start_verse['id'])
            )
            cursor.execute(
                "INSERT OR REPLACE INTO khatm_ranges (group_id, topic_id, start_verse_id, end_verse_id) VALUES (?, ?, ?, ?)",
                (group_id, topic_id, start_verse['id'], end_verse['id'])
            )
            conn.commit()
            logger.info(f"Quran khatm started: topic_id={topic_id}, group_id={group_id}, range={start_verse['id']}-{end_verse['id']}")

        await update.message.reply_text(
            "📖 ختم قرآن فعال شد (پیش‌فرض: کل قرآن). برای تغییر محدوده، از /set_range استفاده کنید (مثال: سوره 1 آیه 1 تا سوره 2 آیه 10).\n"
            "بسم‌الله به‌عنوان آیه اول هر سوره (به جز سوره توبه) شمرده می‌شود."
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in start_khatm_ghoran: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

async def set_salavat_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the count for a salavat khatm."""
    try:
        if "awaiting_salavat" not in context.user_data:
            logger.debug("No awaiting salavat data found")
            return

        if not await is_admin(update, context):
            logger.warning(f"Non-admin user {update.effective_user.id} attempted set_salavat_count")
            await update.message.reply_text("فقط ادمین می‌تواند تعداد صلوات را تنظیم کند.")
            return

        salavat_data = context.user_data.pop("awaiting_salavat")
        group_id = salavat_data["group_id"]
        topic_id = salavat_data["topic_id"]
        user_id = update.effective_user.id

        try:
            count = int(update.message.text)
            if count <= 0:
                raise ValueError("تعداد باید مثبت باشد.")

            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE topics SET stop_number = ? WHERE topic_id = ? AND group_id = ?",
                    (count, topic_id, group_id)
                )
                conn.commit()
                logger.info(f"Salavat khatm set: topic_id={topic_id}, group_id={group_id}, count={count}")

            await update.message.reply_text(f"✅ ختم {count} صلوات تنظیم شد. ختم صلوات آغاز شد!")
            return ConversationHandler.END
        except ValueError:
            logger.warning(f"Invalid salavat count input by user {user_id} in group {group_id}")
            await update.message.reply_text("❌ لطفاً یک عدد معتبر وارد کنید (مثال: 14000).")
            return 2  # SALAVAT_STATE
    except Exception as e:
        logger.error(f"Error in set_salavat_count: {e}")
        await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        return 2  # SALAVAT_STATE

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