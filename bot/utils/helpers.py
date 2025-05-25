import re
import random
import logging
from typing import Optional, List, Dict, Union, Tuple, TYPE_CHECKING
from telegram.ext import ContextTypes
from bot.utils.quran import QuranManager
from bot.database.db import fetch_all, fetch_one
import datetime
from functools import wraps
from telegram import Update

if TYPE_CHECKING:
    from telegram import Message

logger = logging.getLogger(__name__)

quran = QuranManager()

def parse_number(text):
    try:
        text = text.strip().replace("٫", ".").replace(",", "")
        persian_digits = "۰۱۲۳۴۵۶۷۸۹"
        english_digits = "0123456789"
        for p, e in zip(persian_digits, english_digits):
            text = text.replace(p, e)
        number = float(text)
        if number.is_integer():
            number = int(number)
        return number
    except (ValueError, TypeError):
        return None

async def get_random_sepas(group_id):
    try:
        texts = await fetch_all(
            "SELECT text FROM sepas_texts WHERE group_id = ? OR is_default = 1",
            (group_id,)
        )
        texts = [row["text"] for row in texts]
        if not texts:
            return ""
        return random.choice(texts)
    except Exception as e:
        logger.error(f"Failed to get sepas text: {e}")
        return ""

def format_user_link(user_id, username, first_name):
    try:
        name = username.lstrip('@') if username and username.strip() else (first_name or f"کاربر {user_id}")
        link = f"[{name}](tg://user?id={user_id})"
        return link
    except Exception:
        return f"کاربر {user_id}"
    

from typing import Optional, List, Dict, Union
import html
def escape_html(text: str) -> str:
    if not text:
        return ""
    return html.escape(str(text))


async def format_khatm_message(
    khatm_type: str,
    previous_total: int,
    amount: int,
    new_total: int,
    sepas_text: str,
    group_id: int,
    zekr_text: Optional[str] = None,
    verses: Optional[List[Dict]] = None,
    max_display_verses: int = 10,
    completion_count: int = 0
) -> Union[str, List[str]]:
    try:
        separator = "➖➖➖➖➖➖➖➖➖➖"
        final_sepas = f"{escape_html(sepas_text)} 🌱" if sepas_text else ""

        if khatm_type == "ghoran":
            if not verses:
                return ["<b>خطا: اطلاعات آیات موجود نیست.</b> 🌱"]

            processed_verse_count = amount
            if amount < 0:
                processed_verse_count = abs(amount)

            header = f"📖 <b>{processed_verse_count} آیه ثبت شد !</b>"
            if amount < 0:
                header = f"📖 <b>{processed_verse_count} آیه کسر شد !</b>"

            parts = [header]
            if verses:
                current_surah_name = escape_html(verses[0].get('surah_name', 'نامشخص'))
                juz_number = escape_html(str(verses[0].get('juz_number', 'نامشخص')))
                page_number = escape_html(str(verses[0].get('page_number', 'نامشخص')))
                # محاسبه درصد پیشرفت
                range_result = await fetch_one(
                    "SELECT start_verse_id, end_verse_id FROM khatm_ranges WHERE group_id = ?",
                    (group_id,)
                )
                if range_result:
                    start_verse_id = range_result["start_verse_id"]
                    end_verse_id = range_result["end_verse_id"]
                    current_verse_id = verses[-1].get('id', start_verse_id)
                    total_verses = end_verse_id - start_verse_id
                    progress = ((current_verse_id - start_verse_id) / total_verses * 100) if total_verses > 0 else 0
                    progress_text = f"{int(progress)}"
                else:
                    total_verses = 6236  # تعداد کل آیات قرآن
                    current_verse_id = verses[-1].get('id', 1)
                    progress = ((current_verse_id - 1) / total_verses * 100) if total_verses > 0 else 0
                    progress_text = f"{int(progress)}"

                # افزودن هدر پیام
                parts.extend([
                    f"<b>نام سوره فعلی : {current_surah_name}</b>",
                    f"<b>جزء : {juz_number} | صفحه : {page_number}</b>",
                    f"<b>تعداد ختم قرآن انجام شده : {completion_count}</b>",
                    f"<b>پیشرفت ختم : {progress_text}% قران خوانده شده</b>",
                    separator,
                    "<b>اعوذ بالله من الشیطان الرجیم</b>",
                    ""
                ])

            verses_to_display = verses[:max_display_verses]

            messages = []
            current_message_parts = parts.copy()
            current_verse_group = []
            current_chars = len("\n".join(parts))
            max_telegram_chars = 3800

            # ردیابی سوره فعلی برای مدیریت بسم‌الله
            current_surah_number = None

            for v_idx, v in enumerate(verses_to_display):
                verse_surah_number = v.get('surah_number', 0)
                verse_no_in_surah = str(v.get('ayah_number', '')) if v.get('ayah_number') is not None else ''
                text = escape_html(v.get('text', 'متن آیه موجود نیست'))
                translation = escape_html(v.get('translation', 'ترجمه موجود نیست'))

                # بررسی تغییر سوره
                if verse_surah_number != current_surah_number:
                    # اگر سوره جدید است و بسم‌الله دارد (به جز سوره 9)
                    if verse_surah_number != 9 and v.get('bismillah'):
                        bismillah_text = f"🔹<b>{v.get('bismillah', '')}</b>🔹\n"
                        bismillah_chars = len(bismillah_text)+ 3  # +3 برای خطوط جدید

                        # بررسی محدودیت کاراکتر
                        if current_chars + bismillah_chars > max_telegram_chars:
                            if current_verse_group:
                                current_message_parts.extend(current_verse_group)
                                if v_idx < len(verses_to_display):
                                    current_message_parts.append(separator)
                                    current_message_parts.append("... (ادامه آیات در پیام بعدی)")
                                messages.append("\n".join(current_message_parts))
                                current_message_parts = [f"<b>ادامه آیات :</b>", separator]
                                current_verse_group = []
                                current_chars = len("\n".join(current_message_parts))

                        current_verse_group.extend([bismillah_text, ""])
                        current_chars += bismillah_chars
                    current_surah_number = verse_surah_number

                # آماده‌سازی متن آیه و ترجمه
                verse_text = f"▫️<b>آیه {verse_no_in_surah} : {text}</b>"
                translation_text = f"{translation}"
                verse_chars = len(verse_text) + len(translation_text) + 2  # +2 برای خطوط جدید

                # بررسی محدودیت کاراکتر
                if current_chars + verse_chars > max_telegram_chars:
                    if current_verse_group:
                        current_message_parts.extend(current_verse_group)
                        if v_idx < len(verses_to_display):
                            current_message_parts.append(separator)
                            current_message_parts.append("... (ادامه آیات در پیام بعدی)")
                        messages.append("\n".join(current_message_parts))
                        current_message_parts = [f"<b>ادامه آیات:</b>", separator]
                        current_verse_group = []
                        current_chars = len("\n".join(current_message_parts))

                current_verse_group.extend([verse_text, translation_text])
                if v_idx < len(verses_to_display) - 1:
                    current_verse_group.append("")  # خط خالی بین آیات
                current_chars += verse_chars

            # اضافه کردن آیات باقی‌مانده
            if current_verse_group:
                current_message_parts.extend(current_verse_group)
                if amount > max_display_verses:
                    current_message_parts.append(separator)
                    current_message_parts.append("توجه: آیات ارسالی شما از محدوده تعیین‌شده بیشتر است.")
                current_message_parts.append(separator)
                if final_sepas:
                    current_message_parts.append(f"<b>{final_sepas}</b>")
                else:
                    current_message_parts.append("🌱 <b>التماس دعا</b> 🌱")
                messages.append("\n".join(current_message_parts))

            # اگر هیچ پیامی تولید نشده باشد
            if not messages:
                parts.append("هیچ آیه‌ای برای نمایش وجود ندارد.")
                parts.append(separator)
                if final_sepas:
                    parts.append(f"<b>{final_sepas}</b>")
                else:
                    parts.append("🌱 <b>التماس دعا</b> 🌱")
                messages.append("\n".join(parts))

            return messages

        elif khatm_type == "salavat":
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"<b>{abs_amount:,} صلوات {action_text}!</b>",
                f"<b>جمع کل:</b> {new_total:,} صلوات\n"
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("🌱 <b>التماس دعا</b> 🌱")
            message = "\n".join(message_parts)
            return [message]
        elif khatm_type == "zekr":
            if not zekr_text:
                return ["<b>خطا: متن ذکر مشخص نشده است.</b> 🌱"]
            txt_vasat = 'مورد'
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"<b>ذکر :</b> {zekr_text}",
                f"<b>{abs_amount:,} {txt_vasat} {action_text}!</b>",
                f"<b>جمع کل:</b> {new_total:,}"
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("...")
            message = "\n".join(message_parts)
            return [message]

        else:
            return ["<b>خطا: نوع ختم نامعتبر است.</b> 🌱"]

    except Exception as e:
        logger.error(f"Error formatting khatm message: {e}", exc_info=True)
        return ["<b>خطا در تولید پیام ختم.</b> 🌱"]


async def _delete_bot_message_job(context: "ContextTypes.DEFAULT_TYPE"):
    """Deletes a message sent by the bot."""
    job = context.job
    chat_id = job.data.get("chat_id")
    message_id = job.data.get("message_id")
    
    if not chat_id or not message_id:
        logger.warning("Missing chat_id or message_id in _delete_bot_message_job data: %s", job.data)
        return
        
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Successfully deleted bot message {message_id} from chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete bot message {message_id} from chat {chat_id}: {e}", exc_info=True)

async def schedule_message_deletion(context: "ContextTypes.DEFAULT_TYPE", chat_id: int, message_id: int):
    """Checks group settings and schedules a job to delete the bot's message if needed."""
    if not context.job_queue:
        logger.warning("Job queue not found in context, cannot schedule message deletion.")
        return

    try:
        group_settings = await fetch_one("SELECT delete_after FROM groups WHERE group_id = ?", (chat_id,))
        
        if group_settings and group_settings.get("delete_after") and group_settings["delete_after"] > 0:
            delay_minutes = group_settings["delete_after"]
            job_data = {"chat_id": chat_id, "message_id": message_id}
            context.job_queue.run_once(_delete_bot_message_job, delay_minutes * 60, data=job_data, name=f"delete_msg_{chat_id}_{message_id}")
            logger.info(f"Scheduled deletion for message {message_id} in chat {chat_id} after {delay_minutes} minutes.")
    except Exception as e:
        logger.error(f"Error scheduling message deletion for chat {chat_id}, message {message_id}: {e}", exc_info=True)

async def reply_text_and_schedule_deletion(update: "Update", context: "ContextTypes.DEFAULT_TYPE", text: str, **kwargs) -> "Optional[Message]":
    """Sends a reply message and schedules its deletion if configured for the group."""
    sent_message = None
    try:
        sent_message = await update.message.reply_text(text, **kwargs)
        if sent_message and update.effective_chat:
            await schedule_message_deletion(context, update.effective_chat.id, sent_message.message_id)
        return sent_message
    except Exception as e:
        logger.error(f"Error in reply_text_and_schedule_deletion: {e}", exc_info=True)
        # Attempt to send a generic error message if the original reply failed, and schedule IT for deletion
        if update.effective_chat:
            try:
                error_reply = await update.message.reply_text("خطایی در ارسال پیام رخ داد.")
                if error_reply:
                    await schedule_message_deletion(context, update.effective_chat.id, error_reply.message_id)
            except Exception as e_reply:
                logger.error(f"Error sending generic error reply: {e_reply}", exc_info=True)
        return sent_message # Return original sent_message which might be None

async def send_message_and_schedule_deletion(context: "ContextTypes.DEFAULT_TYPE", chat_id: int, text: str, **kwargs) -> "Optional[Message]":
    """Sends a message and schedules its deletion if configured for the group."""
    sent_message = None
    try:
        sent_message = await context.bot.send_message(chat_id, text, **kwargs)
        if sent_message:
            await schedule_message_deletion(context, chat_id, sent_message.message_id)
        return sent_message
    except Exception as e:
        logger.error(f"Error in send_message_and_schedule_deletion for chat {chat_id}: {e}", exc_info=True)
        # Attempt to send a generic error message to the chat if the original send failed
        try:
            error_reply = await context.bot.send_message(chat_id, "خطایی در ارسال پیام رخ داد.")
            if error_reply:
                await schedule_message_deletion(context, chat_id, error_reply.message_id)
        except Exception as e_reply:
            logger.error(f"Error sending generic error reply to chat {chat_id}: {e_reply}", exc_info=True)
        return sent_message # Return original sent_message which might be None

def ignore_old_messages(max_age_minutes=2):
    """
    Decorator to ignore messages older than specified minutes
    to prevent processing backlog messages when bot restarts
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            if not update.message:
                return await func(update, context, *args, **kwargs)
            
            current_utc_time = datetime.datetime.now(datetime.timezone.utc)
            message_age = current_utc_time - update.message.date
            
            if message_age > datetime.timedelta(minutes=max_age_minutes):
                logger.info(
                    f"Ignoring old message/command in handler {func.__name__} from {update.effective_user.id} "
                    f"(age: {message_age.total_seconds() / 60:.2f} minutes)"
                )
                return None
            
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator