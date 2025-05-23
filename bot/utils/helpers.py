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

def format_khatm_message(
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
        separator = "➖➖➖➖➖➖➖➖➖➖➖"
        final_sepas = f" **{sepas_text}** 🌱" if sepas_text else ""

        if khatm_type == "ghoran":
            if not verses:
                return ["**خطا: اطلاعات آیات موجود نیست.** 🌱"]
            
            processed_verse_count = amount
            if amount < 0:
                processed_verse_count = abs(amount)
            
            header = f"**📖 {processed_verse_count} آیه ثبت شد!**"
            if amount < 0:
                header = f"**📖 {processed_verse_count} آیه کسر شد!**"

            parts = [header]
            if verses:
                current_surah_name = verses[0].get('surah_name', 'نامشخص')
                parts.extend([
                    f"**نام سوره فعلی:** {current_surah_name}",
                    f"**تعداد ختم قرآن انجام شده:** {completion_count}",
                    separator
                ])
            
                verses_to_display = verses[:max_display_verses]
                
                # حساب کردن تعداد کاراکترهای پیام تا اینجا
                initial_chars = len("\n".join(parts))
                
                messages = []
                current_message_parts = parts.copy()
                current_verse_group = []
                current_chars = initial_chars
                
                # مقدار تقریبی محدودیت کاراکتر تلگرام (با حاشیه امن)
                max_telegram_chars = 3800
                
                for v_idx, v in enumerate(verses_to_display):
                    verse_no_in_surah = str(v.get('ayah_number')) if v.get('ayah_number') is not None else ''
                    text = v.get('text', 'متن آیه موجود نیست')
                    verse_text = f"{verse_no_in_surah}: {text}"
                    
                    # اگر این آیه را اضافه کنیم، آیا از محدودیت تلگرام بیشتر می‌شود؟
                    verse_chars = len(verse_text) + 1  # +1 برای خط جدید
                    
                    if current_chars + verse_chars > max_telegram_chars:
                        # اگر این گروه آیه پر شد، آن را به پیام اضافه کنیم
                        if current_verse_group:
                            current_message_parts.extend(current_verse_group)
                            
                            # اضافه کردن متن توجه اگر آیات بیشتری وجود دارد
                            if v_idx < len(verses_to_display):
                                current_message_parts.append(separator)
                                current_message_parts.append("... (ادامه آیات در پیام بعدی)")
                            
                            # افزودن پیام به لیست پیام‌ها
                            messages.append("\n".join(current_message_parts))
                            
                            # شروع پیام جدید
                            current_message_parts = [f"**ادامه آیات:**", separator]
                            current_verse_group = []
                            current_chars = len("\n".join(current_message_parts))
                        
                    current_verse_group.append(verse_text)
                    if v_idx < len(verses_to_display) - 1:
                        current_verse_group.append("")  # خط خالی بین آیات
                    
                    current_chars += verse_chars
                
                # اضافه کردن آیات باقی‌مانده به آخرین پیام
                if current_verse_group:
                    current_message_parts.extend(current_verse_group)
                    
                    if amount > max_display_verses:
                        current_message_parts.append(separator)
                        current_message_parts.append("توجه: آیات ارسالی شما از محدوده تعیین‌شده بیشتر بوده است.")
                                        # اضافه کردن متن تشکر یا التماس دعا
                    current_message_parts.append(separator)
                    if final_sepas:
                        current_message_parts.append(final_sepas)
                    else:
                        current_message_parts.append("🌱 **التماس دعا** 🌱")
                    
                    messages.append("\n".join(current_message_parts))
                
                # اگر هیچ پیامی نداریم (که معمولاً نباید اتفاق بیفتد)
                if not messages:
                    # ساخت پیام پیش‌فرض
                    parts.append("هیچ آیه‌ای برای نمایش وجود ندارد.")
                    parts.append(separator)
                    if final_sepas:
                        parts.append(final_sepas)
                    else:
                        parts.append("🌱 **التماس دعا** 🌱")
                    messages.append("\n".join(parts))
                
                return messages
            
            # در صورتی که آیات وجود ندارند
            parts.append("اطلاعات آیات برای نمایش موجود نیست.")
            parts.append(separator)
            if final_sepas:
                parts.append(final_sepas)
            else:
                parts.append("🌱 **التماس دعا** 🌱")
            
            return ["\n".join(parts)]
        
        elif khatm_type == "salavat":
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"**{abs_amount:,} صلوات {action_text}!**",  
                f"**جمع کل:** {new_total:,} صلوات\n"        
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("🌱 **التماس دعا** 🌱")
            message = "\n".join(message_parts)
            return [message]
        elif khatm_type == "zekr":
            if not zekr_text:
                return ["**خطا: متن ذکر مشخص نشده است.** 🌱"]
            txt_vasat='مورد'
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"**ذکر :** {zekr_text}",
                f"**{abs_amount:,} {txt_vasat} {action_text}!**", 
                f"**جمع کل:** {new_total:,}"                   
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("🌱 **التماس دعا** 🌱")
            message = "\n".join(message_parts)
            return [message]

        else:
            return ["**خطا: نوع ختم نامعتبر است.** 🌱"]

    except Exception as e:
        logger.error(f"Error formatting khatm message: {e}", exc_info=True)
        return ["**خطا در تولید پیام ختم.** 🌱"]

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