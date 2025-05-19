import re
import random
import logging
from typing import Optional, List, Dict
from bot.utils.quran import QuranManager
from bot.database.db import fetch_all

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
) -> str:
    try:
        if khatm_type == "ghoran":
            if not verses:
                return "خطا: اطلاعات آیات موجود نیست."
            
            current_surah = verses[0]['surah_name']
            parts = [
                f"نام سوره فعلی: {current_surah}",
                f"تعداد ختم قرآن انجام شده: {completion_count}",
                "———————————————————\n",
            ]
        
            for v in verses[:max_display_verses]:
                verse_no = v.get('id')
                text = v.get('text', 'متن آیه موجود نیست')
                parts.append(f"{verse_no}: {text}")
                parts.append("")

            if len(verses) > max_display_verses:
                parts.append("... (برای آیات بیشتر، محدوده را بررسی کنید)")
                parts.append("")
        
            if sepas_text:
                parts.append("———————————————————\n")
                parts.append(f"🌱 {sepas_text} 🌱")
        
            message = "\n".join(parts)
            return message
        
        elif khatm_type == "salavat":
            message = (
                f"🙏 *{amount} صلوات* ثبت شد!\n"
                f"جمع کل: {new_total} صلوات\n"
            )
            if sepas_text:
                message += f"🌱 {sepas_text} 🌱\n"
            return message

        elif khatm_type == "zekr":
            if not zekr_text:
                return "خطا: متن ذکر مشخص نشده است."
            message = (
                f"📿 *{amount} {zekr_text}* ثبت شد!\n"
                f"جمع کل: {new_total} {zekr_text}\n"
            )
            if sepas_text:
                message += f"🌱 {sepas_text} 🌱\n"
            return message

        else:
            return "خطا: نوع ختم نامعتبر است."

    except Exception as e:
        logger.error(f"Error formatting khatm message: {e}")
        return "خطا در تولید پیام ختم."