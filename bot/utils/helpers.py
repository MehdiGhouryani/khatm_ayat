import re
import random
import logging
import sqlite3
from typing import Optional, List, Dict
from bot.utils.quran import QuranManager
from bot.database.db import get_db_connection

logger = logging.getLogger(__name__)

quran = QuranManager()

def parse_number(text):
    """Parse Persian/English numbers from text."""
    try:
        text = text.strip().replace("٫", ".").replace(",", "")
        persian_digits = "۰۱۲۳۴۵۶۷۸۹"
        english_digits = "0123456789"
        for p, e in zip(persian_digits, english_digits):
            text = text.replace(p, e)
        number = float(text)
        if number.is_integer():
            number = int(number)
        logger.debug(f"Parsed number: {text} -> {number}")
        return number
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse number: {text}, error: {e}")
        return None

def get_random_sepas(group_id, db_conn):
    """Get a random sepas text for the group."""
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT text FROM sepas_texts WHERE group_id = ? OR is_default = 1",
            (group_id,)
        )
        texts = [row["text"] for row in cursor.fetchall()]
        if not texts:
            logger.warning(f"No sepas texts found for group_id={group_id}")
            return ""
        sepas = random.choice(texts)
        logger.debug(f"Selected sepas text: {sepas}")
        return sepas
    except sqlite3.Error as e:
        logger.error(f"Failed to get sepas text: {e}")
        return ""

def format_user_link(user_id, username, first_name):
    """Format user name as a Telegram hyperlink."""
    try:
        name = username.lstrip('@') if username and username.strip() else (first_name or f"کاربر {user_id}")
        link = f"[{name}](tg://user?id={user_id})"
        logger.debug(f"Formatted user link: {link}")
        return link
    except Exception as e:
        logger.error(f"Failed to format user link: {e}")
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
    """Format the khatm contribution message."""
    try:
        if khatm_type == "ghoran":
            if not verses:
                logger.warning("No verses provided for Quran khatm message")
                return "خطا: اطلاعات آیات موجود نیست."
            
            # Get current surah name from the first verse
            current_surah = verses[0]['surah_name']
            
            # Format verse texts with numbering
            verse_texts = []
            for idx, verse in enumerate(verses[:max_display_verses], 1):
                verse_text = verse.get('text', 'متن آیه موجود نیست')
                verse_texts.append(f"{idx}: {verse_text}")
            if len(verses) > max_display_verses:
                verse_texts.append("... (برای آیات بیشتر، محدوده را بررسی کنید)")
            
            message = (
                f"نام سوره فعلی: {current_surah}\n"
                f"تعداد‌ ختم قرآن انجام شده: {completion_count}\n"
                f"————————————-\n"
                f"تعداد آیه سهم شما: {amount} آیه\n"
                "\n".join(verse_texts) + "\n"
                f"————————————-\n"
            )
            if sepas_text:
                message += f"🌱 متن سپاس 🌱 {sepas_text}\n"
            logger.debug(f"Formatted Quran khatm message: {message}")
            return message

        elif khatm_type == "salavat":
            message = (
                f"🙏 *{amount} صلوات* ثبت شد!\n"
                f"جمع کل: {new_total} صلوات\n"
            )
            if sepas_text:
                message += f"🌱 متن سپاس 🌱 {sepas_text}\n"
            logger.debug(f"Formatted salavat khatm message: {message}")
            return message

        elif khatm_type == "zekr":
            if not zekr_text:
                logger.warning("No zekr text provided for zekr khatm message")
                return "خطا: متن ذکر مشخص نشده است."
            message = (
                f"📿 *{amount} {zekr_text}* ثبت شد!\n"
                f"جمع کل: {new_total} {zekr_text}\n"
            )
            if sepas_text:
                message += f"🌱 متن سپاس 🌱 {sepas_text}\n"
            logger.debug(f"Formatted zekr khatm message: {message}")
            return message

        else:
            logger.error(f"Unknown khatm type: {khatm_type}")
            return "خطا: نوع ختم نامعتبر است."

    except Exception as e:
        logger.error(f"Error formatting khatm message: {e}", exc_info=True)
        return "خطا در تولید پیام ختم."