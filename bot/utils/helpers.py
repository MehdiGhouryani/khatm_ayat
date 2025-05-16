import re
import random
import logging
import sqlite3
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

def format_khatm_message(khatm_type, previous_total, number, new_total, sepas_text, group_id, zekr_text=None, verse_id=None):
    """Format khatm response message."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT show_total, sepas_enabled FROM groups WHERE group_id = ?", (group_id,))
            group = cursor.fetchone()
            show_total = group['show_total'] if group else 0
            sepas_enabled = group['sepas_enabled'] if group else 1

        if khatm_type == "ghoran":
            if verse_id is None:
                logger.error("verse_id is required for Quran khatm message")
                return "خطا در نمایش پیام ختم"
            verse = quran.get_verse_by_id(verse_id)
            if not verse:
                logger.error(f"Verse not found: verse_id={verse_id}")
                return "خطا در نمایش پیام ختم"
            message = (
                f"مشارکت ثبت شد: {verse['surah_name']}، آیه {verse['ayah_number']}\n"
                f"متن: {verse['text']}\n"
            )
            if show_total:
                message += f"مجموع آیات: {new_total}\n"
            if sepas_enabled and sepas_text:
                message += f"{sepas_text}"
        elif khatm_type == "zekr":
            message = (
                f"از {previous_total} {zekr_text or 'ذکر'}، {number} {zekr_text or 'ذکر'} گفته شد.\n"
            )
            if show_total:
                message += f"جمع: {new_total}\n"
            if sepas_enabled and sepas_text:
                message += f"{sepas_text}"
        else:  # salavat
            message = (
                f"از {previous_total} صلوات، {number} صلوات فرستاده شد.\n"
            )
            if show_total:
                message += f"جمع: {new_total}\n"
            if sepas_enabled and sepas_text:
                message += f"{sepas_text}"
        logger.debug(f"Formatted khatm message: {message}")
        return message
    except Exception as e:
        logger.error(f"Failed to format khatm message: {e}")
        return "خطا در نمایش پیام ختم"

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