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
        separator = "➖➖➖➖➖➖➖➖➖➖➖"
        final_sepas = f" **{sepas_text}** 🌱" if sepas_text else ""

        if khatm_type == "ghoran":
            if not verses:
                return "**خطا: اطلاعات آیات موجود نیست.** 🌱"
            
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
                for v_idx, v in enumerate(verses_to_display):
                    verse_no_in_surah = str(v.get('ayah_number')) if v.get('ayah_number') is not None else ''
                    text = v.get('text', 'متن آیه موجود نیست')
                    parts.append(f"{verse_no_in_surah}: {text}")
                    
                    if v_idx < len(verses_to_display) - 1:
                        parts.append("")

                # Log values for debugging the attention message condition
                logger.debug(f"Attention message debug: amount={amount}, len(verses_to_display)={len(verses_to_display)}, max_display_verses={max_display_verses}, verses_list_length={len(verses) if verses else 0}")

                if amount > len(verses_to_display) and amount > max_display_verses:
                    parts.append(separator)
                    parts.append(f"**توجه:** {len(verses_to_display)} آیه از {amount} آیه خوانده شده نمایش داده شد. (حداکثر {max_display_verses} آیه برای نمایش)")
                elif len(verses) > max_display_verses:
                    parts.append("... (ادامه آیات)")
            
            if final_sepas:
                parts.append(separator)
                parts.append(final_sepas)
            else:
                parts.append(separator)
                parts.append("🌱 **التماس دعا** 🌱")

            message = "\n".join(parts)
            return message
        
        elif khatm_type == "salavat":
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"**🙏 {abs_amount} صلوات {action_text}!**",
                f"**جمع کل:** {new_total} صلوات\n"
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("🌱 **التماس دعا** 🌱")
            message = "\n".join(message_parts)
            return message

        elif khatm_type == "zekr":
            if not zekr_text:
                return "**خطا: متن ذکر مشخص نشده است.** 🌱"
            txt_vasat='مورد'
            action_text = "ثبت شد" if amount >= 0 else "کسر شد"
            abs_amount = abs(amount)
            message_parts = [
                f"**ذکر :** {zekr_text}\n",
                f"**📿 {abs_amount} {txt_vasat} {action_text}!**\n",
                f"**جمع کل:** {new_total}\n"
            ]
            if final_sepas:
                message_parts.append(separator)
                message_parts.append(final_sepas)
            else:
                message_parts.append(separator)
                message_parts.append("🌱 **التماس دعا** 🌱")
            message = "\n".join(message_parts)
            return message

        else:
            return "**خطا: نوع ختم نامعتبر است.** 🌱"

    except Exception as e:
        logger.error(f"Error formatting khatm message: {e}", exc_info=True)
        return "**خطا در تولید پیام ختم.** 🌱"