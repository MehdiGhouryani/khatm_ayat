import random
import logging
import re
from telegram.ext import ContextTypes
from config.settings import HADITH_CHANNEL

logger = logging.getLogger(__name__)

async def get_random_hadith(context: ContextTypes.DEFAULT_TYPE):
    try:
        # Use a safe range for message IDs (adjust based on channel size)
        max_post_count = 1000  # Should be dynamically fetched if possible
        message_id = random.randint(1, max_post_count)
        messages = await context.bot.get_chat_history(HADITH_CHANNEL, limit=1, offset_id=message_id)
        if messages and messages[0].text:
            text = messages[0].text
            text = re.sub(r"https?://\S+", "", text, flags=re.IGNORECASE)
            return text.strip()
        return "حدیث امروز: با یاد خدا آرامش یابید."
    except Exception as e:
        logger.error(f"Failed to fetch hadith: {e}")
        return "حدیث امروز: با یاد خدا آرامش یابید."