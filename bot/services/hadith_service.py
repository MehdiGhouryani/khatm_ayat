import random
import logging
import re
from telegram.ext import ContextTypes
from config.settings import HADITH_CHANNEL

logger = logging.getLogger(__name__)

async def get_random_hadith(context: ContextTypes.DEFAULT_TYPE):
    """Get a random hadith post from the channel."""
    try:
        post_count = 100  # Placeholder: replace with actual post count
        message_id = random.randint(1, post_count)
        messages = await context.bot.get_chat_history(HADITH_CHANNEL, limit=1, offset_id=message_id)
        if messages and messages[0].text:
            text = messages[0].text
            text = re.sub(r"http[s]?://\S+", "", text)
            logger.info(f"Hadith retrieved: {text[:50]}...")
            return text.strip()
        logger.warning("No valid hadith text found")
        return "حدیث امروز: یا علی، به یاد خدا باشید."
    except Exception as e:
        logger.error(f"Failed to fetch hadith: {e}")
        return None