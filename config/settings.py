import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

def load_settings():
    """Load environment variables with validation."""
    load_dotenv()
    try:
        settings = {
            "TELEGRAM_TOKEN": os.getenv("TELEGRAM_TOKEN"),
            "DATABASE_PATH": os.getenv("DATABASE_PATH", "khatm_bot.db"),
            "HADITH_CHANNEL": os.getenv("HADITH_CHANNEL", "@HadithChannel")
        }
        if not settings["TELEGRAM_TOKEN"]:
            logger.error("TELEGRAM_TOKEN is not set in .env")
            raise ValueError("TELEGRAM_TOKEN is required")
        logger.info("Environment variables loaded successfully")
        return settings
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")
        raise

SETTINGS = load_settings()
TELEGRAM_TOKEN = SETTINGS["TELEGRAM_TOKEN"]
DATABASE_PATH = SETTINGS["DATABASE_PATH"]
HADITH_CHANNEL = SETTINGS["HADITH_CHANNEL"]