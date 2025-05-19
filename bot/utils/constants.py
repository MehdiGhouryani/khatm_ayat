import logging
from datetime import time
from bot.utils.quran import QuranManager


logger = logging.getLogger(__name__)

TOTAL_QURAN_VERSES = 6236

DEFAULT_SEPAS_TEXTS = [
    "الحمدلله",
    "خدا قبول کنه",
    "یا علی",
    "ممنون از مشارکتتون",
    "خدا خیرتون بده"
]
KHATM_TYPES = ["salavat", "zekr", "ghoran"]

DAILY_HADITH_TIME = time(hour=8, minute=0)
DAILY_RESET_TIME = time(hour=0, minute=0)
DAILY_PERIOD_RESET_TIME = time(hour=0, minute=5)
MIN_DELETE_MINUTES = 1
MAX_DELETE_MINUTES = 1440
HADITH_CLEAN_PATTERNS = [
    r'@[A-Za-z0-9_]+',
    r'(?:http[s]?://|t.me/)[^\s]+'
]

MAIN_GROUP_ID = -100123456789
MAX_MESSAGE_LENGTH = 4096
TAG_COOLDOWN_HOURS = 24
TAG_MESSAGE_DELAY = 2



quran = None

async def init_quran_manager():
    """Initialize the global QuranManager instance."""
    global quran
    try:
        quran = QuranManager()
        await quran.initialize()
        logger.info("QuranManager initialized with %d verses", len(quran.verses))
    except Exception as e:
        logger.error("Failed to initialize QuranManager: %s", e)
        raise