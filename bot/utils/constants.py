import logging

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

def load_constants():
    """Load and validate constants."""
    try:
        if not DEFAULT_SEPAS_TEXTS:
            logger.warning("DEFAULT_SEPAS_TEXTS is empty")
        if TOTAL_QURAN_VERSES <= 0:
            logger.error("TOTAL_QURAN_VERSES must be positive")
            raise ValueError("Invalid TOTAL_QURAN_VERSES")
        logger.info("Constants loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load constants: {e}")
        raise

load_constants()