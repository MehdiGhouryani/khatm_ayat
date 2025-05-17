import sqlite3
import logging
from config.settings import DATABASE_PATH

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection."""
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        logger.debug("Database connection established")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def init_db():
    """Initialize the database schema."""
    try:
        with get_db_connection() as conn:
            with open("bot/database/schema.sql", "r") as f:
                conn.executescript(f.read())
            # Ensure max_display_verses and is_active are added to existing tables
            cursor = conn.cursor()
            conn.commit()
            logger.info("Database schema initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Failed to initialize database: {e}")
        # Ignore if columns already exist
        if "duplicate column name" not in str(e):
            raise
    except FileNotFoundError as e:
        logger.error(f"Schema file not found: {e}")
        raise