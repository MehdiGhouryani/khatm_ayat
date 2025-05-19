import sqlite3
import logging
import asyncio
from contextlib import contextmanager
from config.settings import DATABASE_PATH
from datetime import datetime

logger = logging.getLogger(__name__)

write_queue = asyncio.Queue()

@contextmanager
def get_db_connection():
    """Provide a database connection with WAL mode and proper error handling."""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=10)  # Reduced timeout for faster retries
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode=WAL')  # Enable Write-Ahead Logging
        conn.execute('PRAGMA busy_timeout=10000')  # 10 seconds busy timeout
        conn.row_factory = sqlite3.Row
        logger.debug("Database connection opened: %s", DATABASE_PATH)
        yield conn
        conn.commit()
    except sqlite3.OperationalError as e:
        logger.error("Database operational error: %s", e, exc_info=True)
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error("Unexpected database error: %s", e, exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error("Error closing database connection: %s", e, exc_info=True)

def init_db():
    """Initialize the database schema with error handling."""
    try:
        with get_db_connection() as conn:
            with open("bot/database/schema.sql", "r", encoding='utf-8') as f:
                conn.executescript(f.read())
            logger.info("Database schema initialized successfully")
    except sqlite3.Error as e:
        logger.error("Failed to initialize database: %s", e, exc_info=True)
        if "duplicate column name" not in str(e).lower():
            raise
    except FileNotFoundError as e:
        logger.error("Schema file not found: %s", e, exc_info=True)
        raise
    except Exception as e:
        logger.error("Unexpected error during database initialization: %s", e, exc_info=True)
        raise

async def process_queue_request(request):
    """Process a single queue request to write to the database with retry mechanism."""
    max_retries = 3
    retry_delay = 1  # seconds
    req_type = request.get("type")

    for attempt in range(max_retries):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                if req_type == "contribution":
                    cursor.execute(
                        """
                        INSERT INTO contributions (group_id, topic_id, user_id, amount, verse_id)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            request["group_id"],
                            request["topic_id"],
                            request["user_id"],
                            request["amount"],
                            request.get("verse_id"),
                        ),
                    )
                    if request["khatm_type"] == "ghoran":
                        cursor.execute(
                            """
                            UPDATE users SET total_ayat = total_ayat + ?
                            WHERE user_id = ? AND group_id = ? AND topic_id = ?
                            """,
                            (
                                request["amount"],
                                request["user_id"],
                                request["group_id"],
                                request["topic_id"],
                            ),
                        )
                        cursor.execute(
                            """
                            UPDATE topics SET current_verse_id = ?, current_total = current_total + ?
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (
                                request["current_verse_id"],
                                request["amount"],
                                request["topic_id"],
                                request["group_id"],
                            ),
                        )
                    else:
                        total_field = "total_salavat" if request["khatm_type"] == "salavat" else "total_zekr"
                        cursor.execute(
                            f"""
                            UPDATE users SET {total_field} = {total_field} + ?
                            WHERE user_id = ? AND group_id = ? AND topic_id = ?
                            """,
                            (
                                request["amount"],
                                request["user_id"],
                                request["group_id"],
                                request["topic_id"],
                            ),
                        )
                        cursor.execute(
                            """
                            UPDATE topics SET current_total = current_total + ?
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (
                                request["amount"],
                                request["topic_id"],
                                request["group_id"],
                            ),
                        )
                    if request.get("completed"):
                        cursor.execute(
                            """
                            UPDATE topics SET is_active = 0, completion_count = completion_count + 1
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (request["topic_id"], request["group_id"]),
                        )

                elif req_type == "reset_daily":
                    cursor.execute(
                        """
                        UPDATE groups SET reset_daily = ?
                        WHERE group_id = ?
                        """,
                        (1 if request["action"] == "enable" else 0, request["group_id"])
                    )
                    logger.info("Processed %s reset_daily for group_id=%s", request["action"], request["group_id"])

                elif req_type == "reset_daily_group":
                    cursor.execute(
                        """
                        DELETE FROM contributions WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    cursor.execute(
                        """
                        UPDATE topics SET current_total = 0
                        WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    if request["khatm_type"] == "ghoran":
                        cursor.execute(
                            """
                            SELECT start_verse_id FROM khatm_ranges
                            WHERE group_id = ? AND topic_id = ?
                            """,
                            (request["group_id"], request["topic_id"])
                        )
                        range_result = cursor.fetchone()
                        if range_result:
                            cursor.execute(
                                """
                                UPDATE topics SET current_verse_id = ?
                                WHERE group_id = ? AND topic_id = ?
                                """,
                                (range_result["start_verse_id"], request["group_id"], request["topic_id"])
                            )
                    logger.info("Processed reset_daily_group for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "reset_periodic_topic":
                    cursor.execute(
                        """
                        DELETE FROM contributions WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    cursor.execute(
                        """
                        UPDATE topics SET current_total = 0, completion_count = completion_count + 1
                        WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    if request["khatm_type"] == "ghoran":
                        cursor.execute(
                            """
                            SELECT start_verse_id FROM khatm_ranges
                            WHERE group_id = ? AND topic_id = ?
                            """,
                            (request["group_id"], request["topic_id"])
                        )
                        range_result = cursor.fetchone()
                        if range_result:
                            cursor.execute(
                                """
                                UPDATE topics SET current_verse_id = ?
                                WHERE group_id = ? AND topic_id = ?
                                """,
                                (range_result["start_verse_id"], request["group_id"], request["topic_id"])
                            )
                    logger.info("Processed reset_periodic_topic for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "start_khatm_ghoran":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO topics
                        (topic_id, group_id, name, khatm_type, current_verse_id, is_active)
                        VALUES (?, ?, ?, ?, ?, 1)
                        """,
                        (
                            request["topic_id"],
                            request["group_id"],
                            request["topic_name"],
                            request["khatm_type"],
                            request["start_verse_id"],
                        )
                    )
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO khatm_ranges
                        (group_id, topic_id, start_verse_id, end_verse_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            request["group_id"],
                            request["topic_id"],
                            request["start_verse_id"],
                            request["end_verse_id"],
                        )
                    )
                    logger.info("Processed start_khatm_ghoran for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "start_khatm_zekr":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO topics
                        (topic_id, group_id, name, khatm_type, is_active, current_total)
                        VALUES (?, ?, ?, ?, 1, 0)
                        """,
                        (
                            request["topic_id"],
                            request["group_id"],
                            request["topic_name"],
                            request["khatm_type"],
                        )
                    )
                    logger.info("Processed start_khatm_zekr for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "start_khatm_salavat":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO topics
                        (topic_id, group_id, name, khatm_type, is_active, current_total)
                        VALUES (?, ?, ?, ?, 1, 0)
                        """,
                        (
                            request["topic_id"],
                            request["group_id"],
                            request["topic_name"],
                            request["khatm_type"],
                        )
                    )
                    logger.info("Processed start_khatm_salavat for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "deactivate_khatm":
                    cursor.execute(
                        """
                        UPDATE topics SET is_active = 0
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed deactivate_khatm for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "start_from":
                    cursor.execute(
                        """
                        UPDATE topics SET current_total = ?, is_active = 1
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["number"], request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed start_from for group_id=%s, topic_id=%s, number=%d", 
                                request["group_id"], request["topic_id"], request["number"])

                elif req_type == "reset_zekr":
                    cursor.execute(
                        """
                        UPDATE topics 
                        SET current_total = 0, zekr_text = ''
                        WHERE group_id = ? AND topic_id = ? AND khatm_type IN ('zekr', 'salavat')
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    logger.info("Processed reset_zekr for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "reset_kol":
                    cursor.execute(
                        """
                        UPDATE topics 
                        SET current_total = 0, zekr_text = ''
                        WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    if request["khatm_type"] == "ghoran":
                        cursor.execute(
                            """
                            SELECT start_verse_id FROM khatm_ranges 
                            WHERE group_id = ? AND topic_id = ?
                            """,
                            (request["group_id"], request["topic_id"])
                        )
                        range_result = cursor.fetchone()
                        if range_result:
                            cursor.execute(
                                """
                                UPDATE topics 
                                SET current_verse_id = ?
                                WHERE group_id = ? AND topic_id = ?
                                """,
                                (range_result["start_verse_id"], request["group_id"], request["topic_id"])
                            )
                    logger.info("Processed reset_kol for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "set_max":
                    cursor.execute(
                        """
                        UPDATE groups SET max_number = ?
                        WHERE group_id = ?
                        """,
                        (request["max_number"], request["group_id"])
                    )
                    if request["is_digit"]:
                        cursor.execute(
                            """
                            UPDATE topics SET max_ayat = ?
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (request["max_number"], request["topic_id"], request["group_id"])
                        )
                    logger.info("Processed set_max for group_id=%s, topic_id=%s, max_number=%d", 
                                request["group_id"], request["topic_id"], request["max_number"])

                elif req_type == "max_off":
                    cursor.execute(
                        """
                        UPDATE groups SET max_number = 1000000
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    cursor.execute(
                        """
                        UPDATE topics SET max_ayat = 100
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed max_off for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "set_min":
                    cursor.execute(
                        """
                        UPDATE groups SET min_number = ?
                        WHERE group_id = ?
                        """,
                        (request["min_number"], request["group_id"])
                    )
                    if request["is_digit"]:
                        cursor.execute(
                            """
                            UPDATE topics SET min_ayat = ?
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (request["min_number"], request["topic_id"], request["group_id"])
                        )
                    logger.info("Processed set_min for group_id=%s, topic_id=%s, min_number=%d", 
                                request["group_id"], request["topic_id"], request["min_number"])

                elif req_type == "min_off":
                    cursor.execute(
                        """
                        UPDATE groups SET min_number = 0
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    cursor.execute(
                        """
                        UPDATE topics SET min_ayat = 1
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed min_off for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "sepas_on":
                    cursor.execute(
                        """
                        UPDATE groups SET sepas_enabled = 1
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed sepas_on for group_id=%s", request["group_id"])

                elif req_type == "sepas_off":
                    cursor.execute(
                        """
                        UPDATE groups SET sepas_enabled = 0
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed sepas_off for group_id=%s", request["group_id"])

                elif req_type == "add_sepas":
                    cursor.execute(
                        """
                        INSERT INTO sepas_texts (group_id, text, is_default)
                        VALUES (?, ?, 0)
                        """,
                        (request["group_id"], request["sepas_text"])
                    )
                    logger.info("Processed add_sepas for group_id=%s, text=%s", 
                                request["group_id"], request["sepas_text"])

                elif req_type == "reset_number_on":
                    cursor.execute(
                        """
                        UPDATE topics SET reset_on_period = 1
                        WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    logger.info("Processed reset_number_on for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "reset_number_off":
                    cursor.execute(
                        """
                        UPDATE topics SET reset_on_period = 0
                        WHERE group_id = ? AND topic_id = ?
                        """,
                        (request["group_id"], request["topic_id"])
                    )
                    logger.info("Processed reset_number_off for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "set_number":
                    cursor.execute(
                        """
                        UPDATE topics SET period_number = ?, reset_on_period = ?
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (
                            request["period_number"],
                            request["reset_on_period"],
                            request["topic_id"],
                            request["group_id"]
                        )
                    )
                    logger.info("Processed set_number for group_id=%s, topic_id=%s, period_number=%d", 
                                request["group_id"], request["topic_id"], request["period_number"])

                elif req_type == "number_off":
                    cursor.execute(
                        """
                        UPDATE topics SET period_number = 0, reset_on_period = 0
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed number_off for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "stop_on":
                    cursor.execute(
                        """
                        UPDATE topics SET stop_number = ?
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["stop_number"], request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed stop_on for group_id=%s, topic_id=%s, stop_number=%d", 
                                request["group_id"], request["topic_id"], request["stop_number"])

                elif req_type == "stop_on_off":
                    cursor.execute(
                        """
                        UPDATE topics SET stop_number = 0
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed stop_on_off for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "time_off":
                    cursor.execute(
                        """
                        UPDATE groups SET time_off_start = ?, time_off_end = ?
                        WHERE group_id = ?
                        """,
                        (request["start_time"], request["end_time"], request["group_id"])
                    )
                    logger.info("Processed time_off for group_id=%s, start=%s, end=%s", 
                                request["group_id"], request["start_time"], request["end_time"])

                elif req_type == "time_off_disable":
                    cursor.execute(
                        """
                        UPDATE groups SET time_off_start = '', time_off_end = ''
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed time_off_disable for group_id=%s", request["group_id"])

                elif req_type == "lock_on":
                    cursor.execute(
                        """
                        UPDATE groups SET lock_enabled = 1
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed lock_on for group_id=%s", request["group_id"])

                elif req_type == "lock_off":
                    cursor.execute(
                        """
                        UPDATE groups SET lock_enabled = 0
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed lock_off for group_id=%s", request["group_id"])

                elif req_type == "delete_after":
                    cursor.execute(
                        """
                        UPDATE groups SET delete_after = ?
                        WHERE group_id = ?
                        """,
                        (request["minutes"], request["group_id"])
                    )
                    logger.info("Processed delete_after for group_id=%s, minutes=%d", 
                                request["group_id"], request["minutes"])

                elif req_type == "delete_off":
                    cursor.execute(
                        """
                        UPDATE groups SET delete_after = 0
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed delete_off for group_id=%s", request["group_id"])

                elif req_type == "jam_on":
                    cursor.execute(
                        """
                        UPDATE groups SET show_total = 1
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed jam_on for group_id=%s", request["group_id"])

                elif req_type == "jam_off":
                    cursor.execute(
                        """
                        UPDATE groups SET show_total = 0
                        WHERE group_id = ?
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed jam_off for group_id=%s", request["group_id"])

                elif req_type == "set_completion_message":
                    cursor.execute(
                        """
                        UPDATE topics SET completion_message = ?
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (request["message"], request["topic_id"], request["group_id"])
                    )
                    logger.info("Processed set_completion_message for group_id=%s, topic_id=%s", 
                                request["group_id"], request["topic_id"])

                elif req_type == "hadis_on":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled)
                        VALUES (?, 1)
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed hadis_on for group_id=%s", request["group_id"])

                elif req_type == "hadis_off":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled)
                        VALUES (?, 0)
                        """,
                        (request["group_id"],)
                    )
                    logger.info("Processed hadis_off for group_id=%s", request["group_id"])

                elif req_type == "khatm_number":
                    completed = False
                    new_total = request["current_value"] + request["number"]

                    if request["khatm_type"] == "ghoran":
                        cursor.execute(
                            """
                            SELECT t.stop_number, r.start_verse_id, r.end_verse_id 
                            FROM topics t 
                            JOIN khatm_ranges r ON t.group_id = r.group_id AND t.topic_id = r.topic_id 
                            WHERE t.topic_id = ? AND t.group_id = ?
                            """,
                            (request["topic_id"], request["group_id"])
                        )
                        result = cursor.fetchone()
                        if result:
                            if result["stop_number"] > 0 and new_total >= result["stop_number"]:
                                completed = True
                            elif new_total >= (result["end_verse_id"] - result["start_verse_id"] + 1):
                                completed = True
                    else:
                        cursor.execute(
                            """
                            SELECT period_number, reset_on_period 
                            FROM topics 
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (request["topic_id"], request["group_id"])
                        )
                        topic = cursor.fetchone()
                        if topic and topic["period_number"] > 0 and new_total >= topic["period_number"]:
                            if topic["reset_on_period"]:
                                new_total = new_total % topic["period_number"]
                            completed = True

                    cursor.execute(
                        """
                        UPDATE topics SET current_total = ? 
                        WHERE topic_id = ? AND group_id = ?
                        """,
                        (new_total, request["topic_id"], request["group_id"])
                    )

                    if completed:
                        cursor.execute(
                            """
                            UPDATE topics SET is_active = 0, completion_count = completion_count + 1
                            WHERE topic_id = ? AND group_id = ?
                            """,
                            (request["topic_id"], request["group_id"])
                        )

                    logger.info("Processed khatm_number for group_id=%s, topic_id=%s, khatm_type=%s, new_total=%d, completed=%s", 
                                request["group_id"], request["topic_id"], request["khatm_type"], new_total, completed)

                elif req_type == "update_tag_timestamp":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO tag_timestamps (group_id, last_tag_time)
                        VALUES (?, ?)
                        """,
                        (request["group_id"], datetime.now().isoformat())
                    )
                    logger.info("Processed update_tag_timestamp for group_id=%s", request["group_id"])

                else:
                    logger.warning("Unknown request type: %s", req_type)
                    return

                return  # Success, exit retry loop

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning("Database locked on attempt %d for request type=%s, retrying in %d seconds",
                              attempt + 1, req_type, retry_delay)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
            logger.error("Error processing queue request type=%s: %s", req_type, e, exc_info=True)
            raise
        except Exception as e:
            logger.error("Unexpected error processing queue request type=%s: %s", req_type, e, exc_info=True)
            raise

    logger.error("Failed to process queue request after %d retries: %s", max_retries, request)
    raise sqlite3.OperationalError("Failed to process queue request after retries")