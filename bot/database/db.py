import aiosqlite
import logging
import asyncio
import random
from typing import Any, Dict, List, Optional
from config.settings import DATABASE_PATH
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from bot.utils.constants import DEFAULT_COMPLETION_MESSAGE

logger = logging.getLogger(__name__)

write_queue = asyncio.Queue()
_db_connection = None

class DatabaseError(Exception):
    """Custom exception class for database-related errors."""
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error

async def init_db_connection():
    global _db_connection
    if _db_connection is None:
        _db_connection = await aiosqlite.connect(DATABASE_PATH)
        await _db_connection.execute('PRAGMA foreign_keys = ON')
        await _db_connection.execute('PRAGMA journal_mode=WAL')
        await _db_connection.execute('PRAGMA busy_timeout=15000')
        await _db_connection.execute('PRAGMA cache_size=-20000')
        _db_connection.row_factory = aiosqlite.Row
        logger.info("Database connection initialized: %s", DATABASE_PATH)

async def close_db_connection():
    global _db_connection
    if _db_connection:
        await _db_connection.close()
        _db_connection = None
        logger.info("Database connection closed")

async def init_db():
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            with open("bot/database/schema.sql", "r", encoding='utf-8') as f:
                await conn.executescript(f.read())
            await conn.commit()
            logger.info("Database schema initialized successfully")
    except aiosqlite.Error as e:
        error_msg = str(e).lower()
        if "duplicate column name" in error_msg or "already exists" in error_msg:
            logger.warning("Schema application skipped due to existing elements: %s", e)
        else:
            logger.error("Failed to initialize database: %s", e)
            raise
    except FileNotFoundError as e:
        logger.error("Schema file not found: %s", e)
        raise

async def fetch_one(query: str, params: tuple = ()) -> Optional[Dict]:
    try:
        await init_db_connection()
        async with _db_connection.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        raise DatabaseError(f"Error fetching single record: {str(e)}", e)

async def fetch_all(query: str, params: tuple = ()) -> List[Dict]:
    try:
        await init_db_connection()
        async with _db_connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        raise DatabaseError(f"Error fetching multiple records: {str(e)}", e)

async def execute(query: str, params: tuple = ()) -> None:
    try:
        await init_db_connection()
        await _db_connection.execute(query, params)
        await _db_connection.commit()
    except aiosqlite.Error as e:
        logger.error("Database error in execute: %s", e)
        await _db_connection.rollback()
        raise

async def handle_update_user(cursor, request):
    await cursor.execute(
        """
        INSERT OR REPLACE INTO users (user_id, group_id, topic_id, username, first_name)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            request["user_id"],
            request["group_id"],
            request["topic_id"],
            request["username"],
            request["first_name"]
        )
    )
    logger.info("Processed update_user for user_id=%s, group_id=%s, topic_id=%s",
                request["user_id"], request["group_id"], request["topic_id"])

async def handle_contribution(cursor, request):
    try:
        logger.info("Starting handle_contribution: group_id=%s, topic_id=%s, user_id=%s, amount=%d, khatm_type=%s",
                   request["group_id"], request["topic_id"], request["user_id"], request["amount"], request["khatm_type"])

        # First get the current total
        current_topic = await cursor.execute(
            """
            SELECT current_total FROM topics 
            WHERE topic_id = ? AND group_id = ?
            """,
            (request["topic_id"], request["group_id"])
        )
        current_total = (await current_topic.fetchone())["current_total"]
        logger.debug("Current total before contribution: %d", current_total)
        
        # Insert contribution record
        try:
            await cursor.execute(
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
            logger.debug("Successfully inserted contribution record")
        except Exception as e:
            logger.error("Failed to insert contribution record: %s", e, exc_info=True)
            raise

        if request["khatm_type"] == "ghoran":
            try:
                await cursor.execute(
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
                logger.debug("Updated user total_ayat for Quran khatm")

                # Update topics with completion count increment if completed
                if request.get("completed"):
                    await cursor.execute(
                        """
                        UPDATE topics SET current_verse_id = ?, current_total = current_total + ?, 
                        completion_count = completion_count + 1, is_completed = 1
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
                    await cursor.execute(
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
                logger.debug("Updated topic current_verse_id and current_total for Quran khatm")
                # مقداردهی new_total برای قرآن
                updated_topic = await cursor.execute(
                    """
                    SELECT current_total FROM topics 
                    WHERE topic_id = ? AND group_id = ?
                    """,
                    (request["topic_id"], request["group_id"])
                )
                new_total = (await updated_topic.fetchone())["current_total"]
                logger.debug("Updated topic current_total to %d for %s khatm", new_total, request["khatm_type"])
            except Exception as e:
                logger.error("Failed to update Quran khatm totals: %s", e, exc_info=True)
                raise
            
        else:
            try:
                total_field = "total_salavat" if request["khatm_type"] == "salavat" else "total_zekr"
                # Update user totals
                await cursor.execute(
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
                logger.debug("Updated user %s for %s khatm", total_field, request["khatm_type"])
                
                # Atomically update topic total
                await cursor.execute(
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
                # Verify update
                updated_topic = await cursor.execute(
                    """
                    SELECT current_total FROM topics 
                    WHERE topic_id = ? AND group_id = ?
                    """,
                    (request["topic_id"], request["group_id"])
                )
                new_total = (await updated_topic.fetchone())["current_total"]
                logger.debug("Updated topic current_total to %d for %s khatm", new_total, request["khatm_type"])
            except Exception as e:
                logger.error("Failed to update %s khatm totals: %s", request["khatm_type"], e, exc_info=True)
                raise

        if request.get("send_completion") and request.get("bot"):
            try:
                # خواندن completion_message
                topic = await cursor.execute(
                    """
                    SELECT completion_message FROM topics 
                    WHERE topic_id = ? AND group_id = ?
                    """,
                    (request["topic_id"], request["group_id"])
                )
                completion_message = (await topic.fetchone())["completion_message"]
                
                # تنظیم پیام پیش‌فرض
                khatm_type_display = request.get("khatm_type_display", "صلوات" if request["khatm_type"] == "salavat" else "قرآن" if request["khatm_type"] == "ghoran" else "ذکر")
                if not completion_message:
                    completion_message = f"دوره ختم {khatm_type_display} به پایان رسید! 🌸"
                # Build buttons
                keyboard = [
                    [
                        InlineKeyboardButton("صلوات 🙏", callback_data="khatm_salavat"),
                        InlineKeyboardButton("قرآن 📖", callback_data="khatm_ghoran"),
                        InlineKeyboardButton("ذکر 📿", callback_data="khatm_zekr"),
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = f"{completion_message}\n\nآیا می‌خواهید دوره جدیدی شروع کنید؟"                
                # ارسال پیام
                await request["bot"].send_message(
                    chat_id=request["group_id"],
                    message_thread_id=request["topic_id"],
                    text=message,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )
                
                # تنظیم is_completed
                await cursor.execute(
                    """
                    UPDATE topics SET is_completed = 1
                    WHERE topic_id = ? AND group_id = ?
                    """,
                    (request["topic_id"], request["group_id"])
                )
                logger.info("Sent completion message and buttons for group_id=%s, topic_id=%s",
                           request["group_id"], request["topic_id"])
            except Exception as e:
                logger.error("Failed to send completion message: %s", e, exc_info=True)
    except Exception as e:
        logger.error("Failed to mark khatm as completed: %s", e, exc_info=True)
        raise

async def handle_reset_daily(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET reset_daily = ?
        WHERE group_id = ?
        """,
        (1 if request["action"] == "enable" else 0, request["group_id"])
    )
    logger.info("Processed %s reset_daily for group_id=%s", request["action"], request["group_id"])

async def handle_reset_daily_group(cursor, request):
    await cursor.execute(
        """
        DELETE FROM contributions WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    await cursor.execute(
        """
        UPDATE topics SET current_total = 0
        WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    if request["khatm_type"] == "ghoran":
        row = await (await cursor.execute(
            """
            SELECT start_verse_id FROM khatm_ranges
            WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )).fetchone()
        if row:
            await cursor.execute(
                """
                UPDATE topics SET current_verse_id = ?
                WHERE group_id = ? AND topic_id = ?
                """,
                (row["start_verse_id"], request["group_id"], request["topic_id"])
            )
    logger.info("Processed reset_daily_group for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_reset_periodic_topic(cursor, request):
    await cursor.execute(
        """
        DELETE FROM contributions WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    await cursor.execute(
        """
        UPDATE topics SET current_total = 0, completion_count = completion_count + 1
        WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    if request["khatm_type"] == "ghoran":
        row = await (await cursor.execute(
            """
            SELECT start_verse_id FROM khatm_ranges
            WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )).fetchone()
        if row:
            await cursor.execute(
                """
                UPDATE topics SET current_verse_id = ?
                WHERE group_id = ? AND topic_id = ?
                """,
                (row["start_verse_id"], request["group_id"], request["topic_id"])
            )
    logger.info("Processed reset_periodic_topic for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_start_khatm_ghoran(cursor, request):
    try:
        # Start transaction
        await cursor.execute(
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
        await cursor.execute(
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
        # Verify insertion
        range_check = await cursor.execute(
            """
            SELECT start_verse_id, end_verse_id 
            FROM khatm_ranges 
            WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )
        range_result = await range_check.fetchone()
        if not range_result:
            logger.error("Failed to insert khatm_ranges: group_id=%s, topic_id=%s", 
                        request["group_id"], request["topic_id"])
            raise aiosqlite.Error("Failed to insert khatm_ranges")
        logger.info("Processed start_khatm_ghoran for group_id=%s, topic_id=%s, start_verse_id=%d, end_verse_id=%d", 
                   request["group_id"], request["topic_id"], range_result["start_verse_id"], range_result["end_verse_id"])
    except Exception as e:
        logger.error("Error in handle_start_khatm_ghoran: %s", e, exc_info=True)
        raise

async def handle_start_khatm_zekr(cursor, request):
    await cursor.execute(
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

async def handle_start_khatm_salavat(cursor, request):
    await cursor.execute(
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

async def handle_deactivate_khatm(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET is_active = 0
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["topic_id"], request["group_id"])
    )
    logger.info("Processed deactivate_khatm for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_start_from(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET current_total = ?, is_active = 1
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["number"], request["topic_id"], request["group_id"])
    )
    logger.info("Processed start_from for group_id=%s, topic_id=%s, number=%d", 
                request["group_id"], request["topic_id"], request["number"])

async def handle_reset_zekr(cursor, request):
    await cursor.execute(
        """
        UPDATE topics 
        SET current_total = 0, zekr_text = ''
        WHERE group_id = ? AND topic_id = ? AND khatm_type IN ('zekr', 'salavat')
        """,
        (request["group_id"], request["topic_id"])
    )
    logger.info("Processed reset_zekr for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_reset_kol(cursor, request):
    try:
        # First check if topic exists and is active
        topic = await (await cursor.execute(
            """
            SELECT khatm_type, is_active 
            FROM topics 
            WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )).fetchone()

        if not topic:
            logger.warning("Topic not found for reset_kol",
                         extra={"group_id": request["group_id"], "topic_id": request["topic_id"]})
            raise DatabaseError("تاپیک ختم تنظیم نشده است. از /topic یا 'تاپیک' استفاده کنید.")

        if not topic["is_active"]:
            logger.warning("Inactive topic for reset_kol",
                         extra={"group_id": request["group_id"], "topic_id": request["topic_id"]})
            raise DatabaseError("این تاپیک ختم غیرفعال است. لطفاً ابتدا یک ختم را فعال کنید.")

        await cursor.execute(
            """
            DELETE FROM contributions WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )
        await cursor.execute(
            """
            UPDATE topics 
            SET current_total = 0, zekr_text = ''
            WHERE group_id = ? AND topic_id = ?
            """,
            (request["group_id"], request["topic_id"])
        )
        if request["khatm_type"] == "ghoran":
            row = await (await cursor.execute(
                """
                SELECT start_verse_id FROM khatm_ranges 
                WHERE group_id = ? AND topic_id = ?
                """,
                (request["group_id"], request["topic_id"])
            )).fetchone()
            if row:
                await cursor.execute(
                    """
                    UPDATE topics 
                    SET current_verse_id = ?
                    WHERE group_id = ? AND topic_id = ?
                    """,
                    (row["start_verse_id"], request["group_id"], request["topic_id"])
                )
        logger.info("Successfully processed reset_kol", 
                    extra={"group_id": request["group_id"], 
                          "topic_id": request["topic_id"],
                          "khatm_type": topic["khatm_type"]})
    except Exception as e:
        logger.error("Error in handle_reset_kol: %s", str(e),
                    extra={"group_id": request["group_id"], 
                          "topic_id": request["topic_id"],
                          "error": str(e)})
        raise DatabaseError(str(e) if isinstance(e, DatabaseError) else "خطا در بازنشانی ختم. لطفاً دوباره تلاش کنید.")

async def handle_set_max(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET max_number = ?
        WHERE group_id = ?
        """,
        (request["max_number"], request["group_id"])
    )
    if request["is_digit"]:
        await cursor.execute(
            """
            UPDATE topics SET max_ayat = ?
            WHERE topic_id = ? AND group_id = ?
            """,
            (request["max_number"], request["topic_id"], request["group_id"])
        )
    logger.info("Processed set_max for group_id=%s, topic_id=%s, max_number=%d", 
                request["group_id"], request["topic_id"], request["max_number"])

async def handle_max_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET max_number = 1000000
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    await cursor.execute(
        """
        UPDATE topics SET max_ayat = 100
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["topic_id"], request["group_id"])
    )
    logger.info("Processed max_off for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_set_min(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET min_number = ?
        WHERE group_id = ?
        """,
        (request["min_number"], request["group_id"])
    )
    if request["is_digit"]:
        await cursor.execute(
            """
            UPDATE topics SET min_ayat = ?
            WHERE topic_id = ? AND group_id = ?
            """,
            (request["min_number"], request["topic_id"], request["group_id"])
        )
    logger.info("Processed set_min for group_id=%s, topic_id=%s, min_number=%d", 
                request["group_id"], request["topic_id"], request["min_number"])

async def handle_min_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET min_number = 0
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    await cursor.execute(
        """
        UPDATE topics SET min_ayat = 1
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["topic_id"], request["group_id"])
    )
    logger.info("Processed min_off for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_sepas_on(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET sepas_enabled = 1
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed sepas_on for group_id=%s", request["group_id"])

async def handle_sepas_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET sepas_enabled = 0
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed sepas_off for group_id=%s", request["group_id"])

async def handle_add_sepas(cursor, request):
    await cursor.execute(
        """
        INSERT INTO sepas_texts (group_id, text, is_default)
        VALUES (?, ?, 0)
        """,
        (request["group_id"], request["sepas_text"])
    )
    logger.info("Processed add_sepas for group_id=%s, text=%s", 
                request["group_id"], request["sepas_text"])

async def handle_reset_number_on(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET reset_on_period = 1
        WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    logger.info("Processed reset_number_on for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_reset_number_off(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET reset_on_period = 0
        WHERE group_id = ? AND topic_id = ?
        """,
        (request["group_id"], request["topic_id"])
    )
    logger.info("Processed reset_number_off for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_set_number(cursor, request):
    await cursor.execute(
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

async def handle_number_off(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET period_number = 0, reset_on_period = 0
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["topic_id"], request["group_id"])
    )
    logger.info("Processed number_off for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_stop_on(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET stop_number = ?
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["stop_number"], request["topic_id"], request["group_id"])
    )
    logger.info("Processed stop_on for group_id=%s, topic_id=%s, stop_number=%d", 
                request["group_id"], request["topic_id"], request["stop_number"])

async def handle_stop_on_off(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET stop_number = 0
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["topic_id"], request["group_id"])
    )
    logger.info("Processed stop_on_off for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_time_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET time_off_start = ?, time_off_end = ?
        WHERE group_id = ?
        """,
        (request["time_off_start"], request["time_off_end"], request["group_id"])
    )
    logger.info("Processed time_off for group_id=%s, start=%s, end=%s",
                request["group_id"], request["time_off_start"], request["time_off_end"])

async def handle_time_off_disable(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET time_off_start = '', time_off_end = ''
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed time_off_disable for group_id=%s", request["group_id"])

async def handle_lock_on(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET lock_enabled = 1
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed lock_on for group_id=%s", request["group_id"])

async def handle_lock_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET lock_enabled = 0
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed lock_off for group_id=%s", request["group_id"])

async def handle_delete_after(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET delete_after = ?
        WHERE group_id = ?
        """,
        (request["minutes"], request["group_id"])
    )
    logger.info("Processed delete_after for group_id=%s, minutes=%d", 
                request["group_id"], request["minutes"])

async def handle_delete_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET delete_after = 0
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed delete_off for group_id=%s", request["group_id"])

async def handle_jam_on(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET show_total = 1
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed jam_on for group_id=%s", request["group_id"])

async def handle_jam_off(cursor, request):
    await cursor.execute(
        """
        UPDATE groups SET show_total = 0
        WHERE group_id = ?
        """,
        (request["group_id"],)
    )
    logger.info("Processed jam_off for group_id=%s", request["group_id"])

async def handle_set_completion_message(cursor, request):
    await cursor.execute(
        """
        UPDATE topics SET completion_message = ?
        WHERE topic_id = ? AND group_id = ?
        """,
        (request["message"], request["topic_id"], request["group_id"])
    )
    logger.info("Processed set_completion_message for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def handle_hadis_on(cursor, request):
    await cursor.execute(
        """
        INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled)
        VALUES (?, 1)
        """,
        (request["group_id"],)
    )
    logger.info("Processed hadis_on for group_id=%s", request["group_id"])

async def handle_hadis_off(cursor, request):
    await cursor.execute(
        """
        INSERT OR REPLACE INTO hadith_settings (group_id, hadith_enabled)
        VALUES (?, 0)
        """,
        (request["group_id"],)
    )
    logger.info("Processed hadis_off for group_id=%s", request["group_id"])

async def handle_max_ayat(cursor, request):
    """Handle setting maximum number of verses to display."""
    await cursor.execute(
        """
        UPDATE groups SET max_display_verses = ?
        WHERE group_id = ?
        """,
        (request["max_display_verses"], request["group_id"])
    )
    logger.info("Processed max_ayat for group_id=%s, max_display_verses=%d", 
                request["group_id"], request["max_display_verses"])

async def handle_min_ayat(cursor, request):
    """Handle setting minimum number of verses to display."""
    await cursor.execute(
        """
        UPDATE groups SET min_display_verses = ?
        WHERE group_id = ?
        """,
        (request["min_display_verses"], request["group_id"])
    )
    logger.info("Processed min_ayat for group_id=%s, min_display_verses=%d", 
                request["group_id"], request["min_display_verses"])

async def handle_khatm_number(cursor, request):
    completed = False
    new_total = request["current_value"] + request["number"]

    if request["khatm_type"] == "ghoran":
        row = await (await cursor.execute(
            """
            SELECT t.stop_number, r.start_verse_id, r.end_verse_id 
            FROM topics t 
            JOIN khatm_ranges r ON t.group_id = r.group_id AND t.topic_id = r.topic_id 
            WHERE t.topic_id = ? AND t.group_id = ?
            """,
            (request["topic_id"], request["group_id"])
        )).fetchone()
        if row:
            if row["stop_number"] > 0 and new_total >= row["stop_number"]:
                completed = True
            elif new_total >= (row["end_verse_id"] - row["start_verse_id"] + 1):
                completed = True
    else:
        row = await (await cursor.execute(
            """
            SELECT period_number, reset_on_period 
            FROM topics 
            WHERE topic_id = ? AND group_id = ?
            """,
            (request["topic_id"], request["group_id"])
        )).fetchone()
        if row and row["period_number"] > 0 and new_total >= row["period_number"]:
            if row["reset_on_period"]:
                new_total = new_total % row["period_number"]
            completed = True

    await cursor.execute(
        """
        UPDATE topics SET current_total = ? 
        WHERE topic_id = ? AND group_id = ?
        """,
        (new_total, request["topic_id"], request["group_id"])
    )

    if completed:
        await cursor.execute(
            """
            UPDATE topics SET is_active = 0, completion_count = completion_count + 1
            WHERE topic_id = ? AND group_id = ?
            """,
            (request["topic_id"], request["group_id"])
        )

    logger.info("Processed khatm_number for group_id=%s, topic_id=%s, khatm_type=%s, new_total=%d, completed=%s", 
                request["group_id"], request["topic_id"], request["khatm_type"], new_total, completed)

async def handle_update_tag_timestamp(cursor, request):
    await cursor.execute(
        """
        INSERT OR REPLACE INTO tag_timestamps (group_id, last_tag_time)
        VALUES (?, ?)
        """,
        (request["group_id"], datetime.now().isoformat())
    )
    logger.info("Processed update_tag_timestamp for group_id=%s", request["group_id"])

async def handle_set_zekr_text(cursor, request):
    """Handle setting zekr text for a topic."""
    await cursor.execute(
        """
        UPDATE topics 
        SET zekr_text = ?
        WHERE group_id = ? AND topic_id = ? AND khatm_type = 'zekr'
        """,
        (request["zekr_text"], request["group_id"], request["topic_id"])
    )
    logger.info("Processed set_zekr_text for group_id=%s, topic_id=%s", 
                request["group_id"], request["topic_id"])

async def process_queue_request(request: Dict[str, Any]) -> None:
    handlers = {
        "update_user": handle_update_user,
        "contribution": handle_contribution,
        "reset_daily": handle_reset_daily,
        "reset_daily_group": handle_reset_daily_group,
        "reset_periodic_topic": handle_reset_periodic_topic,
        "start_khatm_ghoran": handle_start_khatm_ghoran,
        "start_khatm_zekr": handle_start_khatm_zekr,
        "start_khatm_salavat": handle_start_khatm_salavat,
        "deactivate_khatm": handle_deactivate_khatm,
        "start_from": handle_start_from,
        "reset_zekr": handle_reset_zekr,
        "reset_kol": handle_reset_kol,
        "set_max": handle_set_max,
        "max_off": handle_max_off,
        "set_min": handle_set_min,
        "min_off": handle_min_off,
        "sepas_on": handle_sepas_on,
        "sepas_off": handle_sepas_off,
        "add_sepas": handle_add_sepas,
        "reset_number_on": handle_reset_number_on,
        "reset_number_off": handle_reset_number_off,
        "set_number": handle_set_number,
        "number_off": handle_number_off,
        "stop_on": handle_stop_on,
        "stop_on_off": handle_stop_on_off,
        "time_off": handle_time_off,
        "time_off_disable": handle_time_off_disable,
        "lock_on": handle_lock_on,
        "lock_off": handle_lock_off,
        "delete_after": handle_delete_after,
        "delete_off": handle_delete_off,
        "jam_on": handle_jam_on,
        "jam_off": handle_jam_off,
        "set_completion_message": handle_set_completion_message,
        "hadis_on": handle_hadis_on,
        "hadis_off": handle_hadis_off,
        "max_ayat": handle_max_ayat,
        "min_ayat": handle_min_ayat,
        "khatm_number": handle_khatm_number,
        "update_tag_timestamp": handle_update_tag_timestamp,
        "set_zekr_text": handle_set_zekr_text
    }
    
    req_type = request.get("type")
    handler = handlers.get(req_type)
    if not handler:
        logger.warning("Unknown request type: %s", req_type)
        return

    max_retries = 10
    retry_delay = 0.2
    for attempt in range(max_retries):
        try:
            await init_db_connection()
            async with _db_connection.cursor() as cursor:
                await handler(cursor, request)
                await _db_connection.commit()
            return
        except aiosqlite.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning("Database locked on attempt %d for request type=%s, retrying in %.2f seconds",
                              attempt + 1, req_type, retry_delay)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay + random.uniform(0, 0.2))
                    retry_delay *= 1.5
                    continue
            logger.error("Error processing queue request type=%s: %s", req_type, e)
            await _db_connection.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error processing queue request type=%s: %s", req_type, e)
            await _db_connection.rollback()
            raise

    logger.error("Failed to process queue request after %d retries: type=%s, request=%s", 
                max_retries, req_type, request)
    raise aiosqlite.OperationalError("Failed to process queue request after retries")