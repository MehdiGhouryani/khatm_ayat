import aiosqlite
import logging
import asyncio
import random
from typing import Any, Dict, List, Optional
from config.settings import DATABASE_PATH
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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





# این کد را در bot/database/db.py جایگزین تابع قبلی check_and_apply_migrations کنید

async def check_and_apply_migrations():
    """
    (ناهمزمان) هر بار هنگام اجرای ربات، ساختار دیتابیس را چک کرده
    و ستون‌های گمشده را بدون حذف داده‌ها اضافه می‌کند.
    """
    logger.info("در حال بررسی ساختار دیتابیس و اعمال مهاجرت‌ها (Async)...")
    
    async def get_columns(conn, table_name):
        """یک تابع کمکی برای دریافت لیست ستون‌های فعلی یک جدول."""
        try:
            # این تابع اکنون به درستی کار خواهد کرد چون conn.row_factory تنظیم شده است
            cursor = await conn.execute(f"PRAGMA table_info({table_name})")
            rows = await cursor.fetchall()
            await cursor.close()
            # این خط دیگر خطا نمی‌دهد
            return {row['name'] for row in rows}
        except aiosqlite.OperationalError:
            logger.debug(f"جدول '{table_name}' در زمان بررسی مهاجرت یافت نشد، schema.sql آن را خواهد ساخت.")
            return set()
        except TypeError as e:
            logger.error(f"خطای TypeError در get_columns (آیا row_factory تنظیم شده؟): {e}", exc_info=True)
            return set() # در صورت بروز خطا، مجموعه خالی برمی‌گردانیم

    try:
        # ما از یک اتصال async جدید برای اجرای مهاجرت استفاده می‌کنیم
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            
            # !!!!!!!!!!! راه‌حل اینجاست !!!!!!!!!!!
            # این خط حیاتی، اتصال جدید ما را وادار می‌کند ردیف‌ها را
            # به صورت دیکشنری برگرداند (مانند اتصال اصلی ربات)
            conn.row_factory = aiosqlite.Row
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            # --- مهاجرت جدول 'contributions' (رفع باگ اصلی) ---
            contrib_columns = await get_columns(conn, 'contributions')
            if contrib_columns and 'zekr_id' not in contrib_columns:
                logger.warning("مهاجرت: ستون 'zekr_id' در 'contributions' یافت نشد. در حال افزودن...")
                try:
                    await conn.execute("""
                        ALTER TABLE contributions
                        ADD COLUMN zekr_id INTEGER
                        REFERENCES topic_zekrs(id) ON DELETE SET NULL
                    """)
                    logger.info("مهاجرت موفق: ستون 'zekr_id' (با Foreign Key) به 'contributions' اضافه شد.")
                except aiosqlite.OperationalError as e:
                    logger.warning(f"افزودن Foreign Key برای 'zekr_id' شکست خورد ({e}). در حال افزودن ستون ساده...")
                    await conn.execute("ALTER TABLE contributions ADD COLUMN zekr_id INTEGER")
                    logger.info("مهاجرت موفق: ستون 'zekr_id' (ساده) به 'contributions' اضافه شد.")
                await conn.commit()

            # --- مهاجرت جدول 'groups' ---
            groups_columns = await get_columns(conn, 'groups')
            if groups_columns:
                migrations_applied = False
                if 'min_display_verses' not in groups_columns:
                    logger.warning("مهاجرت: ستون 'min_display_verses' در 'groups' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE groups ADD COLUMN min_display_verses INTEGER DEFAULT 1")
                    migrations_applied = True
                
                if 'invite_link' not in groups_columns:
                    logger.warning("مهاجرت: ستون 'invite_link' در 'groups' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE groups ADD COLUMN invite_link TEXT DEFAULT ''")
                    migrations_applied = True

                if 'title' not in groups_columns:
                    logger.warning("مهاجرت: ستون 'title' در 'groups' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE groups ADD COLUMN title TEXT DEFAULT ''")
                    migrations_applied = True
                
                if migrations_applied:
                    await conn.commit()
                    logger.info("مهاجرت جدول 'groups' با موفقیت انجام شد.")

            # --- مهاجرت جدول 'topics' ---
            topics_columns = await get_columns(conn, 'topics')
            if topics_columns:
                migrations_applied = False
                if 'is_completed' not in topics_columns:
                    logger.warning("مهاجرت: ستون 'is_completed' در 'topics' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE topics ADD COLUMN is_completed INTEGER DEFAULT 0")
                    migrations_applied = True
                
                if 'created_at' not in topics_columns:
                    logger.warning("مهاجرت: ستون 'created_at' در 'topics' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE topics ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    migrations_applied = True

                if 'updated_at' not in topics_columns:
                    logger.warning("مهاجرت: ستون 'updated_at' در 'topics' یافت نشد. در حال افزودن...")
                    await conn.execute("ALTER TABLE topics ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    migrations_applied = True
                
                if migrations_applied:
                    await conn.commit()
                    logger.info("مهاجرت جدول 'topics' با موفقیت انجام شد.")

            logger.info("بررسی مهاجرت دیتابیس با موفقیت کامل شد.")

    except aiosqlite.Error as e:
        logger.error(f"خطای بحرانی در زمان اجرای مهاجرت دیتابیس: {e}", exc_info=True)
        raise




async def init_db():
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            with open("bot/database/schema.sql", "r", encoding='utf-8') as f:
                await conn.executescript(f.read())
            await conn.commit()
            logger.info("Database schema initialized successfully")



        await check_and_apply_migrations()
        
        logger.info("مقداردهی اولیه دیتابیس و بررسی مهاجرت‌ها با موفقیت کامل شد.")
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


async def handle_set_completion_count(cursor, request):
    """Handle setting the completion count for a topic."""
    await cursor.execute(
        """
        UPDATE topics 
        SET completion_count = ?
        WHERE group_id = ? AND topic_id = ?
        """,
        (request["count"], request["group_id"], request["topic_id"])
    )
    logger.info("Processed set_completion_count for group_id=%s, topic_id=%s, count=%d", 
                request["group_id"], request["topic_id"], request["count"])


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




# این تابع را جایگزین تابع فعلی handle_zekr_contribution در خط 839 کنید
async def handle_zekr_contribution(cursor, request: Dict[str, Any]):
    """Handle zekr contribution database transaction and notification."""
    import html  # برای ایمنی متن‌ها در حالت HTML

    user_id = request['user_id']
    group_id = request['group_id']
    topic_id = request['topic_id']
    zekr_id = request['zekr_id']
    amount = request['amount']
    username = request['username']
    first_name = request['first_name']
    bot = request.get('bot')
    chat_id = request.get('chat_id')
    thread_id = request.get('thread_id')

    # توجه: در این ساختار، cursor از بیرون پاس داده می‌شود و ما داخل یک تراکنش هستیم
    # بنابراین برای عملیات دیتابیس از همان cursor استفاده می‌کنیم
    
    # 1. اطمینان از وجود کاربر
    await cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, group_id, topic_id, username, first_name, total_salavat, total_zekr, total_ayat) VALUES (?, ?, ?, ?, ?, 0, 0, 0)",
        (user_id, group_id, topic_id, username, first_name)
    )

    # 2. ثبت مشارکت
    await cursor.execute(
        "INSERT INTO contributions (user_id, group_id, topic_id, amount, zekr_id) VALUES (?, ?, ?, ?, ?)",
        (user_id, group_id, topic_id, amount, zekr_id)
    )

    # 3. آپدیت آمار کاربر
    await cursor.execute(
        "UPDATE users SET total_zekr = total_zekr + ? WHERE user_id = ? AND group_id = ? AND topic_id = ?",
        (amount, user_id, group_id, topic_id)
    )
    
    # 4. آپدیت آمار ذکر خاص
    await cursor.execute(
        "UPDATE topic_zekrs SET current_total = current_total + ? WHERE id = ?",
        (amount, zekr_id)
    )

    # 5. آپدیت آمار کل تاپیک
    await cursor.execute(
        "UPDATE topics SET current_total = current_total + ? WHERE group_id = ? AND topic_id = ?",
        (amount, group_id, topic_id)
    )

    # دریافت اطلاعات برای نمایش پیام
    # نکته: چون cursor فعلی درگیر نوشتن است، برای خواندن مشکلی ندارد اما برای اطمینان
    # مقادیر را پس از آپدیت‌ها می‌خوانیم
    
    # خواندن نام ذکر و تعدادش
    await cursor.execute("SELECT zekr_text, current_total FROM topic_zekrs WHERE id = ?", (zekr_id,))
    zekr_row = await cursor.fetchone()
    
    # خواندن مجموع کل
    await cursor.execute("SELECT current_total FROM topics WHERE group_id = ? AND topic_id = ?", (group_id, topic_id))
    topic_row = await cursor.fetchone()

    # خواندن متن سپاس (انتخاب تصادفی از دیتابیس)
    sepas_text = None
    try:
        # چک کردن آیا سپاس فعال است؟
        await cursor.execute("SELECT sepas_enabled FROM groups WHERE group_id = ?", (group_id,))
        group_sepas = await cursor.fetchone()
        
        if group_sepas and group_sepas['sepas_enabled']:
            # دریافت یک متن تصادفی
            await cursor.execute("""
                SELECT text FROM sepas_texts 
                WHERE (group_id = ? OR is_default = 1) 
                ORDER BY RANDOM() LIMIT 1
            """, (group_id,))
            sepas_row = await cursor.fetchone()
            if sepas_row:
                sepas_text = sepas_row['text']
    except Exception as e:
        logger.warning(f"Error fetching sepas text inside db: {e}")


    # ارسال پیام تایید به گروه (فقط اگر bot وجود داشته باشد)
    if bot and chat_id and zekr_row:
        zekr_text = zekr_row['zekr_text']
        zekr_total = zekr_row['current_total'] # اگر نیاز بود در پیام باشد
        topic_total = topic_row['current_total'] if topic_row else 0
        
        # --- ساخت پیام مشابه صلوات ---
        separator = "➖➖➖➖➖➖➖➖"
        action_text = "ثبت شد" if amount >= 0 else "کسر شد"
        abs_amount = abs(amount)
        
        # خط اول: ۱۰۰ ذکر (سبحان الله) ثبت شد!
        line1 = f"<b>{abs_amount:,} ذکر ({html.escape(zekr_text)}) {action_text}!</b>"
        # خط دوم: جمع کل
        line2 = f"<b>جمع کل: {zekr_total:,}</b>"
        
        message_parts = [line1, line2]
        
        # خط‌کش و متن سپاس
        message_parts.append(separator)
        if sepas_text:
            message_parts.append(f"<b>{html.escape(sepas_text)} 🌱</b>")
        else:
            message_parts.append("<b>🌱 التماس دعا 🌱</b>")
            
        final_message = "\n".join(message_parts)
        
        try:
            # استفاده از create_task برای اینکه منتظر ارسال نمانیم و دیتابیس قفل نشود
            async def send_notification():
                try:
                    sent_msg = await bot.send_message(
                        chat_id=chat_id,
                        text=final_message,
                        message_thread_id=thread_id,
                        parse_mode="HTML" # تغییر به HTML برای استایل جدید
                    )
                    # حذف خودکار
                    # await asyncio.sleep(15) # زمان حذف (قابل تنظیم با تنظیمات گروه)
                    # await sent_msg.delete()
                except Exception as ex:
                    logger.error(f"Failed to send/delete zekr notification: {ex}")

            asyncio.create_task(send_notification())
            
        except Exception as e:
            logger.error("Failed to initiate zekr confirmation message task: %s", e)







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
        "set_zekr_text": handle_set_zekr_text,
        "set_completion_count": handle_set_completion_count,
        "submit_zekr_contribution": handle_zekr_contribution,
        
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



async def is_group_banned(group_id: int) -> bool:
    """Check if a group is banned."""
    try:
        await init_db_connection()
        result = await fetch_one(
            "SELECT group_id FROM banned_groups WHERE group_id = ?",
            (group_id,)
        )
        return bool(result)
    except Exception as e:
        logger.error("Error checking banned group status: %s", e, exc_info=True)
        raise DatabaseError(f"Error checking banned group: {str(e)}", e)

async def ban_group(group_id: int) -> None:
    """Ban a group by adding it to banned_groups table."""
    try:
        await init_db_connection()
        await execute(
            "INSERT OR REPLACE INTO banned_groups (group_id, banned_at) VALUES (?, CURRENT_TIMESTAMP)",
            (group_id,)
        )
        logger.info("Banned group: group_id=%s", group_id)
    except Exception as e:
        logger.error("Error banning group: %s", e, exc_info=True)
        raise DatabaseError(f"Error banning group: {str(e)}", e)

async def unban_group(group_id: int) -> None:
    """Unban a group by removing it from banned_groups table."""
    try:
        await init_db_connection()
        await execute(
            "DELETE FROM banned_groups WHERE group_id = ?",
            (group_id,)
        )
        logger.info("Unbanned group: group_id=%s", group_id)
    except Exception as e:
        logger.error("Error unbanning group: %s", e, exc_info=True)
        raise DatabaseError(f"Error unbanning group: {str(e)}", e)
    


async def get_global_stats() -> dict:
    """Fetch global statistics for the dashboard."""
    try:
        await init_db_connection()
        stats = {}

        # Total groups
        total_groups = await fetch_one("SELECT COUNT(*) as count FROM groups")
        stats['total_groups'] = total_groups['count'] if total_groups else 0

        # Active groups
        active_groups = await fetch_one("SELECT COUNT(*) as count FROM groups WHERE is_active = 1")
        stats['active_groups'] = active_groups['count'] if active_groups else 0

        # Banned groups
        banned_groups = await fetch_one("SELECT COUNT(*) as count FROM banned_groups")
        stats['banned_groups'] = banned_groups['count'] if banned_groups else 0

        # Total users
        total_users = await fetch_one("SELECT COUNT(DISTINCT user_id) as count FROM users")
        stats['total_users'] = total_users['count'] if total_users else 0

        # Total contributions
        total_contributions = await fetch_one("SELECT COUNT(*) as count FROM contributions")
        stats['total_contributions'] = total_contributions['count'] if total_contributions else 0

        # Completed khatms
        completed_khatms = await fetch_one("SELECT COUNT(*) as count FROM topics WHERE is_completed = 1")
        stats['completed_khatms'] = completed_khatms['count'] if completed_khatms else 0

        logger.info("Fetched global stats: %s", stats)
        return stats
    except Exception as e:
        logger.error("Error fetching global stats: %s", e, exc_info=True)
        raise DatabaseError(f"Error fetching global stats: {str(e)}", e)

async def get_paginated_groups(page: int, per_page: int = 10) -> tuple[list, int]:
    """Fetch groups with pagination."""
    try:
        await init_db_connection()
        offset = (page - 1) * per_page
        groups = await fetch_all(
            "SELECT group_id, is_active FROM groups LIMIT ? OFFSET ?",
            (per_page, offset)
        )
        total_groups = await fetch_one("SELECT COUNT(*) as count FROM groups")
        total_pages = (total_groups['count'] + per_page - 1) // per_page if total_groups else 1
        logger.info("Fetched paginated groups: page=%s, per_page=%s, total_pages=%s", page, per_page, total_pages)
        return groups, total_pages
    except Exception as e:
        logger.error("Error fetching paginated groups: %s", e, exc_info=True)
        raise DatabaseError(f"Error fetching paginated groups: {str(e)}", e)
    
async def get_group_details(group_id: int) -> dict:
    """Fetch detailed information about a group."""
    try:
        await init_db_connection()
        details = {}

        # Get member count
        member_count = await fetch_one(
            "SELECT COUNT(DISTINCT user_id) as count FROM users WHERE group_id = ?",
            (group_id,)
        )
        details['member_count'] = member_count['count'] if member_count else 0

        # Get active khatms
        active_khatms = await fetch_one(
            "SELECT COUNT(*) as count FROM topics WHERE group_id = ? AND is_active = 1 AND is_completed = 0",
            (group_id,)
        )
        details['active_khatms'] = active_khatms['count'] if active_khatms else 0

        logger.info("Fetched group details: group_id=%s, details=%s", group_id, details)
        return details
    except Exception as e:
        logger.error("Error fetching group details: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error fetching group details: {str(e)}", e)

async def search_groups(search_id: str) -> list:
    """Search groups by group_id."""
    try:
        await init_db_connection()
        search_pattern = f"%{search_id}%"
        groups = await fetch_all(
            "SELECT group_id, is_active FROM groups WHERE CAST(group_id AS TEXT) LIKE ?",
            (search_pattern,)
        )
        logger.info("Searched groups: search_id=%s, found=%s", search_id, len(groups))
        return groups
    except Exception as e:
        logger.error("Error searching groups: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error searching groups: {str(e)}", e)


async def set_group_invite_link(group_id: int, invite_link: str) -> None:
    """Set or update the invite link for a group."""
    try:
        await init_db_connection()
        await execute(
            "UPDATE groups SET invite_link = ? WHERE group_id = ?",
            (invite_link, group_id)
        )
        logger.info("Set invite link for group: group_id=%s, link=%s", group_id, invite_link)
    except Exception as e:
        logger.error("Error setting invite link: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error setting invite link: {str(e)}", e)

async def get_group_invite_link(group_id: int) -> str:
    """Get the invite link for a group."""
    try:
        await init_db_connection()
        result = await fetch_one(
            "SELECT invite_link FROM groups WHERE group_id = ?",
            (group_id,)
        )
        return result['invite_link'] if result and result['invite_link'] else ""
    except Exception as e:
        logger.error("Error getting invite link: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error getting invite link: {str(e)}", e)

async def remove_group_invite_link(group_id: int) -> None:
    """Remove the invite link for a group."""
    try:
        await init_db_connection()
        await execute(
            "UPDATE groups SET invite_link = '' WHERE group_id = ?",
            (group_id,)
        )
        logger.info("Removed invite link for group: group_id=%s", group_id)
    except Exception as e:
        logger.error("Error removing invite link: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error removing invite link: {str(e)}", e)
async def set_group_title(group_id: int, title: str) -> None:
    """Set or update the title for a group."""
    try:
        await init_db_connection()
        await execute(
            "UPDATE groups SET title = ? WHERE group_id = ?",
            (title, group_id)
        )
        logger.info("Set group title: group_id=%s, title=%s", group_id, title)
    except Exception as e:
        logger.error("Error setting group title: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error setting group title: {str(e)}", e)

async def ban_user(user_id: int) -> None:
    """Ban a user by adding them to banned_users table."""
    try:
        await init_db_connection()
        await execute(
            "INSERT OR REPLACE INTO banned_users (user_id) VALUES (?)",
            (user_id,)
        )
        logger.info("Banned user: user_id=%s", user_id)
    except Exception as e:
        logger.error("Error banning user: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error banning user: {str(e)}", e)

async def unban_user(user_id: int) -> None:
    """Unban a user by removing them from banned_users table."""
    try:
        await init_db_connection()
        await execute(
            "DELETE FROM banned_users WHERE user_id = ?",
            (user_id,)
        )
        logger.info("Unbanned user: user_id=%s", user_id)
    except Exception as e:
        logger.error("Error unbanning user: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error unbanning user: {str(e)}", e)

async def is_user_banned(user_id: int) -> bool:
    """Check if a user is banned."""
    try:
        await init_db_connection()
        result = await fetch_one(
            "SELECT user_id FROM banned_users WHERE user_id = ?",
            (user_id,)
        )
        return bool(result)
    except Exception as e:
        logger.error("Error checking banned user: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error checking banned user: {str(e)}", e)

async def get_group_users(group_id: int, page: int = 1, per_page: int = 10) -> tuple[list, int]:
    """Fetch paginated list of users in a group."""
    try:
        await init_db_connection()
        offset = (page - 1) * per_page
        users = await fetch_all(
            "SELECT user_id FROM users WHERE group_id = ? LIMIT ? OFFSET ?",
            (group_id, per_page, offset)
        )
        total_users = await fetch_one(
            "SELECT COUNT(*) as count FROM users WHERE group_id = ?",
            (group_id,)
        )
        total_pages = (total_users['count'] + per_page - 1) // per_page if total_users else 1
        logger.info("Fetched paginated users: group_id=%s, page=%s, total_pages=%s", group_id, page, total_pages)
        return users, total_pages
    except Exception as e:
        logger.info("GET Group Users : %s",e)



async def generate_invite_links_for_all_groups(bot) -> None:
    """Generate invite links for all groups in the database."""
    try:
        await init_db_connection()
        groups = await fetch_all("SELECT group_id FROM groups")
        for group in groups:
            group_id = group["group_id"]
            try:
                invite_link = await bot.create_chat_invite_link(group_id, member_limit=None)
                await set_group_invite_link(group_id, invite_link.invite_link)
                logger.info("Generated invite link for group: group_id=%s, link=%s", group_id, invite_link.invite_link)
            except Exception as e:
                logger.error("Error generating invite link for group %s: %s", group_id, str(e), exc_info=True)
    except Exception as e:
        logger.error("Error generating invite links for all groups: %s", str(e), exc_info=True)
        raise DatabaseError(f"Error generating invite links: {str(e)}", e)



