import sqlite3
import asyncio
import logging

logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite"

def _execute_query_sync(query, params, fetch_all_rows=False, fetch_one_row=False, commit_changes=False):
    """
    یک تابع همگام برای اجرای کوئری‌ها در یک نخ جداگانه.
    اتصال و نشانگر در همین نخ ایجاد و استفاده می‌شوند.
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row  # برای دسترسی به ستون‌ها با نام
        cursor = conn.cursor()
        cursor.execute(query, params)

        if commit_changes:
            conn.commit()
            logger.debug(f"Query committed: {query} with params {params}")
            return None  # یا مثلا cursor.lastrowid اگر برای INSERT نیاز باشد

        if fetch_all_rows:
            rows = cursor.fetchall()
            # تبدیل ردیف‌های sqlite3.Row به دیکشنری قبل از بازگشت از نخ
            result = [dict(row) for row in rows]
            logger.debug(f"Fetched {len(result)} rows for query: {query}")
            return result
        
        if fetch_one_row:
            row = cursor.fetchone()
            # تبدیل ردیف sqlite3.Row به دیکشنری قبل از بازگشت از نخ
            result = dict(row) if row else None
            logger.debug(f"Fetched one row for query: {query}")
            return result
        
        # اگر نه fetch_all، نه fetch_one و نه commit_changes (که نباید اتفاق بیفتد)
        return None

    except sqlite3.Error as e: # گرفتن خطاهای خاص SQLite
        logger.error(f"SQLite error during DB operation (query: {query}): {e}", exc_info=True)
        raise  # خطا را برای مدیریت احتمالی در لایه بالاتر پرتاب می‌کنیم
    except Exception as e:
        logger.error(f"Unexpected error during DB operation (query: {query}): {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

async def execute(query, params=()):
    """Execute a query (INSERT, UPDATE, DELETE) on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        # پارامتر commit_changes=True برای اطمینان از commit شدن تغییرات
        await loop.run_in_executor(None, _execute_query_sync, query, params, False, False, True)
    except Exception as e:
        # لاگ خطا قبلاً در _execute_query_sync انجام شده، فقط خطا را برای مدیریت احتمالی بالاتر پرتاب می‌کنیم
        logger.error(f"Async wrapper error for execute (query: {query}): {e}", exc_info=True)
        raise

async def fetch_all(query, params=()):
    """Fetch all rows from a query on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _execute_query_sync, query, params, True, False, False)
        return result
    except Exception as e:
        logger.error(f"Async wrapper error for fetch_all (query: {query}): {e}", exc_info=True)
        raise

async def fetch_one(query, params=()):
    """Fetch one row from a query on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _execute_query_sync, query, params, False, True, False)
        return result
    except Exception as e:
        logger.error(f"Async wrapper error for fetch_one (query: {query}): {e}", exc_info=True)
        raise