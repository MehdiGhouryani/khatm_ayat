import sqlite3
import asyncio
import logging

logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite" # اطمینان حاصل کنید این مسیر به فایل members.sqlite شما اشاره دارد

def _execute_query_sync(query, params, fetch_all_rows=False, fetch_one_row=False, commit_changes=False):
    """
    یک تابع همگام (synchronous) برای اجرای کوئری‌ها در یک نخ جداگانه.
    اتصال و نشانگر (cursor) در همین نخ ایجاد و استفاده می‌شوند.
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row  # برای دسترسی به ستون‌ها با نام آنها
        cursor = conn.cursor()
        cursor.execute(query, params)

        if commit_changes:
            conn.commit()
            logger.debug(f"Query committed: {query} with params {params}")
            return None  # برای دستورات INSERT, UPDATE, DELETE می‌توان ID ردیف آخر را نیز برگرداند (cursor.lastrowid)

        if fetch_all_rows:
            rows = cursor.fetchall()
            # تبدیل ردیف‌های sqlite3.Row به دیکشنری قبل از بازگشت از نخ
            # این کار مهم است زیرا اشیاء sqlite3.Row ممکن است محدودیت‌های نخی داشته باشند
            result = [dict(row) for row in rows]
            logger.debug(f"Fetched {len(result)} rows for query: {query}")
            return result
        
        if fetch_one_row:
            row = cursor.fetchone()
            # تبدیل ردیف sqlite3.Row به دیکشنری
            result = dict(row) if row else None
            logger.debug(f"Fetched one row for query: {query}")
            return result
        
        # اگر هیچ‌کدام از fetch_all, fetch_one یا commit_changes (که نباید رخ دهد)
        return None

    except sqlite3.Error as e: # گرفتن خطاهای خاص SQLite
        logger.error(f"SQLite error during DB operation (query: {query}, params: {params}): {e}", exc_info=True)
        raise  # خطا را برای مدیریت احتمالی در لایه بالاتر دوباره پرتاب می‌کنیم
    except Exception as e:
        logger.error(f"Unexpected error during DB operation (query: {query}, params: {params}): {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

async def execute(query, params=()):
    """
    یک کوئری (INSERT, UPDATE, DELETE) را روی members.sqlite اجرا می‌کند.
    """
    try:
        loop = asyncio.get_running_loop()
        # پارامتر commit_changes=True برای اطمینان از commit شدن تغییرات
        await loop.run_in_executor(None, _execute_query_sync, query, params, False, False, True)
    except Exception as e:
        # لاگ خطا قبلاً در _execute_query_sync انجام شده، فقط خطا را برای مدیریت احتمالی بالاتر پرتاب می‌کنیم
        logger.error(f"Async wrapper error for execute (query: {query}): {e}", exc_info=True)
        raise

async def fetch_all(query, params=()):
    """
    تمام ردیف‌های حاصل از یک کوئری را از members.sqlite واکشی می‌کند.
    """
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _execute_query_sync, query, params, True, False, False)
        return result
    except Exception as e:
        logger.error(f"Async wrapper error for fetch_all (query: {query}): {e}", exc_info=True)
        raise

async def fetch_one(query, params=()):
    """
    یک ردیف حاصل از یک کوئری را از members.sqlite واکشی می‌کند.
    """
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _execute_query_sync, query, params, False, True, False)
        return result
    except Exception as e:
        logger.error(f"Async wrapper error for fetch_one (query: {query}): {e}", exc_info=True)
        raise