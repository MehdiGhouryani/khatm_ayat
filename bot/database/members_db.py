import aiosqlite
import logging

logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite"  # اطمینان حاصل کنید این مسیر به فایل members.sqlite شما اشاره دارد

async def execute(query: str, params: tuple = ()):
    """
    یک کوئری (INSERT, UPDATE, DELETE) را روی members.sqlite به صورت ناهمگام اجرا می‌کند.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            await conn.execute(query, params)
            await conn.commit()
            logger.debug(f"Successfully executed query on members.sqlite: {query} with params {params}")
    except aiosqlite.Error as e:
        logger.error(f"aiosqlite error executing query on members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise  # خطا را برای مدیریت احتمالی در لایه بالاتر دوباره پرتاب می‌کنیم
    except Exception as e:
        logger.error(f"Unexpected error executing query on members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise

async def fetch_all(query: str, params: tuple = ()):
    """
    تمام ردیف‌های حاصل از یک کوئری را از members.sqlite به صورت ناهمگام واکشی می‌کند.
    نتایج به صورت لیستی از دیکشنری‌ها بازگردانده می‌شوند.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            conn.row_factory = aiosqlite.Row  # برای دسترسی به ستون‌ها با نام آنها
            async with conn.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                result = [dict(row) for row in rows]
                logger.debug(f"Fetched {len(result)} rows from members.sqlite for query: {query}")
                return result
    except aiosqlite.Error as e:
        logger.error(f"aiosqlite error fetching all from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching all from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise

async def fetch_one(query: str, params: tuple = ()):
    """
    یک ردیف حاصل از یک کوئری را از members.sqlite به صورت ناهمگام واکشی می‌کند.
    نتیجه به صورت یک دیکشنری یا None (اگر ردیفی یافت نشود) بازگردانده می‌شود.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            conn.row_factory = aiosqlite.Row  # برای دسترسی به ستون‌ها با نام آنها
            async with conn.execute(query, params) as cursor:
                row = await cursor.fetchone()
                result = dict(row) if row else None
                logger.debug(f"Fetched one row from members.sqlite for query: {query}")
                return result
    except aiosqlite.Error as e:
        logger.error(f"aiosqlite error fetching one from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching one from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise