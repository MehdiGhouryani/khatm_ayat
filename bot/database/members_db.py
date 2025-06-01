import aiosqlite
import logging
from telegram.error import TelegramError
from telegram import User

logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite"

async def execute(query: str, params: tuple = ()):
    """
    Executes a query (INSERT, UPDATE, DELETE) on members.sqlite asynchronously.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            await conn.execute(query, params)
            await conn.commit()
            logger.debug(f"Successfully executed query on members.sqlite: {query} with params {params}")
    except aiosqlite.Error as e:
        logger.error(f"aiosqlite error executing query on members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error executing query on members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise

async def fetch_all(query: str, params: tuple = ()):
    """
    Fetches all rows from a query on members.sqlite asynchronously.
    Results are returned as a list of dictionaries.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            conn.row_factory = aiosqlite.Row
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
    Fetches one row from a query on members.sqlite asynchronously.
    Returns a dictionary or None if no row is found.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            conn.row_factory = aiosqlite.Row
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