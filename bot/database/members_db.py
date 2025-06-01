import sqlite3
import asyncio
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite"

@contextmanager
def get_db_connection():
    """Provide a database connection for members.sqlite."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

async def execute(query, params=()):
    """Execute a query on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            await loop.run_in_executor(None, cursor.execute, query, params)
            conn.commit()
            logger.debug(f"Executed query: {query} with params {params}")
    except Exception as e:
        logger.error(f"Error executing query: {query}, error: {e}", exc_info=True)
        raise

async def fetch_all(query, params=()):
    """Fetch all rows from a query on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            await loop.run_in_executor(None, cursor.execute, query, params)
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]
            logger.debug(f"Fetched {len(result)} rows for query: {query}")
            return result
    except Exception as e:
        logger.error(f"Error fetching all for query: {query}, error: {e}", exc_info=True)
        raise

async def fetch_one(query, params=()):
    """Fetch one row from a query on members.sqlite."""
    try:
        loop = asyncio.get_running_loop()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            await loop.run_in_executor(None, cursor.execute, query, params)
            row = cursor.fetchone()
            result = dict(row) if row else None
            logger.debug(f"Fetched one row for query: {query}")
            return result
    except Exception as e:
        logger.error(f"Error fetching one for query: {query}, error: {e}", exc_info=True)
        raise