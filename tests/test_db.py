import pytest
import sqlite3
from bot.database.db import get_db_connection, init_db

def test_get_db_connection():
    conn = get_db_connection()
    assert isinstance(conn, sqlite3.Connection)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    conn.close()

def test_init_db(db_connection):
    init_db()  # Re-run init_db to ensure schema is applied
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='groups'")
    assert cursor.fetchone() is not None