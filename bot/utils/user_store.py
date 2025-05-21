import sqlite3
import logging
import os
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

class UserStore:
    """کلاس مدیریت ذخیره‌سازی کاربران گروه"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(UserStore, cls).__new__(cls)
                cls._instance._initialize_db()
            return cls._instance
    
    def _initialize_db(self):
        """ساخت دیتابیس برای اولین بار"""
        os.makedirs("data", exist_ok=True)
        self.db_path = "data/users.db"
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_users (
            chat_id INTEGER,
            user_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        )
        ''')
        self.conn.commit()
        logger.info(f"دیتابیس کاربران در مسیر {self.db_path} آماده شد")
    
    def add_user(self, chat_id, user_id, username=None, first_name=None, last_name=None):
        """افزودن کاربر جدید یا به‌روزرسانی اطلاعات کاربر موجود"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO chat_users (chat_id, user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?, ?)
            ''', (chat_id, user_id, username, first_name, last_name))
            self.conn.commit()
            logger.debug(f"کاربر {user_id} در گروه {chat_id} ذخیره شد")
            return True
        except Exception as e:
            logger.error(f"خطا در ذخیره کاربر: {e}")
            return False
    
    def get_chat_users(self, chat_id, limit=200):
        """دریافت لیست کاربران یک گروه"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            SELECT user_id, username, first_name, last_name FROM chat_users
            WHERE chat_id = ? LIMIT ?
            ''', (chat_id, limit))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"خطا در دریافت لیست کاربران: {e}")
            return []
    
    def close(self):
        """بستن اتصال دیتابیس"""
        if hasattr(self, 'conn'):
            self.conn.close()

# اضافه کردن handler برای بستن دیتابیس هنگام خروج برنامه
import atexit

def _cleanup():
    try:
        UserStore().close()
        logger.info("اتصال دیتابیس کاربران بسته شد")
    except Exception as e:
        logger.error(f"خطا در بستن دیتابیس کاربران: {e}")

atexit.register(_cleanup) 