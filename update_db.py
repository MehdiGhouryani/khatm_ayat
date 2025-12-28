import sqlite3
import os

# تلاش برای پیدا کردن فایل دیتابیس در پوشه جاری یا پوشه‌های والد
POSSIBLE_PATHS = [
    "bot.db",
    "khatm.db",
    "/public_html/khatm_ayat/bot.db",
    "../bot.db"
]

def find_database():
    for path in POSSIBLE_PATHS:
        if os.path.exists(path):
            return path
    return None

def fix_database():
    db_path = find_database()
    if not db_path:
        print("❌ فایل دیتابیس پیدا نشد! لطفا فایل را کنار main.py قرار دهید.")
        return

    print(f"🔧 دیتابیس پیدا شد: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        print("1️⃣ شروع پاکسازی تریگرهای خراب...")
        # گرفتن همه تریگرها
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        all_triggers = cursor.fetchall()
        
        deleted_count = 0
        for name, sql in all_triggers:
            # اگر تریگر به جدول temp اشاره می‌کند
            if "topics_old_temp" in str(sql):
                print(f"   🗑 حذف تریگر خراب: {name}")
                cursor.execute(f"DROP TRIGGER IF EXISTS {name}")
                deleted_count += 1
        
        # اگر تریگر خاصی (مثل update_topics_timestamp) مشکل‌ساز است، آن را بازسازی می‌کنیم
        cursor.execute("DROP TRIGGER IF EXISTS update_topics_timestamp")
        print("   🔄 تریگر update_topics_timestamp حذف شد (برای بازسازی).")
        
        # ساخت مجدد تریگر سالم
        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_topics_timestamp
        AFTER UPDATE ON topics
        FOR EACH ROW
        BEGIN
            UPDATE topics SET updated_at = CURRENT_TIMESTAMP
            WHERE group_id = OLD.group_id AND topic_id = OLD.topic_id;
        END;
        """)
        print("   ✅ تریگر سالم update_topics_timestamp ساخته شد.")

        if deleted_count > 0:
            print(f"🎉 {deleted_count} تریگر خراب دیگر هم پاک شد.")
        else:
            print("✅ تریگر خراب دیگری یافت نشد.")

        conn.commit()
        print("✅ عملیات تعمیر دیتابیس با موفقیت تمام شد.")

    except Exception as e:
        print(f"❌ خطا: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_database()