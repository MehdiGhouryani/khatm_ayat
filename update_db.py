import sqlite3
import os

# مسیر دقیق دیتابیس (طبق لاگ‌های قبلی شما)
DB_PATH = "bot.db"  # چون اسکریپت قبلی با این نام موفق شد

def fix_triggers():
    if not os.path.exists(DB_PATH):
        print(f"❌ دیتابیس {DB_PATH} پیدا نشد!")
        return

    print(f"🔧 در حال اتصال به {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 1. لیست کردن تمام تریگرها
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        triggers = cursor.fetchall()
        
        broken_count = 0
        print(f"🔍 بررسی {len(triggers)} تریگر موجود...")

        for name, sql in triggers:
            if "topics_old_temp" in sql:
                print(f"⚠️ تریگر خراب پیدا شد: {name}")
                cursor.execute(f"DROP TRIGGER IF EXISTS {name}")
                print(f"   🗑 تریگر {name} حذف شد.")
                broken_count += 1
        
        if broken_count == 0:
            print("✅ هیچ تریگر خرابی یافت نشد.")
        else:
            print(f"🎉 تعداد {broken_count} تریگر خراب پاکسازی شد.")
            conn.commit()

        # 2. بررسی جدول topics_old_temp
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='topics_old_temp'")
        if cursor.fetchone():
            print("🗑 جدول موقت topics_old_temp پیدا شد، در حال حذف...")
            cursor.execute("DROP TABLE topics_old_temp")
            conn.commit()
            print("✅ جدول موقت حذف شد.")

    except Exception as e:
        print(f"❌ خطا: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    fix_triggers()