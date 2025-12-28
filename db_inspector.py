import sqlite3
import os

DB_PATH = "bot.db"

def clean_temp_tables():
    if not os.path.exists(DB_PATH):
        print("❌ دیتابیس پیدا نشد.")
        return

    print(f"🧹 شروع عملیات پاکسازی جداول موقت در {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # پیدا کردن تمام جدول‌هایی که اسمشان با _temp تمام می‌شود
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_temp'")
        temp_tables = cursor.fetchall()

        if not temp_tables:
            print("✅ هیچ جدول موقت مزاحمی پیدا نشد.")
        else:
            for (table_name,) in temp_tables:
                print(f"   🗑 در حال حذف جدول موقت: {table_name}")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            conn.commit()
            print(f"🎉 {len(temp_tables)} جدول موقت با موفقیت پاک شدند.")

        # چک کردن دوباره تریگرها برای اطمینان
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        triggers = cursor.fetchall()
        for name, sql in triggers:
            if "_temp" in str(sql):
                print(f"   ⚠️ تریگر مشکوک پیدا شد: {name} -> حذف می‌شود.")
                cursor.execute(f"DROP TRIGGER IF EXISTS {name}")
                conn.commit()

    except Exception as e:
        print(f"❌ خطا: {e}")
    finally:
        cursor.execute("VACUUM") # فشرده‌سازی و بهینه‌سازی نهایی دیتابیس
        conn.close()
        print("✨ دیتابیس بهینه‌سازی (VACUUM) شد.")

if __name__ == "__main__":
    clean_temp_tables()