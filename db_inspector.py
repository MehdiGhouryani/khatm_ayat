import sqlite3
import os
import re

# مسیر دیتابیس
DB_PATH = "bot.db"

def repair_foreign_keys():
    if not os.path.exists(DB_PATH):
        print("❌ فایل دیتابیس پیدا نشد.")
        return

    print(f"🔧 در حال تعمیر کلیدهای خارجی دیتابیس: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 1. پیدا کردن جدول‌های بیمار (که به topics_old_temp اشاره می‌کنند)
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        broken_tables = []
        for name, sql in tables:
            if sql and "topics_old_temp" in sql:
                print(f"⚠️ جدول خراب پیدا شد: {name}")
                broken_tables.append((name, sql))

        if not broken_tables:
            print("✅ هیچ جدول خرابی پیدا نشد (شاید مشکل جای دیگری است).")
            return

        # 2. تعمیر جدول‌ها
        # روش تعمیر: تغییر نام جدول خراب -> ساخت جدول جدید با آدرس درست -> کپی داده‌ها -> حذف جدول خراب
        cursor.execute("PRAGMA foreign_keys=OFF;") # خاموش کردن موقت بررسی
        cursor.execute("BEGIN TRANSACTION;")

        for table_name, old_sql in broken_tables:
            print(f"   🚑 در حال جراحی جدول {table_name}...")
            
            # الف) تغییر نام جدول فعلی
            temp_name = f"{table_name}_broken_temp"
            cursor.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name}")
            
            # ب) ساخت کد جدید (جایگزینی آدرس غلط با درست)
            # با regex کلمه topics_old_temp را با topics عوض می‌کنیم
            new_sql = old_sql.replace("topics_old_temp", "topics")
            
            # ج) ساخت جدول سالم
            cursor.execute(new_sql)
            
            # د) کپی اطلاعات از خراب به سالم
            print(f"      🔄 در حال بازگرداندن اطلاعات {table_name}...")
            cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_name}")
            
            # ه) حذف جدول خراب
            cursor.execute(f"DROP TABLE {temp_name}")
            print(f"      ✅ جدول {table_name} با موفقیت تعمیر شد.")

        conn.commit()
        print("\n🎉 تمام جدول‌های خراب تعمیر شدند. مشکل دیتابیس حل شد!")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ خطا در عملیات تعمیر: {e}")
    finally:
        cursor.execute("PRAGMA foreign_keys=ON;")
        conn.close()

if __name__ == "__main__":
    repair_foreign_keys()