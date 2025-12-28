import sqlite3
import os
import shutil
import time

# مسیر دیتابیس فعلی (طبق لاگ‌های شما)
OLD_DB = "bot.db"
NEW_DB = "bot_new.db"
BACKUP_DB = f"bot_backup_{int(time.time())}.db"

def rebuild_database():
    if not os.path.exists(OLD_DB):
        print(f"❌ دیتابیس {OLD_DB} پیدا نشد!")
        return

    print(f"🏗 شروع بازسازی دیتابیس...")
    print(f"   📂 دیتابیس قدیمی: {OLD_DB}")
    print(f"   🆕 دیتابیس جدید: {NEW_DB}")

    # اتصال به قدیم و جدید
    conn_old = sqlite3.connect(OLD_DB)
    conn_new = sqlite3.connect(NEW_DB)
    
    cursor_old = conn_old.cursor()
    cursor_new = conn_new.cursor()

    try:
        # 1. گرفتن لیست تمام جدول‌ها (فقط جدول‌ها، نه تریگرها!)
        cursor_old.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        tables = cursor_old.fetchall()

        for name, sql in tables:
            # نادیده گرفتن جداول سیستمی و جداول موقت خراب
            if name.startswith("sqlite_") or "temp" in name or "broken" in name:
                continue

            print(f"   📦 در حال انتقال جدول: {name}...")

            # الف) ساخت جدول در دیتابیس جدید
            # اگر در کد ساخت جدول، اشاره‌ای به جدول‌های temp خراب باشد، اصلاح می‌کنیم
            clean_sql = sql
            if "topic_zekrs_broken_temp" in clean_sql:
                clean_sql = clean_sql.replace("topic_zekrs_broken_temp", "topic_zekrs")
            if "topics_old_temp" in clean_sql:
                clean_sql = clean_sql.replace("topics_old_temp", "topics")
            
            cursor_new.execute(clean_sql)

            # ب) کپی کردن داده‌ها
            cursor_old.execute(f"SELECT * FROM {name}")
            rows = cursor_old.fetchall()
            
            if rows:
                # ساخت دستور INSERT دینامیک
                placeholders = ",".join(["?"] * len(rows[0]))
                cursor_new.executemany(f"INSERT INTO {name} VALUES ({placeholders})", rows)
                print(f"      ✅ {len(rows)} ردیف منتقل شد.")
            else:
                print("      ⚠️ جدول خالی است (منتقل شد).")

        conn_new.commit()
        print("\n✅ انتقال اطلاعات تمام شد. دیتابیس جدید فاقد تریگرهای خراب است.")
        
        # بستن اتصالات
        conn_old.close()
        conn_new.close()

        # 2. جایگزینی فایل‌ها
        print("\n🔄 در حال جایگزینی فایل‌ها...")
        
        # بکاپ گرفتن از فایل خراب فعلی
        shutil.move(OLD_DB, BACKUP_DB)
        print(f"   بکاپ فایل قدیمی ذخیره شد در: {BACKUP_DB}")
        
        # جایگزین کردن فایل جدید
        shutil.move(NEW_DB, OLD_DB)
        print(f"   🎉 فایل جدید جایگزین شد: {OLD_DB}")
        
        print("\n🚀 عملیات با موفقیت کامل شد. حالا ربات را اجرا کنید.")

    except Exception as e:
        print(f"\n❌ خطا در بازسازی: {e}")
        # اگر خطا داد، فایل‌های نیمه کاره را پاک کن
        if os.path.exists(NEW_DB):
            os.remove(NEW_DB)
    finally:
        if conn_old: conn_old.close()
        if conn_new: conn_new.close()

if __name__ == "__main__":
    rebuild_database()