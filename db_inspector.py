import sqlite3
import os
import datetime

# لیست مسیرهای احتمالی دیتابیس
POSSIBLE_PATHS = [
    "bot.db",
    "khatm.db",
    "../bot.db",
    "/home/rhaegali/public_html/khatm_ayat/bot.db", # مسیر سرور شما طبق لاگ
    "/public_html/khatm_ayat/bot.db"
]

OUTPUT_FILE = "db_report.txt"

def find_database():
    """پیدا کردن فایل دیتابیس"""
    for path in POSSIBLE_PATHS:
        if os.path.exists(path):
            return path
    # جستجو در پوشه جاری
    for file in os.listdir("."):
        if file.endswith(".db") and "user" not in file:
            return file
    return None

def inspect_database():
    db_path = find_database()
    
    # باز کردن فایل برای نوشتن گزارش
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        
        # تابع کمکی برای چاپ همزمان در فایل و کنسول
        def log(text=""):
            print(text)
            f.write(text + "\n")

        log("="*60)
        log(f"🕵️‍♂️ گزارش وضعیت دیتابیس - {datetime.datetime.now()}")
        log("="*60)

        if not db_path:
            log("❌ فایل دیتابیس پیدا نشد! لطفاً این فایل را کنار main.py اجرا کنید.")
            return

        log(f"📁 دیتابیس متصل شده: {os.path.abspath(db_path)}")
        
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # -----------------------------------------------------
            # 1. لیست جدول‌ها
            # -----------------------------------------------------
            log("\n📊 [1] لیست جدول‌های موجود:")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            table_names = [t['name'] for t in tables]
            for name in table_names:
                log(f"   - {name}")

            # -----------------------------------------------------
            # 2. جزئیات جدول‌های مشکوک
            # -----------------------------------------------------
            target_tables = ['topics', 'doa_items', 'topics_old_temp', 'groups']
            log("\n🏗  [2] ساختار جدول‌های حیاتی:")
            
            for tbl in target_tables:
                log(f"\n   🔹 بررسی جدول: {tbl}")
                if tbl not in table_names:
                    log("      ❌ این جدول وجود ندارد (اگر topics_old_temp است، یعنی پاک شده).")
                    continue
                
                try:
                    cursor.execute(f"PRAGMA table_info({tbl})")
                    columns = cursor.fetchall()
                    for col in columns:
                        log(f"      - {col['name']} ({col['type']})")
                except Exception as e:
                    log(f"      ❌ خطا: {e}")

            # -----------------------------------------------------
            # 3. بررسی دقیق تریگرها (بخش اصلی مشکل)
            # -----------------------------------------------------
            log("\n🔫 [3] بررسی تریگرها (Triggers):")
            cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='trigger'")
            triggers = cursor.fetchall()
            
            if not triggers:
                log("   ✅ هیچ تریگری یافت نشد.")
            
            problem_found = False
            for trig in triggers:
                name = trig['name']
                tbl_name = trig['tbl_name']
                sql_content = trig['sql']
                
                log(f"\n   🔸 نام تریگر: {name}")
                log(f"      متصل به جدول: {tbl_name}")
                log(f"      کد SQL: {sql_content}")
                
                if "topics_old_temp" in str(sql_content):
                    log(f"      🚩 [خطرناک] این تریگر عامل ارور است!")
                    problem_found = True
                else:
                    log("      ✅ وضعیت: به نظر سالم می‌رسد.")

            log("\n" + "="*60)
            if problem_found:
                log("🚨 نتیجه نهایی: تریگرهای خراب پیدا شدند. فایل گزارش را بررسی کنید.")
            else:
                log("✅ نتیجه نهایی: تریگر خرابی با نام topics_old_temp یافت نشد.")
            log("="*60)

        except Exception as e:
            log(f"\n❌ خطای غیرمنتظره در حین بازرسی: {e}")
        finally:
            if 'conn' in locals():
                conn.close()
            log(f"\n📄 گزارش کامل در فایل '{OUTPUT_FILE}' ذخیره شد.")

if __name__ == "__main__":
    inspect_database()