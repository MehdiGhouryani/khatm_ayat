import sqlite3
import os

def fix_database_triggers():
    # 1. تعریف مسیر در ابتدای تابع برای جلوگیری از خطا
    target_db_path = "/public_html/khatm_ayat/bot.db"
    
    # بررسی وجود فایل و اصلاح مسیر در صورت نیاز
    if not os.path.exists(target_db_path):
        if os.path.exists("bot.db"):
            target_db_path = "bot.db"
            print(f"⚠️ مسیر کامل یافت نشد، استفاده از مسیر نسبی: {target_db_path}")
        # اگر فایل کلا پیدا نشد، پایین‌تر ارور میدهیم

    print(f"🔧 در حال اتصال به دیتابیس در مسیر : {target_db_path}")

    if not os.path.exists(target_db_path):
        print(f"❌ فایل دیتابیس در مسیر زیر پیدا نشد:\n{target_db_path}")
        return

    conn = sqlite3.connect(target_db_path)
    cursor = conn.cursor()

    try:
        # 2. پیدا کردن تریگرهای خراب
        print("🔍 در حال جستجوی تریگرهای خراب...")
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
        all_triggers = cursor.fetchall()
        
        broken_triggers = []
        for name, sql in all_triggers:
            if sql and "topics_old_temp" in sql:
                broken_triggers.append(name)
        
        if not broken_triggers:
            print("✅ هیچ تریگر خرابی پیدا نشد.")
        else:
            print(f"⚠️ تعداد {len(broken_triggers)} تریگر خراب پیدا شد.")
            for trigger_name in broken_triggers:
                print(f"   🗑 در حال حذف تریگر: {trigger_name} ...")
                cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
            
            conn.commit()
            print("🎉 تمام تریگرهای خراب با موفقیت حذف شدند.")

        # 3. پاکسازی نهایی
        cursor.execute("DROP TABLE IF EXISTS topics_old_temp")
        conn.commit()
        print("🧹 پاکسازی نهایی انجام شد.")

    except Exception as e:
        print(f"❌ خطا در تعمیر دیتابیس: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("🔒 اتصال دیتابیس بسته شد.")

if __name__ == "__main__":
    fix_database_triggers()