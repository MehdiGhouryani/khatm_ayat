import sqlite3
import os

# مسیر دقیق دیتابیس شما
DATABASE_PATH = "/public_html/khatm_ayat/bot.db"

def fix_database_triggers():
    print(f"🔧 در حال اتصال به دیتابیس در مسیر: {DATABASE_PATH}")
    
    # بررسی وجود فایل قبل از اتصال
    if not os.path.exists(DATABASE_PATH):
        # تلاش برای مسیر نسبی اگر مسیر کامل پیدا نشد
        if os.path.exists("bot.db"):
            DATABASE_PATH = "bot.db"
            print(f"⚠️ مسیر کامل یافت نشد، استفاده از مسیر نسبی: {DATABASE_PATH}")
        else:
            print("❌ فایل دیتابیس پیدا نشد! لطفاً مسیر را بررسی کنید.")
            return

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # 1. پیدا کردن تریگرهای خراب (تریگرهایی که به topics_old_temp اشاره دارند)
        print("🔍 در حال جستجوی تریگرهای خراب...")
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
        all_triggers = cursor.fetchall()
        
        broken_triggers = []
        for name, sql in all_triggers:
            if sql and "topics_old_temp" in sql:
                broken_triggers.append(name)
        
        if not broken_triggers:
            print("✅ هیچ تریگر خرابی پیدا نشد. دیتابیس سالم به نظر می‌رسد.")
        else:
            print(f"⚠️ تعداد {len(broken_triggers)} تریگر خراب پیدا شد.")
            for trigger_name in broken_triggers:
                print(f"   🗑 در حال حذف تریگر: {trigger_name} ...")
                cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
            
            conn.commit()
            print("🎉 تمام تریگرهای خراب با موفقیت حذف شدند.")

        # 2. پاکسازی نهایی (حذف جدول موقت اگر باقی مانده باشد)
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