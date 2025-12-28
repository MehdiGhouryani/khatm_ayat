import sqlite3
import os

# تلاش برای پیدا کردن مسیر دیتابیس
# اگر نام دیتابیس شما چیز دیگری است، اینجا تغییر دهید (مثلا bot.db یا khatm.db)
DATABASE_PATH = "khatm.db" 
# اگر فایل settings دارید:
try:
    from config.settings import DATABASE_PATH
except ImportError:
    pass

def fix_database_triggers():
    print(f"🔧 در حال بررسی و تعمیر دیتابیس: {DATABASE_PATH}")
    
    if not os.path.exists(DATABASE_PATH):
        print("❌ فایل دیتابیس پیدا نشد!")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # 1. پیدا کردن تریگرهای خراب
        # تریگرهایی که در کدشان به topics_old_temp اشاره شده است
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
        all_triggers = cursor.fetchall()
        
        broken_triggers = []
        for name, sql in all_triggers:
            if sql and "topics_old_temp" in sql:
                broken_triggers.append(name)
        
        if not broken_triggers:
            print("✅ هیچ تریگر خرابی که به topics_old_temp اشاره کند پیدا نشد.")
        else:
            print(f"⚠️ تعداد {len(broken_triggers)} تریگر خراب پیدا شد.")
            for trigger_name in broken_triggers:
                print(f"   🗑 در حال حذف تریگر: {trigger_name} ...")
                cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
            
            conn.commit()
            print("🎉 تمام تریگرهای خراب با موفقیت حذف شدند.")

        # 2. محض اطمینان، حذف جدول موقت اگر مانده باشد
        cursor.execute("DROP TABLE IF EXISTS topics_old_temp")
        conn.commit()

    except Exception as e:
        print(f"❌ خطا در تعمیر: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_database_triggers()