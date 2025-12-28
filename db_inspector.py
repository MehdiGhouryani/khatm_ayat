import sqlite3
import os

# تلاش برای یافتن دیتابیس در مسیرهای مختلف
POSSIBLE_PATHS = [
    "bot.db",
    "/home/rhaegali/public_html/khatm_ayat/bot.db",
    "khatm.db"
]

def find_db():
    for p in POSSIBLE_PATHS:
        if os.path.exists(p):
            return p
    return None

def rescue_database():
    db_path = find_db()
    if not db_path:
        print("❌ دیتابیس پیدا نشد!")
        return

    print(f"🚑 شروع عملیات نجات روی: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 1. حذف تمام تریگرها (منبع اصلی شرارت!)
        print("🔫 در حال حذف تمام تریگرها...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
        triggers = cursor.fetchall()
        
        if not triggers:
            print("   ✅ هیچ تریگری پیدا نشد.")
        
        for (name,) in triggers:
            print(f"   🗑 حذف تریگر: {name}")
            cursor.execute(f"DROP TRIGGER IF EXISTS {name}")

        # 2. حذف تمام جداول موقت و خراب (با پسوند temp)
        print("\n🧹 در حال جستجوی جداول موقت مزاحم...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for (name,) in tables:
            if "temp" in name.lower():
                print(f"   🗑 حذف جدول موقت: {name}")
                cursor.execute(f"DROP TABLE IF EXISTS {name}")

        # 3. اطمینان از وجود جدول‌های اصلی
        print("\n🏥 چکاپ نهایی جداول اصلی...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='topic_zekrs'")
        if cursor.fetchone():
            print("   ✅ جدول topic_zekrs سالم است.")
        else:
            print("   ⚠️ هشدار: جدول topic_zekrs پیدا نشد! (این عجیب است)")

        conn.commit()
        
        # 4. بهینه‌سازی نهایی
        print("\n✨ فشرده‌سازی و بازسازی دیتابیس (VACUUM)...")
        cursor.execute("VACUUM")
        
        print("\n✅✅ عملیات تمام شد. دیتابیس الان باید مثل روز اول کار کند.")

    except Exception as e:
        print(f"❌ خطا: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    rescue_database()