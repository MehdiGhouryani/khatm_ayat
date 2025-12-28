import sqlite3
import os

# لیست مسیرهای احتمالی دیتابیس (بر اساس لاگ‌های شما)
POSSIBLE_PATHS = [
    "bot.db",
    "khatm.db",
    "/public_html/khatm_ayat/bot.db",
    "../bot.db"
]

def find_database():
    """پیدا کردن فایل دیتابیس در مسیرهای مختلف"""
    for path in POSSIBLE_PATHS:
        if os.path.exists(path):
            return path
    # جستجو در پوشه جاری اگر نام دیگری دارد
    for file in os.listdir("."):
        if file.endswith(".db"):
            return file
    return None

def inspect_database():
    db_path = find_database()
    
    print("="*60)
    print("🕵️‍♂️  بازرس دیتابیس (Database Inspector)")
    print("="*60)

    if not db_path:
        print("❌ فایل دیتابیس پیدا نشد! لطفاً فایل را کنار همین اسکریپت قرار دهید.")
        return

    print(f"📁 دیتابیس پیدا شد: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # 1. بررسی جدول‌های موجود
        print("\n📊 [1] لیست جدول‌های موجود:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for t in tables:
            print(f"   - {t['name']}")

        # 2. بررسی ساختار جدول‌های حیاتی
        target_tables = ['topics', 'doa_items', 'topics_old_temp']
        print("\n🏗  [2] ساختار جدول‌های مهم:")
        
        for tbl in target_tables:
            print(f"\n   🔹 جدول: {tbl}")
            try:
                cursor.execute(f"PRAGMA table_info({tbl})")
                columns = cursor.fetchall()
                if columns:
                    for col in columns:
                        print(f"      - {col['name']} ({col['type']})")
                else:
                    print("      ❌ این جدول وجود ندارد (که برای topics_old_temp خوب است)")
            except Exception:
                print("      ❌ خطا در خواندن اطلاعات جدول")

        # 3. شکار تریگرهای خراب (بخش حیاتی)
        print("\n🔫 [3] بررسی تریگرها (Triggers):")
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        triggers = cursor.fetchall()
        
        if not triggers:
            print("   ✅ هیچ تریگری یافت نشد.")
        
        problem_found = False
        for trig in triggers:
            name = trig['name']
            sql_content = trig['sql']
            
            # بررسی اینکه آیا به جدول حذف شده اشاره می‌کند
            if "topics_old_temp" in str(sql_content):
                print(f"   🚩 [خطرناک] تریگر: {name}")
                print(f"      ⚠️  این تریگر به topics_old_temp اشاره می‌کند و عامل ارور است!")
                problem_found = True
            else:
                print(f"   ✅ [سالم] تریگر: {name}")

        print("\n" + "="*60)
        if problem_found:
            print("🚨 نتیجه: مشکل پیدا شد! تریگرهای علامت‌گذاری شده با 🚩 باید حذف شوند.")
        else:
            print("✅ نتیجه: هیچ تریگر خرابی پیدا نشد (شاید مشکل جای دیگری است).")
        print("="*60)

    except Exception as e:
        print(f"❌ خطای غیرمنتظره: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    inspect_database()