import sqlite3
import os

# تلاش برای پیدا کردن مسیر دیتابیس از تنظیمات، اگر نشد پیش‌فرض را می‌گیرد
try:
    from config.settings import DATABASE_PATH
except ImportError:
    DATABASE_PATH = "khatm.db" # نام پیش‌فرض دیتابیس شما

def update_database_schema():
    print(f"🚀 شروع عملیات آپدیت دیتابیس روی فایل: {DATABASE_PATH}")
    
    if not os.path.exists(DATABASE_PATH):
        print("❌ فایل دیتابیس یافت نشد! لطفاً مطمئن شوید ربات حداقل یکبار اجرا شده باشد.")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # غیرفعال کردن چک کردن کلید خارجی برای جلوگیری از خطا هنگام تغییر نام جداول
        cursor.execute("PRAGMA foreign_keys=OFF;")
        cursor.execute("BEGIN TRANSACTION;")

        # ---------------------------------------------------------
        # گام 1: اصلاح جدول topics برای پشتیبانی از نوع 'doa'
        # ---------------------------------------------------------
        print("1️⃣  بررسی و آپدیت جدول topics...")
        
        # گرفتن لیست ستون‌های جدول فعلی
        cursor.execute("PRAGMA table_info(topics)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        columns_str = ", ".join(column_names)

        # تغییر نام جدول قدیمی
        cursor.execute("ALTER TABLE topics RENAME TO topics_old_temp;")

        # ساخت جدول جدید با CHECK constraint اصلاح شده
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS topics (
                group_id INTEGER,
                name TEXT, 
                topic_id INTEGER,
                khatm_type TEXT NOT NULL CHECK(khatm_type IN ('ghoran', 'salavat', 'zekr', 'doa')),
                current_total INTEGER DEFAULT 0,
                period_number INTEGER DEFAULT 0,
                reset_on_period INTEGER DEFAULT 0,
                max_ayat INTEGER DEFAULT 100,
                min_ayat INTEGER DEFAULT 1,
                stop_number INTEGER DEFAULT 0,
                completion_message TEXT DEFAULT '',
                completion_count INTEGER DEFAULT 0,
                current_verse_id INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                is_completed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, topic_id),
                FOREIGN KEY (group_id) REFERENCES groups(group_id)
            );
        """)

        # بازگردانی اطلاعات از جدول قدیم به جدید
        # ما فقط ستون‌هایی را کپی می‌کنیم که در هر دو وجود دارند تا خطا ندهد
        cursor.execute(f"INSERT INTO topics ({columns_str}) SELECT {columns_str} FROM topics_old_temp;")
        
        # حذف جدول موقت
        cursor.execute("DROP TABLE topics_old_temp;")
        print("   ✅ جدول topics با موفقیت آپدیت شد (نوع 'doa' اضافه شد).")

        # ---------------------------------------------------------
        # گام 2: ساخت جدول topic_doas (نسخه ساده/تکی)
        # ---------------------------------------------------------
        print("2️⃣  ساخت جدول topic_doas...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS topic_doas (
                group_id INTEGER,
                topic_id INTEGER,
                title TEXT NOT NULL,
                link TEXT,
                PRIMARY KEY (group_id, topic_id),
                FOREIGN KEY (group_id, topic_id) REFERENCES topics(group_id, topic_id) ON DELETE CASCADE
            );
        """)
        print("   ✅ جدول topic_doas آماده است.")

        # ---------------------------------------------------------
        # گام 3: ساخت جدول doa_items (نسخه لیست‌دار/جدید)
        # ---------------------------------------------------------
        print("3️⃣  ساخت جدول doa_items (ویژگی جدید)...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS doa_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                topic_id INTEGER,
                title TEXT NOT NULL,
                link TEXT,
                category TEXT,
                current_total INTEGER DEFAULT 0,
                FOREIGN KEY (group_id, topic_id) REFERENCES topics(group_id, topic_id) ON DELETE CASCADE
            );
        """)
        
        # ساخت ایندکس برای سرعت
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doa_items_group_topic ON doa_items(group_id, topic_id);
        """)
        print("   ✅ جدول doa_items آماده است.")

        conn.commit()
        print("\n🎉 تمام تغییرات با موفقیت انجام شد! دیتابیس آماده است.")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ خطا در عملیات آپدیت: {e}")
        print("⚠️ تغییرات برگشت داده شد (Rollback).")
    finally:
        cursor.execute("PRAGMA foreign_keys=ON;")
        conn.close()

if __name__ == "__main__":
    update_database_schema()