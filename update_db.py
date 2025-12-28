import sqlite3
import os

# تلاش برای پیدا کردن مسیر دیتابیس از تنظیمات
try:
    from config.settings import DATABASE_PATH
except ImportError:
    DATABASE_PATH = "khatm.db"

def update_database_schema():
    print(f"🚀 شروع عملیات آپدیت دیتابیس روی فایل: {DATABASE_PATH}")
    
    if not os.path.exists(DATABASE_PATH):
        print("❌ فایل دیتابیس یافت نشد! لطفاً مطمئن شوید ربات حداقل یکبار اجرا شده باشد.")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # غیرفعال کردن چک کردن کلید خارجی
        cursor.execute("PRAGMA foreign_keys=OFF;")
        cursor.execute("BEGIN TRANSACTION;")

        # ---------------------------------------------------------
        # گام 1: آپدیت جدول topics (با اضافه کردن zekr_text)
        # ---------------------------------------------------------
        print("1️⃣  بررسی و آپدیت جدول topics...")
        
        # 1. گرفتن لیست ستون‌های جدول فعلی
        cursor.execute("PRAGMA table_info(topics)")
        columns_info = cursor.fetchall()
        # نام ستون‌های جدول قدیمی را نگه می‌داریم
        old_columns = [col[1] for col in columns_info]

        # 2. تغییر نام جدول قدیمی
        cursor.execute("ALTER TABLE topics RENAME TO topics_old_temp;")

        # 3. ساخت جدول جدید (ستون zekr_text اضافه شد)
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
                zekr_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, topic_id),
                FOREIGN KEY (group_id) REFERENCES groups(group_id)
            );
        """)

        # 4. فقط ستون‌هایی که در هر دو جدول (قدیم و جدید) مشترک هستند را کپی می‌کنیم
        # این کار از خطای "no column named X" جلوگیری می‌کند
        cursor.execute("PRAGMA table_info(topics)")
        new_columns_info = cursor.fetchall()
        new_columns = [col[1] for col in new_columns_info]
        
        # پیدا کردن ستون‌های مشترک
        common_columns = [col for col in old_columns if col in new_columns]
        columns_str = ", ".join(common_columns)

        print(f"   🔄 در حال کپی اطلاعات ستون‌های: {columns_str}")
        
        if columns_str:
            cursor.execute(f"INSERT INTO topics ({columns_str}) SELECT {columns_str} FROM topics_old_temp;")
        
        # 5. حذف جدول موقت
        cursor.execute("DROP TABLE topics_old_temp;")
        print("   ✅ جدول topics با موفقیت بازسازی شد.")

        # ---------------------------------------------------------
        # گام 2: ساخت جدول topic_doas
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

        # ---------------------------------------------------------
        # گام 3: ساخت جدول doa_items
        # ---------------------------------------------------------
        print("3️⃣  ساخت جدول doa_items...")
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
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doa_items_group_topic ON doa_items(group_id, topic_id);
        """)

        conn.commit()
        print("\n🎉 تمام تغییرات با موفقیت انجام شد!")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ خطا در عملیات آپدیت: {e}")
        print("⚠️ تغییرات برگشت داده شد (Rollback).")
    finally:
        cursor.execute("PRAGMA foreign_keys=ON;")
        conn.close()

if __name__ == "__main__":
    update_database_schema()