import sqlite3
import os

DB_PATH = "bot.db"

def add_limit_columns():
    if not os.path.exists(DB_PATH):
        print("❌ دیتابیس پیدا نشد.")
        return

    print(f"🔧 در حال آپدیت دیتابیس {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # اضافه کردن ستون max_number به جدول topics
        try:
            cursor.execute("ALTER TABLE topics ADD COLUMN max_number INTEGER DEFAULT NULL")
            print("✅ ستون max_number اضافه شد.")
        except Exception as e:
            print(f"ℹ️ ستون max_number احتمالاً وجود دارد: {e}")

        # اضافه کردن ستون min_number به جدول topics
        try:
            cursor.execute("ALTER TABLE topics ADD COLUMN min_number INTEGER DEFAULT NULL")
            print("✅ ستون min_number اضافه شد.")
        except Exception as e:
            print(f"ℹ️ ستون min_number احتمالاً وجود دارد: {e}")

        conn.commit()
        print("🎉 دیتابیس با موفقیت آپدیت شد.")

    except Exception as e:
        print(f"❌ خطا: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_limit_columns()