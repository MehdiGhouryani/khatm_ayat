import sqlite3

DB_PATH = "bot.db"

# لیست گروه‌هایی که در لاگ ارور داده‌اند
BAD_GROUPS = [
    -1003165641310, # Chat not found
    -1003086499196, # Chat not found
    -1002945552819, # Chat not found
    -1002687739294, # Not enough rights
    -1002655364407, # Not enough rights
    -1002646881131, # Not enough rights
    -1002527451082, # Not enough rights
    -1002418192967, # Chat not found
    -1002105708239, # Not enough rights
    -5075384381,    # Not enough rights
    -4993388081,    # Not enough rights
    -4955743823,    # Forbidden
    -4931062746,    # Not enough rights
    -4907173889,    # Not enough rights
    -4807269622,    # Forbidden
    -4607665006,    # Not enough rights
]

# گروه‌هایی که تغییر ID داده‌اند (Migrated)
MIGRATED_GROUPS = {
    -4964230569: -1003165641310,
    -4902839150: -1002935045396,
    -4886411990: -1002960690770,
    -4812687122: -1003328262510
}

def clean_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🧹 شروع پاکسازی گروه‌ها...")

    try:
        # 1. حذف/غیرفعال کردن گروه‌های خراب
        for gid in BAD_GROUPS:
            cursor.execute("UPDATE groups SET is_active = 0 WHERE group_id = ?", (gid,))
            print(f"🚫 گروه {gid} غیرفعال شد.")

        # 2. آپدیت گروه‌های منتقل شده
        for old_id, new_id in MIGRATED_GROUPS.items():
            # چک کنیم اگر گروه جدید وجود ندارد، آیدی قدیم را آپدیت کنیم
            cursor.execute("SELECT 1 FROM groups WHERE group_id = ?", (new_id,))
            if not cursor.fetchone():
                cursor.execute("UPDATE groups SET group_id = ? WHERE group_id = ?", (new_id, old_id))
                cursor.execute("UPDATE topics SET group_id = ? WHERE group_id = ?", (new_id, old_id))
                # سایر جداول وابسته هم باید آپدیت شوند (users, contributions, ...)
                print(f"🔄 گروه {old_id} به {new_id} منتقل شد.")
            else:
                # اگر گروه جدید قبلاً هست، قدیمی را حذف می‌کنیم
                cursor.execute("DELETE FROM groups WHERE group_id = ?", (old_id,))
                print(f"🗑 گروه قدیمی {old_id} حذف شد (نسخه جدید موجود است).")

        conn.commit()
        print("✅ پاکسازی تمام شد.")

    except Exception as e:
        print(f"❌ خطا: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clean_database()