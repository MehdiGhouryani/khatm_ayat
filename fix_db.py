import asyncio
import sqlite3
from bot.database.db import init_db, execute, fetch_one

async def check_and_fix_db():
    # Initialize database connection
    await init_db()
    
    group_id = -1002606818765
    topic_id = 697
    user_id = 2088114041
    username = "Transitoory"  # Username from logs
    
    # Check group
    group = await fetch_one("SELECT * FROM groups WHERE group_id = ?", (group_id,))
    if not group:
        print("Group not found, creating...")
        await execute(
            "INSERT INTO groups (group_id, is_active, max_display_verses) VALUES (?, 1, 10)",
            (group_id,)
        )
        print("Group created successfully")
    else:
        print(f"Group exists: {group}")
    
    # Check topic
    topic = await fetch_one(
        "SELECT * FROM topics WHERE group_id = ? AND topic_id = ?",
        (group_id, topic_id)
    )
    if not topic:
        print("Topic not found, creating...")
        await execute(
            """
            INSERT INTO topics 
            (topic_id, group_id, name, khatm_type, is_active, current_total) 
            VALUES (?, ?, 'اصلی', 'salavat', 1, 0)
            """,
            (topic_id, group_id)
        )
        print("Topic created successfully")
    else:
        print(f"Topic exists: {topic}")
    
    # Check if user exists
    user = await fetch_one(
        "SELECT * FROM users WHERE user_id = ? AND group_id = ? AND topic_id = ?",
        (user_id, group_id, topic_id)
    )
    if not user:
        print("User not found, creating...")
        await execute(
            """
            INSERT INTO users 
            (user_id, group_id, topic_id, username, total_salavat, total_zekr, total_ayat) 
            VALUES (?, ?, ?, ?, 0, 0, 0)
            """,
            (user_id, group_id, topic_id, username)
        )
        print("User created successfully")
    else:
        print(f"User exists: {user}")

if __name__ == "__main__":
    asyncio.run(check_and_fix_db()) 