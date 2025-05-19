import logging
from bot.database.db import fetch_one, fetch_all
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

quran = QuranManager()

async def get_group_stats(group_id, topic_id):
    try:
        result = await fetch_one(
            """
            SELECT 
                SUM(total_salavat) as total_salavat,
                SUM(total_zekr) as total_zekr,
                SUM(total_ayat) as total_ayat
            FROM users
            WHERE group_id = ? AND topic_id = ?
            """,
            (group_id, topic_id)
        )
        stats = {
            "total_salavat": result["total_salavat"] or 0,
            "total_zekr": result["total_zekr"] or 0,
            "total_ayat": result["total_ayat"] or 0
        }
        return stats
    except Exception as e:
        logger.error(f"Error in get_group_stats: {e}")
        return {}

async def get_ranking(group_id, topic_id):
    try:
        rankings = await fetch_all(
            """
            SELECT user_id, username, first_name, total_salavat, total_zekr, total_ayat
            FROM users
            WHERE group_id = ? AND topic_id = ?
            ORDER BY (total_salavat + total_zekr + total_ayat) DESC
            LIMIT 10
            """,
            (group_id, topic_id)
        )
        result = []
        for user_data in rankings:
            user_data = dict(user_data)
            if user_data["total_ayat"] > 0:
                verses = await fetch_all(
                    """
                    SELECT verse_id
                    FROM contributions
                    WHERE group_id = ? AND topic_id = ? AND user_id = ? AND verse_id IS NOT NULL
                    """,
                    (group_id, topic_id, user_data["user_id"])
                )
                user_data["verses"] = [
                    {
                        "surah_name": quran.get_verse_by_id(row["verse_id"])["surah_name"],
                        "ayah_number": quran.get_verse_by_id(row["verse_id"])["ayah_number"],
                        "text": quran.get_verse_by_id(row["verse_id"])["text"]
                    }
                    for row in verses if quran.get_verse_by_id(row["verse_id"])
                ]
            else:
                user_data["verses"] = []
            result.append(user_data)
        return result
    except Exception as e:
        logger.error(f"Error in get_ranking: {e}")
        return []