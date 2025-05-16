import logging
import sqlite3
from bot.database.db import get_db_connection
from bot.utils.quran import QuranManager

logger = logging.getLogger(__name__)

quran = QuranManager()

def get_group_stats(group_id, topic_id):
    """Get total stats for a group or topic."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
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
            result = cursor.fetchone()
            stats = {
                "total_salavat": result["total_salavat"] or 0,
                "total_zekr": result["total_zekr"] or 0,
                "total_ayat": result["total_ayat"] or 0
            }
            logger.debug(f"Stats retrieved: group_id={group_id}, topic_id={topic_id}, stats={stats}")
            return stats
    except sqlite3.Error as e:
        logger.error(f"Database error in get_group_stats: {e}")
        return None

def get_ranking(group_id, topic_id):
    """Get user rankings sorted by total contributions."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT user_id, username, first_name, total_salavat, total_zekr, total_ayat
                FROM users
                WHERE group_id = ? AND topic_id = ?
                ORDER BY (total_salavat + total_zekr + total_ayat) DESC
                LIMIT 10
                """,
                (group_id, topic_id)
            )
            rankings = []
            for row in cursor.fetchall():
                user_data = dict(row)
                # Optionally fetch verse details for Quran contributions
                if user_data["total_ayat"] > 0:
                    cursor.execute(
                        """
                        SELECT verse_id
                        FROM contributions
                        WHERE group_id = ? AND topic_id = ? AND user_id = ? AND verse_id IS NOT NULL
                        """,
                        (group_id, topic_id, user_data["user_id"])
                    )
                    verses = [row["verse_id"] for row in cursor.fetchall()]
                    user_data["verses"] = [
                        {
                            "surah_name": quran.get_verse_by_id(vid)["surah_name"],
                            "ayah_number": quran.get_verse_by_id(vid)["ayah_number"],
                            "text": quran.get_verse_by_id(vid)["text"]
                        }
                        for vid in verses if quran.get_verse_by_id(vid)
                    ] if verses else []
                else:
                    user_data["verses"] = []
                rankings.append(user_data)
            logger.debug(f"Rankings retrieved: group_id={group_id}, topic_id={topic_id}, count={len(rankings)}")
            return rankings
    except sqlite3.Error as e:
        logger.error(f"Database error in get_ranking: {e}")
        return None