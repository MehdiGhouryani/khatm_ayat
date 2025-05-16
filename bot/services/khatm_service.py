import logging
import sqlite3

logger = logging.getLogger(__name__)

def process_khatm_number(group_id, topic_id, number, khatm_type, current_value, db_conn):
    """Process a khatm contribution and update database."""
    try:
        cursor = db_conn.cursor()
        completed = False

        if khatm_type == "ghoran":
            new_total = current_value + number
            cursor.execute(
                """
                SELECT t.stop_number, r.start_verse_id, r.end_verse_id 
                FROM topics t 
                JOIN khatm_ranges r ON t.group_id = r.group_id AND t.topic_id = r.topic_id 
                WHERE t.topic_id = ? AND t.group_id = ?
                """,
                (topic_id, group_id)
            )
            result = cursor.fetchone()
            if result:
                if result["stop_number"] > 0 and new_total >= result["stop_number"]:
                    completed = True
                elif new_total >= (result["end_verse_id"] - result["start_verse_id"] + 1):
                    completed = True
            cursor.execute(
                "UPDATE topics SET current_total = ? WHERE topic_id = ? AND group_id = ?",
                (new_total, topic_id, group_id)
            )
            db_conn.commit()
            logger.info(f"Quran khatm processed: topic_id={topic_id}, new_total={new_total}")
            return current_value, new_total, completed
        else:
            new_total = current_value + number
            cursor.execute(
                "SELECT period_number, reset_on_period FROM topics WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id)
            )
            topic = cursor.fetchone()
            if topic["period_number"] > 0 and new_total >= topic["period_number"]:
                if topic["reset_on_period"]:
                    new_total = new_total % topic["period_number"]
                completed = True
            cursor.execute(
                "UPDATE topics SET current_total = ? WHERE topic_id = ? AND group_id = ?",
                (new_total, topic_id, group_id)
            )
            db_conn.commit()
            logger.info(f"{khatm_type} khatm processed: topic_id={topic_id}, new_total={new_total}")
            return current_value, new_total, completed
    except sqlite3.Error as e:
        logger.error(f"Database error in process_khatm_number: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in process_khatm_number: {e}")
        raise