import logging
from bot.database.db import write_queue

logger = logging.getLogger(__name__)

async def process_khatm_number(group_id, topic_id, number, khatm_type, current_value):
    """Queue a khatm contribution for processing."""
    try:
        request = {
            "type": "khatm_number",
            "group_id": group_id,
            "topic_id": topic_id,
            "number": number,
            "khatm_type": khatm_type,
            "current_value": current_value
        }
        await write_queue.put(request)
        logger.info("Queued khatm_number: group_id=%s, topic_id=%s, khatm_type=%s, number=%d, current_value=%d", 
                    group_id, topic_id, khatm_type, number, current_value)
        return current_value, current_value + number, False  # Placeholder return, actual processing is queued
    except Exception as e:
        logger.error("Error queuing khatm_number: group_id=%s, topic_id=%s, khatm_type=%s, error=%s", 
                     group_id, topic_id, khatm_type, e, exc_info=True)
        raise