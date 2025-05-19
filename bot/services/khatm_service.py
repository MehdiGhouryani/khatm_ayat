import logging
from bot.database.db import write_queue

logger = logging.getLogger(__name__)

async def process_khatm_number(group_id, topic_id, number, khatm_type, current_value):
    try:
        # Validate inputs
        if not isinstance(number, (int, float)) or number < 0:
            raise ValueError("Number must be a non-negative number")
        if not isinstance(current_value, (int, float)) or current_value < 0:
            raise ValueError("Current value must be a non-negative number")
        if khatm_type not in ("ghoran", "salavat", "zekr"):
            raise ValueError("Invalid khatm type")

        request = {
            "type": "khatm_number",
            "group_id": group_id,
            "topic_id": topic_id,
            "number": number,
            "khatm_type": khatm_type,
            "current_value": current_value
        }
        await write_queue.put(request)
        return current_value, current_value + number, False
    except Exception as e:
        logger.error("Error queuing khatm_number: group_id=%s, topic_id=%s, khatm_type=%s, error=%s", 
                     group_id, topic_id, khatm_type, e)
        raise