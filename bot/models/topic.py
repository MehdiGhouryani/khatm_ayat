import logging
from bot.utils.constants import KHATM_TYPES

logger = logging.getLogger(__name__)

class Topic:
    def __init__(self, topic_id, group_id, name, khatm_type="salavat", **kwargs):
        try:
            self.topic_id = int(topic_id)
            self.group_id = int(group_id)
            self.name = str(name)
            if khatm_type not in KHATM_TYPES:
                logger.error(f"Invalid khatm_type: {khatm_type}")
                raise ValueError("Invalid khatm_type")
            self.khatm_type = khatm_type
            self.zekr_text = str(kwargs.get("zekr_text", ""))
            self.current_total = int(kwargs.get("current_total", 0))
            self.period_number = int(kwargs.get("period_number", 0))
            self.reset_on_period = int(kwargs.get("reset_on_period", 0))
            self.min_ayat = int(kwargs.get("min_ayat", 1))
            self.max_ayat = int(kwargs.get("max_ayat", 100))
            self.stop_number = int(kwargs.get("stop_number", 0))
            logger.debug(f"Topic created: topic_id={self.topic_id}, group_id={self.group_id}")
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid topic data: {e}")
            raise ValueError("Invalid topic data")