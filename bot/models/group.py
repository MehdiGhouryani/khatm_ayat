import logging

logger = logging.getLogger(__name__)

class Group:
    def __init__(self, group_id, is_active=0, **kwargs):
        try:
            self.group_id = int(group_id)
            self.is_active = int(is_active)
            self.is_topic_enabled = int(kwargs.get("is_topic_enabled", 0))
            self.max_number = int(kwargs.get("max_number", 1000000))
            self.min_number = int(kwargs.get("min_number", 0))
            self.max_ayat = int(kwargs.get("max_ayat", 100))
            self.min_ayat = int(kwargs.get("min_ayat", 1))
            self.sepas_enabled = int(kwargs.get("sepas_enabled", 1))
            self.delete_after = int(kwargs.get("delete_after", 0))
            self.lock_enabled = int(kwargs.get("lock_enabled", 0))
            self.reset_daily = int(kwargs.get("reset_daily", 0))
            self.stop_number = int(kwargs.get("stop_number", 0))
            self.time_off_start = str(kwargs.get("time_off_start", ""))
            self.time_off_end = str(kwargs.get("time_off_end", ""))
            logger.debug(f"Group created: group_id={self.group_id}")
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid group data: {e}")
            raise ValueError("Invalid group data")