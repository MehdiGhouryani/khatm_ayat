import logging

logger = logging.getLogger(__name__)

class User:
    def __init__(self, user_id, group_id, topic_id, **kwargs):
        try:
            self.user_id = int(user_id)
            self.group_id = int(group_id)
            self.topic_id = int(topic_id)
            self.username = str(kwargs.get("username", ""))
            self.first_name = str(kwargs.get("first_name", ""))
            self.total_ayat = int(kwargs.get("total_ayat", 0))
            self.total_zekr = int(kwargs.get("total_zekr", 0))
            self.total_salavat = int(kwargs.get("total_salavat", 0))
            logger.debug(f"User created: user_id={self.user_id}, group_id={self.group_id}, topic_id={self.topic_id}")
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid user data: {e}")
            raise ValueError("Invalid user data")