import logging

logger = logging.getLogger(__name__)

class Sepas:
    def __init__(self, text, group_id=None, is_default=0):
        try:
            self.text = str(text)
            self.group_id = int(group_id) if group_id else None
            self.is_default = int(is_default)
            logger.debug(f"Sepas created: text={self.text}, group_id={self.group_id}")
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid sepas data: {e}")
            raise ValueError("Invalid sepas data")