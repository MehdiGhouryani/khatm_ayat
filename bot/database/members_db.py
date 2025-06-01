import aiosqlite
import logging
from bot.utils.constants import MAIN_GROUP_ID

import logging
from telegram.error import TelegramError
from telegram import User  # Import User class
logger = logging.getLogger(__name__)

DATABASE_PATH = "members.sqlite"  # اطمینان حاصل کنید این مسیر به فایل members.sqlite شما اشاره دارد


async def fetch_all(query: str, params: tuple = ()):
    """
    Fetches all rows from a query on members.sqlite asynchronously.
    Results are returned as a list of dictionaries.
    """
    try:
        async with aiosqlite.connect(DATABASE_PATH) as conn:
            conn.row_factory = aiosqlite.Row  # Access columns by name
            async with conn.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                result = [dict(row) for row in rows]
                logger.debug(f"Fetched {len(result)} rows from members.sqlite for query: {query}")
                return result
    except aiosqlite.Error as e:
        logger.error(f"aiosqlite error fetching all from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching all from members.sqlite (query: {query}, params: {params}): {e}", exc_info=True)
        raise

    
async def _fetch_members(self, chat_id):
    """Fetch active members from members.sqlite for main group, or from Telegram API for other groups."""
    members = []
    try:
        if chat_id == MAIN_GROUP_ID:
            logger.info(f"Fetching active users from members.sqlite for main group_id: {chat_id}")
            # اصلاح کوئری برای فیلتر کردن بر اساس group_id
            db_users = await fetch_all(
                """
                SELECT user_id, username, first_name, last_name
                FROM members
                WHERE group_id = ? AND is_deleted = 0 AND is_bot = 0
                """,
                (chat_id,)
            )
            
            if not db_users:
                logger.warning("No active members found in members.sqlite for group %s", chat_id)
                return []
                
            for user_data in db_users:
                user_id = user_data["user_id"]
                username = user_data["username"]
                first_name = user_data["first_name"] or "User"
                last_name = user_data["last_name"]
                user = User(
                    id=user_id,
                    first_name=first_name,
                    last_name=last_name,
                    is_bot=False,
                    username=username
                )
                members.append(user)
                logger.debug(f"Added user {user_id} ({username or first_name}) from members.sqlite to tag list")
            
            logger.info(f"Total {len(members)} active members found for tagging in main group {chat_id}")
        else:
            logger.info(f"Fetching members from Telegram API for group_id: {chat_id}")
            try:
                async for member in self.context.bot.get_chat_members(chat_id):
                    if not member.user.is_bot:
                        members.append(member.user)
                        logger.debug(f"Added user {member.user.id} ({member.user.username or member.user.first_name}) from API to tag list")
            except TelegramError as e:
                logger.error(f"Telegram API error fetching members for group {chat_id}: {e}", exc_info=True)
                return []
            
            logger.info(f"Total {len(members)} members found for tagging in group {chat_id}")
        
        return members
    except aiosqlite.Error as e:
        logger.error(f"Database error fetching members for group {chat_id}: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching members for group {chat_id}: {e}", exc_info=True)
        return []