import logging
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ChatMember
from telegram.ext import ContextTypes, CommandHandler
from telegram.error import TelegramError
from bot.utils.constants import MAIN_GROUP_ID, MAX_MESSAGE_LENGTH, TAG_COOLDOWN_HOURS, TAG_MESSAGE_DELAY
from bot.database.db import get_db_connection

logger = logging.getLogger(__name__)

class TagManager:
    def __init__(self, context):
        self.context = context
        self.is_cancelled = False

    async def tag_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        user = update.effective_user

        if chat.type not in ["group", "supergroup"]:
            logger.debug(f"Non-group chat {chat.id} attempted /tag")
            return

        if not await self._is_admin(chat.id, user.id):
            logger.debug(f"Non-admin user {user.id} attempted /tag in chat {chat.id}")
            return

        if chat.id != MAIN_GROUP_ID and not await self._check_cooldown(chat.id):
            logger.debug(f"Cooldown active for group {chat.id}")
            return

        context.chat_data["tag_task"] = self
        try:
            await self._tag_all_members(chat)
            await self._update_timestamp(chat.id)
        except Exception as e:
            logger.error(f"Error during tagging in chat {chat.id}: {e}")
        finally:
            context.chat_data.pop("tag_task", None)

    async def cancel_tag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        if not await self._is_admin(chat.id, update.effective_user.id):
            logger.debug(f"Non-admin user {update.effective_user.id} attempted /cancel_tag in chat {chat.id}")
            return

        if "tag_task" not in context.chat_data:
            logger.debug(f"No active tag task to cancel in chat {chat.id}")
            return

        self.is_cancelled = True
        context.chat_data.pop("tag_task", None)

    async def _is_admin(self, chat_id, user_id):
        try:
            chat_member = await self.context.bot.get_chat_member(chat_id, user_id)
            return chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
        except TelegramError as e:
            logger.error(f"Error checking admin status in chat {chat_id}: {e}")
            return False

    async def _check_cooldown(self, group_id):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT last_tag_time FROM tag_timestamps WHERE group_id = ?", (group_id,))
                result = cursor.fetchone()

                if result:
                    last_tag_time = datetime.fromisoformat(result["last_tag_time"])
                    cooldown_end = last_tag_time + timedelta(hours=TAG_COOLDOWN_HOURS)
                    return datetime.now() >= cooldown_end
                return True
        except Exception as e:
            logger.error(f"Error checking cooldown for group {group_id}: {e}")
            return False

    async def _update_timestamp(self, group_id):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO tag_timestamps (group_id, last_tag_time) VALUES (?, ?)",
                    (group_id, datetime.now().isoformat())
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating timestamp for group {group_id}: {e}")

    async def _tag_all_members(self, chat):
        members = await self._fetch_members(chat.id)
        if not members:
            logger.debug(f"No members to tag in chat {chat.id}")
            return

        messages = self._prepare_messages(members)
        for message in messages:
            if self.is_cancelled:
                logger.debug(f"Tag operation cancelled in chat {chat.id}")
                break
            try:
                await chat.send_message(message, parse_mode="Markdown")
                await asyncio.sleep(TAG_MESSAGE_DELAY)
            except TelegramError as e:
                logger.error(f"Error sending message in chat {chat.id}: {e}")

    async def _fetch_members(self, chat_id):
        members = []
        try:
            async for member in self.context.bot.get_chat_members(chat_id):
                if not member.user.is_bot and await self._is_active_member(member.user, chat_id):
                    members.append(member.user)
        except TelegramError as e:
            logger.error(f"Error fetching members for chat {chat_id}: {e}")
        return members

    async def _is_active_member(self, user, chat_id):
        try:
            chat_member = await self.context.bot.get_chat_member(chat_id, user.id)
            return chat_member.status in ["member", "administrator", "creator"]
        except TelegramError as e:
            logger.error(f"Error checking member status for user {user.id} in chat {chat_id}: {e}")
            return True

    def _prepare_messages(self, members):
        messages = []
        current_message = ""
        separator = " - "

        for user in members:
            tag = self._format_tag(user)
            if len(current_message) + len(tag) + len(separator) > MAX_MESSAGE_LENGTH:
                messages.append(current_message.rstrip(separator))
                current_message = ""
            current_message += tag + separator

        if current_message:
            messages.append(current_message.rstrip(separator))

        return messages

    def _format_tag(self, user):
        if user.username:
            return f"[{user.username}](tg://user?id={user.id})"
        name = user.first_name or str(user.id)
        return f"[{name}](tg://user?id={user.id})"

def setup_handlers():
    manager = TagManager(None)
    return [
         CommandHandler("tag", lambda update, context: TagManager(context).tag_command(update, context)),
         CommandHandler("cancel_tag", lambda update, context: TagManager(context).cancel_tag(update, context))
    ]