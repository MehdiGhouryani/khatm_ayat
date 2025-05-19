import logging
import asyncio
from datetime import datetime, timedelta
from telegram import Update, ChatMember
from telegram.ext import ContextTypes, CommandHandler
from telegram.error import TelegramError
from bot.utils.constants import MAIN_GROUP_ID, MAX_MESSAGE_LENGTH, TAG_COOLDOWN_HOURS, TAG_MESSAGE_DELAY
from bot.database.db import fetch_one, fetch_all, write_queue

logger = logging.getLogger(__name__)

class TagManager:
    def __init__(self, context):
        self.context = context
        self.is_cancelled = False

    async def tag_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /tag command to tag active group members."""
        chat = update.effective_chat
        user = update.effective_user

        if chat.type not in ["group", "supergroup"]:
            logger.debug("Non-group chat %s attempted /tag", chat.id)
            await update.message.reply_text("این دستور فقط در گروه‌ها قابل استفاده است.")
            return

        if not await self._is_admin(chat.id, user.id):
            logger.debug("Non-admin user %s attempted /tag in chat %s", user.id, chat.id)
            await update.message.reply_text("فقط ادمین‌ها می‌توانند از این دستور استفاده کنند.")
            return

        if chat.id != MAIN_GROUP_ID and not await self._check_cooldown(chat.id):
            logger.debug("Cooldown active for group %s", chat.id)
            await update.message.reply_text(f"لطفاً {TAG_COOLDOWN_HOURS} ساعت صبر کنید.")
            return

        context.chat_data["tag_task"] = self
        try:
            await self._tag_all_members(chat)
            await write_queue.put({
                "type": "update_tag_timestamp",
                "group_id": chat.id
            })
            logger.info("Tag command completed and timestamp update queued for group_id=%s", chat.id)
        except Exception as e:
            logger.error("Error during tagging in chat %s: %s", chat.id, e)
            await update.message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        finally:
            context.chat_data.pop("tag_task", None)

    async def cancel_tag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_tag command to stop tagging."""
        chat = update.effective_chat
        if not await self._is_admin(chat.id, update.effective_user.id):
            logger.debug("Non-admin user %s attempted /cancel_tag in chat %s", update.effective_user.id, chat.id)
            await update.message.reply_text("فقط ادمین‌ها می‌توانند این دستور را اجرا کنند.")
            return

        if "tag_task" not in context.chat_data:
            logger.debug("No active tag task to cancel in chat %s", chat.id)
            await update.message.reply_text("هیچ عملیات تگی در حال اجرا نیست.")
            return

        self.is_cancelled = True
        context.chat_data.pop("tag_task", None)
        await update.message.reply_text("عملیات تگ متوقف شد.")

    async def _is_admin(self, chat_id, user_id):
        """Check if user is an admin in the chat."""
        try:
            chat_member = await self.context.bot.get_chat_member(chat_id, user_id)
            return chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
        except TelegramError as e:
            logger.error("Error checking admin status in chat %s for user %s: %s", chat_id, user_id, e)
            return False

    async def _check_cooldown(self, group_id):
        """Check if tagging cooldown has expired."""
        try:
            result = await fetch_one("SELECT last_tag_time FROM tag_timestamps WHERE group_id = ?", (group_id,))
            if result:
                last_tag_time = datetime.fromisoformat(result["last_tag_time"])
                cooldown_end = last_tag_time + timedelta(hours=TAG_COOLDOWN_HOURS)
                return datetime.utcnow() >= cooldown_end
            return True
        except Exception as e:
            logger.error("Error checking cooldown for group %s: %s", group_id, e)
            return False

    async def _tag_all_members(self, chat):
        """Tag all active members in the chat."""
        members = await self._fetch_members(chat.id)
        if not members:
            logger.debug("No members to tag in chat %s", chat.id)
            await chat.send_message("هیچ عضو فعالی برای تگ کردن یافت نشد.")
            return

        messages = self._prepare_messages(members)
        for i, message in enumerate(messages):
            if self.is_cancelled:
                logger.debug("Tag operation cancelled in chat %s", chat.id)
                await chat.send_message("عملیات تگ توسط ادمین لغو شد.")
                break
            try:
                await chat.send_message(message, parse_mode="Markdown")
                await asyncio.sleep(TAG_MESSAGE_DELAY + 0.1 * i)  # Stagger to avoid rate limits
            except TelegramError as e:
                logger.error("Error sending message in chat %s: %s", chat.id, e)

    async def _fetch_members(self, chat_id):
        """Fetch active members from the users table."""
        members = []
        try:
            rows = await fetch_all(
                """
                SELECT user_id, username, first_name
                FROM users
                WHERE group_id = ? AND (total_zekr > 0 OR total_salavat > 0 OR total_ayat > 0)
                """,
                (chat_id,)
            )
            for row in rows:
                user = type('User', (), {
                    'id': row["user_id"],
                    'username': row["username"],
                    'first_name': row["first_name"],
                    'is_bot': False
                })()
                if await self._is_active_member(user, chat_id):
                    members.append(user)
        except Exception as e:
            logger.error("Error fetching members for chat %s: %s", chat_id, e)
        return members

    async def _is_active_member(self, user, chat_id):
        """Check if user is an active member of the chat."""
        try:
            chat_member = await self.context.bot.get_chat_member(chat_id, user.id)
            return chat_member.status in ["member", "administrator", "creator"]
        except TelegramError as e:
            logger.error("Error checking member status for user %s in chat %s: %s", user.id, chat_id, e)
            return False

    def _prepare_messages(self, members):
        """Prepare messages with user tags."""
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
        """Format user tag for Markdown."""
        if user.username:
            return f"[{user.username}](tg://user?id={user.id})"
        name = user.first_name or str(user.id)
        return f"[{name}](tg://user?id={user.id})"

def setup_handlers():
    """Set up tag command handlers."""
    return [
        CommandHandler("tag", lambda update, context: TagManager(context).tag_command(update, context)),
        CommandHandler("cancel_tag", lambda update, context: TagManager(context).cancel_tag(update, context))
    ]