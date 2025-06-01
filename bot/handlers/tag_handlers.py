import asyncio
import logging
import time
from datetime import datetime, timedelta
from telegram import Update, ChatMember, User
from telegram.ext import ContextTypes, CommandHandler
from telegram.error import TelegramError
from telegram.constants import ParseMode
from bot.database.members_db import fetch_all
from bot.utils.constants import MAIN_GROUP_ID

# تنظیم لاگ‌گذاری
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tag_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# تنظیمات
TAG_COOLDOWN_HOURS = 1  # کول‌داون 1 ساعته
USERS_PER_MESSAGE = 100  # حداکثر 100 کاربر در هر پیام
TAG_MESSAGE_DELAY = 1.5  # تأخیر 1.5 ثانیه بین پیام‌ها
MAX_MESSAGE_LENGTH = 4096  # حداکثر طول پیام تلگرام

class TagManager:
    def __init__(self, context):
        self.context = context
        self.is_cancelled = False

    async def tag_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /tag command to tag group members, using members.sqlite for main group and API for others."""
        start_time = time.time()
        chat = update.effective_chat
        user = update.effective_user
        message_thread_id = update.message.message_thread_id if getattr(chat, 'is_forum', False) else None
        logger.info("Received /tag command in chat %s (thread: %s) from user %s", chat.id, message_thread_id, user.id)

        # بررسی نوع چت
        if chat.type not in ["group", "supergroup"]:
            logger.warning("Non-group chat %s attempted /tag", chat.id)
            await self._safe_send_message(
                update.message, "این دستور فقط در گروه‌ها قابل استفاده است.", message_thread_id
            )
            return

        # بررسی ادمین بودن
        try:
            if not await self._is_admin(chat.id, user.id):
                logger.warning("Non-admin user %s attempted /tag in chat %s", user.id, chat.id)
                await self._safe_send_message(
                    update.message, "فقط ادمین‌ها می‌توانند این دستور را اجرا کنند.", message_thread_id
                )
                return
        except Exception as e:
            logger.error("Error checking admin status: %s", e, exc_info=True)
            await self._safe_send_message(
                update.message, "خطا در بررسی وضعیت ادمین.", message_thread_id
            )
            return

        # بررسی کول‌داون
        try:
            if not await self._check_cooldown(chat.id, context):
                logger.warning("Cooldown active for group %s", chat.id)
                await self._safe_send_message(
                    update.message, f"لطفاً {TAG_COOLDOWN_HOURS} ساعت صبر کنید تا دوباره از این دستور استفاده کنید.", message_thread_id
                )
                return
        except Exception as e:
            logger.error("Error checking cooldown: %s", e, exc_info=True)
            await self._safe_send_message(
                update.message, "خطا در بررسی کول‌داون.", message_thread_id
            )
            return

        context.chat_data["tag_task"] = self
        
        try:
            logger.debug("Starting tag operation for chat %s", chat.id)
            members = await self._fetch_members(chat.id)
            if not members:
                logger.warning("No members to tag in chat %s", chat.id)
                await self._safe_send_message(
                    update.message, "هیچ عضوی برای تگ کردن یافت نشد.", message_thread_id
                )
                return
                
            messages = self._prepare_messages(members)
            sent_messages = 0
            
            for i, message_text in enumerate(messages):
                if self.is_cancelled:
                    logger.info("Tag operation cancelled during message %d in chat %s", i, chat.id)
                    return
                    
                try:
                    # اضافه کردن شماره بخش برای زیبایی
                    header = f"📋 بخش {i+1} از {len(messages)}\n\n"
                    full_message = header + message_text
                    await update.message.reply_text(
                        text=full_message,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        disable_web_page_preview=True,
                        message_thread_id=message_thread_id
                    )
                    sent_messages += 1
                    await asyncio.sleep(TAG_MESSAGE_DELAY)
                except Exception as e:
                    logger.error("Error sending tag message %d: %s", i+1, str(e))
            
            context.chat_data["last_tag_time"] = datetime.utcnow().isoformat()
            logger.info("Tag operation completed for chat %s: sent %d messages in %.2f seconds", 
                        chat.id, sent_messages, time.time() - start_time)
            
        except Exception as e:
            logger.error("Error during tagging in chat %s: %s", chat.id, e, exc_info=True)
            await self._safe_send_message(
                update.message, "خطایی در عملیات تگ رخ داد.", message_thread_id
            )
        finally:
            context.chat_data.pop("tag_task", None)
            logger.debug("Cleaned up tag_task from chat_data for chat %s", chat.id)

    async def cancel_tag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_tag command to stop tagging."""
        chat = update.effective_chat
        message_thread_id = update.message.message_thread_id if getattr(chat, 'is_forum', False) else None
        logger.info("Received /cancel_tag command in chat %s (thread: %s) from user %s", 
                    chat.id, message_thread_id, update.effective_user.id)

        try:
            if not await self._is_admin(chat.id, update.effective_user.id):
                logger.warning("Non-admin user %s attempted /cancel_tag in chat %s", 
                              update.effective_user.id, chat.id)
                await self._safe_send_message(
                    update.message, "فقط ادمین‌ها می‌توانند این دستور را اجرا کنند.", message_thread_id
                )
                return
        except Exception as e:
            logger.error("Error checking admin status for /cancel_tag: %s", e, exc_info=True)
            await self._safe_send_message(
                update.message, "خطا در بررسی وضعیت ادمین.", message_thread_id
            )
            return

        if "tag_task" not in context.chat_data:
            logger.debug("No active tag task to cancel in chat %s", chat.id)
            await self._safe_send_message(
                update.message, "هیچ عملیات تگی در حال اجرا نیست.", message_thread_id
            )
            return

        self.is_cancelled = True
        context.chat_data.pop("tag_task", None)
        logger.info("Tag operation cancelled for chat %s", chat.id)
        await self._safe_send_message(
            update.message, "عملیات تگ متوقف شد.", message_thread_id
        )

    async def _is_admin(self, chat_id, user_id):
        """Check if user is an admin in the chat using Telegram API."""
        try:
            chat_member = await self.context.bot.get_chat_member(chat_id, user_id)
            is_admin = chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
            logger.debug("User %s admin status in chat %s: %s", user_id, chat_id, is_admin)
            return is_admin
        except TelegramError as e:
            logger.error("Telegram API error checking admin status in chat %s for user %s: %s", 
                         chat_id, user_id, e, exc_info=True)
            raise
        except Exception as e:
            logger.error("Unexpected error checking admin status in chat %s for user %s: %s", 
                         chat_id, user_id, e, exc_info=True)
            raise

    async def _check_cooldown(self, chat_id, context):
        """Check if tagging cooldown has expired."""
        try:
            last_tag_time = context.chat_data.get("last_tag_time")
            if last_tag_time:
                last_tag_time = datetime.fromisoformat(last_tag_time)
                cooldown_end = last_tag_time + timedelta(hours=TAG_COOLDOWN_HOURS)
                is_cooldown_expired = datetime.utcnow() >= cooldown_end
                logger.debug("Cooldown check for chat %s: last_tag=%s, cooldown_end=%s, expired=%s", 
                             chat_id, last_tag_time, cooldown_end, is_cooldown_expired)
                return is_cooldown_expired
            logger.debug("No previous tag time for chat %s, allowing tag", chat_id)
            return True
        except Exception as e:
            logger.error("Error checking cooldown for chat %s: %s", chat_id, e, exc_info=True)
            raise

    async def _fetch_members(self, chat_id):
        """Fetch active members from members.sqlite for main group, or from Telegram API for other groups."""
        members = []
        try:
            if chat_id == MAIN_GROUP_ID:
                # برای گروه اصلی از members.sqlite استفاده می‌کنیم
                logger.info(f"Fetching active users from members.sqlite for main group_id: {chat_id}")
                db_users = await fetch_all(
                    """
                    SELECT user_id, username, first_name, last_name 
                    FROM members 
                    WHERE group_id = ? AND is_deleted = 0 AND is_bot = 0
                    """,
                    (chat_id,)
                )
                
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
                # برای گروه‌های دیگر از API تلگرام استفاده می‌کنیم
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
            
        except Exception as e:
            logger.error(f"Failed to fetch members for group {chat_id}: {e}", exc_info=True)
            return []

    def _prepare_messages(self, members):
        """Prepare messages with user tags, up to 100 users per message, with optimized formatting."""
        messages = []
        current_message = ""
        separator = " • "

        for i, user in enumerate(members):  # بدون محدودیت، برای پشتیبانی از 1300 نفر
            tag = self._format_tag(user)
            if len(current_message) + len(tag) + len(separator) > MAX_MESSAGE_LENGTH - 50:  # 50 کاراکتر برای هدر
                messages.append(current_message.rstrip(separator))
                current_message = ""
                logger.debug("Split message at index %d due to length limit", i)
            if i % USERS_PER_MESSAGE == 0 and i != 0:
                messages.append(current_message.rstrip(separator))
                current_message = ""
                logger.debug("Split message at index %d due to user limit (%d)", i, USERS_PER_MESSAGE)
            current_message += tag + separator

        if current_message:
            messages.append(current_message.rstrip(separator))

        logger.debug("Prepared %d messages with %d total tags", len(messages), len(members))
        return messages

    def _format_tag(self, user):
        """Format user tag for MarkdownV2, escaping special characters."""
        try:
            name = user.first_name or str(user.id)
            if user.username:
                name = user.username
            special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
            for char in special_chars:
                name = name.replace(char, f"\\{char}")
            tag = f"[{name}](tg://user?id={user.id})"
            logger.debug("Formatted tag for user %s: %s", user.id, tag)
            return tag
        except Exception as e:
            logger.error("Error formatting tag for user %s: %s", user.id, e, exc_info=True)
            return f"[User\\_{user.id}](tg://user?id={user.id})"

    async def _safe_send_message(self, message, text, message_thread_id=None, parse_mode=None):
        """Safely send a message with error handling."""
        try:
            if len(text) > MAX_MESSAGE_LENGTH:
                logger.warning("Message too long (%d characters), truncating to %d", 
                              len(text), MAX_MESSAGE_LENGTH)
                text = text[:MAX_MESSAGE_LENGTH - 3] + "..."
            
            sent_message = await message.reply_text(
                text=text,
                parse_mode=parse_mode,
                message_thread_id=message_thread_id,
                disable_web_page_preview=True
            )
            logger.debug("Successfully sent message to chat %s (thread: %s): %s", 
                         message.chat_id, message_thread_id, text[:50] + "..." if len(text) > 50 else text)
            return sent_message
        except Exception as e:
            logger.error("Failed to send message: %s", e)
            if parse_mode:
                try:
                    return await message.reply_text(
                        text=f"خطا در ارسال پیام تگ: {str(e)}",
                        message_thread_id=message_thread_id,
                        disable_web_page_preview=True
                    )
                except Exception as e2:
                    logger.error("Failed to send error message: %s", e2)

def setup_handlers():
    """Set up tag command handlers."""
    logger.info("Setting up command handlers")
    return [
        CommandHandler("tag", lambda update, context: TagManager(context).tag_command(update, context)),
        CommandHandler("cancel_tag", lambda update, context: TagManager(context).cancel_tag(update, context))
    ]