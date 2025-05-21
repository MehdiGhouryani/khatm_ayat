import asyncio
import logging
import time
from datetime import datetime, timedelta
from telegram import Update, ChatMember, User
from telegram.ext import Updater, CommandHandler, ContextTypes
from telegram.error import TelegramError, NetworkError, BadRequest, Forbidden, RetryAfter
from telegram.constants import ParseMode
from bot.utils.user_store import UserStore
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
        """Handle /tag command to tag group members, supporting topics."""
        start_time = time.time()
        chat = update.effective_chat
        user = update.effective_user
        message_thread_id = update.message.message_thread_id if getattr(chat, 'is_forum', False) else None
        logger.info("Received /tag command in chat %s (thread: %s) from user %s", chat.id, message_thread_id, user.id)

        # بررسی نوع چت
        if chat.type not in ["group", "supergroup"]:
            logger.warning("Non-group chat %s attempted /tag", chat.id)
            return

        # بررسی ادمین بودن
        try:
            if not await self._is_admin(chat.id, user.id):
                logger.warning("Non-admin user %s attempted /tag in chat %s", user.id, chat.id)
                return
        except Exception as e:
            logger.error("Error checking admin status: %s", e, exc_info=True)
            return

        # بررسی کول‌داون
        try:
            if not await self._check_cooldown(chat.id, context):
                logger.warning("Cooldown active for group %s", chat.id)
                return
        except Exception as e:
            logger.error("Error checking cooldown: %s", e, exc_info=True)
            return

        context.chat_data["tag_task"] = self
        
        try:
            logger.debug("Starting tag operation for chat %s", chat.id)
            members = await self._fetch_members(chat.id)
            if not members:
                logger.warning("No members to tag in chat %s", chat.id)
                return
                
            messages = self._prepare_messages(members)
            sent_messages = 0
            
            for i, message_text in enumerate(messages):
                if self.is_cancelled:
                    logger.info("Tag operation cancelled during message %d in chat %s", i, chat.id)
                    return
                    
                try:
                    await update.message.reply_text(
                        text=message_text,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        disable_web_page_preview=True
                    )
                    sent_messages += 1
                    await asyncio.sleep(TAG_MESSAGE_DELAY)
                except Exception as e:
                    logger.error("Error sending tag message: %s", str(e))
            
            context.chat_data["last_tag_time"] = datetime.utcnow().isoformat()
            
        except Exception as e:
            logger.error("Error during tagging in chat %s: %s", chat.id, e, exc_info=True)
        finally:
            context.chat_data.pop("tag_task", None)
            logger.debug("Cleaned up tag_task from chat_data for chat %s", chat.id)

    async def cancel_tag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_tag command to stop tagging, supporting topics."""
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
        """Check if user is an admin in the chat."""
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
        """Fetch accessible members from the chat using Telegram API and database."""
        members = []
        user_store = UserStore()
        
        try:
            # دریافت ادمین‌ها از API تلگرام
            logger.info(f"Fetching administrators for chat_id: {chat_id}")
            administrators = await self.context.bot.get_chat_administrators(chat_id)
            admin_ids = set()
            
            for admin in administrators:
                if not admin.user.is_bot:
                    members.append(admin.user)
                    admin_ids.add(admin.user.id)
                    logger.debug(f"Added admin {admin.user.id} ({admin.user.username}) to tag list")
            
            # دریافت سایر کاربران از دیتابیس
            logger.info(f"Fetching regular users from database for chat_id: {chat_id}")
            db_users = user_store.get_chat_users(chat_id)
            
            for user_data in db_users:
                user_id, username, first_name, last_name = user_data
                # اگر کاربر قبلاً به عنوان ادمین اضافه نشده باشد
                if user_id not in admin_ids:
                    user = User(
                        id=user_id,
                        first_name=first_name or "User",
                        is_bot=False,
                        username=username
                    )
                    members.append(user)
                    logger.debug(f"Added user {user_id} from database to tag list")
            
            logger.info(f"Total {len(members)} members found for tagging in chat {chat_id}")
            return members
            
        except Exception as e:
            logger.error(f"Failed to fetch members: {e}", exc_info=True)
            return []

    def _prepare_messages(self, members):
        """Prepare messages with user tags, up to 100 users per message."""
        messages = []
        current_message = ""
        separator = " • "  # تغییر جداکننده از خط تیره به نقطه

        for i, user in enumerate(members[:200]):  # محدود به 200 عضو
            tag = self._format_tag(user)
            if len(current_message) + len(tag) + len(separator) > MAX_MESSAGE_LENGTH:
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

        logger.debug("Prepared %d messages with %d total tags", len(messages), len(members[:200]))
        return messages

    def _format_tag(self, user):
        """Format user tag for MarkdownV2, escaping special characters."""
        try:
            name = user.first_name or str(user.id)
            if user.username:
                name = user.username
            # فرمت کردن کاراکترهای خاص برای MarkdownV2
            # کاراکترهای خاص در MarkdownV2 که باید escape شوند:
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
            # اگر پیام ارسال نشد، سعی می‌کنیم بدون parse_mode دوباره ارسال کنیم
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