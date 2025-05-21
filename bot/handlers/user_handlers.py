import logging
from telegram import Update, ChatMemberUpdated
from telegram.ext import ContextTypes
from telegram.constants import ChatMemberStatus
from bot.utils.user_store import UserStore

logger = logging.getLogger(__name__)

async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """مدیریت تغییرات وضعیت کاربران (عضویت، خروج، و...)."""
    if not update.chat_member:
        return
    
    chat_member: ChatMemberUpdated = update.chat_member
    
    # اگر کاربر جدید عضو شده است
    if chat_member.new_chat_member.status == ChatMemberStatus.MEMBER:
        try:
            user_store = UserStore()
            user = chat_member.new_chat_member.user
            chat_id = update.effective_chat.id
            
            # ذخیره اطلاعات کاربر جدید
            result = user_store.add_user(
                chat_id=chat_id,
                user_id=user.id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
            
            if result:
                logger.info(f"New user {user.id} joined and saved to database for chat {chat_id}")
            else:
                logger.error(f"Failed to save new user {user.id} to database for chat {chat_id}")
                
        except Exception as e:
            logger.error(f"Error processing chat member update: {e}", exc_info=True)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ذخیره اطلاعات کاربران هنگام ارسال پیام."""
    # اگر پیام یا کاربر وجود نداشته باشد، خارج می‌شویم
    if not update.effective_message or not update.effective_user:
        return
        
    try:
        user = update.effective_user
        chat_id = update.effective_chat.id
        
        # اگر چت خصوصی است، خارج می‌شویم
        if update.effective_chat.type == "private":
            return
            
        user_store = UserStore()
        
        # ذخیره اطلاعات کاربر
        result = user_store.add_user(
            chat_id=chat_id,
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        
        if result:
            logger.debug(f"User {user.id} saved/updated in database for chat {chat_id}")
            
    except Exception as e:
        logger.error(f"Error saving user info from message: {e}", exc_info=True)

def setup_handlers():
    """هندلرهای مرتبط با مدیریت کاربران را برمی‌گرداند."""
    # این هندلرها در main.py استفاده می‌شوند
    return []  # هندلرها مستقیماً در main.py اضافه می‌شوند 