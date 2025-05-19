import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, Forbidden, BadRequest

logger = logging.getLogger(__name__)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot."""
    try:
        error = context.error
        if not update or not update.effective_chat:
            logger.warning("No valid chat to send error message: error=%s", error)
            return

        chat_id = update.effective_chat.id
        if isinstance(error, TimedOut):
            logger.warning("Request timed out: %s", error)
            try:
                await context.bot.send_message(chat_id, "درخواست با تأخیر مواجه شد. لطفاً دوباره تلاش کنید.")
            except (BadRequest, Forbidden) as e:
                logger.debug("Failed to send TimedOut message: %s", e)
        elif isinstance(error, Forbidden):
            logger.warning("Forbidden error: %s", error)
            try:
                await context.bot.send_message(chat_id, "دسترسی به گروه یا کاربر ممکن نیست.")
            except (BadRequest, Forbidden) as e:
                logger.debug("Failed to send Forbidden message: %s", e)
        elif isinstance(error, BadRequest):
            logger.warning("Bad request: %s", error)
            try:
                await context.bot.send_message(chat_id, "درخواست نامعتبر است. لطفاً ورودی را بررسی کنید.")
            except (BadRequest, Forbidden) as e:
                logger.debug("Failed to send BadRequest message: %s", e)
        else:
            logger.error("Unexpected error: %s", error, exc_info=True)
            try:
                await context.bot.send_message(chat_id, "خطایی رخ داد. لطفاً دوباره تلاش کنید.")
            except (BadRequest, Forbidden) as e:
                logger.debug("Failed to send error message: %s", e)

        # Notify admin for critical errors (optional)
        if not isinstance(error, (TimedOut, Forbidden, BadRequest)):
            admin_id = 123456789  # Replace with actual admin ID
            try:
                await context.bot.send_message(admin_id, f"Error in bot: {error}")
            except Exception as e:
                logger.debug("Failed to notify admin: %s", e)
    except Exception as e:
        logger.error("Error in error_handler: %s", e, exc_info=True)