import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import TimedOut, Forbidden, BadRequest

logger = logging.getLogger(__name__)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot."""
    try:
        error = context.error
        if isinstance(error, TimedOut):
            logger.warning("Request timed out: %s", error)
            if update and update.effective_message:
                await update.effective_message.reply_text("درخواست با تأخیر مواجه شد. لطفاً دوباره تلاش کنید.")
        elif isinstance(error, Forbidden):
            logger.warning("Forbidden error: %s", error)
            if update and update.effective_message:
                await update.effective_message.reply_text("دسترسی به گروه یا کاربر ممکن نیست.")
        elif isinstance(error, BadRequest):
            logger.warning("Bad request: %s", error)
            if update and update.effective_message:
                await update.effective_message.reply_text("درخواست نامعتبر است. لطفاً ورودی را بررسی کنید.")
        else:
            logger.error("Unexpected error: %s", error, exc_info=True)
            if update and update.effective_message:
                await update.effective_message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")

        # Notify admin for critical errors (optional)
        if not isinstance(error, (TimedOut, Forbidden, BadRequest)):
            admin_id = 123456789  # Replace with actual admin ID
            await context.bot.send_message(admin_id, f"Error in bot: {error}")
    except Exception as e:
        logger.error("Error in error_handler: %s", e)