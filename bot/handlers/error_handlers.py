import logging
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot."""
    try:
        logger.error(f"Update {update} caused error: {context.error}", exc_info=True)
        if update and update.effective_message:
            await update.effective_message.reply_text("خطایی رخ داد. لطفاً دوباره تلاش کنید.")
    except Exception as e:
        logger.error(f"Error in error_handler: {e}")