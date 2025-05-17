import logging
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler
from telegram import Update
from telegram.ext import ContextTypes
from bot.handlers.admin_handlers import (
    start, stop, topic, khatm_selection, set_zekr_text, help_command, set_range,
    start_khatm_zekr, start_khatm_salavat, start_khatm_ghoran, set_salavat_count
)
from bot.handlers.khatm_handlers import handle_khatm_message
from bot.handlers.settings_handlers import (
    reset_zekr, reset_kol, stop_on, stop_on_off, set_max, max_off, set_min, min_off, sepas_on, sepas_off, add_sepas,
    set_number, number_off, reset_daily, reset_off, time_off, time_off_disable, lock_on, lock_off, delete_after, delete_off,
    jam_off, jam_on, set_completion_message, reset_number_on, reset_number_off
)
from bot.handlers.stats_handlers import show_total_stats, show_ranking
from bot.handlers.hadith_handlers import hadis_on, hadis_off
from bot.handlers.error_handlers import error_handler
from bot.database.db import get_db_connection, init_db
from bot.utils.constants import DEFAULT_SEPAS_TEXTS
from bot.utils.quran import QuranManager
from config.settings import TELEGRAM_TOKEN

# تنظیم لاگ‌گذاری
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ثابت‌های حالت برای ConversationHandler
ZEKR_STATE, SALAVAT_STATE, QURAN_STATE = range(1, 4)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current conversation."""
    logger.debug(f"User {update.effective_user.id} cancelled conversation in chat {update.effective_chat.id}")
    await update.message.reply_text("❌ عملیات لغو شد.")
    context.user_data.clear()
    return ConversationHandler.END

def main():
    try:
        # Initialize database
        init_db()
        logger.info("Database initialized successfully")

        # Insert default sepas texts
        with get_db_connection() as conn:
            cursor = conn.cursor()
            for text in DEFAULT_SEPAS_TEXTS:
                cursor.execute(
                    "INSERT OR IGNORE INTO sepas_texts (text, is_default, group_id) VALUES (?, 1, NULL)",
                    (text,)
                )
            conn.commit()
        logger.info("Default sepas texts inserted")

        # Initialize QuranManager
        global quran
        quran = QuranManager()
        logger.info("QuranManager initialized")

        # Build the application
        app = Application.builder().token(TELEGRAM_TOKEN).build()

        # Conversation handler for khatm settings
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("khatm_zekr", start_khatm_zekr),
                CommandHandler("khatm_salavat", start_khatm_salavat),
                CommandHandler("khatm_ghoran", start_khatm_ghoran),
                CallbackQueryHandler(khatm_selection, pattern="khatm_(zekr|salavat|ghoran)"),
            ],
            states={
                ZEKR_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_zekr_text)],
                SALAVAT_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_salavat_count)],
                QURAN_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_range)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        app.add_handler(conv_handler)

        # Add other command handlers
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("stop", stop))
        app.add_handler(CommandHandler("topic", topic))
        app.add_handler(CommandHandler("set_range", set_range))
        app.add_handler(CommandHandler("reset_zekr", reset_zekr))
        app.add_handler(CommandHandler("reset_kol", reset_kol))
        app.add_handler(CommandHandler("stop_on", stop_on))
        app.add_handler(CommandHandler("stop_on_off", stop_on_off))
        app.add_handler(CommandHandler("number", set_number))
        app.add_handler(CommandHandler("number_off", number_off))
        app.add_handler(CommandHandler("reset_number_on", reset_number_on))
        app.add_handler(CommandHandler("reset_number_off", reset_number_off))
        app.add_handler(CommandHandler("reset_on", reset_daily))
        app.add_handler(CommandHandler("reset_off", reset_off))
        app.add_handler(CommandHandler("hadis_on", hadis_on))
        app.add_handler(CommandHandler("hadis_off", hadis_off))
        app.add_handler(CommandHandler("time_off", time_off))
        app.add_handler(CommandHandler("time_off_disable", time_off_disable))
        app.add_handler(CommandHandler("lock_on", lock_on))
        app.add_handler(CommandHandler("lock_off", lock_off))
        app.add_handler(CommandHandler("delete_on", delete_after))
        app.add_handler(CommandHandler("delete_off", delete_off))
        app.add_handler(CommandHandler("amar_kol", show_total_stats))
        app.add_handler(CommandHandler("amar_list", show_ranking))
        app.add_handler(CommandHandler("max", set_max))
        app.add_handler(CommandHandler("max_off", max_off))
        app.add_handler(CommandHandler("min", set_min))
        app.add_handler(CommandHandler("min_off", min_off))
        app.add_handler(CommandHandler("sepas_on", sepas_on))
        app.add_handler(CommandHandler("sepas_off", sepas_off))
        app.add_handler(CommandHandler("addsepas", add_sepas))
        app.add_handler(CommandHandler("jam_on", jam_on))
        app.add_handler(CommandHandler("jam_off", jam_off))
        app.add_handler(CommandHandler("set_completion_message", set_completion_message))

        # Message handler for khatm contributions
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_khatm_message))

        # Error handler
        app.add_error_handler(error_handler)

        # Start the bot
        logger.info("Starting bot...")
        app.run_polling(allowed_updates=["message", "chat_member", "callback_query"], timeout=20)

    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()