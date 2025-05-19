import asyncio
import logging
import backoff
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler
from telegram import Update
from telegram.ext import ContextTypes
from bot.handlers.admin_handlers import start, stop, topic, khatm_selection, set_zekr_text, help_command, set_range, start_khatm_zekr, start_khatm_salavat, start_khatm_ghoran, set_salavat_count, TEXT_COMMANDS
from bot.handlers.khatm_handlers import handle_khatm_message, subtract_khatm, start_from, khatm_status
from bot.handlers.settings_handlers import reset_zekr, reset_kol, stop_on, stop_on_off, set_max, max_off, set_min, min_off, sepas_on, sepas_off, add_sepas, set_number, number_off, time_off, time_off_disable, lock_on, lock_off, jam_off, jam_on, set_completion_message, reset_daily, reset_off, reset_number_on, reset_number_off, delete_after, delete_off, reset_daily_groups, reset_periodic_topics, handle_new_message
from bot.handlers.stats_handlers import show_total_stats, show_ranking
from bot.handlers.hadith_handlers import hadis_on, hadis_off, send_daily_hadith
from bot.handlers.tag_handlers import setup_handlers, TagManager
from bot.handlers.error_handlers import error_handler
from bot.database.db import init_db, process_queue_request, execute, write_queue, close_db_connection
from bot.utils.constants import DEFAULT_SEPAS_TEXTS, DAILY_HADITH_TIME, DAILY_RESET_TIME, DAILY_PERIOD_RESET_TIME, init_quran_manager
from config.settings import TELEGRAM_TOKEN
from bot.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)

ZEKR_STATE, SALAVAT_STATE, QURAN_STATE = range(1, 4)

@backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=10)
async def process_queue_periodically(context: ContextTypes.DEFAULT_TYPE):
    while not write_queue.empty():
        request = await write_queue.get()
        await process_queue_request(request)
        write_queue.task_done()

def map_handlers():
    handler_map = {
        "start": start,
        "stop": stop,
        "topic": topic,
        "khatm_selection": khatm_selection,
        "set_zekr_text": set_zekr_text,
        "help_command": help_command,
        "set_range": set_range,
        "start_khatm_zekr": start_khatm_zekr,
        "start_khatm_salavat": start_khatm_salavat,
        "start_khatm_ghoran": start_khatm_ghoran,
        "set_salavat_count": set_salavat_count,
        "reset_zekr": reset_zekr,
        "reset_kol": reset_kol,
        "stop_on": stop_on,
        "stop_on_off": stop_on_off,
        "set_max": set_max,
        "max_off": max_off,
        "set_min": set_min,
        "min_off": min_off,
        "sepas_on": sepas_on,
        "sepas_off": sepas_off,
        "add_sepas": add_sepas,
        "set_number": set_number,
        "number_off": number_off,
        "time_off": time_off,
        "time_off_disable": time_off_disable,
        "lock_on": lock_on,
        "lock_off": lock_off,
        "delete_after": delete_after,
        "delete_off": delete_off,
        "show_total_stats": show_total_stats,
        "show_ranking": show_ranking,
        "hadis_on": hadis_on,
        "hadis_off": hadis_off,
        "reset_daily": reset_daily,
        "reset_off": reset_off,
        "reset_number_on": reset_number_on,
        "reset_number_off": reset_number_off,
        "jam_on": jam_on,
        "jam_off": jam_off,
        "set_completion_message": set_completion_message,
        "subtract_khatm": subtract_khatm,
        "start_from": start_from,
        "tag_command": lambda update, context: TagManager(context).tag_command(update, context),
        "cancel_tag": lambda update, context: TagManager(context).cancel_tag(update, context),
    }
    for cmd, info in TEXT_COMMANDS.items():
        handler_name = info["handler"]
        if handler_name in handler_map:
            info["handler"] = handler_map[handler_name]
        else:
            raise ValueError(f"Handler {handler_name} not found for command {cmd}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ عملیات لغو شد.")
    context.user_data.clear()
    return ConversationHandler.END

async def initialize_app():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN is required")
    await init_db()
    for text in DEFAULT_SEPAS_TEXTS:
        await execute("INSERT OR IGNORE INTO sepas_texts (text, is_default, group_id) VALUES (?, 1, NULL)", (text,))
    await init_quran_manager()

def register_handlers(app: Application):
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
        per_message=False,
    )
    command_handlers = [
        CommandHandler("help", help_command),
        CommandHandler("start", start),
        CommandHandler("stop", stop),
        CommandHandler("topic", topic),
        CommandHandler("set_range", set_range),
        CommandHandler("reset_zekr", reset_zekr),
        CommandHandler("reset_kol", reset_kol),
        CommandHandler("stop_on", stop_on),
        CommandHandler("stop_on_off", stop_on_off),
        CommandHandler("number", set_number),
        CommandHandler("number_off", number_off),
        CommandHandler("reset_number_on", reset_number_on),
        CommandHandler("reset_number_off", reset_number_off),
        CommandHandler("reset_on", reset_daily),
        CommandHandler("reset_off", reset_off),
        CommandHandler("hadis_on", hadis_on),
        CommandHandler("hadis_off", hadis_off),
        CommandHandler("time_off", time_off),
        CommandHandler("time_off_disable", time_off_disable),
        CommandHandler("lock_on", lock_on),
        CommandHandler("lock_off", lock_off),
        CommandHandler("delete_on", delete_after),
        CommandHandler("delete_off", delete_off),
        CommandHandler("amar_kol", show_total_stats),
        CommandHandler("amar_list", show_ranking),
        CommandHandler("max", set_max),
        CommandHandler("max_off", max_off),
        CommandHandler("min", set_min),
        CommandHandler("min_off", min_off),
        CommandHandler("sepas_on", sepas_on),
        CommandHandler("sepas_off", sepas_off),
        CommandHandler("addsepas", add_sepas),
        CommandHandler("jam_on", jam_on),
        CommandHandler("jam_off", jam_off),
        CommandHandler("set_completion_message", set_completion_message),
        CommandHandler("khatm_status", khatm_status),
    ] + setup_handlers()
    app.add_handler(conv_handler)
    for handler in command_handlers:
        app.add_handler(handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_khatm_message))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, handle_new_message))
    app.add_handler(MessageHandler(filters.Regex(r'^[-]?\d+$'), subtract_khatm))
    app.add_handler(MessageHandler(filters.Regex(r'^(شروع از)\s*(\d+)$'), start_from))
    app.add_handler(MessageHandler(filters.COMMAND, lambda update, context: None))
    app.add_error_handler(error_handler)

def register_jobs(app: Application):
    job_queue = app.job_queue
    if not job_queue:
        raise RuntimeError("JobQueue initialization failed")
    job_queue.run_daily(send_daily_hadith, DAILY_HADITH_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_daily_hadith")
    job_queue.run_daily(reset_daily_groups, DAILY_RESET_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_daily_reset")
    job_queue.run_daily(reset_periodic_topics, DAILY_PERIOD_RESET_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_periodic_reset")
    job_queue.run_repeating(process_queue_periodically, interval=0.5, first=1.0, name="job_queue_worker")

async def shutdown(app: Application):
    await app.stop()
    await app.updater.stop()
    await close_db_connection()

async def main():
    setup_logging()
    map_handlers()
    await initialize_app()
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    register_handlers(app)
    register_jobs(app)
    await app.initialize()
    await app.updater.start_polling(allowed_updates=["message", "chat_member", "callback_query"], timeout=30, drop_pending_updates=True)
    await app.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        await shutdown(app)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()