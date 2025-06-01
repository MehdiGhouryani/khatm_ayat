import asyncio
import logging
import backoff
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler, ChatMemberHandler
from telegram import Update
from telegram.ext import ContextTypes
from bot.handlers.admin_handlers import start, stop, topic, khatm_selection, set_zekr_text, help_command, set_range, start_khatm_zekr, start_khatm_salavat, start_khatm_ghoran, set_khatm_target_number, TEXT_COMMANDS,set_completion_count
from bot.handlers.khatm_handlers import handle_khatm_message, subtract_khatm, start_from, khatm_status
from bot.handlers.settings_handlers import reset_zekr, reset_kol, stop_on, stop_on_off, set_max, max_off, set_min, min_off, sepas_on, sepas_off, add_sepas, number_off, time_off, time_off_disable, lock_on, lock_off, jam_off, jam_on, set_completion_message, reset_daily, reset_off, reset_number_on, reset_number_off, delete_after, delete_off, reset_daily_groups, reset_periodic_topics, handle_new_message, max_ayat, min_ayat
from bot.handlers.stats_handlers import show_total_stats, show_ranking
from bot.handlers.hadith_handlers import hadis_on, hadis_off, send_daily_hadith
from bot.handlers.tag_handlers import setup_handlers, TagManager
from bot.handlers.user_handlers import chat_member_handler, message_handler as user_message_handler
from bot.handlers.error_handlers import error_handler
from bot.database.db import init_db, process_queue_request, execute, write_queue, close_db_connection,is_group_banned,set_group_invite_link,fetch_one,generate_invite_links_for_all_groups,fetch_all
from bot.utils.constants import DEFAULT_SEPAS_TEXTS, DAILY_HADITH_TIME, DAILY_RESET_TIME, DAILY_PERIOD_RESET_TIME,MONITOR_CHANNEL_ID
from config.settings import TELEGRAM_TOKEN
from bot.utils.logging_config import setup_logging
from bot.utils.helpers import ignore_old_messages
from bot.handlers.dashboard import setup_dashboard_handlers
from datetime import time

logger = logging.getLogger(__name__)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

ZEKR_STATE = 1

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
        "set_khatm_target_number": set_khatm_target_number,
        "reset_zekr": reset_zekr,
        "reset_kol": reset_kol,
        "stop_on": stop_on,
        "stop_on_off": stop_on_off,
        "set_max": set_max,
        "max_off": max_off,
        "max_ayat": max_ayat,
        "set_min": set_min,
        "min_off": min_off,
        "min_ayat": min_ayat,
        "sepas_on": sepas_on,
        "sepas_off": sepas_off,
        "add_sepas": add_sepas,
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
        "khatm_status": khatm_status,
        "set_completion_count": set_completion_count, 
        "tag_command": lambda update, context: TagManager(context).tag_command(update, context),
        "cancel_tag": lambda update, context: TagManager(context).cancel_tag(update, context),
    }
    for cmd, info in TEXT_COMMANDS.items():
        handler_name = info["handler"]
        if handler_name in handler_map:
            info["handler"] = handler_map[handler_name]
        else:
            raise ValueError(f"Handler {handler_name} not found for command {cmd}")
async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        chat_member = update.chat_member
        if chat_member.new_chat_member.user.id == context.bot.id:
            # بررسی اگر بات به گروه اضافه شده یا ادمین است
            if chat_member.new_chat_member.status in ["member", "administrator"]:
                chat_id = update.effective_chat.id
                try:
                    # بررسی وجود گروه در دیتابیس
                    group_exists = await fetch_one("SELECT group_id FROM groups WHERE group_id = ?", (chat_id,))
                    if not group_exists:
                        await execute("INSERT OR IGNORE INTO groups (group_id, is_active) VALUES (?, 1)", (chat_id,))
                    
                    # ایجاد لینک دعوت
                    invite_link = await context.bot.create_chat_invite_link(chat_id, member_limit=None)
                    await set_group_invite_link(chat_id, invite_link.invite_link)
                    logger.info("Auto-set invite link for group: chat_id=%s, link=%s", chat_id, invite_link.invite_link)
                except Exception as e:
                    logger.error("Error creating invite link for group %s: %s", chat_id, str(e), exc_info=True)
    except Exception as e:
        logger.error("Error in chat_member_handler: %s", str(e), exc_info=True)

async def refresh_invite_links(context: ContextTypes.DEFAULT_TYPE):
    try:
        groups = await fetch_all("SELECT group_id, invite_link FROM groups")
        for group in groups:
            group_id = group["group_id"]
            try:
                # بررسی وضعیت لینک
                chat = await context.bot.get_chat(group_id)
                if group["invite_link"]:
                    # اگر لینک منقضی شده یا نامعتبر است، لینک جدید ایجاد کنید
                    try:
                        await context.bot.get_chat_invite_link(group_id, group["invite_link"])
                    except Exception:
                        new_link = await context.bot.create_chat_invite_link(group_id, member_limit=None)
                        await set_group_invite_link(group_id, new_link.invite_link)
                        logger.info("Refreshed invite link for group: group_id=%s", group_id)
            except Exception as e:
                logger.error("Error refreshing link for group %s: %s", group_id, str(e), exc_info=True)
    except Exception as e:
        logger.error("Error in refresh_invite_links: %s", str(e), exc_info=True)

async def handle_new_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle new messages in groups."""
    try:
        chat_id = update.effective_chat.id
        message = update.effective_message
        user_id = update.effective_user.id
        logger.debug("Handling new message: chat_id=%s, user_id=%s", chat_id, user_id)

        # Check if group is banned
        if await is_group_banned(chat_id):
            logger.debug("Ignoring message from banned group: chat_id=%s", chat_id)
            return

        # Forward message to monitoring channel
        try:
            await context.bot.forward_message(
                chat_id=MONITOR_CHANNEL_ID,
                from_chat_id=chat_id,
                message_id=message.message_id
            )
            logger.info("Message forwarded to monitoring channel: chat_id=%s, message_id=%s", chat_id, message.message_id)
        except Exception as e:
            logger.error("Error forwarding message to monitoring channel: %s", str(e), exc_info=True)

        # Continue with existing logic
        # (Keep the rest of the original handle_new_message logic here)
        # For brevity, assuming the rest remains unchanged
    except Exception as e:
        logger.error("Error handling new message: %s", str(e), exc_info=True)


@ignore_old_messages()
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ عملیات لغو شد.")
    context.user_data.clear()
    return ConversationHandler.END

@ignore_old_messages()
async def ignore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        await context.bot.send_message(update.effective_chat.id, "دستور ناشناخته! از /help استفاده کنید.")

async def initialize_app():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN is required")
    await init_db()
    for text in DEFAULT_SEPAS_TEXTS:
        await execute("INSERT OR IGNORE INTO sepas_texts (text, is_default, group_id) VALUES (?, 1, NULL)", (text,))

    

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
        CommandHandler("number", set_khatm_target_number),
        CommandHandler("number_off", number_off),
        CommandHandler("reset_number_on", reset_number_off),
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
        CommandHandler("max_ayat", max_ayat),
        CommandHandler("min", set_min),
        CommandHandler("min_off", min_off),
        CommandHandler("min_ayat", min_ayat),
        CommandHandler("sepas_on", sepas_on),
        CommandHandler("sepas_off", sepas_off),
        CommandHandler("addsepas", add_sepas),
        CommandHandler("jam_on", jam_on),
        CommandHandler("jam_off", jam_off),
        CommandHandler("set_completion_message", set_completion_message),
        CommandHandler("khatm_status", khatm_status),
        CommandHandler("subtract", subtract_khatm),
        CommandHandler("set_completion_count", set_completion_count), 
    ] + setup_handlers() + setup_dashboard_handlers()
    app.add_handler(conv_handler)
    for handler in command_handlers:
        app.add_handler(handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_khatm_message))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, handle_new_message), group=900)
    
    subtract_pattern = r'^[-]?\d+$'
    app.add_handler(MessageHandler(
        filters.Regex(subtract_pattern) & filters.ChatType.GROUPS,
        subtract_khatm
    ))
    
    start_from_pattern = r'^(شروع از|start from)\s*(\d+)$'
    app.add_handler(MessageHandler(
        filters.Regex(start_from_pattern) & filters.ChatType.GROUPS,
        start_from
    ))
    
    # افزودن هندلرهای جدید برای مدیریت کاربران
    app.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ALL, user_message_handler), group=999)  # اولویت پایین

    app.add_handler(MessageHandler(filters.COMMAND, ignore_command))
    app.add_error_handler(error_handler)

def register_jobs(app: Application):
    job_queue = app.job_queue
    if not job_queue:
        raise RuntimeError("JobQueue initialization failed")
    job_queue.run_daily(send_daily_hadith, DAILY_HADITH_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_daily_hadith")
    job_queue.run_daily(reset_daily_groups, DAILY_RESET_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_daily_reset")
    job_queue.run_daily(reset_periodic_topics, DAILY_PERIOD_RESET_TIME, days=(0, 1, 2, 3, 4, 5, 6), name="job_period_reset")
    job_queue.run_repeating(process_queue_periodically, interval=1.0, first=1.0, name="job_queue_worker")
    job_queue.run_daily(refresh_invite_links, time(hour=0, minute=0), name="refresh_invite_links")

async def shutdown(app: Application):
    await app.stop()
    await app.updater.stop()
    await close_db_connection()

async def main():
    await initialize_app()

    setup_logging()
    map_handlers()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    await generate_invite_links_for_all_groups(app.bot)
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