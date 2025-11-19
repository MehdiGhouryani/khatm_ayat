import asyncio
import logging
import backoff
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ChatMemberHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import ContextTypes
from typing import Optional


# ایمپورت‌های به‌روز شده از هندلرها
from bot.handlers.admin_handlers import (
    start, stop, topic, khatm_selection, help_command, set_range, 
    start_khatm_zekr, start_khatm_salavat, start_khatm_ghoran, 
    set_khatm_target_number, TEXT_COMMANDS, set_completion_count,
    add_zekr, remove_zekr, list_zekrs, handle_remove_zekr_click, # <--- توابع جدید و صحیح ادمین
    is_admin
)
from bot.handlers.khatm_handlers import (
    handle_khatm_message, subtract_khatm, start_from, khatm_status,
    handle_zekr_selection # <--- تابع جدید دکمه‌های ذکر
)
from bot.handlers.settings_handlers import (
    reset_zekr, reset_kol, stop_on, stop_on_off, set_max, max_off, 
    set_min, min_off, sepas_on, sepas_off, add_sepas, number_off, 
    time_off, time_off_disable, lock_on, lock_off, jam_off, jam_on, 
    set_completion_message, reset_daily, reset_off, reset_number_on, 
    reset_number_off, delete_after, delete_off, reset_daily_groups, 
    reset_periodic_topics, handle_new_message, max_ayat, min_ayat
)
from bot.handlers.stats_handlers import show_total_stats, show_ranking
from bot.handlers.hadith_handlers import hadis_on, hadis_off, send_daily_hadith
from bot.handlers.tag_handlers import setup_handlers, TagManager
from bot.handlers.user_handlers import chat_member_handler as user_chat_member_handler, message_handler as user_message_handler
from bot.handlers.error_handlers import error_handler
from bot.database.db import init_db, process_queue_request, execute, write_queue, close_db_connection, is_group_banned, set_group_invite_link, fetch_one, generate_invite_links_for_all_groups, fetch_all
from bot.database.members_db import execute as members_execute
from bot.utils.constants import DEFAULT_SEPAS_TEXTS, DAILY_HADITH_TIME, DAILY_RESET_TIME, DAILY_PERIOD_RESET_TIME, MONITOR_CHANNEL_ID, MAIN_GROUP_ID
from config.settings import TELEGRAM_TOKEN
from bot.utils.logging_config import setup_logging
from bot.utils.helpers import ignore_old_messages
from bot.handlers.dashboard import setup_dashboard_handlers
from datetime import time
import time as time_module

logger = logging.getLogger(__name__)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# ZEKR_STATE حذف شد چون دیگر نیازی به ConversationHandler نیست

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
        # "set_zekr_text": set_zekr_text, # حذف شد
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
        "add_zekr": add_zekr,       # اضافه شد
        "remove_zekr": remove_zekr, # اضافه شد
        "list_zekrs": list_zekrs,   # اضافه شد
        "tag_command": lambda update, context: TagManager(context).tag_command(update, context),
        "cancel_tag": lambda update, context: TagManager(context).cancel_tag(update, context),
    }
    for cmd, info in TEXT_COMMANDS.items():
        handler_name = info["handler"]
        if handler_name in handler_map:
            info["handler"] = handler_map[handler_name]
        else:
            # اگر هندلر پیدا نشد، فقط لاگ می‌کنیم تا برنامه متوقف نشود (برای امنیت بیشتر)
            logger.warning(f"Handler {handler_name} not found for command {cmd}")

async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        chat_member = update.chat_member
        if not chat_member:
            return

        chat = update.effective_chat
        user = chat_member.new_chat_member.user
        user_id = user.id
        
        if user.id == context.bot.id:
            old_status = chat_member.old_chat_member.status
            new_status = chat_member.new_chat_member.status

            if new_status == ChatMember.MEMBER and old_status != ChatMember.MEMBER:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text="از اینکه من را به گروه دعوت کردید سپاسگزارم. 🤖\nبرای عملکرد کامل، لطفاً ربات را به عنوان ادمین گروه انتخاب کنید."
                )
                logger.info(f"Bot was added as a member to group {chat.id}. Sent admin request message.")
                return

            if new_status == ChatMember.ADMINISTRATOR and old_status == ChatMember.MEMBER:
                logger.info(f"Bot was promoted to admin in group {chat.id}.")

                group_exists = await fetch_one("SELECT group_id FROM groups WHERE group_id = ?", (chat.id,))
                if not group_exists:
                    await execute("INSERT OR IGNORE INTO groups (group_id, is_active) VALUES (?, 1)", (chat.id,))

                try:
                    invite_link = await context.bot.create_chat_invite_link(chat.id)
                    await set_group_invite_link(chat.id, invite_link.invite_link)
                    logger.info(f"Auto-set invite link for group: chat_id={chat.id}")
                except Exception as e:
                    logger.error(f"Failed to create invite link for group {chat.id} after promotion: {e}")

                full_chat = await context.bot.get_chat(chat.id)
                if full_chat.is_forum:
                    await context.bot.send_message(
                        chat_id=chat.id,
                        text="✅ ربات با موفقیت ادمین شد.\n\n"
                             "این گروه <b>تاپیک‌دار</b> است. برای راه‌اندازی ختم در هر تاپیک، لطفاً وارد تاپیک مورد نظر شده و از دستور /topic یا 'تاپیک' استفاده کنید.",
                        parse_mode='HTML'
                    )
                else:
                    keyboard = [
                        [
                            InlineKeyboardButton("صلوات 🙏", callback_data="khatm_salavat"),
                            InlineKeyboardButton("قرآن 📖", callback_data="khatm_ghoran"),
                            InlineKeyboardButton("ذکر 📿", callback_data="khatm_zekr"),
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.send_message(
                        chat_id=chat.id,
                        text="✅ ربات با موفقیت ادمین شد!\n\n"
                             "برای شروع، لطفاً نوع ختم اصلی گروه را از دکمه‌های زیر انتخاب کنید:",
                        reply_markup=reply_markup
                    )
            return

        if chat.id == MAIN_GROUP_ID:
            status = chat_member.new_chat_member.status
            current_timestamp = int(time_module.time())
            if status in ["member", "administrator", "creator"]:
                username = user.username or None
                first_name = user.first_name or "User"
                last_name = user.last_name or None
                is_bot = 1 if user.is_bot else 0
                await members_execute(
                    """
                    INSERT OR REPLACE INTO members (
                        user_id, group_id, username, first_name, last_name,
                        is_bot, is_deleted, scraped_timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (user_id, chat.id, username, first_name, last_name, is_bot, current_timestamp)
                )
                logger.info("User %s (%s) added/updated as active in main group %s", user.id, username or first_name, chat.id)
            elif status in ["left", "kicked"]:
                await members_execute(
                    """
                    UPDATE members
                    SET is_deleted = 1, scraped_timestamp = ?
                    WHERE user_id = ? AND group_id = ?
                    """,
                    (current_timestamp, user.id, chat.id)
                )
                logger.info("User %s marked as deleted in main group %s", user.id, chat.id)
    except Exception as e:
        logger.error("Error in chat_member_handler: %s", str(e), exc_info=True)

async def refresh_invite_links(context: ContextTypes.DEFAULT_TYPE):
    try:
        groups = await fetch_all("SELECT group_id, invite_link FROM groups")
        for group in groups:
            group_id = group["group_id"]
            try:
                chat = await context.bot.get_chat(group_id)
                if group["invite_link"]:
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

        if await is_group_banned(chat_id):
            logger.debug("Ignoring message from banned group: chat_id=%s", chat_id)
            return

        try:
            await context.bot.forward_message(
                chat_id=MONITOR_CHANNEL_ID,
                from_chat_id=chat_id,
                message_id=message.message_id
            )
            logger.info("Message forwarded to monitoring channel: chat_id=%s, message_id=%s", chat_id, message.message_id)
        except Exception as e:
            logger.error("Error forwarding message to monitoring channel: %s", str(e), exc_info=True)
    except Exception as e:
        logger.error("Error handling new message: %s", str(e), exc_info=True)

@ignore_old_messages()
async def ignore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        await context.bot.send_message(update.effective_chat.id, "دستور ناشناخته! از /help استفاده کنید.")

async def initialize_app():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN is required")

    # پاک‌سازی متن‌های سپاس تکراری (یک‌بار اجرا)
    logger.info("Attempting to clean up duplicate default sepas texts...")
    try:
        from bot.database.db import execute, init_db_connection
        await init_db_connection()
        await execute(
            """
            DELETE FROM sepas_texts
            WHERE is_default = 1 AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM sepas_texts
                WHERE is_default = 1
                GROUP BY text
            )
            """
        )
        logger.info("Database cleanup successful. Duplicate default texts removed.")
    except Exception as e:
        logger.error(f"An error occurred during the one-time cleanup, but we will proceed: {e}")

    await init_db()

    import aiosqlite
    from bot.database.db import execute
    for text in DEFAULT_SEPAS_TEXTS:
        try:
            await execute(
                "INSERT INTO sepas_texts (text, is_default, group_id) VALUES (?, 1, NULL)",
                (text,)
            )
        except aiosqlite.IntegrityError:
            pass

def register_handlers(app: Application):
    # --- هندلرهای دکمه (Callback Query) - اولویت بالا ---
    # دکمه‌های مدیریت ذکر (حذف توسط ادمین)
    app.add_handler(CallbackQueryHandler(handle_remove_zekr_click, pattern=r"^del_zekr_"))
    
    # دکمه‌های انتخاب ذکر (توسط کاربر برای ثبت)
    app.add_handler(CallbackQueryHandler(handle_zekr_selection, pattern=r"^zekr_"))

    # انتخاب نوع ختم (منوی تاپیک یا استارت)
    app.add_handler(CallbackQueryHandler(khatm_selection, pattern="khatm_(zekr|salavat|ghoran)"))

    # --- دستورات ---
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
        # دستورات شروع ختم
        CommandHandler("khatm_zekr", start_khatm_zekr),
        CommandHandler("khatm_salavat", start_khatm_salavat),
        CommandHandler("khatm_ghoran", start_khatm_ghoran),
        # دستورات جدید مدیریت ذکر
        CommandHandler("add_zekr", add_zekr),
        CommandHandler("remove_zekr", remove_zekr),
        CommandHandler("list_zekrs", list_zekrs),
    ] + setup_handlers() + setup_dashboard_handlers()

    for handler in command_handlers:
        app.add_handler(handler)

    # هندلرهای پیام
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_khatm_message))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_new_message), group=900)
    
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
    
    app.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ALL, user_message_handler), group=999)

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
    """
    ربات را راه‌اندازی، مقداردهی اولیه و شروع به کار می‌کند.
    """
    
    # ۱. تمام کارهای راه‌اندازی async را انجام بده
    # (فرض می‌کنم initialize_app تابع init_db() را صدا می‌زند که مهاجرت‌ها را اجرا می‌کند)
    await initialize_app() 
    
    setup_logging()
    map_handlers()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # ۲. کارهای مربوط به app را انجام بده
    await generate_invite_links_for_all_groups(app.bot)
    register_handlers(app)
    register_jobs(app)
    
    # ۳. ربات را راه‌اندازی و شروع کن
    await app.initialize()
    await app.updater.start_polling(
        allowed_updates=["message", "chat_member", "callback_query"], 
        timeout=30, 
        drop_pending_updates=True
    )
    await app.start()
    
    logger.info("ربات با موفقیت شروع به کار کرد...")
    
    # ۴. خود اپلیکیشن را برگردان تا در بخش main قابل دسترس باشد
    return app

async def shutdown(app: Application):
    """
    توابع مورد نیاز برای خاموش کردن ربات را اجرا می‌کند.
    """
    # (این بخش را مطابق نیاز خودتان تغییر دهید)
    logger.info("در حال اجرای توابع خاموش شدن...")
    await app.stop()
    await app.updater.stop()
    await app.shutdown()
    logger.info("خاموش شدن با موفقیت انجام شد.")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app: Optional[Application] = None  # متغیر app را بیرون تعریف می‌کنیم
    
    try:
        # ۵. تابع main را اجرا کن تا ربات راه‌اندازی شود و app را برگرداند
        app = loop.run_until_complete(main())
        
        # ۶. حالا که ربات در حال اجراست، loop را به صورت دستی زنده نگه دار
        loop.run_forever()
        
    except KeyboardInterrupt:
        # ۷. وقتی Ctrl+C زده شد، run_forever متوقف می‌شود
        logger.warning("دریافت سیگنال توقف (Ctrl+C)...")
        
    except Exception as e:
        logger.error(f"خطای پیش‌بینی نشده در سطح اصلی: {e}", exc_info=True)

    finally:
        # ۸. در هر صورت (خطا یا Ctrl+C)، ربات را خاموش کن
        if app:
            logger.info("در حال خاموش کردن ربات...")
            loop.run_until_complete(shutdown(app)) 
            
        logger.info("درحال بستن event loop...")
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logger.info("ربات با موفقیت متوقف شد.")