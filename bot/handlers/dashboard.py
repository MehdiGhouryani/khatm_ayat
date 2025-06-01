import logging
import re
import sqlite3
from typing import List, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telegram.error import BadRequest, Forbidden
from bot.database.db import fetch_all, fetch_one, is_group_banned, ban_group, unban_group, get_global_stats, get_group_users, set_group_invite_link, get_group_invite_link, remove_group_invite_link, ban_user, unban_user, is_user_banned
from bot.utils.constants import SUPER_ADMIN_IDS, MONITOR_CHANNEL_ID
from bot.utils.helpers import ignore_old_messages

logger = logging.getLogger(__name__)

# پیام‌های متنی
MESSAGES = {
    "unauthorized": "❌ دسترسی غیرمجاز. فقط مدیران ارشد می‌توانند به داشبورد دسترسی داشته باشند.",
    "error_generic": "❌ خطا در پردازش درخواست.",
    "error_database": "❌ خطا در دسترسی به پایگاه داده. لطفاً دوباره تلاش کنید.",
    "error_api": "❌ خطا در ارتباط با تلگرام. لطفاً دوباره تلاش کنید.",
    "dashboard_closed": "✅ داشبورد بسته شد.",
    "no_groups": "📋 هیچ گروهی یافت نشد.",
    "no_users": "هیچ کاربری در این گروه یافت نشد.",
    "invalid_group_id": "❌ شناسه گروه نامعتبر است. لطفاً یک عدد وارد کنید.",
    "group_not_found": "❌ گروهی با این شناسه وجود ندارد.",
    "invalid_link": "❌ لینک نامعتبر است. باید با https://t.me/ شروع شود.",
    "edit_message_failed": "❌ خطا در ویرایش پیام. منوی جدید ارسال شد.",
    "no_users_found": "🔍 هیچ کاربری با این مشخصات یافت نشد.",
    "select_users": "👤 کاربران انتخاب‌شده: {}\nلطفاً اقدام را انتخاب کنید:",
    "bulk_action_success": "✅ عملیات با موفقیت برای {} کاربر انجام شد.",
    "invalid_user_id": "❌ شناسه یا نام کاربری نامعتبر است.",
}

# حالت‌های ConversationHandler
DASHBOARD_MAIN, MANAGE_BANNED_GROUPS, VIEW_GROUPS_PAGINATED, SEARCH_GROUPS, VIEW_MONITORING, MANAGE_USERS, SET_GROUP_LINK, SEARCH_USERS = range(8)

def log_function_call(func):
    async def wrapper(*args, **kwargs):
        logger.debug(f"Entering function: {func.__name__}")
        try:
            result = await func(*args, **kwargs)
            logger.debug(f"Exiting function: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in function {func.__name__}: {str(e)}", exc_info=True)
            raise
    return wrapper

def create_main_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📋 لیست گروه‌ها", callback_data="view_groups")],
        [InlineKeyboardButton("🚫 گروه‌های مسدود", callback_data="manage_banned_groups")],
        [InlineKeyboardButton("📊 آمار کلی", callback_data="view_stats")],
        [InlineKeyboardButton("🔍 جستجوی گروه", callback_data="search_groups")],
        [InlineKeyboardButton("🔍 جستجوی کاربر", callback_data="search_users")],
        [InlineKeyboardButton("📩 پیام‌های نظارتی", callback_data="view_monitoring")],
        [InlineKeyboardButton("👤 مدیریت کاربران", callback_data="manage_users")],
        [InlineKeyboardButton("❌ بستن", callback_data="close_dashboard")]
    ]
    return InlineKeyboardMarkup(keyboard)

@ignore_old_messages()
@log_function_call
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    logger.info("Processing dashboard command: user_id=%s", user_id)
    try:
        if user_id not in SUPER_ADMIN_IDS:
            logger.warning("Unauthorized dashboard access attempt: user_id=%s", user_id)
            await update.message.reply_text(MESSAGES['unauthorized'])
            return ConversationHandler.END
        context.user_data.clear()
        reply_markup: InlineKeyboardMarkup = create_main_menu()
        await update.message.reply_text(
            "<b>🎛 داشبورد مدیریت</b>\nلطفاً یک گزینه را انتخاب کنید:",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        logger.info("Dashboard menu sent to user_id=%s", user_id)
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in dashboard_command: {str(e)}", exc_info=True)
        await update.message.reply_text(MESSAGES["error_generic"])
        return ConversationHandler.END

@ignore_old_messages()
@log_function_call
async def dashboard_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id: int = update.effective_user.id
    logger.info("Processing dashboard callback: user_id=%s, data=%s", user_id, query.data)
    try:
        if user_id not in SUPER_ADMIN_IDS:
            logger.warning("Unauthorized callback attempt: %s", user_id)
            await query.message.edit_text(MESSAGES["unauthorized"])
            return ConversationHandler.END
        if query.data == "close_dashboard":
            await query.message.edit_text(MESSAGES["dashboard_closed"])
            context.user_data.clear()
            return ConversationHandler.END
        elif query.data == "view_groups":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            context.user_data['current_page'] = 1
            return await view_groups(update, context)
        elif query.data == "manage_banned_groups":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            return await manage_banned_groups(update, context)
        elif query.data == "view_stats":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            return await view_stats(update, context)
        elif query.data == "search_groups":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            await query.message.edit_text("🔍 لطفاً شناسه گروه را وارد کنید:")
            return SEARCH_GROUPS
        elif query.data == "view_monitoring":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            return await view_monitoring(update, context)
        elif query.data == "manage_users":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            await query.message.edit_text("🔍 شناسه گروه را برای مدیریت کاربران وارد کنید:")
            return MANAGE_USERS
        elif query.data == "search_users":
            context.user_data['previous_state'] = DASHBOARD_MAIN
            await query.message.edit_text("🔍 لطفاً شناسه یا نام کاربری را وارد کنید:")
            return SEARCH_USERS
        elif query.data.startswith("ban_group_"):
            group_id = int(query.data.split("_")[-1])
            if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
                await query.message.edit_text(MESSAGES["group_not_found"])
                return await manage_banned_groups(update, context)
            await ban_group(group_id)
            await query.message.edit_text(f"✅ گروه {group_id} با موفقیت مسدود شد.")
            return await manage_banned_groups(update, context)
        elif query.data.startswith("unban_group_"):
            group_id = int(query.data.split("_")[-1])
            if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
                await query.message.edit_text(MESSAGES["group_not_found"])
                return await manage_banned_groups(update, context)
            await unban_group(group_id)
            await query.message.edit_text(f"✅ گروه {group_id} با موفقیت رفع مسدودیت شد.")
            return await manage_banned_groups(update, context)
        elif query.data.startswith("page_"):
            page = int(query.data.split("_")[-1])
            context.user_data['current_page'] = page
            return await view_groups(update, context)
        elif query.data.startswith("set_link_"):
            group_id = int(query.data.split("_")[-1])
            if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
                await query.message.edit_text(MESSAGES["group_not_found"])
                return await view_groups(update, context)
            context.user_data['link_group_id'] = group_id
            context.user_data['previous_state'] = VIEW_GROUPS_PAGINATED
            await query.message.edit_text(f"🔗 لینک دعوت برای گروه {group_id} را وارد کنید:")
            return SET_GROUP_LINK
        elif query.data.startswith("remove_link_"):
            group_id = int(query.data.split("_")[-1])
            if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
                await query.message.edit_text(MESSAGES["group_not_found"])
                return await view_groups(update, context)
            await remove_group_invite_link(group_id)
            await query.message.edit_text(f"✅ لینک گروه {group_id} حذف شد.")
            return await view_groups(update, context)
        elif query.data.startswith("generate_link_"):
            group_id = int(query.data.split("_")[-1])
            if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
                await query.message.edit_text(MESSAGES["group_not_found"])
                return await view_groups(update, context)
            try:
                chat = await context.bot.get_chat(group_id)
                if not chat.permissions.can_invite_users:
                    await query.message.edit_text(f"❌ ربات اجازه ایجاد لینک دعوت برای گروه {group_id} را ندارد.")
                    return await view_groups(update, context)
                invite_link = await context.bot.create_chat_invite_link(group_id, member_limit=None)
                await set_group_invite_link(group_id, invite_link.invite_link)
                await query.message.edit_text(f"✅ لینک گروه {group_id} با موفقیت ایجاد شد.")
            except Exception as e:
                logger.error("Error generating link for group %s: %s", group_id, str(e), exc_info=True)
                await query.message.edit_text(f"❌ خطا در ایجاد لینک برای گروه {group_id}: {str(e)}")
            return await view_groups(update, context)
        elif query.data.startswith("ban_user_"):
            user_id = int(query.data.split("_")[-1])
            await ban_user(user_id)
            await query.message.edit_text(f"✅ کاربر {user_id} با موفقیت مسدود شد.")
            return await manage_users(update, context)
        elif query.data.startswith("unban_user_"):
            user_id = int(query.data.split("_")[-1])
            await unban_user(user_id)
            await query.message.edit_text(f"✅ کاربر {user_id} با موفقیت رفع مسدودیت شد.")
            return await manage_users(update, context)
        elif query.data.startswith("user_page_"):
            data_parts = query.data.split("_")
            group_id = int(data_parts[2])
            page = int(data_parts[3])
            context.user_data['user_group_id'] = group_id
            context.user_data['user_page'] = page
            return await manage_users(update, context)
        elif query.data.startswith("select_user_"):
            user_id = int(query.data.split("_")[-1])
            selected_users = context.user_data.get('selected_users', set())
            selected_users.add(user_id)
            context.user_data['selected_users'] = selected_users
            await query.message.edit_text(
                MESSAGES["select_users"].format(len(selected_users)),
                reply_markup=create_bulk_action_keyboard(),
                parse_mode="HTML"
            )
            return MANAGE_USERS
        elif query.data == "bulk_ban":
            selected_users = context.user_data.get('selected_users', set())
            if not selected_users:
                await query.message.edit_text("❌ هیچ کاربری انتخاب نشده است.")
                return await manage_users(update, context)
            for user_id in selected_users:
                await ban_user(user_id)
            count = len(selected_users)
            context.user_data['selected_users'] = set()
            await query.message.edit_text(MESSAGES["bulk_action_success"].format(count))
            return await manage_users(update, context)
        elif query.data == "bulk_unban":
            selected_users = context.user_data.get('selected_users', set())
            if not selected_users:
                await query.message.edit_text("❌ هیچ کاربری انتخاب نشده است.")
                return await manage_users(update, context)
            for user_id in selected_users:
                await unban_user(user_id)
            count = len(selected_users)
            context.user_data['selected_users'] = set()
            await query.message.edit_text(MESSAGES["bulk_action_success"].format(count))
            return await manage_users(update, context)
        elif query.data == "clear_selection":
            context.user_data['selected_users'] = set()
            await query.message.edit_text("✅ انتخاب‌ها پاک شد.")
            return await manage_users(update, context)
        elif query.data == "filter_banned":
            context.user_data['user_filter'] = "banned"
            return await manage_users(update, context)
        elif query.data == "filter_unbanned":
            context.user_data['user_filter'] = "unbanned"
            return await manage_users(update, context)
        elif query.data == "filter_all":
            context.user_data['user_filter'] = "all"
            return await manage_users(update, context)
        elif query.data == "back_to_previous":
            previous_state = context.user_data.get('previous_state', DASHBOARD_MAIN)
            if previous_state == MANAGE_USERS:
                return await manage_users(update, context)
            elif previous_state == VIEW_GROUPS_PAGINATED:
                return await view_groups(update, context)
            elif previous_state == MANAGE_BANNED_GROUPS:
                return await manage_banned_groups(update, context)
            else:
                return await back_to_main(update, context)
        elif query.data == "back_to_main":
            context.user_data['selected_users'] = set()
            return await back_to_main(update, context)
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in dashboard_callback: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return ConversationHandler.END

@log_function_call
async def view_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    try:
        page: int = context.user_data.get('current_page', 1)
        per_page: int = 10
        logger.info("Fetching group list for dashboard: page=%s", page)
        query_str = """
            SELECT g.group_id, g.title, g.is_active, bg.group_id AS is_banned, 
                   COUNT(DISTINCT u.user_id) AS member_count, 
                   COUNT(DISTINCT t.topic_id) AS active_khatms, g.invite_link
            FROM groups g
            LEFT JOIN banned_groups bg ON g.group_id = bg.group_id
            LEFT JOIN users u ON g.group_id = u.group_id
            LEFT JOIN topics t ON g.group_id = t.group_id AND t.is_active = 1
            GROUP BY g.group_id
            LIMIT ? OFFSET ?
        """
        try:
            groups: List[dict] = await fetch_all(query_str, (per_page, (page - 1) * per_page))
            total_groups: int = (await fetch_one("SELECT COUNT(*) FROM groups"))["COUNT(*)"]
        except sqlite3.OperationalError as db_error:
            logger.error(f"Database error in view_groups: {str(db_error)}")
            try:
                await query.message.edit_text(MESSAGES["error_database"])
            except (BadRequest, Forbidden) as api_error:
                logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
                await query.message.reply_text(MESSAGES["error_database"])
            context.user_data.clear()
            return DASHBOARD_MAIN
        total_pages: int = (total_groups + per_page - 1) // per_page
        if not groups:
            try:
                await query.message.edit_text(MESSAGES["no_groups"])
            except (BadRequest, Forbidden) as api_error:
                logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
                await query.message.reply_text(MESSAGES["no_groups"])
            return DASHBOARD_MAIN
        message = f"<b>📋 لیست گروه‌ها (صفحه {page} از {total_pages})</b>\n\n"
        keyboard: List[List[InlineKeyboardButton]] = []
        for group in groups:
            title = group["title"] or f"شناسه {group['group_id']}"
            title_display = f'<a href="{group["invite_link"]}">{title}</a>' if group["invite_link"] else title
            status = "✅ فعال" if group["is_active"] else "❌ غیرفعال"
            banned_status = "🚫 مسدود" if group["is_banned"] else ""
            link_text = f'<a href="{group["invite_link"]}">لینک گروه</a>' if group["invite_link"] else "🔗 بدون لینک"
            message += (
                f"گروه: {title_display} ({group['group_id']})\n"
                f"{status} {banned_status}\n"
                f"👥 اعضا: {group['member_count']} | 🕋 ختم‌های فعال: {group['active_khatms']}\n"
                f"{link_text}\n\n"
            )
            buttons = []
            if not group["invite_link"]:
                buttons.append(InlineKeyboardButton("ایجاد لینک", callback_data=f"generate_link_{group['group_id']}"))
            buttons.append(InlineKeyboardButton(
                "تنظیم لینک" if not group["invite_link"] else "به‌روزرسانی لینک",
                callback_data=f"set_link_{group['group_id']}"
            ))
            buttons.append(InlineKeyboardButton("حذف لینک", callback_data=f"remove_link_{group['group_id']}"))
            keyboard.append(buttons)
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ صفحه قبل", callback_data=f"page_{page-1}")])
        if page < total_pages:
            keyboard.append([InlineKeyboardButton("➡️ صفحه بعد", callback_data=f"page_{page+1}")])
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message in view_groups: {str(api_error)}. Sending new message.")
            await query.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Paginated group list sent to dashboard: page=%s", page)
        return VIEW_GROUPS_PAGINATED
    except Exception as e:
        logger.error(f"Error in view_groups: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def view_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    try:
        logger.info("Fetching global stats for dashboard")
        stats = await get_global_stats()
        message = "<b>📊 آمار کلی سیستم</b>\n\n"
        message += f"👥 تعداد کل گروه‌ها: {stats['total_groups']}\n"
        message += f"✅ گروه‌های فعال: {stats['active_groups']}\n"
        message += f"🚫 گروه‌های مسدود: {stats['banned_groups']}\n"
        message += f"🙋 تعداد کل کاربران: {stats['total_users']}\n"
        message += f"📝 تعداد مشارکت‌ها: {stats['total_contributions']}\n"
        message += f"🏆 ختم‌های تکمیل‌شده: {stats['completed_khatms']}\n"
        keyboard = [[InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Global stats sent to dashboard")
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in view_stats: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def manage_banned_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    try:
        logger.info("Fetching banned groups for dashboard")
        banned_groups = await fetch_all("SELECT g.group_id, g.title, g.invite_link FROM banned_groups bg JOIN groups g ON bg.group_id = g.group_id")
        all_groups = await fetch_all("SELECT group_id, is_active, title, invite_link FROM groups")
        message = "<b>🚫 مدیریت گروه‌های مسدود</b>\n\n<b>گروه‌های مسدود:</b>\n"
        if banned_groups:
            for group in banned_groups:
                title = group["title"] or f"شناسه {group['group_id']}"
                title_display = f'<a href="{group["invite_link"]}">{title}</a>' if group["invite_link"] else title
                message += f"گروه {title_display} ({group['group_id']}) 🚫\n"
        else:
            message += "هیچ گروه مسدودی وجود ندارد.\n"
        message += "\n<b>همه گروه‌ها:</b>\n"
        keyboard = []
        for group in all_groups:
            banned = await is_group_banned(group["group_id"])
            status = "✅ فعال" if group["is_active"] else "❌ غیرفعال"
            title = group["title"] or f"شناسه {group['group_id']}"
            title_display = f'<a href="{group["invite_link"]}">{title}</a>' if group["invite_link"] else title
            message += f"گروه {title_display} ({group['group_id']}): {status} {'🚫' if banned else ''}\n"
            action_button = InlineKeyboardButton(
                "رفع مسدودیت" if banned else "مسدود کردن",
                callback_data=f"{'unban' if banned else 'ban'}_group_{group['group_id']}"
            )
            keyboard.append([action_button])
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Banned groups management menu sent")
        return MANAGE_BANNED_GROUPS
    except Exception as e:
        logger.error(f"Error in manage_banned_groups: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def view_monitoring(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    try:
        message = (
            "<b>📩 پیام‌های نظارتی</b>\n\n"
            f"پیام‌های گروه‌ها به کانال {MONITOR_CHANNEL_ID} ارسال می‌شوند.\n"
            "برای مشاهده پیام‌ها به کانال مراجعه کنید.\n\n"
            "⚠️ نکته: این بخش فقط اطلاعات کلی ارائه می‌دهد."
        )
        keyboard = [[InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Monitoring info sent to dashboard")
        return VIEW_MONITORING
    except Exception as e:
        logger.error(f"Error in view_monitoring: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def search_groups_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        search_id = update.message.text.strip()
        if not re.match(r"^-?\d+$", search_id):
            await update.message.reply_text(MESSAGES["invalid_group_id"])
            return await back_to_previous(update, context)
        query_str = """
            SELECT g.group_id, g.title, g.is_active, bg.group_id AS is_banned, 
                   COUNT(DISTINCT u.user_id) AS member_count, 
                   COUNT(DISTINCT t.topic_id) AS active_khatms, g.invite_link
            FROM groups g
            LEFT JOIN banned_groups bg ON g.group_id = bg.group_id
            LEFT JOIN users u ON g.group_id = u.group_id
            LEFT JOIN topics t ON g.group_id = t.group_id AND t.is_active = 1
            WHERE g.group_id = ?
            GROUP BY g.group_id
        """
        groups = await fetch_all(query_str, (int(search_id),))
        logger.info("Searching groups: search_id=%s", search_id)
        if not groups:
            await update.message.reply_text("🔍 هیچ گروهی با این شناسه یافت نشد.")
            return await back_to_previous(update, context)
        message = "<b>🔍 نتایج جستجو</b>\n\n"
        for group in groups:
            title = group["title"] or f"شناسه {group['group_id']}"
            title_display = f'<a href="{group["invite_link"]}">{title}</a>' if group["invite_link"] else title
            status = "✅ فعال" if group["is_active"] else "❌ غیرفعال"
            banned_status = "🚫 مسدود" if group["is_banned"] else ""
            link_text = f'<a href="{group["invite_link"]}">لینک گروه</a>' if group["invite_link"] else "🔗 بدون لینک"
            message += (
                f"گروه: {title_display} ({group['group_id']})\n"
                f"{status} {banned_status}\n"
                f"👥 اعضا: {group['member_count']} | 🕋 ختم‌های فعال: {group['active_khatms']}\n"
                f"{link_text}\n\n"
            )
        keyboard = [[InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Search results sent: found=%s", len(groups))
        return SEARCH_GROUPS
    except sqlite3.OperationalError as db_error:
        logger.error(f"Database error in search_groups_handler: {str(db_error)}")
        await update.message.reply_text(MESSAGES["error_database"])
        context.user_data.clear()
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in search_groups_handler: {str(e)}", exc_info=True)
        await update.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def set_group_link_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        invite_link = update.message.text.strip()
        group_id = context.user_data.get('link_group_id')
        if not group_id or not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
            await update.message.reply_text(MESSAGES["group_not_found"])
            context.user_data.clear()
            return DASHBOARD_MAIN
        if not invite_link.startswith("https://t.me/"):
            await update.message.reply_text(MESSAGES["invalid_link"])
            return SET_GROUP_LINK
        await set_group_invite_link(group_id, invite_link)
        await update.message.reply_text(f"✅ لینک گروه {group_id} با موفقیت تنظیم شد.")
        context.user_data.clear()
        return await view_groups(update, context)
    except sqlite3.OperationalError as db_error:
        logger.error(f"Database error in set_group_link_handler: {str(db_error)}")
        await update.message.reply_text(MESSAGES["error_database"])
        context.user_data.clear()
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in set_group_link_handler: {str(e)}", exc_info=True)
        await update.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

def create_bulk_action_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🚫 مسدود کردن", callback_data="bulk_ban")],
        [InlineKeyboardButton("✅ رفع مسدودیت", callback_data="bulk_unban")],
        [InlineKeyboardButton("🗑 پاک کردن انتخاب‌ها", callback_data="clear_selection")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")]
    ]
    return InlineKeyboardMarkup(keyboard)

@log_function_call
async def manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    try:
        if 'user_group_id' not in context.user_data and query.data == "manage_users":
            return MANAGE_USERS
        group_id = context.user_data.get('user_group_id')
        if not group_id or not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
            try:
                await query.message.edit_text(MESSAGES["group_not_found"])
            except (BadRequest, Forbidden) as api_error:
                logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
                await query.message.reply_text(MESSAGES["group_not_found"])
            context.user_data.clear()
            return DASHBOARD_MAIN
        page: int = context.user_data.get('user_page', 1)
        per_page: int = 10
        user_filter = context.user_data.get('user_filter', 'all')
        logger.info("Fetching users for group: group_id=%s, page=%s, filter=%s", group_id, page, user_filter)
        users, total_pages = await get_group_users(group_id, page, per_page)
        if user_filter == "banned":
            users = [user for user in users if await is_user_banned(user["user_id"])]
        elif user_filter == "unbanned":
            users = [user for user in users if not await is_user_banned(user["user_id"])]
        total_users = await fetch_one("SELECT COUNT(DISTINCT user_id) AS count FROM users WHERE group_id = ?", (group_id,))
        total_users = total_users["count"] if total_users else 0
        group_info = await fetch_one("SELECT title, is_active FROM groups WHERE group_id = ?", (group_id,))
        is_banned = await is_group_banned(group_id)
        group_title = group_info['title'] or f"شناسه {group_id}"
        status = "✅ فعال" if group_info["is_active"] else "❌ غیرفعال"
        banned_status = "🚫 مسدود" if is_banned else ""
        message = (
            f"<b>👤 مدیریت کاربران - گروه {group_title} ({group_id})</b>\n"
            f"{status} {banned_status}\n"
            f"👥 تعداد کل کاربران: {total_users}\n"
            f"📄 صفحه {page} از {total_pages}\n\n"
        )
        if not users:
            message += MESSAGES["no_users"]
        else:
            for user in users:
                user_id = user["user_id"]
                banned = await is_user_banned(user_id)
                banned_status = "🚫 مسدود" if banned else "✅ فعال"
                username = user.get("username", "بدون نام کاربری")
                first_name = user.get("first_name", "بدون نام")
                total_ayat = user.get("total_ayat", 0)
                total_salavat = user.get("total_salavat", 0)
                total_zekr = user.get("total_zekr", 0)
                message += (
                    f"کاربر: <b>{user_id}</b> ({first_name}, @{username})\n"
                    f"وضعیت: {banned_status}\n"
                    f"📖 آیات: {total_ayat} | 🙏 صلوات: {total_salavat} | 📿 ذکر: {total_zekr}\n\n"
                )
        keyboard = []
        for user in users:
            user_id = user["user_id"]
            banned = await is_user_banned(user_id)
            action_button = InlineKeyboardButton(
                "رفع مسدودیت" if banned else "مسدود کردن",
                callback_data=f"{'unban' if banned else 'ban'}_user_{user_id}"
            )
            select_button = InlineKeyboardButton(
                "✅ انتخاب",
                callback_data=f"select_user_{user_id}"
            )
            keyboard.append([action_button, select_button])
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ صفحه قبل", callback_data=f"user_page_{group_id}_{page-1}")])
        if page < total_pages:
            keyboard.append([InlineKeyboardButton("➡️ صفحه بعد", callback_data=f"user_page_{group_id}_{page+1}")])
        keyboard.append([
            InlineKeyboardButton("🚫 فقط مسدود", callback_data="filter_banned"),
            InlineKeyboardButton("✅ فقط فعال", callback_data="filter_unbanned"),
            InlineKeyboardButton("👥 همه", callback_data="filter_all")
        ])
        selected_users = context.user_data.get('selected_users', set())
        if selected_users:
            keyboard.append([InlineKeyboardButton(f"👤 انتخاب‌شده: {len(selected_users)}", callback_data="noop")])
            keyboard.extend(create_bulk_action_keyboard().inline_keyboard)
        else:
            keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Users management menu sent: group_id=%s, page=%s", group_id, page)
        return MANAGE_USERS
    except sqlite3.OperationalError as db_error:
        logger.error(f"Database error in manage_users: {str(db_error)}")
        try:
            await query.message.edit_text(MESSAGES["error_database"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_database"])
        context.user_data.clear()
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in manage_users: {str(e)}", exc_info=True)
        try:
            await query.message.edit_text(MESSAGES["error_generic"])
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def select_group_for_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        text = update.message.text.strip()
        if not re.match(r"^-?\d+$", text):
            await update.message.reply_text(MESSAGES["invalid_group_id"])
            return MANAGE_USERS
        group_id = int(text)
        if not await fetch_one("SELECT 1 FROM groups WHERE group_id = ?", (group_id,)):
            await update.message.reply_text(MESSAGES["group_not_found"])
            return MANAGE_USERS
        context.user_data['user_group_id'] = group_id
        context.user_data['user_page'] = 1
        context.user_data['user_filter'] = 'all'
        context.user_data['selected_users'] = set()
        context.user_data['previous_state'] = MANAGE_USERS
        logger.info("Group selected for user management: group_id=%s", group_id)
        return await manage_users(update, context)
    except sqlite3.OperationalError as db_error:
        logger.error(f"Database error in select_group_for_users: {str(db_error)}")
        await update.message.reply_text(MESSAGES["error_database"])
        context.user_data.clear()
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in select_group_for_users: {str(e)}", exc_info=True)
        await update.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@log_function_call
async def search_users_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        search_term = update.message.text.strip()
        query_str = """
            SELECT user_id, group_id, username, first_name, total_ayat, total_salavat, total_zekr
            FROM users
            WHERE user_id = ? OR username LIKE ?
        """
        try:
            users = await fetch_all(query_str, (search_term if search_term.isdigit() else 0, f"%{search_term}%"))
        except sqlite3.OperationalError as db_error:
            logger.error(f"Database error in search_users_handler: {str(db_error)}")
            await update.message.reply_text(MESSAGES["error_database"])
            context.user_data.clear()
            return DASHBOARD_MAIN
        if not users:
            await update.message.reply_text(MESSAGES["no_users_found"])
            return await back_to_previous(update, context)
        message = "<b>🔍 نتایج جستجوی کاربران</b>\n\n"
        keyboard = []
        for user in users:
            user_id = user["user_id"]
            banned = await is_user_banned(user_id)
            banned_status = "🚫 مسدود" if banned else "✅ فعال"
            username = user.get("username", "بدون نام کاربری")
            first_name = user.get("first_name", "بدون نام")
            total_ayat = user.get("total_ayat", 0)
            total_salavat = user.get("total_salavat", 0)
            total_zekr = user.get("total_zekr", 0)
            message += (
                f"کاربر: <b>{user_id}</b> ({first_name}, @{username})\n"
                f"گروه: {user['group_id']}\n"
                f"وضعیت: {banned_status}\n"
                f"📖 آیات: {total_ayat} | 🙏 صلوات: {total_salavat} | 📿 ذکر: {total_zekr}\n\n"
            )
            action_button = InlineKeyboardButton(
                "رفع مسدودیت" if banned else "مسدود کردن",
                callback_data=f"{'unban' if banned else 'ban'}_user_{user_id}"
            )
            select_button = InlineKeyboardButton(
                "✅ انتخاب",
                callback_data=f"select_user_{user_id}"
            )
            keyboard.append([action_button, select_button])
        selected_users = context.user_data.get('selected_users', set())
        if selected_users:
            keyboard.append([InlineKeyboardButton(f"👤 انتخاب‌شده: {len(selected_users)}", callback_data="noop")])
            keyboard.extend(create_bulk_action_keyboard().inline_keyboard)
        else:
            keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_previous")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
        logger.info("Search users results sent: found=%s", len(users))
        return SEARCH_USERS
    except Exception as e:
        logger.error(f"Error in search_users_handler: {str(e)}", exc_info=True)
        await update.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return DASHBOARD_MAIN

@ignore_old_messages()
@log_function_call
async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    try:
        context.user_data.clear()
        reply_markup = create_main_menu()
        logger.info("Attempting to edit message to return to main dashboard menu")
        try:
            await query.message.edit_text(
                "<b>🎛 داشبورد مدیریت</b>\nلطفاً یک گزینه را انتخاب کنید:",
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
            logger.info("Successfully edited message to return to main dashboard menu")
        except (BadRequest, Forbidden) as api_error:
            logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
            await query.message.reply_text(
                MESSAGES["edit_message_failed"],
                parse_mode="HTML"
            )
            await query.message.reply_text(
                "<b>🎛 داشبورد مدیریت</b>\nلطفاً یک گزینه را انتخاب کنید:",
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
            logger.info("Sent new message for main dashboard menu")
        return DASHBOARD_MAIN
    except Exception as e:
        logger.error(f"Error in back_to_main: {str(e)}", exc_info=True)
        await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return ConversationHandler.END

@ignore_old_messages()
@log_function_call
async def back_to_previous(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    try:
        previous_state = context.user_data.get('previous_state', DASHBOARD_MAIN)
        logger.info("Returning to previous state: %s", previous_state)
        if previous_state == MANAGE_USERS:
            return await manage_users(update, context)
        elif previous_state == VIEW_GROUPS_PAGINATED:
            return await view_groups(update, context)
        elif previous_state == MANAGE_BANNED_GROUPS:
            return await manage_banned_groups(update, context)
        elif previous_state == DASHBOARD_MAIN:
            context.user_data.clear()
            reply_markup = create_main_menu()
            try:
                await query.message.edit_text(
                    "<b>🎛 داشبورد مدیریت</b>\nلطفاً یک گزینه را انتخاب کنید:",
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
                logger.info("Returned to main dashboard menu")
            except (BadRequest, Forbidden) as api_error:
                logger.warning(f"Failed to edit message: {str(api_error)}. Sending new message.")
                await query.message.reply_text(
                    MESSAGES["edit_message_failed"],
                    parse_mode="HTML"
                )
                await query.message.reply_text(
                    "<b>🎛 داشبورد مدیریت</b>\nلطفاً یک گزینه را انتخاب کنید:",
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
                logger.info("Sent new message for main dashboard menu")
            return DASHBOARD_MAIN
        else:
            return await back_to_main(update, context)
    except Exception as e:
        logger.error(f"Error in back_to_previous: {str(e)}", exc_info=True)
        await query.message.reply_text(MESSAGES["error_generic"])
        context.user_data.clear()
        return ConversationHandler.END

def setup_dashboard_handlers():
    try:
        return [
            ConversationHandler(
                entry_points=[CommandHandler("dashboard", dashboard_command)],
                states={
                    DASHBOARD_MAIN: [CallbackQueryHandler(dashboard_callback)],
                    MANAGE_BANNED_GROUPS: [
                        CallbackQueryHandler(dashboard_callback, pattern="^(ban_group_|unban_group_)"),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ],
                    VIEW_GROUPS_PAGINATED: [
                        CallbackQueryHandler(dashboard_callback, pattern="^(page_|set_link_|remove_link_|generate_link_)"),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ],
                    SEARCH_GROUPS: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, search_groups_handler),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ],
                    VIEW_MONITORING: [CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")],
                    MANAGE_USERS: [
                        CallbackQueryHandler(dashboard_callback, pattern="^(ban_user_|unban_user_|user_page_|select_user_|bulk_ban|bulk_unban|clear_selection|filter_)"),
                        MessageHandler(filters.TEXT & ~filters.COMMAND, select_group_for_users),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ],
                    SET_GROUP_LINK: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, set_group_link_handler),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ],
                    SEARCH_USERS: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, search_users_handler),
                        CallbackQueryHandler(dashboard_callback, pattern="^(ban_user_|unban_user_|select_user_|bulk_ban|bulk_unban|clear_selection)"),
                        CallbackQueryHandler(back_to_previous, pattern="^back_to_previous$")
                    ]
                },
                fallbacks=[
                    CommandHandler("cancel", lambda update, context: ConversationHandler.END),
                    CommandHandler("dashboard", dashboard_command)
                ],
                per_message=False,
            )
        ]
    except Exception as e:
        logger.error(f"Error setting up dashboard handlers: {str(e)}", exc_info=True)
        raise