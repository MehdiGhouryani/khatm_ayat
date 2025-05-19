from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.database.db import fetch_all, execute
from bot.services.hadith_service import get_random_hadith
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def start_scheduler(app):
    try:
        scheduler = AsyncIOScheduler()

        @scheduler.scheduled_job("cron", hour=0, minute=0)
        async def daily_reset():
            try:
                groups = await fetch_all("SELECT group_id FROM groups WHERE reset_daily = 1")
                for group in groups:
                    await execute(
                        "UPDATE topics SET current_total = 0 WHERE group_id = ?",
                        (group["group_id"],)
                    )
                logger.info(f"Daily reset completed: {len(groups)} groups")
            except Exception as e:
                logger.error(f"Error in daily_reset: {e}")

        @scheduler.scheduled_job("cron", hour=8, minute=0)
        async def daily_hadith():
            try:
                groups = await fetch_all("SELECT group_id FROM hadith_settings WHERE hadith_enabled = 1")
                for group in groups:
                    hadith_text = await get_random_hadith(app)
                    if hadith_text:
                        await app.bot.send_message(chat_id=group["group_id"], text=hadith_text)
                        logger.info(f"Hadith sent to group_id={group['group_id']}")
            except Exception as e:
                logger.error(f"Error in daily_hadith: {e}")

        @scheduler.scheduled_job("interval", minutes=1)
        async def check_time_off():
            try:
                now = datetime.now().strftime("%H:%M")
                groups = await fetch_all(
                    "SELECT group_id, time_off_start, time_off_end FROM groups WHERE time_off_start != '' AND time_off_end != ''"
                )
                for group in groups:
                    start = group["time_off_start"]
                    end = group["time_off_end"]
                    is_off = False
                    if start <= end:
                        is_off = start <= now <= end
                    else:
                        is_off = now >= start or now <= end
                    await execute(
                        "UPDATE groups SET is_active = ? WHERE group_id = ?",
                        (0 if is_off else 1, group["group_id"])
                    )
                logger.debug(f"Time-off checked: {len(groups)} groups processed")
            except Exception as e:
                logger.error(f"Error in check_time_off: {e}")

        scheduler.start()
        logger.info("Scheduler initialized")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        raise