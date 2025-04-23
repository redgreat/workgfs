import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
from sync_handler import sync_task
import os

def load_config():
    with open('../config/config.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)



def main():
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, 'sync.log')
    logger.remove()
    logger.add(log_path, rotation="10 MB", retention="10 days", encoding="utf-8", enqueue=True, backtrace=True, diagnose=True)
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    config = load_config()
    cron_expr = config.get('schedule_cron', '0 * * * *')
    cron_parts = cron_expr.split()
    minute = cron_parts[0] if len(cron_parts) > 0 else '0'
    hour = cron_parts[1] if len(cron_parts) > 1 else '*'
    day = cron_parts[2] if len(cron_parts) > 2 else '*'
    month = cron_parts[3] if len(cron_parts) > 3 else '*'
    day_of_week = cron_parts[4] if len(cron_parts) > 4 else '*'
    scheduler = BlockingScheduler()
    scheduler.add_job(
        sync_task,
        'cron',
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week
    )
    logger.info(f"Scheduler started. Sync will run at cron: {cron_expr}")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == '__main__':
    main()
