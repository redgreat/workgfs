import sys
from sync_handler import sync_task
from loguru import logger

if __name__ == '__main__':
    # 支持命令行参数传递日期区间
    # 用法: python manual_sync.py 2024-03-01 2024-04-01
    start_date = None
    end_date = None
    if len(sys.argv) == 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    logger.info(f'[MANUAL] Manual sync triggered via manual_sync.py, start_date={start_date}, end_date={end_date}')
    try:
        sync_task(start_date, end_date)
        logger.info('[MANUAL] Manual sync completed successfully.')
    except Exception as e:
        logger.exception(f'[MANUAL] Manual sync failed: {e}')
