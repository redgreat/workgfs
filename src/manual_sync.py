from sync_handler import sync_task
from loguru import logger

if __name__ == '__main__':
    logger.info('[MANUAL] Manual sync triggered via manual_sync.py')
    try:
        sync_task()
        logger.info('[MANUAL] Manual sync completed successfully.')
    except Exception as e:
        logger.exception(f'[MANUAL] Manual sync failed: {e}')
