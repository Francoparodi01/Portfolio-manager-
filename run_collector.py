# run_collector.py
import os
import logging
from dotenv import load_dotenv
from src.collector.collector_service import CollectorService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    load_dotenv()
    
    # Database
    database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/cocos_inversiones')
    
    # Paths
    raw_data_path = os.getenv('RAW_DATA_PATH', 'data/raw')
    
    # Cocos
    cocos_email = os.getenv('COCOS_EMAIL')
    cocos_password = os.getenv('COCOS_PASSWORD')
    
    # Telegram
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    # Options
    headless = os.getenv('HEADLESS', 'false').lower() == 'true'
    
    if not cocos_email or not cocos_password:
        logger.error("COCOS_EMAIL y COCOS_PASSWORD requeridos")
        return False
    
    collector = CollectorService(
        database_url=database_url,
        raw_data_path=raw_data_path,
        cocos_email=cocos_email,
        cocos_password=cocos_password,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        headless=headless
    )
    
    return collector.collect_snapshot()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)