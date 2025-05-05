# client_side/main.py
import os
import time
import logging
from dotenv import load_dotenv
from src.log_config import logger
from loop import run_email_processor, clean_failed_batches, retry_failed_batches

# Load environment variables
load_dotenv()

def setup_logging():
    """Configure logging for the application"""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('email_processor.log')
        ]
    )
    logger.info(f"Logging initialized at {log_level} level")

def main():
    """Main entry point for the email processing application"""
    setup_logging()
    
    logger.info("=== Email Processing System Starting ===")
    logger.info("Settings:")
    logger.info(f"- MAIL_SEND_ENABLED: {os.getenv('MAIL_SEND_ENABLED', 'False')}")
    logger.info(f"- SFTP_ENABLED: {os.getenv('SFTP_ENABLED', 'True')}")
    logger.info(f"- BATCH_SIZE: {os.getenv('BATCH_SIZE', '125')}")
    logger.info(f"- BATCH_INTERVAL: {os.getenv('BATCH_INTERVAL', '600')} seconds")
    
    try:
        # Clean up existing failed batches
        clean_failed_batches()
        
        # Retry any failed batches
        retry_failed_batches()
        
        # Start the main processing loop
        logger.info("Starting main email processing loop")
        run_email_processor()
    except KeyboardInterrupt:
        logger.info("System shutdown requested (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Email Processing System Shutdown ===")

if __name__ == "__main__":
    main()