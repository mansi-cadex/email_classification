"""
main.py - Application entry point for the Email Classification System

This module serves as the main entry point for the email classification system.
It handles:
1. Initializing logging
2. Parsing command-line arguments
3. Starting the main email processing loop
4. Graceful shutdown and error reporting
"""

import os
import sys
import time
import argparse
import logging
import signal
from typing import Dict, Any
from dotenv import load_dotenv

# Import from refactored modules
from src.log_config import logger
from loop import run_email_processor, clean_failed_batches, retry_failed_batches, process_batch

# Load environment variables
load_dotenv()

# Global flag for graceful shutdown
SHUTDOWN_REQUESTED = False

def setup_logging():
    """Configure logging for the application."""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    # Determine log file location
    log_dir = os.getenv("LOG_DIR", "logs")
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            print(f"Could not create log directory {log_dir}: {str(e)}")
            log_dir = "."
    
    log_file = os.path.join(log_dir, 'email_processor.log')
    
    try:
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_file)
            ]
        )
        
        # Set third-party loggers to a higher level to reduce noise
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        
        logger.info(f"Logging initialized at {log_level} level to {log_file}")
    except Exception as e:
        print(f"Error setting up logging: {str(e)}")
        sys.exit(1)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Email Classification System")
    
    # Mode selection
    parser.add_argument(
        "--mode", "-m",
        choices=["daemon", "single", "retry", "cleanup"],
        default="daemon",
        help="Operation mode - daemon (continuous), single (one batch), retry (failed batches), cleanup (mark failed batches)"
    )
    
    # Batch ID for single batch processing
    parser.add_argument(
        "--batch-id", "-b",
        help="Process a specific batch ID (only used with single mode)"
    )
    
    # Override environment variables
    parser.add_argument(
        "--batch-size", "-s",
        type=int,
        help="Override batch size from environment variable"
    )
    
    parser.add_argument(
        "--interval", "-i",
        type=int,
        help="Override batch interval in seconds from environment variable"
    )
    
    parser.add_argument(
        "--send-mail",
        action="store_true",
        help="Enable mail sending regardless of environment setting"
    )
    
    parser.add_argument(
        "--force-drafts",
        action="store_true",
        help="Force all emails to be saved as drafts regardless of environment setting"
    )
    
    # Parse the arguments
    args = parser.parse_args()
    
    # Update environment variables if arguments are provided
    if args.batch_size:
        os.environ["BATCH_SIZE"] = str(args.batch_size)
    
    if args.interval:
        os.environ["BATCH_INTERVAL"] = str(args.interval)
    
    if args.send_mail:
        os.environ["MAIL_SEND_ENABLED"] = "True"
    
    if args.force_drafts:
        os.environ["FORCE_DRAFTS"] = "True"
    
    return args


def handle_signal(sig, frame):
    """Handle termination signals for graceful shutdown."""
    global SHUTDOWN_REQUESTED
    
    signal_names = {
        signal.SIGINT: "SIGINT",
        signal.SIGTERM: "SIGTERM"
    }
    
    signal_name = signal_names.get(sig, f"Signal {sig}")
    logger.info(f"Received {signal_name}, initiating graceful shutdown...")
    
    SHUTDOWN_REQUESTED = True


def register_signal_handlers():
    """Register signal handlers for graceful shutdown."""
    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        logger.info("Signal handlers registered for graceful shutdown")
    except (AttributeError, ValueError) as e:
        # This can happen on some systems where signals are not supported
        logger.warning(f"Could not register signal handlers: {str(e)}")


def get_environment_settings() -> Dict[str, Any]:
    """Get important environment settings for logging."""
    return {
        "MAIL_SEND_ENABLED": os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "yes", "1"],
        "FORCE_DRAFTS": os.getenv("FORCE_DRAFTS", "False").lower() in ["true", "yes", "1"],
        "SFTP_ENABLED": os.getenv("SFTP_ENABLED", "False").lower() in ["true", "yes", "1"],
        "BATCH_SIZE": int(os.getenv("BATCH_SIZE", "125")),
        "BATCH_INTERVAL": int(os.getenv("BATCH_INTERVAL", "600")),
        "MODEL_API_URL": os.getenv("MODEL_API_URL", "http://localhost:8000")
    }


def run_daemon_mode():
    """Run the system in continuous daemon mode."""
    logger.info("Starting daemon mode - continuous processing")
    
    # Register signal handlers for graceful shutdown
    register_signal_handlers()
    
    # First clean up and retry existing batches
    clean_failed_batches()
    retry_failed_batches()
    
    # Start the main processing loop
    logger.info("Starting main email processing loop")
    
    try:
        run_email_processor()
    except Exception as e:
        logger.error(f"Error in daemon mode: {str(e)}", exc_info=True)
        return False
    
    return True


def run_single_batch(batch_id=None):
    """Run a single batch processing cycle."""
    logger.info(f"Processing single batch{' with ID ' + batch_id if batch_id else ''}")
    
    try:
        success, processed, failed, drafts = process_batch(batch_id)
        
        logger.info(f"Batch processing completed: success={success}, processed={processed}, "
                   f"failed={failed}, drafts={drafts}")
        
        return success
    except Exception as e:
        logger.error(f"Error processing single batch: {str(e)}", exc_info=True)
        return False


def run_retry_mode():
    """Retry failed batches that aren't permanently failed."""
    logger.info("Running in retry mode - processing failed batches")
    
    try:
        success = retry_failed_batches()
        logger.info(f"Retry processing completed: success={success}")
        return success
    except Exception as e:
        logger.error(f"Error in retry mode: {str(e)}", exc_info=True)
        return False


def run_cleanup_mode():
    """Clean up failed batches by marking them permanently failed."""
    logger.info("Running in cleanup mode - marking failed batches as permanently failed")
    
    try:
        success = clean_failed_batches()
        logger.info(f"Cleanup completed: success={success}")
        return success
    except Exception as e:
        logger.error(f"Error in cleanup mode: {str(e)}", exc_info=True)
        return False


def main():
    """Main entry point for the email processing application."""
    # Initialize logging
    setup_logging()
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Get important environment settings
    settings = get_environment_settings()
    
    # Print startup banner
    logger.info("=== Email Classification System Starting ===")
    logger.info("Settings:")
    for key, value in settings.items():
        logger.info(f"- {key}: {value}")
    
    success = False
    
    try:
        # Execute appropriate mode
        if args.mode == "daemon":
            success = run_daemon_mode()
        elif args.mode == "single":
            success = run_single_batch(args.batch_id)
        elif args.mode == "retry":
            success = run_retry_mode()
        elif args.mode == "cleanup":
            success = run_cleanup_mode()
        else:
            logger.error(f"Unknown mode: {args.mode}")
            success = False
            
    except KeyboardInterrupt:
        logger.info("System shutdown requested (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Email Classification System Shutdown ===")
    
    # Return appropriate exit code
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())