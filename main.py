#!/usr/bin/env python3
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
import signal
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.log_config import logger
from loop import clean_failed_batches, retry_failed_batches, process_batch
from flask import Flask, jsonify

# Initialize Flask app
app = Flask(__name__)

@app.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    return jsonify({"status": "healthy"}), 200

# Emergency exit handler - will force exit immediately
def emergency_exit(signum, frame):
    print("\nEmergency exit triggered. Terminating immediately...")
    os._exit(1)  # Force exit without cleanup


# Register emergency exit for Ctrl+\
signal.signal(signal.SIGQUIT, emergency_exit)


# Load environment variables
load_dotenv()


def setup_logging():
    """Configure logging for the application"""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    log_dir = os.getenv("LOG_DIR", "logs")
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except:
            log_dir = "."
    
    log_file = os.path.join(log_dir, 'email_processor.log')
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )
    logger.info(f"Logging initialized at {log_level} level")


def get_env_int(key: str, default: int) -> int:
    """Safely get integer from environment variable."""
    value = os.getenv(key, str(default))
    # Remove any comments and whitespace
    value = value.split('#')[0].strip()
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid value for {key}: '{value}'. Using default: {default}")
        return default


def main():
    """Main entry point for the email processing application"""
    # Set flag to track if shutdown is requested
    shutdown_requested = False
    
    # Define signal handler for graceful shutdown
    def signal_handler(sig, frame):
        nonlocal shutdown_requested
        signal_name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        shutdown_requested = True
        
        # Set a timeout to force exit if graceful shutdown takes too long
        def force_exit():
            logger.warning("Graceful shutdown is taking too long. Forcing exit...")
            os._exit(1)
            
        # Schedule force exit after 5 seconds
        signal.alarm(5)
    
    # Register handlers for SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register SIGALRM to force exit
    signal.signal(signal.SIGALRM, lambda sig, frame: os._exit(1))
    
    # Initialize logging
    setup_logging()
    
    logger.info("=== Email Processing System Starting ===")
    logger.info("Settings:")
    logger.info(f"- MAIL_SEND_ENABLED: {os.getenv('MAIL_SEND_ENABLED', 'False')}")
    logger.info(f"- FORCE_DRAFTS: {os.getenv('FORCE_DRAFTS', 'False')}")
    logger.info(f"- SFTP_ENABLED: {os.getenv('SFTP_ENABLED', 'False')}")
    logger.info(f"- BATCH_SIZE: {get_env_int('BATCH_SIZE', 20)}")
    logger.info(f"- BATCH_INTERVAL: {get_env_int('BATCH_INTERVAL', 3600)} seconds")
    
    # Start Flask in a separate thread
    from threading import Thread
    flask_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=5000))
    flask_thread.daemon = True
    flask_thread.start()
    
    try:
        # Clean up existing failed batches
        if not shutdown_requested:
            clean_failed_batches()
        
        # Retry any failed batches
        if not shutdown_requested:
            retry_failed_batches()
        
        # Start the main processing loop
        if not shutdown_requested:
            logger.info("Starting main email processing loop")
            
            # Get batch interval from environment or use default
            batch_interval = get_env_int('BATCH_INTERVAL', 300)
            
            while not shutdown_requested:
                # Process a batch
                start_time = datetime.now()
                logger.info(f"Starting batch at {start_time.isoformat()}")
                
                # Run a single batch
                try:
                    # Run a single batch if not shutting down
                    if not shutdown_requested:
                        process_batch()
                        
                    # Calculate time until next batch
                    elapsed = (datetime.now() - start_time).total_seconds()
                    wait_time = max(0, batch_interval - elapsed)
                    
                    # Log time until next batch
                    if not shutdown_requested:
                        logger.info(f"Batch complete. Next batch in {wait_time:.1f} seconds")
                    
                    # Wait until next batch, checking for shutdown frequently
                    for _ in range(int(wait_time)):
                        if shutdown_requested:
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}", exc_info=True)
                    # Wait a bit but still check for shutdown
                    for _ in range(min(60, batch_interval)):
                        if shutdown_requested:
                            break
                        time.sleep(1)
                
                # Exit the loop if shutdown was requested
                if shutdown_requested:
                    break
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Email Processing System Shutdown ===")
        # Force exit after logging shutdown message
        os._exit(0)


if __name__ == "__main__":
    main()