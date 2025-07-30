import os
import logging

# Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# Get the logger
logger = logging.getLogger("email_processor")

# Only configure if not already configured
if not logger.handlers:
    # Set level
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    
    # Console handler only
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)
    
    # Don't propagate to root
    logger.propagate = False
    
    # Silence noisy libraries
    for lib in ["requests", "urllib3", "werkzeug"]:
        logging.getLogger(lib).setLevel(logging.WARNING)