import os
import logging
import logging.handlers
from datetime import datetime

# Configuration
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 5  # Keep 5 backup files

# Safe level parsing
level_name = os.getenv("LOG_LEVEL", "INFO").upper()
if not hasattr(logging, level_name):
    print(f"Unknown LOG_LEVEL '{level_name}', defaulting to INFO")
    level_name = "INFO"
LOG_LEVEL = level_name

# Define a SPECIFIC log directory path - update this to your desired location
CUSTOM_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")

# Ensure directory exists
try:
    os.makedirs(CUSTOM_LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(CUSTOM_LOG_DIR, f"email_processor_{datetime.now().strftime('%Y%m%d')}.log")
    log_file_enabled = True
except (PermissionError, OSError):
    # Fall back to /tmp if available
    try:
        tmp_dir = "/tmp"
        os.makedirs(tmp_dir, exist_ok=True)
        LOG_FILE = os.path.join(tmp_dir, f"email_processor_{datetime.now().strftime('%Y%m%d')}.log")
        log_file_enabled = True
    except (PermissionError, OSError):
        # Last resort: disable file logging entirely
        LOG_FILE = None
        log_file_enabled = False
        print(f"WARNING: Unable to create log directory. File logging disabled. Will log to stdout only.")

# Get the logger
logger = logging.getLogger("email_processor")

# Configure httpx logger specifically
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(getattr(logging, LOG_LEVEL))

# Only configure if handlers aren't already set up
if not logger.handlers:
    # Create formatter with ISO-8601 compatible timestamp format
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%dT%H:%M:%S%z')

    # Create console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure main logger
    logger.setLevel(getattr(logging, LOG_LEVEL))
    logger.addHandler(console_handler)
    
    # Configure httpx logger with the same handlers
    httpx_logger.handlers = []
    httpx_logger.addHandler(console_handler)

    # Add file handler if logging to file is enabled
    if log_file_enabled and LOG_FILE:
        try:
            # Create file handler for all logs
            file_handler = logging.handlers.RotatingFileHandler(
                LOG_FILE,
                maxBytes=MAX_LOG_SIZE,
                backupCount=BACKUP_COUNT
            )
            file_handler.setFormatter(formatter)
            
            # Add to both loggers
            logger.addHandler(file_handler)
            httpx_logger.addHandler(file_handler)
            
            # Only log this if DEBUG level is enabled
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"File logging enabled to: {LOG_FILE}")
        except (PermissionError, OSError) as e:
            logger.warning(f"Failed to set up file logging to {LOG_FILE}: {str(e)}")
            logger.warning("Continuing with console logging only")

    # Prevent logs from propagating to the root logger
    logger.propagate = False
    httpx_logger.propagate = False

    # Silence other noisy libraries
    for lib_logger in ["requests", "urllib3", "werkzeug"]:
        logging.getLogger(lib_logger).setLevel(logging.WARNING)

    # Only log this if DEBUG level is enabled
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Logger initialized")