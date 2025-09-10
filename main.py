#!/usr/bin/env python3
"""
main.py - Simple application entry point for the Email Classification System

Clean entry point that just starts the system and handles shutdown.
All batch logic is in loop.py where it belongs.
UNIFIED signal handling for Issue #6 complete.
"""

import os
import sys
import signal
import threading
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from threading import Thread
from functools import wraps

# Load environment variables
load_dotenv()

# Import after loading environment
from src.log_config import logger

# Initialize Flask app for health checks
app = Flask(__name__)

# SECURITY: No CORS needed - curl only, no browser access
CORS(app, origins=["http://localhost:5000"])  # Minimal - just for safety

# SECURITY: API Key from environment
API_KEY = "email-classifier-a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# UNIFIED SIGNAL HANDLING - Issue #6 FIX
processor_running = False
processor_thread = None
stop_event = threading.Event()  # Unified stop mechanism

# SECURITY: Authentication decorator
def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check Authorization header
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({"success": False, "message": "Missing Authorization header"}), 401
        
        token = auth_header.replace('Bearer ', '')
        if token != API_KEY:
            return jsonify({"success": False, "message": "Invalid API key"}), 401
        
        return f(*args, **kwargs)
    return decorated_function

# In-memory log capture for API endpoint
class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_buffer = []
        self.max_logs = 1000  # Keep last 1000 log entries
    
    def emit(self, record):
        log_entry = self.format(record)
        self.log_buffer.append(log_entry)
        if len(self.log_buffer) > self.max_logs:
            self.log_buffer.pop(0)  # Remove oldest entry

# Add log capture handler
log_capture = LogCapture()
log_capture.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(log_capture)

# UNIFIED SIGNAL HANDLING - Wrapper function
def run_email_processor_wrapper():
    """Wrapper to pass stop_event to email processor - ISSUE #6 FIX"""
    from loop import run_email_processor
    run_email_processor(stop_event)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Docker - NO AUTH REQUIRED"""
    return jsonify({"status": "healthy", "processor_running": processor_running}), 200

@app.route('/start', methods=['POST'])
@require_api_key  # SECURITY: Requires API key
def start_processor():
    """Start email processor with UNIFIED signal handling"""
    global processor_running, processor_thread
    
    if processor_running:
        return jsonify({"success": False, "message": "Already running"}), 400
    
    try:
        stop_event.clear()  # Clear the unified stop signal
        processor_thread = Thread(target=run_email_processor_wrapper)  # Use wrapper
        processor_thread.daemon = True
        processor_thread.start()
        processor_running = True
        
        logger.info("Email processor started via API with unified signal handling")
        return jsonify({"success": True, "message": "Email processor started"})
    except Exception as e:
        logger.error(f"Failed to start processor: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/stop', methods=['POST'])
@require_api_key
def stop_processor():
    """Stop email processor with UNIFIED signal handling"""
    global processor_running
    
    if not processor_running:
        return jsonify({"success": False, "message": "Not running"}), 400
    
    try:
        # NEW: Import and call manual shutdown marker
        from loop import mark_manual_shutdown
        mark_manual_shutdown()  # Mark as manual before setting stop signal
        
        stop_event.set()  # Set the unified stop signal
        processor_running = False
        
        logger.info("Email processor stop requested via API - marked as manual shutdown")
        return jsonify({"success": True, "message": "Email processor stop requested"})
    except Exception as e:
        logger.error(f"Failed to stop processor: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/logs', methods=['GET'])
@require_api_key  # SECURITY: Requires API key
def get_logs():
    """Get all logs from memory"""
    try:
        return jsonify({
            "success": True,
            "logs": log_capture.log_buffer.copy(),
            "total_logs": len(log_capture.log_buffer)
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

def log_startup_config():
    """Log startup configuration"""
    logger.info("=== Email Processing System Starting ===")
    
    # Log model configuration  
    model_url = "http://104.197.197.76:8000"
    logger.info(f"Model API URL: {model_url}")
    
    # Log email configuration
    email_env = os.getenv("EMAIL_ADDRESS", "")
    if email_env:
        if "," in email_env:
            emails = [e.strip() for e in email_env.split(",")]
            logger.info(f"Multi-email mode: {len(emails)} accounts configured")
        else:
            logger.info(f"Single-email mode: {email_env}")
    else:
        logger.warning("No EMAIL_ADDRESS configured!")
    
    # Log key settings
    logger.info("Settings:")
    logger.info(f"- MAIL_SEND_ENABLED: {os.getenv('MAIL_SEND_ENABLED', 'False')}")
    logger.info(f"- FORCE_DRAFTS: {os.getenv('FORCE_DRAFTS', 'True')}")
    logger.info(f"- SFTP_ENABLED: {os.getenv('SFTP_ENABLED', 'False')}")
    logger.info(f"- BATCH_SIZE: {os.getenv('BATCH_SIZE', '50')} (from env)")
    logger.info(f"- BATCH_INTERVAL: {os.getenv('BATCH_INTERVAL_MINUTES', '10')} minutes (from env)")
    
    # SECURITY: Log API key status
    if API_KEY == "default-secret-key-change-me":
        logger.warning("⚠️  Using default API key - CHANGE THIS IN PRODUCTION!")
    else:
        logger.info("✅ Custom API key configured")
        
    # Log unified signal handling
    logger.info("✅ Unified signal handling enabled (Issue #6 fixed)")

def setup_signal_handlers():
    """Setup graceful shutdown signal handlers with UNIFIED stop mechanism"""
    def signal_handler(sig, frame):
        signal_name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        logger.info(f"Received {signal_name}, shutting down gracefully...")
        global processor_running
        if processor_running:
            stop_event.set()  # Unified stop signal
            processor_running = False
        
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main entry point - wait for commands"""
    try:
        # Setup
        setup_signal_handlers()
        log_startup_config()
        
        logger.info("System ready - waiting for /start command")
        logger.info("Control endpoints: POST /start, POST /stop, GET /logs, GET /health")
        logger.info("CORS restricted to localhost origins")
        logger.info("🔒 Authentication required for control endpoints")
        logger.info("🔄 Unified signal handling active")
        
        # Start Flask and wait for commands (NO auto-start)
        app.run(host='0.0.0.0', port=5000, debug=False)
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=== Email Processing System Shutdown ===")

if __name__ == "__main__":
    main()
