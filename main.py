#!/usr/bin/env python3
"""
main.py - Simple application entry point for the Email Classification System

Clean entry point that just starts the system and handles shutdown.
All batch logic is in loop.py where it belongs.
"""

import os
import sys
import signal
import threading
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS  # Add this import
from threading import Thread

# Load environment variables
load_dotenv()

# Import after loading environment
from src.log_config import logger
from loop import run_email_processor

# Initialize Flask app for health checks
app = Flask(__name__)

# Enable CORS for all routes - THIS FIXES THE ERROR
CORS(app, origins="*")  # Allow all origins, or specify your domain

# Simple global state for processor control
processor_running = False
processor_thread = None
stop_event = threading.Event()

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

@app.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    return jsonify({"status": "healthy", "processor_running": processor_running}), 200

@app.route('/start')
@app.route('/start/<int:batch_size>')
def start_processor(batch_size=None):
    """Start email processor with optional batch size"""
    global processor_running, processor_thread
    
    # Check if thread is actually running
    if processor_thread and processor_thread.is_alive():
        return jsonify({"success": False, "message": "Already running"}), 400
    
    try:
        # Clean up stop signal file if it exists
        try:
            os.remove("/tmp/stop_email_processor")
        except:
            pass
        
        # Set batch size in environment if provided
        if batch_size:
            os.environ['RUNTIME_BATCH_SIZE'] = str(batch_size)
            logger.info(f"Starting processor with custom batch size: {batch_size}")
        else:
            # Remove any existing runtime batch size
            if 'RUNTIME_BATCH_SIZE' in os.environ:
                del os.environ['RUNTIME_BATCH_SIZE']
            
        stop_event.clear()
        processor_thread = Thread(target=run_email_processor)
        processor_thread.daemon = True
        processor_thread.start()
        processor_running = True
        
        effective_batch_size = batch_size or int(os.getenv("BATCH_SIZE", 120))
        return jsonify({
            "success": True, 
            "message": f"Email processor started with batch size: {effective_batch_size}"
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/stop')
def stop_processor():
    """Stop email processor"""
    global processor_running
    
    # Check if thread is actually running
    if not processor_thread or not processor_thread.is_alive():
        processor_running = False  # Sync the flag
        return jsonify({"success": False, "message": "Not running"}), 400
    
    try:
        # Create stop signal file
        with open("/tmp/stop_email_processor", "w") as f:
            f.write("stop")
        
        processor_running = False
        logger.info("Stop signal sent to email processor")
        
        return jsonify({"success": True, "message": "Stop signal sent - processor will stop after current batch"})
        
    except Exception as e:
        logger.error(f"Error stopping processor: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/status')
def get_status():
    """Get actual processor status"""
    thread_alive = processor_thread and processor_thread.is_alive()
    return jsonify({
        "processor_running_flag": processor_running,
        "thread_actually_running": thread_alive,
        "stop_file_exists": os.path.exists("/tmp/stop_email_processor")
    })
    
@app.route('/logs')
def get_logs():
    """Get all logs from memory with proper line breaks"""
    try:
        # Return logs as plain text with proper line breaks instead of JSON array
        formatted_logs = "\n".join(log_capture.log_buffer)
        return formatted_logs, 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

def log_startup_config():
    """Log startup configuration"""
    logger.info("=== Email Processing System Starting ===")
    
    # Log model configuration  
    model_url = os.getenv("MODEL_API_URL", "http://34.72.113.155:8000")
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
    
    # Log key settings - CORRECT VALUES from .env
    batch_size = os.getenv("BATCH_SIZE", "50")
    batch_interval = os.getenv("BATCH_INTERVAL_MINUTES", "10")
    
    logger.info("Settings:")
    logger.info(f"- MAIL_SEND_ENABLED: {os.getenv('MAIL_SEND_ENABLED', 'False')}")
    logger.info(f"- FORCE_DRAFTS: {os.getenv('FORCE_DRAFTS', 'True')}")
    logger.info(f"- SFTP_ENABLED: {os.getenv('SFTP_ENABLED', 'False')}")
    logger.info(f"- BATCH_SIZE: {batch_size}")
    logger.info(f"- BATCH_INTERVAL: {batch_interval} minutes")

def setup_signal_handlers():
    """Setup graceful shutdown signal handlers"""
    def signal_handler(sig, frame):
        signal_name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        logger.info(f"Received {signal_name}, shutting down gracefully...")
        global processor_running
        if processor_running:
            stop_event.set()
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
        logger.info("Control endpoints: /start /stop /logs /health")
        logger.info("CORS enabled for cross-origin requests")
        
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