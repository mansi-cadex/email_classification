"""
loop.py - Batch processing and orchestration for email classification system

Updated based on test code approach:
1. Orchestration and scheduling
2. Excel export using model data  
3. Database batch management
4. No email_sender dependencies (replies handled in fetch_reply.py)
5. Simple draft counting from MongoDB
"""

import os
import sys
import time
import uuid
import io
import pandas as pd
import requests 
import paramiko 
from paramiko import SSHClient
from scp import SCPClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Tuple, Dict, List, Optional, Any

# Import from refactored modules
from src.fetch_reply import process_unread_emails
from src.db import get_mongo, get_postgres, PostgresConnector
from src.log_config import logger

# Load environment variables
load_dotenv()

# Configuration - Hardcoded settings (no DevOps dependency)
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "yes", "1"]
FORCE_DRAFTS = os.getenv("FORCE_DRAFTS", "True").lower() in ["true", "yes", "1"]

# SFTP Configuration
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_ENABLED = os.getenv("SFTP_ENABLED", "False").lower() in ["true", "yes", "1"]

def check_model_health() -> bool:
    """Check if model API is available and responding"""
    try:
        model_url = "http://34.26.80.201:8000"
        response = requests.get(f"{model_url}/api/health", timeout=10)
        if response.status_code == 200:
            return True
        else:
            logger.warning(f"Model health check failed: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.warning(f"Model health check failed: {str(e)}")
        return False
    except Exception as e:
        logger.warning(f"Model health check error: {str(e)}")
        return False

def get_batch_size():
    """Get batch size from runtime override or .env"""
    runtime_size = os.getenv("RUNTIME_BATCH_SIZE")
    if runtime_size:
        return int(runtime_size)
    return int(os.getenv("BATCH_SIZE", 50))

def get_batch_interval():
    """Get batch interval in seconds (convert minutes to seconds)"""
    minutes = int(os.getenv("BATCH_INTERVAL_MINUTES", 10))
    return minutes * 60  # Convert to seconds

def wait_for_model_recovery():
    """Wait for model to come back online"""
    logger.info("Waiting for model to recover...")
    
    while True:
        if check_model_health():
            logger.info("Model recovered - resuming processing")
            break
        else:
            logger.info("Model still down - checking again in 60 seconds")
            time.sleep(60)

def check_incomplete_batch():
    """Check if there's an incomplete batch to resume"""
    mongo = get_mongo()
    if not mongo:
        return None
        
    # Find batch that has emails but isn't marked as complete
    pipeline = [
        {
            "$group": {
                "_id": "$batch_id",
                "total_emails": {"$sum": 1},
                "batch_complete": {"$first": "$batch_complete"}
            }
        },
        {
            "$match": {
                "_id": {"$ne": None},
                "batch_complete": {"$ne": True}
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 1}
    ]
    
    result = list(mongo.collection.aggregate(pipeline))
    if result:
        batch_id = result[0]["_id"]
        email_count = result[0]["total_emails"]
        logger.info(f"Found incomplete batch {batch_id} with {email_count} emails")
        return batch_id
        
    return None

def mark_batch_complete(batch_id: str):
    """Mark a batch as complete for Excel generation"""
    mongo = get_mongo()
    if mongo:
        # Mark all emails in this batch as batch_complete
        result = mongo.collection.update_many(
            {"batch_id": batch_id},
            {"$set": {"batch_complete": True}}
        )
        logger.info(f"Marked batch {batch_id} complete: {result.modified_count} emails")

def get_batch_email_count(batch_id: str) -> int:
    """Get current email count in batch"""
    mongo = get_mongo()
    if mongo:
        count = mongo.collection.count_documents({"batch_id": batch_id})
        return count
    return 0

def count_drafts_created(batch_id: str) -> int:
    """Count how many drafts were created for this batch - like test code"""
    mongo = get_mongo()
    if mongo:
        draft_count = mongo.collection.count_documents({
            "batch_id": batch_id,
            "draft_created": True
        })
        return draft_count
    return 0

def should_continue_batch(batch_id: str, target_size: int = 120) -> bool:
    """Check if batch should continue or is complete"""
    current_count = get_batch_email_count(batch_id)
    
    # If we have target_size emails, batch is full
    if current_count >= target_size:
        logger.info(f"Batch {batch_id} is full: {current_count}/{target_size} emails")
        mark_batch_complete(batch_id)
        return False
    
    logger.info(f"Batch {batch_id} continuing: {current_count}/{target_size} emails")
    return True

def ensure_batch_record_exists(batch_id: str) -> bool:
    """Ensure batch record exists in both PostgreSQL and MongoDB."""
    if not batch_id:
        return False
        
    # Check PostgreSQL
    pg_conn = get_postgres()
    if not pg_conn:
        return False
        
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM core.batch_runs WHERE id = %s", (batch_id,))
            if cur.fetchone() is None:
                # Insert if not exists
                cur.execute(
                    """
                    INSERT INTO core.batch_runs (id, status, created_at, response_processed, mail_sent)
                    VALUES (%s, %s, NOW(), %s, %s)
                    """,
                    (batch_id, "in_progress", False, False)
                )
                pg_conn.commit()
                logger.info(f"Created missing batch record in PostgreSQL: {batch_id}")
    finally:
        if pg_conn:
            PostgresConnector.return_connection(pg_conn)
    
    # Check MongoDB
    mongo = get_mongo()
    if not mongo:
        return False
        
    batch = mongo.batch_runs_collection.find_one({"id": batch_id})
    if not batch:
        mongo.batch_runs_collection.insert_one({
            "id": batch_id,
            "status": "in_progress",
            "created_at": datetime.utcnow(),
            "retry_count": 0,
            "processed_count": 0,
            "failed_count": 0,
            "draft_count": 0,
            "permanently_failed": False
        })
        logger.info(f"Created missing batch record in MongoDB: {batch_id}")
        
    return True

def update_batch_id_only(batch_id, email_data=None):
    """Insert a record with batch_id and real email data in PostgreSQL."""
    if not batch_id:
        return 0

    conn = None
    try:
        conn = get_postgres()
        if not conn:
            return 0
            
        conn.autocommit = True
        with conn.cursor() as cur:
            # Use real email data when available
            if email_data and isinstance(email_data, dict):
                to_email = email_data.get('recipient', '')
                from_email = email_data.get('sender', '')
                email_subject = email_data.get('subject', '')
                is_sent = email_data.get('is_sent', False)
                debtor_number = email_data.get('debtor_number', '')
                debtor_id = email_data.get('debtor_id', None)
            else:
                # If no email_data is provided, use minimal values
                to_email = ''
                from_email = 'system@abc-amega.com'
                email_subject = ''
                is_sent = False
                debtor_number = ''
                debtor_id = None
            
            # Insert with appropriate values including model data
            cur.execute(
                """
                INSERT INTO core.account_email
                       (batch_id, to_email, from_email, email_subject, is_sent, debtor_number, debtor_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (batch_id, to_email, from_email, email_subject, is_sent, debtor_number, debtor_id)
            )
            
            logger.info(f"Successfully inserted batch tracking record: batch_id={batch_id}")
            return 1
            
    except Exception as exc:
        logger.error(f"Error in update_batch_id_only: {str(exc)}")
        return 0
    finally:
        if conn:
            PostgresConnector.return_connection(conn)

def upload_to_sftp(filename: str, file_content: Optional[bytes] = None, 
                  max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Upload file to SFTP server with stop signal checks."""
    if not SFTP_ENABLED:
        logger.info(f"SFTP disabled - skipping upload of {filename}")
        return False
    
    # ✅ CHECK STOP BEFORE SFTP
    if os.path.exists("/tmp/stop_email_processor"):
        logger.info("IMMEDIATE STOP: Stop signal detected before SFTP upload - stopping NOW")
        return False
    
    # Create unique filename to prevent overwrites
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uuid_str = uuid.uuid4().hex[:8]
    base_name, extension = os.path.splitext(filename)
    remote_name = f"{base_name}_{timestamp}_{uuid_str}{extension}"
    
    logger.info(f"Generated unique filename: {remote_name}")
    
    # Create temp file if content is provided
    temp_filename = None
    if file_content:
        temp_filename = f"/tmp/{remote_name}"
        with open(temp_filename, "wb") as f:
            f.write(file_content)
    
    retries = 0
    while retries < max_retries:
        # ✅ CHECK STOP DURING SFTP RETRIES
        if os.path.exists("/tmp/stop_email_processor"):
            logger.info("IMMEDIATE STOP: Stop signal detected during SFTP retry - stopping NOW")
            # Clean up temp file
            if temp_filename and os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False
        
        ssh = None
        sftp = None
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            logger.info(f"Connecting to SFTP server {SFTP_HOST}:{SFTP_PORT}")
            ssh.connect(
                SFTP_HOST,
                port=SFTP_PORT,
                username=SFTP_USERNAME,
                password=SFTP_PASSWORD,
                timeout=60,
                look_for_keys=False,
                allow_agent=False
            )
            
            # ✅ CHECK STOP AFTER SFTP CONNECTION
            if os.path.exists("/tmp/stop_email_processor"):
                logger.info("IMMEDIATE STOP: Stop signal detected during SFTP connection - stopping NOW")
                if temp_filename and os.path.exists(temp_filename):
                    os.remove(temp_filename)
                return False
            
            # Try SFTP first
            try:
                transport = ssh.get_transport()
                transport.set_keepalive(30)
                sftp = transport.open_sftp_client()
                
                source_path = temp_filename if temp_filename else filename
                logger.info(f"Uploading file {filename} to {remote_name} via SFTP")
                sftp.put(source_path, remote_name)
                logger.info(f"Successfully uploaded {filename} to SFTP server as {remote_name}")
                
                # Clean up temp file
                if temp_filename and os.path.exists(temp_filename):
                    os.remove(temp_filename)
                    
                return True
                
            except Exception as sftp_err:
                logger.info(f"SFTP failed: {str(sftp_err)}, trying SCP")
                # Fall back to SCP
                with SCPClient(ssh.get_transport(), socket_timeout=60) as scp:
                    source_path = temp_filename if temp_filename else filename
                    scp.put(source_path, remote_name)
                    logger.info(f"Successfully uploaded {filename} via SCP")
                    
                    if temp_filename and os.path.exists(temp_filename):
                        os.remove(temp_filename)
                        
                    return True
                
        except paramiko.AuthenticationException as e:
            logger.error(f"Authentication failed: {str(e)}")
            if temp_filename and os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False
            
        except Exception as e:
            logger.info(f"SFTP upload error (attempt {retries+1}/{max_retries}): {str(e)}")
            
        finally:
            if sftp:
                sftp.close()
            if ssh:
                ssh.close()
        
        retries += 1
        if retries < max_retries:
            current_delay = retry_delay * (2 ** (retries - 1))
            logger.info(f"Retrying SFTP upload in {current_delay} seconds...")
            
            # ✅ CHECK STOP DURING SFTP RETRY DELAY
            for i in range(current_delay):
                if os.path.exists("/tmp/stop_email_processor"):
                    logger.info("IMMEDIATE STOP: Stop signal detected during SFTP retry delay - stopping NOW")
                    if temp_filename and os.path.exists(temp_filename):
                        os.remove(temp_filename)
                    return False
                time.sleep(1)
    
    # Clean up temp file if it still exists
    if temp_filename and os.path.exists(temp_filename):
        os.remove(temp_filename)
            
    logger.info(f"Failed to upload {filename} to SFTP after {max_retries} attempts")
    return False

def export_processed_emails_to_excel(batch_id: str) -> Optional[str]:
    """Export processed emails to Excel using MODEL DATA - like test code."""
    if not batch_id:
        return None

    mongo = get_mongo()
    if not mongo:
        return None
        
    emails = list(mongo.collection.find({"batch_id": batch_id}))
    
    if not emails:
        logger.info(f"No emails for batch {batch_id} to export")
        return None

    if not SFTP_ENABLED:
        logger.info("SFTP disabled - Excel report not generated")
        return None

    rows = []
    
    for e in emails:
        # Get email body and truncate if too long for Excel
        email_body = e.get("body", "") or e.get("text", "")
        if len(email_body) > 32767:
            email_body = email_body[:32764] + "..."
        
        # Get cleaned body from model (like test code)
        cleaned_body = e.get("cleaned_body", "")
        if len(cleaned_body) > 32767:
            cleaned_body = cleaned_body[:32764] + "..."
        
        # Get RAW values directly
        event_type_raw = e.get("event_type", "")
        target_folder_raw = e.get("target_folder", "")
        reply_sent = e.get("reply_sent", "no_response")
        
        row = {
            # CLIENT DATA - Basic email metadata
            "EmailFrom": e.get("sender", ""),
            "EmailTo": e.get("recipient", e.get("to", "")),
            "SubjectLine": e.get("subject", ""),
            "Date": e.get("received_at", e.get("date", "")),
            "Body": email_body,
            
            # MODEL DATA - RAW format like test code
            "Event Type": event_type_raw,
            "TargetFolder": target_folder_raw,
            "ReplySent": reply_sent,
            "CleanedBody": cleaned_body,  # ✅ ADD CLEANED BODY like test code
            "PrimaryFileNumber": e.get("debtor_number", ""),
            "NewContactEmail": e.get("new_contact_email", ""),
            "NewContactPhone": e.get("new_contact_phone", ""),
            "ContactStatus": e.get("contact_status", "active")
        }
        
        rows.append(row)

    df = pd.DataFrame(rows)
    
    # Column order - 13 columns (added CleanedBody like test code)
    cols = [
        "EmailFrom", "EmailTo", "SubjectLine", "Date",
        "Event Type", "TargetFolder", "ReplySent", "Body", "CleanedBody",
        "PrimaryFileNumber", "NewContactEmail", "NewContactPhone", "ContactStatus"
    ]
    
    # Ensure all columns exist
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    
    df = df[cols]
    
    # Generate filename
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_suffix = batch_id[-8:] if batch_id else uuid.uuid4().hex[:8]
    fname = f"AI_Agent_Data_Load_{ts}_{batch_suffix}.xlsx"

    # Write to Excel
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    
    logger.info(f"Excel export complete: {len(emails)} emails processed for batch {batch_id} (13 columns)")
    
    # Upload to SFTP
    upload_success = upload_to_sftp(fname, buf.getvalue())
    
    if upload_success:
        logger.info(f"Excel file exported and uploaded: {fname}")
    
    return fname if upload_success else None

def process_batch(batch_id: Optional[str] = None) -> Tuple[bool, int, int, int]:
    """Process one batch of emails - simplified without email_sender dependencies."""
    start_time = time.time()
    
    # Get current batch size
    batch_size = get_batch_size()
    
    # Generate batch ID if not provided
    if not batch_id:
        batch_id = PostgresConnector.insert_batch_run()
        if not batch_id:
            return False, 0, 0, 0
    
    logger.info(f"Processing batch {batch_id} (max {batch_size} emails)")
    logger.info(f"Mail sending is {'ENABLED' if MAIL_SEND_ENABLED else 'DISABLED'}, Force drafts is {'ENABLED' if FORCE_DRAFTS else 'DISABLED'}")

    # Ensure batch record exists
    ensure_batch_record_exists(batch_id)

    # ✅ Process emails (replies are created during processing in fetch_reply.py)
    email_result = process_unread_emails(batch_id, batch_size)
    
    if not email_result["success"]:
        # Update batch status to failed
        mongo = get_mongo()
        if mongo:
            mongo.update_batch_result(
                batch_id, 
                email_result.get("emails_processed", 0), 
                email_result.get("emails_errored", 0), 
                0,
                "failed"
            )
        
        PostgresConnector.update_batch_result(
            batch_id,
            email_result.get("emails_processed", 0),
            email_result.get("emails_errored", 0),
            "failed"
        )
        
        return False, email_result.get("emails_processed", 0), email_result.get("emails_errored", 0), 0

    # Store processing results
    processed_count = email_result.get("emails_processed", 0)
    failed_count = email_result.get("emails_errored", 0)
    batch_id = email_result.get("batch_id", batch_id)

    # ✅ Count drafts created during processing - like test code
    draft_count = count_drafts_created(batch_id)
    logger.info(f"Drafts created during processing: {draft_count}")
    
    # ✅ No separate email sending step - it happens during processing
    
    # Calculate totals
    total_processed = processed_count
    total_failed = failed_count
    total_draft_count = draft_count
    
    # Determine status
    status = "success" if total_failed == 0 else "partial"
    
    # Update batch status in both databases
    ensure_batch_record_exists(batch_id)
    
    PostgresConnector.update_batch_result(
        batch_id, 
        total_processed, 
        total_failed, 
        status, 
        total_draft_count
    )
    
    mongo = get_mongo()
    if mongo:
        mongo.update_batch_result(
            batch_id, 
            total_processed, 
            total_failed, 
            total_draft_count, 
            status
        )

    # Update tracking record for reporting
    if processed_count > 0:
        mongo = get_mongo()
        emails = list(mongo.collection.find({"batch_id": batch_id}).limit(1))
        email_data = emails[0] if emails else None
        
        update_batch_id_only(batch_id, email_data)
        logger.info(f"Inserted batch tracking record with batch_id={batch_id}")

    # Log completion
    elapsed = time.time() - start_time
    logger.info(f"Batch {batch_id} completed in {elapsed:.1f}s: {total_processed} processed, "
               f"{total_failed} failed, {total_draft_count} drafts created")
    
    return True, total_processed, total_failed, total_draft_count

def run_batch_processor() -> bool:
    """Run a single batch processing cycle with resumption logic."""
    logger.info(f"Starting batch processor run")
    
    # Get current batch size
    batch_size = get_batch_size()
    
    # Check for incomplete batch first
    batch_id = check_incomplete_batch()
    
    if batch_id:
        logger.info(f"Resuming incomplete batch: {batch_id}")
        current_count = get_batch_email_count(batch_id)
        logger.info(f"Current batch has {current_count} emails, targeting {batch_size}")
        
        # Check if this batch should continue
        if not should_continue_batch(batch_id, batch_size):
            logger.info(f"Batch {batch_id} is complete, starting new batch")
            batch_id = PostgresConnector.insert_batch_run()
    else:        
        logger.info("No incomplete batch found, starting new batch")
        batch_id = PostgresConnector.insert_batch_run()
    
    if not batch_id:
        return False
   
    logger.info(f"Processing with batch ID: {batch_id}")
   
    ensure_batch_record_exists(batch_id)
   
    success, processed_count, failed_count, draft_count = process_batch(batch_id)
    
    if success:
        # Always check total count and handle appropriately
        total_count = get_batch_email_count(batch_id)
        
        if total_count > 0:
            # ✅ Generate Excel for ANY number of emails (1, 5, 15, 30, 120, etc.)
            logger.info(f"Batch {batch_id} has {total_count} emails - generating Excel")
            mark_batch_complete(batch_id)
            
            # Generate Excel for ANY number of emails (including just 1 email)
            excel_file = export_processed_emails_to_excel(batch_id)
            if excel_file:
                logger.info(f"Excel file generated: {excel_file}")
            else:
                logger.info(f"Excel generation completed (SFTP disabled or failed)")
        else:
            # No emails processed - still mark as complete
            logger.info(f"Batch {batch_id} has no emails - marking complete, no Excel needed")
            mark_batch_complete(batch_id)
   
    return success

def run_email_processor():
    """Main batch processing loop with IMMEDIATE stop support."""
    # Get current settings
    batch_size = get_batch_size()
    batch_interval = get_batch_interval()

    logger.info(f"Starting email batch processor (batch size: {batch_size}, interval: {batch_interval}s)")
    logger.info(f"Mail sending is {'ENABLED' if MAIL_SEND_ENABLED else 'DISABLED'}")
    logger.info(f"Force drafts is {'ENABLED' if FORCE_DRAFTS else 'DISABLED'}")
    logger.info(f"SFTP export is {'ENABLED' if SFTP_ENABLED else 'DISABLED'}")
    
    model_url = os.getenv("MODEL_API_URL", "http://localhost:8000")
    logger.info(f"Model API URL: {model_url}")
    
    consecutive_failures = 0
    max_failures = 3
   
    while True:
        # ✅ CHECK STOP SIGNAL EVERY ITERATION
        if os.path.exists("/tmp/stop_email_processor"):
            logger.info("IMMEDIATE STOP: Stop signal detected - shutting down processor NOW")
            try:
                os.remove("/tmp/stop_email_processor")
            except:
                pass
            break
            
        start_time = datetime.now()
        logger.info(f"Starting batch at {start_time.isoformat()}")
       
        try:
            # Check model health before processing
            if not check_model_health():
                logger.info("Model is not available")
                consecutive_failures += 1
                
                if consecutive_failures >= max_failures:
                    logger.info(f"Model down for {max_failures} consecutive attempts - waiting for recovery")
                    wait_for_model_recovery()
                    consecutive_failures = 0
                else:
                    logger.info(f"Waiting 60 seconds before retry (failure {consecutive_failures}/{max_failures})")
                    
                    # ✅ CHECK STOP DURING WAIT
                    for i in range(60):
                        if os.path.exists("/tmp/stop_email_processor"):
                            logger.info("IMMEDIATE STOP: Stop signal detected during wait - shutting down NOW")
                            try:
                                os.remove("/tmp/stop_email_processor")
                            except:
                                pass
                            return
                        time.sleep(1)
                continue
            
            # Reset failure counter on successful health check
            consecutive_failures = 0
            
            # Run main batch processor
            run_batch_processor()
           
            # Calculate time to next batch
            elapsed = (datetime.now() - start_time).total_seconds()
            wait_time = max(0, batch_interval - elapsed)
           
            logger.info(f"Batch complete. Next batch in {wait_time:.1f} seconds")
            
            # ✅ CHECK STOP DURING BATCH INTERVAL WAIT
            for i in range(int(wait_time)):
                if os.path.exists("/tmp/stop_email_processor"):
                    logger.info("IMMEDIATE STOP: Stop signal detected during batch interval - shutting down NOW")
                    try:
                        os.remove("/tmp/stop_email_processor")
                    except:
                        pass
                    return
                time.sleep(1)
            
            # Handle remaining fractional seconds
            remaining = wait_time - int(wait_time)
            if remaining > 0:
                time.sleep(remaining)
            
        except KeyboardInterrupt:
            logger.info("Batch processor interrupted by user")
            break
        except Exception as e:
            logger.exception(f"Unhandled error in batch processor: {str(e)}")
            consecutive_failures += 1
            logger.info(f"Waiting 60 seconds before retry due to error")
            
            # ✅ CHECK STOP DURING ERROR WAIT
            for i in range(60):
                if os.path.exists("/tmp/stop_email_processor"):
                    logger.info("IMMEDIATE STOP: Stop signal detected during error wait - shutting down NOW")
                    try:
                        os.remove("/tmp/stop_email_processor")
                    except:
                        pass
                    return
                time.sleep(1)
    
    logger.info("Email processor stopped IMMEDIATELY")

if __name__ == "__main__":
    try:
        run_email_processor()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Unhandled exception in main:")
        sys.exit(1)