import os
import sys
import time
import uuid
import io
import socket
import pandas as pd
import paramiko
from paramiko import SSHClient, SFTPClient
from scp import SCPClient
import psycopg2
import re
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Set up Python path to find modules correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, "src"))

# Import modules directly
from src.fetch_reply import EmailProcessor
from src.log_config import logger
from src.db import get_mongo, PostgresHelper
from src.email_sender import send_email, save_as_draft

# Load environment variables
load_dotenv()

# Batch / retry tuning
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "125"))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", "600"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "60"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "900"))
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() == "true"

# SFTP
SFTP_HOST = os.getenv("SFTP_HOST", "sftp.abc-amega.com")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME", "Sanskar")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "Pl@ying&Create1886!")
SFTP_ENABLED = os.getenv("SFTP_ENABLED", "True").lower() == "true"

# PostgreSQL credentials
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "email_batch_test")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "0000")  # Make sure this is correct

# ── Helper functions ─────────────────────────────────────────────────────────

def send_email_with_retries(to_address, subject, body, retries=3, delay=30):
    """Retry wrapper around src.email_sender.send_email()"""
    for attempt in range(1, retries + 1):
        try:
            ok = send_email(to_address, subject, body)
            if ok:
                return True
            logger.warning(f"send_email retry {attempt}/{retries} failed – waiting {delay}s")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error in send_email attempt {attempt}: {str(e)}")
            time.sleep(delay)
    return False

# ── PG helper ─────────────────────────────────────────────────────────────────
def get_pg_connection():
    """Return a new psycopg2 connection using env vars, autocommit ON."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        conn.autocommit = True
        
        # Test the connection
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise

def ensure_batch_record_exists(batch_id):
    """Ensure the batch record exists in both PostgreSQL and MongoDB."""
    try:
        # Check PostgreSQL
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM core.batch_runs WHERE id = %s", (batch_id,))
                if cur.fetchone() is None:
                    # Insert if not exists
                    cur.execute(
                        "INSERT INTO core.batch_runs (id, status, created_at) VALUES (%s, %s, NOW())",
                        (batch_id, "in_progress")
                    )
                    logger.info(f"Created missing batch record in PostgreSQL: {batch_id}")
        
        # Check MongoDB
        mongo = get_mongo()
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
    except Exception as e:
        logger.error(f"Error ensuring batch record exists: {str(e)}")
        return False

def update_batch_id_only(batch_id, limit=1, email_data=None):
    """
    Insert a record with batch_id and either real email data or dummy values.
    In production, real email metadata will be populated dynamically when available.
    """
    if not batch_id:
        logger.warning("No batch_id provided to update_batch_id_only()")
        return 0

    conn = get_pg_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check if we have real email data or need to use dummy values
            if email_data and isinstance(email_data, dict):
                # Production: Use real email data when available
                to_email = email_data.get('to_email', email_data.get('recipient', email_data.get('sender', '')))
                email_subject = email_data.get('subject', email_data.get('email_subject', ''))
                is_sent = email_data.get('is_sent', False)
            else:
                # Development/Testing: Use dummy values to satisfy NOT NULL constraints
                to_email = ''
                email_subject = ''
                is_sent = False
                logger.debug("Using dummy values for NOT NULL constraints. In production, real email metadata will be used.")
            
            # Insert with appropriate values
            cur.execute(
                """
                INSERT INTO core.account_email (batch_id, to_email, email_subject, is_sent)
                VALUES (%s, %s, %s, %s)
                """,
                (batch_id, to_email, email_subject, is_sent)
            )
            
            logger.info(f"Successfully inserted batch_id={batch_id} into account_email")
            return 1
            
    except Exception as exc:
        logger.error(f"Error in update_batch_id_only: {exc}")
        return 0
    finally:
        if conn:
            conn.close()

# ── SFTP helper ───────────────────────────────────────────────────────────────
def upload_to_sftp(filename=None, file_content=None, max_retries=3, retry_delay=5):
    """
    Upload files to SFTP server with improved reliability and error handling.
    
    Args:
        filename (str): The name of the file to upload
        file_content (bytes, optional): File content to upload, if provided. Otherwise uploads existing file.
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 3.
        retry_delay (int, optional): Initial delay between retries in seconds. Doubles after each attempt. Defaults to 5.
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not SFTP_ENABLED:
        logger.info(f"SFTP disabled – skipping upload of {filename}")
        return False
    
    # Create a more distinctive unique name with timestamp + UUID to ensure no overwrites
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    uuid_str = uuid.uuid4().hex[:8]
    base_name, extension = os.path.splitext(filename)
    remote_name = f"{base_name}_{timestamp}_{uuid_str}{extension}"
    
    logger.info(f"Generated unique filename: {remote_name} to prevent overwrites")
    
    # Create temp file if content is provided
    temp_filename = None
    if file_content:
        temp_filename = f"/tmp/{remote_name}"  # Use unique name for temp file too
        logger.debug(f"Writing temporary file to {temp_filename}")
        with open(temp_filename, "wb") as f:
            f.write(file_content)
    
    retries = 0
    while retries < max_retries:
        ssh = None
        sftp = None
        try:
            # Use paramiko for direct SFTP instead of SCP
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect with longer timeouts
            logger.info(f"Connecting to SFTP server {SFTP_HOST}:{SFTP_PORT} with username {SFTP_USERNAME}")
            ssh.connect(
                SFTP_HOST,
                port=SFTP_PORT,
                username=SFTP_USERNAME,
                password=SFTP_PASSWORD,
                timeout=60,  # Longer connection timeout
                banner_timeout=60,
                auth_timeout=60,
                look_for_keys=False,  # Don't try to use SSH keys
                allow_agent=False  # Don't use SSH agent
            )
            
            # Try SFTP directly instead of SCP
            try:
                logger.info(f"Opening SFTP session for {filename}")
                transport = ssh.get_transport()
                transport.set_keepalive(30)  # Keep connection alive
                sftp = transport.open_sftp_client()
                
                source_path = temp_filename if temp_filename else filename
                logger.info(f"Uploading file {filename} to {remote_name} via SFTP")
                sftp.put(source_path, remote_name)
                logger.info(f"Successfully uploaded {filename} to SFTP server as {remote_name}")
                
                # Clean up temp file if it was created
                if temp_filename and os.path.exists(temp_filename):
                    logger.debug(f"Removing temporary file {temp_filename}")
                    os.remove(temp_filename)
                    
                return True
                
            except Exception as sftp_err:
                logger.warning(f"SFTP method failed: {str(sftp_err)}, attempting SCP method")
                # Fall back to SCP if SFTP doesn't work
                with SCPClient(ssh.get_transport(), socket_timeout=60) as scp:
                    source_path = temp_filename if temp_filename else filename
                    logger.info(f"Uploading file {filename} to {remote_name} via SCP (fallback)")
                    scp.put(source_path, remote_name)
                    logger.info(f"Successfully uploaded {filename} to SFTP server as {remote_name} via SCP")
                    
                    # Clean up temp file if it was created
                    if temp_filename and os.path.exists(temp_filename):
                        logger.debug(f"Removing temporary file {temp_filename}")
                        os.remove(temp_filename)
                        
                    return True
                
        except paramiko.AuthenticationException as e:
            logger.error(f"Authentication failed (attempt {retries+1}/{max_retries}): {str(e)}")
            # No retry for authentication errors
            if temp_filename and os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False
            
        except paramiko.SSHException as e:
            logger.error(f"SSH error during SFTP upload (attempt {retries+1}/{max_retries}): {str(e)}")
            
        except socket.timeout as e:
            logger.error(f"Socket timeout during SFTP operation (attempt {retries+1}/{max_retries}): {str(e)}")
            
        except FileNotFoundError as e:
            logger.error(f"File not found during SFTP upload (attempt {retries+1}/{max_retries}): {str(e)}")
            # No retry for file not found
            if temp_filename and os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False
            
        except IOError as e:
            logger.error(f"I/O error during SFTP upload (attempt {retries+1}/{max_retries}): {str(e)}")
            
        except Exception as e:
            logger.error(f"Unexpected error uploading {filename} to SFTP (attempt {retries+1}/{max_retries}): {str(e)}")
            
        finally:
            # Ensure connections are properly closed
            if sftp:
                try:
                    sftp.close()
                except:
                    pass
                    
            if ssh:
                try:
                    ssh.close()
                except:
                    pass
        
        # Increment retry counter and wait before retrying with exponential backoff
        retries += 1
        current_delay = retry_delay * (2 ** (retries - 1))  # Exponential backoff
        
        if retries < max_retries:
            logger.info(f"Retrying SFTP upload in {current_delay} seconds...")
            time.sleep(current_delay)
    
    # Clean up temp file if it still exists
    if temp_filename and os.path.exists(temp_filename):
        try:
            os.remove(temp_filename)
        except:
            pass
            
    logger.error(f"Failed to upload {filename} to SFTP after {max_retries} attempts")
    return False

def extract_contact_info(email_doc):
    """
    Extract contact information from email document metadata.
    In the decoupled architecture, entities are extracted by the model server.
    """
    contact_info = {"new_contact_email": "", "new_contact_name": "", "new_contact_phone": ""}
    
    # Use metadata from API response
    meta = email_doc.get("metadata", {})
    ents = meta.get("entities", {})
    
    # Extract emails, phones, and people from entities
    if ents.get("emails"):
        contact_info["new_contact_email"] = ents["emails"][0]
    if ents.get("phones"):
        contact_info["new_contact_phone"] = ents["phones"][0]
    if ents.get("people"):
        contact_info["new_contact_name"] = ents["people"][0]

    # Additional extraction from special cases
    # Left company info
    left_company = meta.get("left_company", {})
    if left_company:
        replacement = left_company.get("replacement", {})
        if replacement:
            # Email
            if replacement.get("email") and not contact_info["new_contact_email"]:
                contact_info["new_contact_email"] = replacement.get("email", "")
            # Phone
            if replacement.get("phone") and not contact_info["new_contact_phone"]:
                contact_info["new_contact_phone"] = replacement.get("phone", "")
            # Name
            if replacement.get("name") and not contact_info["new_contact_name"]:
                contact_info["new_contact_name"] = replacement.get("name", "")

    # Out of office info
    ooo = meta.get("out_of_office", {})
    if ooo:
        contact_person = ooo.get("contact_person", {})
        if contact_person:
            # Email
            if contact_person.get("email") and not contact_info["new_contact_email"]:
                contact_info["new_contact_email"] = contact_person.get("email", "")
            # Phone
            if contact_person.get("phone") and not contact_info["new_contact_phone"]:
                contact_info["new_contact_phone"] = contact_person.get("phone", "")
            # Name
            if contact_person.get("name") and not contact_info["new_contact_name"]:
                contact_info["new_contact_name"] = contact_person.get("name", "")

    return contact_info

def build_reply_summary(email_doc, contact_info):
    """
    Return a short status string for the ReplyText column.
    """
    label = email_doc.get("prediction", "")
    
    # 1-4: purely informational – no update
    if label in [
        "no_reply_no_info", "auto_reply_no_info",
        "no_reply_with_info", "auto_reply_with_info"
    ]:
        # Did we get any contact details?
        if contact_info["new_contact_email"] and contact_info["new_contact_phone"]:
            return "contact email & phone updated"
        elif contact_info["new_contact_email"]:
            return "new email"
        elif contact_info["new_contact_phone"]:
            return "new phone"
        else:
            return "no action"
    
    # Business-flow labels
    if label == "invoice_request_no_info":
        return "invoice info requested"
    if label == "claims_paid_no_proof":
        return "payment claim – awaiting proof"
    
    # Fallback
    return "manual review"

def extract_invoice_info(email_doc):
    """Extract invoice and payment information from email document."""
    invoice_info = {}
    
    # Initialize with empty values
    invoice_info["invoice_number"] = ""
    invoice_info["amount"] = ""
    invoice_info["due_date"] = ""
    invoice_info["payment_date"] = ""
    invoice_info["reference_number"] = ""
    
    # Try to extract from metadata
    metadata = email_doc.get("metadata", {})
    
    # Extract from entities
    entities = metadata.get("entities", {})
    
    # Get invoice numbers
    invoice_numbers = entities.get("invoice_numbers", [])
    if invoice_numbers and len(invoice_numbers) > 0:
        invoice_info["invoice_number"] = invoice_numbers[0]
    
    # Get amounts
    amounts = entities.get("amounts", [])
    if amounts and len(amounts) > 0:
        invoice_info["amount"] = amounts[0]
    
    # Get due dates
    due_dates = entities.get("due_dates", [])
    if due_dates and len(due_dates) > 0:
        invoice_info["due_date"] = due_dates[0]
    
    # Get payment dates
    payment_dates = entities.get("payment_dates", [])
    if payment_dates and len(payment_dates) > 0:
        invoice_info["payment_date"] = payment_dates[0]
    
    # Get reference numbers
    reference_numbers = entities.get("reference_numbers", [])
    if reference_numbers and len(reference_numbers) > 0:
        invoice_info["reference_number"] = reference_numbers[0]
    
    return invoice_info

# ── Excel export ─────────────────────────────────────────────────────────────
def export_processed_emails_to_excel(batch_id):
    mongo = get_mongo()
    emails = list(mongo.collection.find({"batch_id": batch_id}))
    
    if not emails:
        logger.info(f"No emails for batch {batch_id} to export")
        return None

    if not SFTP_ENABLED:
        logger.info("SFTP disabled – Excel not generated")
        return None

    rows = []
    for e in emails:
        # Extract contact information with improved contact extraction
        contact_info = extract_contact_info(e)
        
        # Determine reply status
        reply_sent = (
            "sent" if e.get("response_sent") is True
            else "draft" if e.get("save_as_draft") is True and e.get("draft_saved") is True
            else "manual_review"
        )
        
        # Get short reply summary
        short_reply = build_reply_summary(e, contact_info)
        
        # Create row with all information
        row = {
            "EmailFrom": e.get("sender", ""),
            "EmailTo": e.get("recipient", e.get("to", "")),
            "SubjectLine": e.get("subject", ""),
            "Date": e.get("received_at", e.get("date", "")),
            "Event Type": e.get("classification", e.get("prediction", e.get("event_type", ""))),
            "TargetFolder": e.get("target_folder", "") or e.get("classification", e.get("prediction", "")),
            "ReplySent": reply_sent,
            "ReplyText": short_reply,
            "NewContactEmail": contact_info.get("new_contact_email", ""),
            "NewContactPhone": contact_info.get("new_contact_phone", "")
        }
        
        rows.append(row)

    df = pd.DataFrame(rows)
    
    # Define all possible columns to ensure consistent output
    cols = [
        "EmailFrom", "EmailTo", "SubjectLine", "Date",
        "Event Type", "TargetFolder", "ReplySent", "ReplyText", 
        "NewContactEmail", "NewContactPhone"
    ]
    
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    
    # Ensure columns are in the right order
    df = df[cols]
    
    # Generate timestamp and filename with both timestamp and batch_id for uniqueness
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_suffix = batch_id[-8:] if batch_id else uuid.uuid4().hex[:8]
    fname = f"AI_Agent_Data_Load_{ts}_{batch_suffix}.xlsx"

    # Write to Excel
    try:
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        
        # Upload to SFTP
        upload_success = upload_to_sftp(fname, buf.getvalue())
        return fname if upload_success else None
    except Exception as e:
        logger.error(f"Error creating Excel file: {str(e)}")
        return None
        
def send_replies_if_enabled(batch_id):
    """Send replies for a batch if mail sending is enabled."""
    if not MAIL_SEND_ENABLED:
        logger.info(f"Mail sending is disabled. Skipping batch {batch_id}")
        return 0, 0
    
    flags = PostgresHelper.get_batch_flags(batch_id)
    if not flags or flags[2] == "failed" or not flags[0] or flags[1]:
        reason = "not found" if not flags else "failed" if flags[2] == "failed" else "responses not processed" if not flags[0] else "already sent"
        logger.info(f"Skipping mail send for batch {batch_id}: {reason}")
        return 0, 0
    
    # Get emails to send
    mongo = get_mongo()
    emails = list(mongo.collection.find({
        "batch_id": batch_id,
        "response_sent": False,
        "response": {"$exists": True, "$ne": ""},
        "save_as_draft": {"$ne": True}
    }))
    
    if not emails:
        logger.info(f"No emails to send for batch {batch_id}")
        return 0, 0
    
    logger.info(f"Found {len(emails)} emails to send for batch {batch_id}")
    
    sent_count = 0
    failed_count = 0
    
    for email in emails:
        success = send_email_with_retries(
            to_address=email.get("sender", ""),
            subject=f"Re: {email.get('subject', 'No Subject')}",
            body=email.get("response", "")
        )
        
        if success:
            mongo.mark_email_sent(email.get("message_id", ""))
            sent_count += 1
            logger.info(f"Reply sent for message_id {email.get('message_id', '')}")
        else:
            failed_count += 1
            logger.error(f"Failed to send reply for {email.get('message_id', '')}")
    
    # Mark batch as mail sent if any emails were sent successfully
    if sent_count > 0:
        PostgresHelper.mark_batch_mail_sent(batch_id)
        logger.info(f"Batch {batch_id} marked as mail_sent = true after sending {sent_count} replies")
    
    return sent_count, failed_count

def mark_permanently_failed(batch_id, reason):
    """Mark a batch as permanently failed in both MongoDB and PostgreSQL atomically."""
    try:
        # Update MongoDB
        mongo = get_mongo()
        mongo_result = mongo.mark_batch_permanently_failed(batch_id, reason)
        
        # Update PostgreSQL
        pg_result = PostgresHelper.mark_batch_permanently_failed(batch_id)
        
        if mongo_result and pg_result:
            logger.info(f"Batch {batch_id} marked as permanently failed in both MongoDB and PostgreSQL: {reason}")
            return True
        else:
            logger.warning(f"Batch {batch_id} not fully marked as permanently failed: MongoDB={mongo_result}, PostgreSQL={pg_result}")
            
            # Try to reconcile if one succeeded and the other failed
            if mongo_result and not pg_result:
                PostgresHelper.mark_batch_permanently_failed(batch_id)
            elif pg_result and not mongo_result:
                mongo.mark_batch_permanently_failed(batch_id, reason)
                
            return mongo_result or pg_result
    except Exception as e:
        logger.error(f"Error marking batch {batch_id} as permanently failed: {str(e)}")
        return False

# ── Batch processing ─────────────────────────────────────────────────────────
def process_batch(batch_id=None):
    """Process one batch and return (success, processed_count, failed_count, draft_count)."""
    start_time = time.time()
    logger.info(f"Processing batch {batch_id} (max {BATCH_SIZE})")

    # Make sure batch record exists in both PostgreSQL and MongoDB
    try:
        ensure_batch_record_exists(batch_id)
    except Exception as e:
        logger.error(f"Failed to ensure batch record exists: {str(e)}")
        return False, 0, 0, 0

    try:
        # Run the EmailProcessor with our enforced batch size
        processor = EmailProcessor()
        processor.batch_size = BATCH_SIZE
        processor.batch_id = batch_id

        # Save reference to original method
        original_process_emails = processor.process_unread_emails

        # Define limited process method with timeout check
        def limited_process():
            # Check for timeout during processing
            def check_timeout():
                elapsed = time.time() - start_time
                if elapsed > BATCH_TIMEOUT:
                    logger.warning(f"Batch {batch_id} processing is taking too long: {elapsed:.1f}s")
                    return True
                return False
                
            # Add timeout check to processor
            processor.check_timeout = check_timeout
            
            os.environ["BATCH_SIZE"] = str(BATCH_SIZE)
            return original_process_emails()

        processor.process_unread_emails = limited_process
        success, processed, failed, drafts = processor.process_unread_emails()

        if not success:
            logger.error(f"Batch {batch_id} failed: processed={processed}, failed={failed}, drafts={drafts}")
            
            # Ensure both PostgreSQL and MongoDB are updated atomically
            try:
                # Make sure batch record exists
                ensure_batch_record_exists(batch_id)
                
                PostgresHelper.update_batch_result(batch_id, processed, failed, "failed", draft_count=drafts)
                
                # Also update MongoDB
                mongo = get_mongo()
                mongo.update_batch_result(batch_id, processed, failed, drafts, "failed")
            except Exception as e:
                logger.error(f"Error updating batch status: {str(e)}")
                
            return False, processed, failed, drafts

        # Process drafts and get the count
        draft_proc, draft_fail = process_draft_emails(batch_id)
        total_draft_count = drafts + draft_proc

        # Update batch status and counts
        mongo = get_mongo()
        if processed > 0:
            # Check if any responses were generated
            has_responses = mongo.collection.count_documents({
                "batch_id": batch_id,
                "response": {"$exists": True, "$ne": ""}
            })
            
            # Mark as response_processed if responses were generated
            if has_responses > 0:
                PostgresHelper.mark_batch_response_processed(batch_id)

        status = "success" if failed == 0 else "partial"
        
        # Ensure both PostgreSQL and MongoDB are updated
        try:
            # Make sure batch record exists
            ensure_batch_record_exists(batch_id)
            
            PostgresHelper.update_batch_result(batch_id, processed, failed, status=status, draft_count=total_draft_count)
            
            # Also update MongoDB
            mongo.update_batch_result(batch_id, processed, failed, total_draft_count, status)
        except Exception as e:
            logger.error(f"Error updating batch result: {str(e)}")

        # Draft flagging when sending disabled
        if not MAIL_SEND_ENABLED and processed:
            try:
                # Only update emails in RESPONSE_LABELS categories
                response_update = mongo.collection.update_many(
                    {
                        "batch_id": batch_id, 
                        "response": {"$exists": True, "$ne": ""},
                        "prediction": {"$in": ["invoice_request_no_info", "claims_paid_no_proof"]}
                    },
                    {"$set": {"save_as_draft": True, "response_process": True}}
                )
                
                # Mark non-response emails separately
                non_response_update = mongo.collection.update_many(
                    {
                        "batch_id": batch_id, 
                        "prediction": {"$nin": ["invoice_request_no_info", "claims_paid_no_proof"]}
                    },
                    {"$set": {"save_as_draft": False, "response_process": False}}
                )
                
                logger.info(
                    f"Updated email flags: {response_update.modified_count} emails set for response, "
                    f"{non_response_update.modified_count} set as non-response"
                )
            except Exception as e:
                logger.error(f"Error updating email flags: {str(e)}")

        # Only update batch_id on existing records
        if processed > 0:
            try:
                # Get first email from the batch if available for real data
                emails = list(mongo.collection.find({"batch_id": batch_id}).limit(1))
                email_data = emails[0] if emails else None
                
                updated_records = update_batch_id_only(batch_id, processed, email_data)
                logger.info(f"Inserted batch tracking record with batch_id={batch_id}")
            except Exception as e:
                logger.error(f"Error updating batch ID: {str(e)}")
                
        # Send emails if enabled
        if processed:
            try:
                send_replies_if_enabled(batch_id)
            except Exception as e:
                logger.error(f"Error sending replies: {str(e)}")

        # Export to Excel and upload to SFTP
        try:
            excel_file = export_processed_emails_to_excel(batch_id)
            if excel_file:
                logger.info(f"Excel file exported and uploaded: {excel_file}")
        except Exception as e:
            logger.error(f"Error exporting and uploading Excel file: {str(e)}")

        # Log completion time
        elapsed = time.time() - start_time
        logger.info(f"Batch {batch_id} completed in {elapsed:.1f}s: {processed} processed, {failed} failed, {total_draft_count} drafts")
        
        return True, processed, failed, total_draft_count
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"Unhandled error in process_batch after {elapsed:.1f}s: {str(e)}")
        
        # Try to update batch status to failed
        try:
            PostgresHelper.update_batch_result(batch_id, 0, 1, "failed", draft_count=0)
            mongo = get_mongo()
            mongo.update_batch_result(batch_id, 0, 1, 0, "failed")
        except Exception as nested_e:
            logger.error(f"Error updating batch status after unhandled error: {str(nested_e)}")
            
        return False, 0, 1, 0

def process_draft_emails(batch_id):
   """Process emails that should be saved as drafts"""
   # Make sure batch record exists
   ensure_batch_record_exists(batch_id)
   
   mongo = get_mongo()
   # Fix: Improved query to find all draft emails, including those with response_process=True
   draft_emails = list(mongo.collection.find({
       "batch_id": batch_id,
       "response_sent": False,
       "save_as_draft": True,
       "$or": [
           {"response_process": False},
           {"draft_saved": {"$ne": True}}
       ]
   }))
   
   if not draft_emails:
       logger.info(f"No draft emails found for batch {batch_id}")
       return 0, 0
       
   logger.info(f"Found {len(draft_emails)} emails to save as drafts in batch {batch_id}")
   
   processed = 0
   failed = 0
   
   for email in draft_emails:
       try:
           # Skip if no response is set
           if not email.get("response"):
               logger.info(f"Skipping draft for email {email.get('message_id', '')} - no response set")
               continue
               
           draft_id = save_as_draft(
               to_address=email.get("sender", ""),
               subject=f"Re: {email.get('subject', 'No Subject')}",
               body=email.get("response", "")
           )
           
           if draft_id:
               mongo.mark_email_draft_saved(email.get("message_id", ""), draft_id)
               processed += 1
               logger.info(f"Saved draft for email {email.get('message_id', '')}")
           else:
               failed += 1
               logger.error(f"Failed to save draft for email {email.get('message_id', '')}")
       except Exception as e:
           logger.error(f"Error saving draft for email {email.get('message_id', '')}: {str(e)}")
           failed += 1
           
   logger.info(f"Draft processing complete: {processed} saved as drafts, {failed} failed")
   return processed, failed

def clean_failed_batches():
   """Mark existing failed batches as permanently failed"""
   logger.info("Cleaning up existing failed batches")
   
   try:
       # Get batch IDs of failed batches
       conn = get_pg_connection()
       cur = conn.cursor()
       cur.execute("""
           SELECT id FROM core.batch_runs 
           WHERE status = 'failed' 
           OR (status = 'in_progress' AND created_at < now() - interval '1 day')
       """)
       
       batch_ids = [row[0] for row in cur.fetchall()]
       
       if not batch_ids:
           logger.info("No failed batches to clean up")
           return True
           
       logger.info(f"Found {len(batch_ids)} failed batches to mark as permanently failed")
       
       # Mark each batch as permanently failed
       for batch_id in batch_ids:
           try:
               mark_permanently_failed(batch_id, "Marked as permanently failed during cleanup")
               logger.info(f"Batch {batch_id} marked as permanently failed")
           except Exception as e:
               logger.error(f"Error marking batch {batch_id} as permanently failed: {str(e)}")
               continue
       
       conn.commit()
       cur.close()
       logger.info(f"Cleanup complete: {len(batch_ids)} batches marked as permanently failed")
       return True
   except Exception as e:
       logger.error(f"Error in clean_failed_batches: {str(e)}")
       return False

def retry_failed_batches():
   """Retry emails from failed or timed-out batches with retry limit"""
   logger.info("Starting retry of failed batches")
   timeout_minutes = BATCH_TIMEOUT // 60
   failed_batches = PostgresHelper.get_failed_batches(timeout_minutes)
   
   if not failed_batches:
       logger.info("No failed batches to retry")
       return True
       
   logger.info(f"Found {len(failed_batches)} failed batches to retry")
   
   for batch in failed_batches:
       batch_id = batch.get('id', '')
       if not batch_id:
           logger.warning("Skipping batch with no ID")
           continue
           
       # Make sure batch record exists
       ensure_batch_record_exists(batch_id)
           
       # Check if batch has been retried too many times
       mongo = get_mongo()
       batch_record = mongo.batch_runs_collection.find_one({"id": batch_id})
       
       # Initialize or increment retry counter
       retry_count = 0
       if batch_record:
           retry_count = batch_record.get('retry_count', 0)
           
           # Skip if too many retries or marked as permanently failed
           if retry_count >= MAX_RETRIES or batch_record.get('permanently_failed', False):
               logger.info(f"Skipping batch {batch_id}: {'max retries reached' if retry_count >= MAX_RETRIES else 'permanently failed'}")
               
               # If reached max retries but not marked as permanently failed yet, do so now
               if retry_count >= MAX_RETRIES and not batch_record.get('permanently_failed', False):
                   mark_permanently_failed(batch_id, f"Max retries ({MAX_RETRIES}) reached")
               
               continue
           
           # Increment retry counter
           mongo.increment_batch_retry(batch_id)
       else:
           # Create batch record if it doesn't exist in MongoDB
           mongo.batch_runs_collection.insert_one({
               "id": batch_id,
               "retry_count": 1,
               "last_retry": datetime.utcnow(),
               "created_at": datetime.utcnow(),
               "status": batch.get('status', 'unknown')
           })
           
       logger.info(f"Retrying batch {batch_id} (retry #{retry_count + 1})")
       
       # If mail sending is disabled, set all emails to be saved as drafts
       if not MAIL_SEND_ENABLED:
           result = mongo.collection.update_many(
               {"batch_id": batch_id, "response": {"$exists": True, "$ne": ""}, "save_as_draft": {"$ne": True}},
               {"$set": {"save_as_draft": True, "response_process": True}}  # Also set response_process to True
           )
           logger.info(f"Set save_as_draft=True and response_process=True for {result.modified_count} emails in batch {batch_id}")
       
       # Process draft emails first
       draft_processed, draft_failed = process_draft_emails(batch_id)
       
       # Check if mail sending is enabled
       if MAIL_SEND_ENABLED:
           # Get regular emails that weren't sent
           unsent_emails = list(mongo.collection.find({
               "batch_id": batch_id,
               "response_sent": False,
               "response": {"$exists": True, "$ne": ""},
               "save_as_draft": {"$ne": True}
           }))
           
           if not unsent_emails and draft_processed == 0:
               logger.info(f"No unprocessed emails found for batch {batch_id}")
               
               # Update both PostgreSQL and MongoDB
               try:
                   PostgresHelper.update_batch_result(
                       batch_id, 
                       batch.get('processed_count', 0), 
                       0, 
                       status="success", 
                       draft_count=batch.get('draft_count', 0)
                   )
                   
                   mongo.update_batch_result(
                       batch_id, 
                       batch.get('processed_count', 0), 
                       0, 
                       batch.get('draft_count', 0), 
                       "success"
                   )
               except Exception as e:
                   logger.error(f"Error updating batch result: {str(e)}")
               
               # Mark as permanently failed if this was the last retry and still no emails
               if retry_count + 1 >= MAX_RETRIES:
                   mark_permanently_failed(batch_id, "No emails after max retries")
                   
               continue
               
           if unsent_emails:
               logger.info(f"Found {len(unsent_emails)} unsent emails in batch {batch_id}")
           
           # Process each unsent email
           processed = 0
           failed = 0
           
           for email in unsent_emails:
               success = send_email_with_retries(
                   to_address=email.get("sender", ""),
                   subject=f"Re: {email.get('subject', 'No Subject')}",
                   body=email.get("response", "")
               )
               
               if success:
                   mongo.mark_email_sent(email.get("message_id", ""))
                   processed += 1
               else:
                   failed += 1
           
           # Mark batch as mail sent if emails were sent
           if processed > 0:
               PostgresHelper.mark_batch_mail_sent(batch_id)
           
           status = "success" if failed == 0 and draft_failed == 0 else "partial"
           if failed > 0 and retry_count + 1 >= MAX_RETRIES:
               # On last retry with failures, mark as permanently failed
               status = "permanently_failed"
               mark_permanently_failed(batch_id, f"Failed to send {failed} emails after {MAX_RETRIES} retries")
           
           new_processed_count = batch.get('processed_count', 0) + processed
           new_failed_count = failed
           new_draft_count = batch.get('draft_count', 0) + draft_processed
           
           # Update both PostgreSQL and MongoDB
           try:
               PostgresHelper.update_batch_result(
                   batch_id, 
                   new_processed_count, 
                   new_failed_count, 
                   status=status, 
                   draft_count=new_draft_count
               )
               
               mongo.update_batch_result(
                   batch_id, 
                   new_processed_count, 
                   new_failed_count, 
                   new_draft_count, 
                   status
               )
           except Exception as e:
               logger.error(f"Error updating batch result: {str(e)}")
           
           logger.info(f"Batch {batch_id} retry complete: {processed} sent, {draft_processed} drafts, {failed+draft_failed} failed")
       else:
           logger.info(f"Mail sending disabled. Only draft emails processed for batch {batch_id}")
           
           # Mark as permanently failed if last retry and still has issues
           if retry_count + 1 >= MAX_RETRIES:
               mark_permanently_failed(batch_id, "Max retries reached with mail sending disabled")
       
       # Export the data to Excel and SFTP
       excel_file = export_processed_emails_to_excel(batch_id)
       if excel_file:
           logger.info(f"Excel export completed for retried batch {batch_id}: {excel_file}")
   
   return True

def run_batch_processor():
   """Main batch processing function with batch tracking"""
   logger.info(f"Starting new batch processor run")
   
   batch_id = PostgresHelper.insert_batch_run()
   if not batch_id:
       logger.error("Failed to create batch in PostgreSQL, creating UUID locally")
       batch_id = str(uuid.uuid4())
   
   logger.info(f"Created new batch with ID: {batch_id}")
   
   # Make sure batch record exists in both PostgreSQL and MongoDB
   ensure_batch_record_exists(batch_id)
   
   retry_count = 0
   success = False
   processed_count = 0
   failed_count = 0
   draft_count = 0
   
   while not success and retry_count < MAX_RETRIES:
       if retry_count > 0:
           logger.info(f"Retry attempt {retry_count+1}/{MAX_RETRIES} for batch {batch_id}")
           
       success, processed_count, failed_count, draft_count = process_batch(batch_id)
       
       if not success:
           retry_count += 1
           if retry_count < MAX_RETRIES:
               logger.warning(f"Retrying batch {batch_id} in {RETRY_DELAY} seconds...")
               time.sleep(RETRY_DELAY)
   
   if not success:
       logger.error(f"Batch {batch_id} failed after {MAX_RETRIES} attempts")
       
       # Update both PostgreSQL and MongoDB
       try:
           # Make sure batch record exists
           ensure_batch_record_exists(batch_id)
           
           PostgresHelper.update_batch_result(batch_id, processed_count, failed_count, 
                                          status="failed", draft_count=draft_count)
           
           # Also track retry count in MongoDB
           mongo = get_mongo()
           mongo.batch_runs_collection.update_one(
               {"id": batch_id},
               {"$set": {"retry_count": retry_count, "status": "failed"}},
               upsert=True
           )
       except Exception as e:
           logger.error(f"Error updating batch status: {str(e)}")
   
   return success

def run_email_processor():
   """Main batch processing loop with retry functionality"""
   logger.info(f"Starting email batch processor (batch size: {BATCH_SIZE}, interval: {BATCH_INTERVAL}s)")
   logger.info(f"Mail sending is {'ENABLED' if MAIL_SEND_ENABLED else 'DISABLED'}")
   logger.info(f"SFTP export is {'ENABLED' if SFTP_ENABLED else 'DISABLED'}")
   
   # Clean up existing failed batches on startup
   clean_failed_batches()
   
   while True:
       start_time = datetime.now()
       logger.info(f"Starting batch at {start_time.isoformat()}")
       
       # First, retry any failed batches that aren't permanently failed
       retry_failed_batches()
       
       # Then run the main batch processor
       run_batch_processor()
       
       # Calculate time to next batch
       elapsed = (datetime.now() - start_time).total_seconds()
       wait_time = max(0, BATCH_INTERVAL - elapsed)
       
       logger.info(f"Batch complete. Next batch in {wait_time:.1f} seconds")
       time.sleep(wait_time)

if __name__ == "__main__":
   run_email_processor()