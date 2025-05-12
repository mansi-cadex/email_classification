"""
loop.py - Batch processing and orchestration for email classification system

This module provides functionality for:
1. Managing batch lifecycle (create, process, finalize)
2. Retrying failed batches
3. Exporting data to Excel reports
4. Uploading reports to SFTP
5. Scheduling and running email processing

The module focuses on orchestration and scheduling while delegating specific
operations to appropriate modules (fetch_reply.py, email_sender.py, db.py).
"""

import os
import sys
import time
import uuid
import io
import socket
import pandas as pd
import paramiko 
from paramiko import SSHClient
from scp import SCPClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Tuple, Dict, List, Optional, Any, Union, Callable

# Import from refactored modules
from src.fetch_reply import process_unread_emails
from src.email_sender import (
    save_as_draft, 
    send_email, 
    process_draft_emails, 
    send_pending_replies
)
from src.db import get_mongo, get_postgres, PostgresConnector
from src.log_config import logger

# Load environment variables
load_dotenv()

# Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "125"))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", "600"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "60"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "900"))
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "yes", "1"]
FORCE_DRAFTS = os.getenv("FORCE_DRAFTS", "False").lower() in ["true", "yes", "1"]

# SFTP Configuration
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_ENABLED = os.getenv("SFTP_ENABLED", "False").lower() in ["true", "yes", "1"]

# Labels that should receive responses
RESPONSE_LABELS = ["invoice_request_no_info", "claims_paid_no_proof"]


# ==========================
# Helper Functions
# ==========================
def send_email_with_retries(to_address: str, subject: str, body: str, message_id: str = None, 
                           batch_id: str = None, retries: int = 3, delay: int = 30) -> bool:
    """Send an email with automatic retries on failure.
    
    Args:
        to_address: Recipient email address
        subject: Email subject
        body: Email body content
        message_id: Optional message ID for database update
        batch_id: Optional batch ID for tracking
        retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(1, retries + 1):
        try:
            # Use refactored email_sender module
            success = send_email(to_address, subject, body, message_id, batch_id)
            if success:
                return True
                
            logger.warning(f"send_email retry {attempt}/{retries} failed - waiting {delay}s")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error in send_email attempt {attempt}: {str(e)}")
            time.sleep(delay)
            
    return False


def ensure_batch_record_exists(batch_id: str) -> bool:
    """Ensure the batch record exists in both PostgreSQL and MongoDB.
    
    Args:
        batch_id: The batch ID to check/create
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not batch_id:
        logger.warning("Cannot ensure batch record exists: No batch_id provided")
        return False
        
    try:
        # Check PostgreSQL
        pg_conn = get_postgres()
        if not pg_conn:
            logger.error("Failed to get PostgreSQL connection")
            return False
            
        try:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM core.batch_runs WHERE id = %s", (batch_id,))
                if cur.fetchone() is None:
                    # Insert if not exists
                    cur.execute(
                        """
                        INSERT INTO core.batch_runs (id, status, created_at)
                        VALUES (%s, %s, NOW())
                        """,
                        (batch_id, "in_progress")
                    )
                    pg_conn.commit()
                    logger.info(f"Created missing batch record in PostgreSQL: {batch_id}")
        finally:
            if pg_conn:
                PostgresConnector.return_connection(pg_conn)
        
        # Check MongoDB
        mongo = get_mongo()
        if not mongo:
            logger.error("Failed to get MongoDB connection")
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
    except Exception as e:
        logger.error(f"Error ensuring batch record exists: {str(e)}")
        return False


def update_batch_id_only(batch_id, limit=1, email_data=None):
    """Insert a record with batch_id and either real email data or dummy values in PostgreSQL."""
    if not batch_id:
        logger.warning("No batch_id provided to update_batch_id_only()")
        return 0

    conn = None
    try:
        conn = get_postgres()
        if not conn:
            logger.error("Failed to get PostgreSQL connection")
            return 0
            
        conn.autocommit = True
        with conn.cursor() as cur:
            # Use real email data when available
            if email_data and isinstance(email_data, dict):
                to_email = email_data.get('to_email', email_data.get('recipient', ''))
                from_email = email_data.get('from_email', email_data.get('sender', ''))
                email_subject = email_data.get('subject', email_data.get('email_subject', ''))
                is_sent = email_data.get('is_sent', False)
            else:
                # If no email_data is provided, use minimal values to prevent NULL
                to_email = ''
                from_email = 'system@abc-amega.com'  # Default from_email to prevent NULL
                email_subject = ''
                is_sent = False
                logger.debug("No email data provided. Using minimal values to prevent NULL.")
            
            # Insert with appropriate values, ensuring from_email is never NULL
            cur.execute(
                """
                INSERT INTO core.account_email
                       (batch_id, to_email, from_email, email_subject, is_sent)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (batch_id, to_email, from_email, email_subject, is_sent)
            )
            
            logger.info(f"Successfully inserted batch_id={batch_id} into account_email")
            return 1
            
    except Exception as exc:
        logger.error(f"Error in update_batch_id_only: {str(exc)}")
        return 0
    finally:
        if conn:
            PostgresConnector.return_connection(conn)

# ==========================
# SFTP Operations
# ==========================
def upload_to_sftp(filename: str, file_content: Optional[bytes] = None, 
                  max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Upload files to SFTP server with improved reliability and error handling.
    
    Args:
        filename: The name of the file to upload
        file_content: File content to upload, if provided. Otherwise uploads existing file.
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds. Doubles after each attempt.
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not SFTP_ENABLED:
        logger.info(f"SFTP disabled - skipping upload of {filename}")
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
        try:
            with open(temp_filename, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error(f"Error writing temporary file: {str(e)}")
            return False
    
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


# ==========================
# Data Extraction Functions
# ==========================
def extract_contact_info(email_doc: Dict) -> Dict:
    """Extract contact information from email document metadata.
    
    Args:
        email_doc: Email document from MongoDB
        
    Returns:
        dict: Contact information with status
    """
    contact_info = {
        "new_contact_email": "", 
        "new_contact_name": "", 
        "new_contact_phone": "",
        "contact_status": "active"  # Default status
    }
    
    # First try to get entities directly from the stored API response
    entities = email_doc.get("entities", {})
    
    # If not found directly, try looking in metadata
    if not entities:
        meta = email_doc.get("metadata", {})
        entities = meta.get("entities", {})
    
    if not entities:
        # Check in `extracted_data` (used by the model server)
        entities = email_doc.get("extracted_data", {}).get("entities", {})
    
    # Direct extraction from entities
    if entities:
        # Extract emails
        emails = entities.get("emails", [])
        if emails and isinstance(emails, list) and len(emails) > 0:
            contact_info["new_contact_email"] = emails[0]
        
        # Extract phones
        phones = entities.get("phones", [])
        if phones and isinstance(phones, list) and len(phones) > 0:
            contact_info["new_contact_phone"] = phones[0]
        
        # Extract people
        people = entities.get("people", [])
        if people and isinstance(people, list) and len(people) > 0:
            contact_info["new_contact_name"] = people[0]
    
    # Check metadata for special cases
    meta = email_doc.get("metadata", {})
    
    # Check for permanent departure (left company info)
    left_company = meta.get("left_company", {})
    if left_company:
        # Set contact status to permanent departure
        contact_info["contact_status"] = "permanent_departure"
        
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

    # Check for temporary absence (out of office info)
    ooo = meta.get("out_of_office", {})
    if ooo:
        # Only set to temporary if not already set to permanent
        if contact_info["contact_status"] != "permanent_departure":
            contact_info["contact_status"] = "temporary_absence"
            
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
    
    # Direct access to ooo_contact_person if available
    ooo_contact = email_doc.get("ooo_contact_person", {})
    if ooo_contact:
        # Only set to temporary if not already set to permanent
        if contact_info["contact_status"] != "permanent_departure":
            contact_info["contact_status"] = "temporary_absence"
            
        if ooo_contact.get("email") and not contact_info["new_contact_email"]:
            contact_info["new_contact_email"] = ooo_contact.get("email", "")
        if ooo_contact.get("phone") and not contact_info["new_contact_phone"]:
            contact_info["new_contact_phone"] = ooo_contact.get("phone", "")
        if ooo_contact.get("name") and not contact_info["new_contact_name"]:
            contact_info["new_contact_name"] = ooo_contact.get("name", "")
    
    # Check for direct left_person fields
    left_person = email_doc.get("left_person", {})
    if left_person:
        contact_info["contact_status"] = "permanent_departure"
    
    return contact_info


def build_reply_summary(email_doc: Dict, contact_info: Dict) -> str:
    """Return a short status string for the ReplyText column.
    
    Args:
        email_doc: Email document from MongoDB
        contact_info: Contact information extracted from the email
        
    Returns:
        str: Short reply summary
    """
    label = email_doc.get("prediction") or email_doc.get("classification") or ""
    contact_status = contact_info.get("contact_status", "active")
    
    # 1-4: purely informational – no update
    if label in [
        "no_reply_no_info", "auto_reply_no_info",
        "no_reply_with_info", "auto_reply_with_info"
    ]:
        # Include contact status in response if relevant
        if contact_status == "permanent_departure":
            prefix = "contact left company - "
        elif contact_status == "temporary_absence":
            prefix = "contact temporarily away - "
        else:
            prefix = ""
            
        # Did we get any contact details?
        if contact_info["new_contact_email"] and contact_info["new_contact_phone"]:
            return f"{prefix}contact email & phone updated"
        elif contact_info["new_contact_email"]:
            return f"{prefix}new email"
        elif contact_info["new_contact_phone"]:
            return f"{prefix}new phone"
        else:
            return f"{prefix}no action"
    
    # Business-flow labels
    if label == "invoice_request_no_info":
        return "invoice info requested"
    if label == "claims_paid_no_proof":
        return "payment claim – awaiting proof"
    
    # Manual review case
    if label == "manual_review":
        return "manual review – flagged by model"
    
    # Default for any other non-response case
    return "no_response"


def extract_invoice_info(email_doc: Dict) -> Dict:
    """Extract invoice and payment information from email document.
    
    Args:
        email_doc: Email document from MongoDB
        
    Returns:
        dict: Invoice information
    """
    invoice_info = {
        "invoice_number": "",
        "amount": "",
        "due_date": "",
        "payment_date": "",
        "reference_number": ""
    }
    
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


# ==========================
# Report Generation
# ==========================
def export_processed_emails_to_excel(batch_id: str) -> Optional[str]:
    """Export processed emails to Excel and upload to SFTP.
    
    Args:
        batch_id: Batch ID to export
        
    Returns:
        str: Filename if successful, None otherwise
    """
    if not batch_id:
        logger.warning("Cannot export to Excel: No batch_id provided")
        return None

    try:
        mongo = get_mongo()
        if not mongo:
            logger.error("Failed to get MongoDB connection")
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
            # Extract contact information
            contact_info = extract_contact_info(e)
            
            # Determine reply status
            if e.get("response_sent"):
                reply_sent = "sent"
            elif e.get("draft_saved"):
                reply_sent = "draft"
            elif e.get("prediction") in RESPONSE_LABELS:
                # Should have a reply, but it failed somewhere
                reply_sent = "reply_missing"
            else:
                reply_sent = "no_response"
            
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
                "NewContactPhone": contact_info.get("new_contact_phone", ""),
                "ContactStatus": contact_info.get("contact_status", "active")
            }
            
            rows.append(row)

        df = pd.DataFrame(rows)
        
        # Define all possible columns to ensure consistent output
        cols = [
            "EmailFrom", "EmailTo", "SubjectLine", "Date",
            "Event Type", "TargetFolder", "ReplySent", "ReplyText", 
            "NewContactEmail", "NewContactPhone", "ContactStatus"
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
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        
        # Upload to SFTP
        upload_success = upload_to_sftp(fname, buf.getvalue())
        return fname if upload_success else None
        
    except Exception as e:
        logger.error(f"Error exporting to Excel: {str(e)}")
        return None


# ==========================
# Batch Management Functions
# ==========================
def mark_permanently_failed(batch_id: str, reason: str) -> bool:
    """Mark a batch as permanently failed in both MongoDB and PostgreSQL.
    
    Args:
        batch_id: Batch ID to mark
        reason: Reason for failure
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not batch_id:
        logger.warning("Cannot mark batch as permanently failed: No batch_id provided")
        return False
        
    try:
        # Update MongoDB first
        mongo = get_mongo()
        if not mongo:
            logger.error("Failed to get MongoDB connection")
            return False
            
        mongo_result = mongo.batch_runs_collection.update_one(
            {"id": batch_id},
            {
                "$set": {
                    "permanently_failed": True,
                    "status": "permanently_failed",
                    "failure_reason": reason,
                    "failure_timestamp": datetime.utcnow()
                }
            },
            upsert=True  # Create record if it doesn't exist
        )
        
        # Update PostgreSQL
        success = PostgresConnector.mark_batch_permanently_failed(batch_id)
        
        if success:
            logger.info(f"Batch {batch_id} marked as permanently failed: {reason}")
        else:
            logger.warning(f"Failed to mark batch {batch_id} as permanently failed in PostgreSQL")
            
        return success
        
    except Exception as e:
        logger.error(f"Error marking batch {batch_id} as permanently failed: {str(e)}")
        return False


def process_batch(batch_id: Optional[str] = None) -> Tuple[bool, int, int, int]:
    """Process one batch of emails and manage the full lifecycle.
    
    Args:
        batch_id: Optional batch ID to use (generated if not provided)
        
    Returns:
        tuple: (success, processed_count, failed_count, draft_count)
    """
    start_time = time.time()
    
    # Generate batch ID if not provided
    if not batch_id:
        batch_id = PostgresConnector.insert_batch_run()
        if not batch_id:
            logger.error("Failed to create batch in PostgreSQL")
            return False, 0, 0, 0
    
    logger.info(f"Processing batch {batch_id} (max {BATCH_SIZE})")
    logger.info(f"Mail sending is {'ENABLED' if MAIL_SEND_ENABLED else 'DISABLED'}, Force drafts is {'ENABLED' if FORCE_DRAFTS else 'DISABLED'}")

    # Make sure batch record exists in both PostgreSQL and MongoDB
    try:
        ensure_batch_record_exists(batch_id)
    except Exception as e:
        logger.error(f"Failed to ensure batch record exists: {str(e)}")
        return False, 0, 0, 0

    try:
        # Use the new interface from fetch_reply.py
        email_result = process_unread_emails(batch_id)
        
        if not email_result["success"]:
            logger.error(f"Batch {batch_id} failed during email processing: {email_result}")
            
            # Ensure batch record is updated properly
            mongo = get_mongo()
            if mongo:
                mongo.update_batch_result(
                    batch_id, 
                    email_result.get("emails_processed", 0), 
                    email_result.get("emails_errored", 0), 
                    0,  # No drafts yet
                    "failed"
                )
            
            # Update PostgreSQL
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
        emails_classified = email_result.get("emails_classified", 0)
        batch_id = email_result.get("batch_id", batch_id)

        # Process draft emails using the refactored interface
        logger.info(f"Processing drafts for batch {batch_id}")
        draft_success, draft_failed = process_draft_emails(batch_id)
        
        # Send emails if appropriate
        logger.info(f"Processing replies for batch {batch_id}")
        sent_success, sent_failed = send_pending_replies(batch_id)
        
        # Update total counts
        total_processed = processed_count
        total_failed = failed_count + draft_failed + sent_failed
        total_draft_count = draft_success
        
        # Determine overall status
        status = "success" if total_failed == 0 else "partial"
        
        # Update batch status in both databases
        try:
            # Make sure batch record exists
            ensure_batch_record_exists(batch_id)
            
            # Update PostgreSQL
            PostgresConnector.update_batch_result(
                batch_id, 
                total_processed, 
                total_failed, 
                status, 
                total_draft_count
            )
            
            # Update MongoDB
            mongo = get_mongo()
            if mongo:
                mongo.update_batch_result(
                    batch_id, 
                    total_processed, 
                    total_failed, 
                    total_draft_count, 
                    status
                )
        except Exception as e:
            logger.error(f"Error updating batch result: {str(e)}")

        # Update tracking record for reporting
        if processed_count > 0:
            try:
                # Get first email from the batch if available for real data
                mongo = get_mongo()
                emails = list(mongo.collection.find({"batch_id": batch_id}).limit(1))
                email_data = emails[0] if emails else None
                
                updated_records = update_batch_id_only(batch_id, processed_count, email_data)
                logger.info(f"Inserted batch tracking record with batch_id={batch_id}")
            except Exception as e:
                logger.error(f"Error updating batch ID: {str(e)}")

        # Export to Excel and upload to SFTP
        try:
            excel_file = export_processed_emails_to_excel(batch_id)
            if excel_file:
                logger.info(f"Excel file exported and uploaded: {excel_file}")
        except Exception as e:
            logger.error(f"Error exporting and uploading Excel file: {str(e)}")

        # Log completion time
        elapsed = time.time() - start_time
        logger.info(f"Batch {batch_id} completed in {elapsed:.1f}s: {total_processed} processed, "
                   f"{total_failed} failed, {total_draft_count} drafts, {sent_success} emails sent")
        
        return True, total_processed, total_failed, total_draft_count
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"Unhandled error in process_batch after {elapsed:.1f}s: {str(e)}")
        
        # Try to update batch status to failed
        try:
            PostgresConnector.update_batch_result(batch_id, 0, 1, "failed", 0)
            mongo = get_mongo()
            if mongo:
                mongo.update_batch_result(batch_id, 0, 1, 0, "failed")
        except Exception as nested_e:
            logger.error(f"Error updating batch status after unhandled error: {str(nested_e)}")
            
        return False, 0, 1, 0


def clean_failed_batches() -> bool:
    """Mark existing failed batches as permanently failed in both databases.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Cleaning up existing failed batches")
    
    try:
        # Get batch IDs of failed batches from PostgreSQL
        failed_batches = PostgresConnector.get_failed_batches(24 * 60)  # 24 hours in minutes
        
        if not failed_batches:
            logger.info("No failed batches to clean up")
            return True
            
        logger.info(f"Found {len(failed_batches)} failed batches to mark as permanently failed")
        
        # Also check MongoDB for additional failed batches
        mongo = get_mongo()
        if mongo:
            mongo_failed_query = {
                "$or": [
                    {"status": "failed"},
                    {"status": "in_progress", "created_at": {"$lt": datetime.utcnow() - timedelta(days=1)}}
                ],
                "permanently_failed": {"$ne": True}
            }
            mongo_failed_batches = list(mongo.batch_runs_collection.find(mongo_failed_query, {"id": 1}))
            mongo_batch_ids = [batch.get('id') for batch in mongo_failed_batches if batch.get('id')]
            
            # Add MongoDB batch IDs to the list of PostgreSQL batch IDs
            batch_ids = [batch.get('id') for batch in failed_batches]
            for batch_id in mongo_batch_ids:
                if batch_id not in batch_ids:
                    batch_ids.append(batch_id)
        else:
            # Use only PostgreSQL batch IDs
            batch_ids = [batch.get('id') for batch in failed_batches]
        
        if not batch_ids:
            logger.info("No failed batches to clean up")
            return True
            
        logger.info(f"Found {len(batch_ids)} failed batches to mark as permanently failed")
        
        # Mark each batch as permanently failed in both systems
        for batch_id in batch_ids:
            mark_permanently_failed(batch_id, "Marked as permanently failed during cleanup")
            logger.info(f"Batch {batch_id} marked as permanently failed")
        
        logger.info(f"Cleanup complete: {len(batch_ids)} batches marked as permanently failed")
        return True
    except Exception as e:
        logger.error(f"Error in clean_failed_batches: {str(e)}")
        return False


def retry_failed_batches() -> bool:
    """Retry failed batches that haven't reached the maximum retry count.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting retry of failed batches")
    
    try:
        # Get failed batches from PostgreSQL
        timeout_minutes = BATCH_TIMEOUT // 60
        failed_batches = PostgresConnector.get_failed_batches(timeout_minutes)
        
        if not failed_batches:
            logger.info("No failed batches to retry")
            return True
            
        logger.info(f"Found {len(failed_batches)} failed batches to retry")
        
        # Process each batch
        for batch in failed_batches:
            batch_id = batch.get('id')
            if not batch_id:
                logger.warning("Skipping batch with no ID")
                continue
            
            # Get MongoDB batch info
            mongo = get_mongo()
            if not mongo:
                logger.error("Failed to get MongoDB connection")
                continue
                
            batch_info = mongo.get_batch_retry_info(batch_id)
            
            if batch_info.get("permanently_failed", False):
                logger.info(f"Skipping batch {batch_id}: permanently failed in MongoDB")
                continue
                
            # Check retry count
            retry_count = batch_info.get("retry_count", 0)
            
            if retry_count >= MAX_RETRIES:
                logger.info(f"Skipping batch {batch_id}: max retries reached ({retry_count}/{MAX_RETRIES})")
                mark_permanently_failed(batch_id, f"Max retries ({MAX_RETRIES}) reached")
                continue
                
            # Increment retry counter in MongoDB
            mongo.increment_batch_retry(batch_id)
            logger.info(f"Retrying batch {batch_id} (retry #{retry_count + 1}/{MAX_RETRIES})")
            
            # Process the batch again
            success, processed, failed, drafts = process_batch(batch_id)
            
            if success:
                logger.info(f"Retry successful for batch {batch_id}: {processed} processed, {drafts} drafts")
            else:
                logger.warning(f"Retry failed for batch {batch_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in retry_failed_batches: {str(e)}")
        return False


def run_batch_processor() -> bool:
    """Run a single batch processing cycle.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Starting new batch processor run")
    
    # Create a new batch ID
    batch_id = PostgresConnector.insert_batch_run()
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
   
    # Try processing the batch with retries
    while not success and retry_count < MAX_RETRIES:
        if retry_count > 0:
            logger.info(f"Retry attempt {retry_count+1}/{MAX_RETRIES} for batch {batch_id}")
           
        success, processed_count, failed_count, draft_count = process_batch(batch_id)
       
        if not success:
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.warning(f"Retrying batch {batch_id} in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
   
    # Handle failure after all retries
    if not success:
        logger.error(f"Batch {batch_id} failed after {MAX_RETRIES} attempts")
       
        # Update both PostgreSQL and MongoDB
        try:
            # Make sure batch record exists
            ensure_batch_record_exists(batch_id)
           
            PostgresConnector.update_batch_result(
                batch_id, 
                processed_count, 
                failed_count, 
                "failed", 
                draft_count
            )
           
            # Also track retry count in MongoDB
            mongo = get_mongo()
            if mongo:
                mongo.batch_runs_collection.update_one(
                    {"id": batch_id},
                    {"$set": {
                        "retry_count": retry_count, 
                        "status": "failed",
                        "processed_count": processed_count,
                        "failed_count": failed_count,
                        "draft_count": draft_count
                    }},
                    upsert=True
                )
        except Exception as e:
            logger.error(f"Error updating batch status: {str(e)}")
   
    return success


def run_email_processor():
    """Main batch processing loop with regular intervals.
    
    This function runs indefinitely, processing batches at regular intervals
    and handling failed batches.
    """
    logger.info(f"Starting email batch processor (batch size: {BATCH_SIZE}, interval: {BATCH_INTERVAL}s)")
    logger.info(f"Mail sending is {'ENABLED' if MAIL_SEND_ENABLED else 'DISABLED'}")
    logger.info(f"Force drafts is {'ENABLED' if FORCE_DRAFTS else 'DISABLED'}")
    logger.info(f"SFTP export is {'ENABLED' if SFTP_ENABLED else 'DISABLED'}")
   
    # Clean up existing failed batches on startup
    clean_failed_batches()
   
    while True:
        start_time = datetime.now()
        logger.info(f"Starting batch at {start_time.isoformat()}")
       
        try:
            # First, retry any failed batches that aren't permanently failed
            retry_failed_batches()
           
            # Then run the main batch processor
            run_batch_processor()
           
            # Calculate time to next batch
            elapsed = (datetime.now() - start_time).total_seconds()
            wait_time = max(0, BATCH_INTERVAL - elapsed)
           
            logger.info(f"Batch complete. Next batch in {wait_time:.1f} seconds")
            time.sleep(wait_time)
        except KeyboardInterrupt:
            logger.info("Batch processor interrupted by user")
            break
        except Exception as e:
            logger.exception(f"Unhandled error in batch processor: {str(e)}")
            # Wait a bit to avoid tight loop in case of persistent errors
            time.sleep(60)


# ==========================
# Entry Point
# ==========================
if __name__ == "__main__":
    try:
        run_email_processor()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Unhandled exception in main:")
        sys.exit(1)