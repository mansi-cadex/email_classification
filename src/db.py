"""
db.py - Database connections and operations for email classification system.

This module provides interfaces for:
1. MongoDB operations for email data storage
2. PostgreSQL operations for batch tracking
3. Helper functions for common database tasks

The module follows a clear separation of concerns between MongoDB and PostgreSQL
operations, with consistent error handling and transaction management.
"""

import os
import uuid
from functools import lru_cache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
from psycopg2 import pool, extras
from psycopg2.extras import execute_batch
from src.log_config import logger
from dotenv import load_dotenv
load_dotenv()

# =====================================
# Configuration Constants
# =====================================

# Email classification labels
ALLOWED_LABELS = [
    "no_reply_no_info", 
    "no_reply_with_info", 
    "auto_reply_no_info", 
    "auto_reply_with_info", 
    "invoice_request_no_info", 
    "claims_paid_no_proof", 
    "manual_review"
]

# Labels that should NOT receive responses
NO_RESPONSE_LABELS = [
    "no_reply_no_info",
    "no_reply_with_info",
    "auto_reply_no_info", 
    "auto_reply_with_info",
    "manual_review"
]

# Labels that SHOULD receive responses
RESPONSE_LABELS = [
    "invoice_request_no_info",
    "claims_paid_no_proof"
]

# =====================================
# MongoDB Operations
# =====================================
class MongoConnector:
    """MongoDB operations for email classification system."""
    
    def __init__(self, uri=None, db_name=None, collection_name=None):
        """Initialize MongoDB connection with database and collections."""
        try:
            self.uri = uri or os.getenv("MONGO_URI")
            if not self.uri:
                raise ValueError("MONGO_URI is not set. Please define it in your .env file.")
            
            self.client = MongoClient(self.uri)
            self.db_name = db_name or os.getenv("MONGO_DB")
            self.collection_name = collection_name or os.getenv("MONGO_COLLECTION")
            
            if not self.db_name or not self.collection_name:
                raise ValueError("MONGO_DB and MONGO_COLLECTION must be set in your .env file.")
            
            self.db = self.client[self.db_name]

            # Initialize collections
            self.collection = self.db[self.collection_name]
            self.template_collection = self.db["email_templates"]
            self.payment_collection = self.db["payments"]
            self.contact_collection = self.db["contacts"]
            self.archive_collection = self.db["archived_emails"]
            self.billing_collection = self.db["billing_records"]
            self.batch_runs_collection = self.db["batch_runs"]
            
            # Setup indexes for all collections
            self._setup_indexes()
            
            self.current_batch_id = None
            
            logger.info(f"Connected to MongoDB: db={self.db_name}, collection={self.collection_name}")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {str(e)}")
            raise

    def _setup_indexes(self):
        """Create necessary indexes for efficient queries."""
        try:
            # Emails collection indexes
            index_definitions = [
                ([("message_id", ASCENDING)], {"unique": True}),
                ([("prediction", ASCENDING)], {}),
                ([("manual_review", ASCENDING)], {}),
                ([("sender", ASCENDING)], {}),
                ([("created_at", ASCENDING)], {}),
                ([("batch_id", ASCENDING)], {}),
                ([("response_process", ASCENDING)], {}),
                ([("save_as_draft", ASCENDING)], {}),
                ([("draft_saved", ASCENDING)], {}),
                ([("response_sent", ASCENDING)], {})
            ]
            
            for index_fields, index_options in index_definitions:
                self.collection.create_index(index_fields, **index_options)
            
            # Batch runs collection indexes
            self.batch_runs_collection.create_index([("id", ASCENDING)], unique=True)
            self.batch_runs_collection.create_index([("status", ASCENDING)])
            self.batch_runs_collection.create_index([("created_at", ASCENDING)])
            self.batch_runs_collection.create_index([("permanently_failed", ASCENDING)])
            self.batch_runs_collection.create_index([("retry_count", ASCENDING)])
            
            # TTL index for auto-archiving
            ttl_days = os.getenv("EMAIL_TTL_DAYS")
            if ttl_days and ttl_days.isdigit() and int(ttl_days) > 0:
                self.archive_collection.create_index(
                    [("created_at", ASCENDING)], 
                    expireAfterSeconds=int(ttl_days) * 86400
                )
            
            # Other collection indexes
            self.payment_collection.create_index([("invoice_number", ASCENDING)], unique=True)
            self.payment_collection.create_index([("email", ASCENDING)])
            self.contact_collection.create_index([("original_email", ASCENDING)], unique=True)
            self.contact_collection.create_index([("is_processed", ASCENDING)])
            self.billing_collection.create_index([("timestamp", 1)])
            
            logger.info("MongoDB indexes setup complete")
        except Exception as e:
            logger.error(f"Error setting up MongoDB indexes: {str(e)}")
            raise
    
    def _validate_and_process_email(self, email_data):
        """Validate and process email data before inserting/updating."""
        try:
            # Validate required fields
            required_fields = ["message_id", "text"]
            missing = [field for field in required_fields if field not in email_data]
            if missing:
                raise ValueError(f"Missing required fields in email data: {missing}")
            
            # Add creation timestamp if not present
            if "created_at" not in email_data:
                email_data["created_at"] = datetime.utcnow()
            
            # Process email labels and flags
            is_manual_review = email_data.get("prediction", "").lower() == "manual_review"
            email_data["manual_review"] = is_manual_review
            
            # Handle no-response labels
            if email_data.get("prediction", "").lower() in NO_RESPONSE_LABELS:
                email_data["response_sent"] = None
            
            # Set default values
            email_data.setdefault("response_process", False)
            email_data.setdefault("save_as_draft", is_manual_review)
            
            # Ensure batch_id is preserved if it exists
            if "batch_id" not in email_data and hasattr(self, 'current_batch_id') and self.current_batch_id:
                email_data["batch_id"] = self.current_batch_id
                
            return email_data
        except Exception as e:
            logger.error(f"Error validating email data: {str(e)}")
            raise

    # ==== Email Operations ====
    
    def insert_email(self, email_data: Dict[str, Any]) -> Optional[Any]:
        """Insert a single email document into MongoDB."""
        try:
            # Process and validate email data
            email_data = self._validate_and_process_email(email_data)
            
            # Insert the document
            result = self.collection.insert_one(email_data)
            logger.info(f"Email inserted with ID: {result.inserted_id}")
                
            # Extract and store contacts if needed
            label = email_data.get("prediction", "").lower()
            if ("with_info" in label) and "contact_info" in email_data:
                sender = email_data.get("sender", email_data.get("email", ""))
                self._update_contact_info(sender, email_data["contact_info"])

            return result
        except DuplicateKeyError:
            logger.warning(f"Duplicate email with message_id: {email_data.get('message_id')}")
            return None
        except Exception as e:
            logger.error(f"Error inserting email data: {str(e)}")
            return None

    def update_email_with_response(self, message_id: str, response_text: str) -> Optional[Any]:
        """Attach generated response to an email entry."""
        try:
            if not message_id:
                logger.warning("Cannot update email: No message_id provided")
                return None
                
            filters = {"message_id": message_id}
            update_fields = {
                "response": response_text,
                "response_sent": False,
                "response_process": True,
                "response_timestamp": datetime.utcnow()
            }
            # Update MongoDB
            result = self.collection.update_one(filters, {"$set": update_fields})
            
            if result.modified_count > 0:
                logger.info(f"Updated email with response for message ID: {message_id}")
            else:
                logger.warning(f"No matching document found for message ID: {message_id}")
            
            return result
        except PyMongoError as e:
            logger.error(f"MongoDB error updating email with response: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error updating email with response: {str(e)}")
            return None

    def email_exists(self, message_id: str, sender: str = None, subject: str = None, received_at: str = None) -> bool:
        """Check if an email with the given message_id exists.
        
        Args:
            message_id (str): The message ID to check
            sender (str, optional): Unused parameter kept for backward compatibility
            subject (str, optional): Unused parameter kept for backward compatibility
            received_at (str, optional): Unused parameter kept for backward compatibility
            
        Returns:
            bool: True if an email with the message_id exists, False otherwise
        """
        try:
            if not message_id:
                return False                    # nothing to check
            return self.collection.count_documents({"message_id": message_id}, limit = 1) > 0

        except Exception as e:
            logger.error(f"Error checking if email exists: {e}")
            return False

    def find_emails(self, query: Dict[str, Any], *args, **kwargs) -> List[Dict[str, Any]]:
        """Wrapper for collection.find() to get emails matching a query."""
        try:
            return list(self.collection.find(query, *args, **kwargs))
        except Exception as e:
            logger.error(f"Error finding emails: {str(e)}")
            return []

    def find_pending_responses(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Find emails that need a response but don't have one yet."""
        try:
            query = {
                "response_sent": False,
                "manual_review": False,
                "save_as_draft": False
            }
            return list(self.collection.find(query).limit(limit))
        except Exception as e:
            logger.error(f"Error finding pending responses: {str(e)}")
            return []
    # Add this method inside the MongoConnector class in db.py
    def update_message_id(self, old_id, new_id):
        """Update message ID after a successful move operation."""
        try:
            if not old_id or not new_id:
                logger.warning("Cannot update message ID: Missing old_id or new_id")
                return False
                
            result = self.collection.update_one(
                {"message_id": old_id},
                {"$set": {"message_id": new_id, "previous_message_id": old_id}}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated message ID from {old_id} to {new_id}")
                return True
            else:
                logger.warning(f"No document found with message_id: {old_id}")
                return False
        except Exception as e:
            logger.error(f"Error updating message ID: {str(e)}")
            return False

    def find_draft_emails(self, batch_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Find emails that should be saved as drafts."""
        try:
            # Construct the base query
            query = {
                "$or": [
                    # Case 1: Emails explicitly marked as drafts that haven't been saved
                    {
                        "save_as_draft": True,
                        "$or": [
                            {"draft_saved": {"$ne": True}},
                            {"draft_saved": {"$exists": False}}
                        ]
                    },
                    # Case 2: Emails with response labels that have responses
                    {
                        "prediction": {"$in": RESPONSE_LABELS},
                        "response": {"$exists": True, "$ne": ""},
                        "response_sent": False
                    }
                ]
            }
            
            # Add batch_id filter if provided
            if batch_id:
                query["batch_id"] = batch_id
            
            # Log the query for debugging
            logger.debug(f"Finding draft emails with query: {query}")
            
            # Find matching emails with a reasonable limit
            return list(self.collection.find(query).limit(limit))
        except Exception as e:
            logger.error(f"Error finding draft emails: {str(e)}")
            return []

    def fetch_unreplied_emails(self, batch_id: str) -> List[Dict[str, Any]]:
        """Get all emails in a batch that haven't been replied to yet."""
        try:
            if not batch_id:
                logger.warning("fetch_unreplied_emails called without batch_id")
                return []
                
            query = {
                "batch_id": batch_id,
                "response_sent": False,
                "save_as_draft": False  # Don't retry emails meant for drafts
            }
            return list(self.collection.find(query))
        except Exception as e:
            logger.error(f"Error fetching unreplied emails: {str(e)}")
            return []

    def mark_email_sent(self, message_id: str) -> Optional[Any]:
        """Mark an email as having been sent a response."""
        try:
            if not message_id:
                logger.warning("Cannot mark email as sent: No message_id provided")
                return None
                
            # Update MongoDB
            result = self.collection.update_one(
                {"message_id": message_id},
                {"$set": {
                    "response_sent": True,
                    "response_timestamp": datetime.utcnow()
                }}
            )
            
            if result.modified_count > 0:
                logger.info(f"Marked email {message_id} as sent")
            else:
                logger.warning(f"No email found with message ID: {message_id}")
                
            return result
        except Exception as e:
            logger.error(f"Error marking email as sent: {str(e)}")
            return None
            
    def mark_email_draft_saved(self, message_id: str, draft_id: Optional[str] = None) -> Optional[Any]:
        """Mark an email as having been saved as a draft."""
        try:
            if not message_id:
                logger.warning("Cannot mark email as draft saved: No message_id provided")
                return None
                
            # Prepare update data for MongoDB
            update_data = {
                "response_process": True,
                "draft_saved": True,
                "draft_timestamp": datetime.utcnow()
            }
            
            if draft_id:
                update_data["draft_id"] = draft_id
            
            # Update MongoDB
            result = self.collection.update_one(
                {"message_id": message_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Marked email {message_id} as saved to draft")
            else:
                logger.warning(f"No email found with message ID: {message_id}")
                
            return result
        except Exception as e:
            logger.error(f"Error marking email as draft saved: {str(e)}")
            return None

    # ==== Batch Operations ====
    
    def set_batch_id(self, batch_id: str) -> 'MongoConnector':
        """Set the current batch ID for subsequent operations."""
        self.current_batch_id = batch_id
        return self

    def insert_batch_run(self) -> Optional[str]:
        """Create a new batch run record and return its ID."""
        try:
            batch_id = str(uuid.uuid4())
            self.batch_runs_collection.insert_one({
                "id": batch_id,
                "created_at": datetime.utcnow(),
                "status": "in_progress",
                "processed_count": 0,
                "failed_count": 0,
                "draft_count": 0,
                "retry_count": 0,
                "permanently_failed": False
            })
            
            logger.info(f"Created new batch run with ID: {batch_id}")
            self.current_batch_id = batch_id  # Store current batch ID
            return batch_id
        except Exception as e:
            logger.error(f"Error inserting batch run: {str(e)}")
            return None
    
    def update_batch_result(self, batch_id: str, processed_count: int, failed_count: int, 
                           draft_count: int = 0, status: str = "success", 
                           error_log: Optional[str] = None) -> Optional[Any]:
        """Update a batch run record with results."""
        try:
            if not batch_id:
                logger.warning("Cannot update batch result: No batch_id provided")
                return None
                
            update_data = {
                "status": status,
                "processed_at": datetime.utcnow(),
                "processed_count": processed_count,
                "failed_count": failed_count,
                "draft_count": draft_count,
                "last_updated": datetime.utcnow()
            }
            
            if error_log:
                update_data["error_log"] = error_log
                
            result = self.batch_runs_collection.update_one(
                {"id": batch_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated batch {batch_id} with status: {status}")
            else:
                logger.warning(f"No batch record found for ID: {batch_id}")
                
            return result
        except Exception as e:
            logger.error(f"Error updating batch result: {str(e)}")
            return None

    def mark_batch_permanently_failed(self, batch_id: str, reason: Optional[str] = None) -> bool:
        """Mark a batch as permanently failed so it won't be retried."""
        try:
            if not batch_id:
                logger.warning("Cannot mark batch as failed: No batch_id provided")
                return False
                
            update_data = {
                "permanently_failed": True,
                "failure_timestamp": datetime.utcnow(),
                "status": "permanently_failed"
            }
            
            if reason:
                update_data["failure_reason"] = reason
                
            result = self.batch_runs_collection.update_one(
                {"id": batch_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Marked batch {batch_id} as permanently failed: {reason}")
                return True
            else:
                logger.warning(f"No batch record found for ID: {batch_id}")
                return False
        except Exception as e:
            logger.error(f"Error marking batch as permanently failed: {str(e)}")
            return False
    
    def increment_batch_retry(self, batch_id: str) -> bool:
        """Increment the retry counter for a batch."""
        try:
            if not batch_id:
                logger.warning("Cannot increment batch retry: No batch_id provided")
                return False
                
            now = datetime.utcnow()
            
            result = self.batch_runs_collection.update_one(
                {"id": batch_id},
                {
                    "$inc": {"retry_count": 1},
                    "$set": {"last_retry": now}
                }
            )
            
            if result.modified_count > 0:
                return True
            else:
                # If no document was modified, it might not exist, so create it
                self.batch_runs_collection.insert_one({
                    "id": batch_id,
                    "retry_count": 1,
                    "last_retry": now,
                    "created_at": now,
                    "permanently_failed": False
                })
                return True
        except Exception as e:
            logger.error(f"Error incrementing batch retry: {str(e)}")
            return False

    def get_batch_retry_info(self, batch_id: str) -> Dict[str, Any]:
        """Get retry information for a batch with improved reliability."""
        try:
            if not batch_id:
                return {"retry_count": 0, "permanently_failed": False, "last_retry": None, "status": "unknown"}
                
            # First try to get from MongoDB
            batch = self.batch_runs_collection.find_one(
                {"id": batch_id},
                {"retry_count": 1, "permanently_failed": 1, "last_retry": 1, "status": 1}
            )
            
            if batch:
                return {
                    "retry_count": batch.get("retry_count", 0),
                    "permanently_failed": batch.get("permanently_failed", False),
                    "last_retry": batch.get("last_retry"),
                    "status": batch.get("status", "unknown")
                }
            else:
                # Create a new record if it doesn't exist
                self.batch_runs_collection.insert_one({
                    "id": batch_id,
                    "retry_count": 0,
                    "permanently_failed": False,
                    "status": "unknown",
                    "created_at": datetime.utcnow()
                })
                
                return {
                    "retry_count": 0, 
                    "permanently_failed": False, 
                    "last_retry": None, 
                    "status": "unknown"
                }
        except Exception as e:
            logger.error(f"Error getting batch retry info: {str(e)}")
            return {"retry_count": 0, "permanently_failed": False, "last_retry": None, "status": "error"}
    
    # ==== Payment Operations ====
    
    def create_or_update_payment_record(self, payment_data: Dict[str, Any]) -> Optional[Any]:
        """Create or update a payment record in the payment collection."""
        try:
            if not payment_data or "invoice_number" not in payment_data:
                logger.warning("Missing invoice number in payment data")
                return None
                    
            invoice_number = payment_data["invoice_number"]
            now = datetime.utcnow()
            
            # Check if payment record exists
            existing_record = self.payment_collection.find_one({"invoice_number": invoice_number})
            payment_data["updated_at"] = now
            
            if existing_record:
                # Update existing record
                result = self.payment_collection.update_one(
                    {"invoice_number": invoice_number},
                    {"$set": payment_data}
                )
                logger.info(f"Updated payment record for invoice {invoice_number}")
            else:
                # Create new payment record
                payment_data["created_at"] = now
                result = self.payment_collection.insert_one(payment_data)
                logger.info(f"Created new payment record for invoice {invoice_number}")
                
            return result
        except Exception as e:
            logger.error(f"Error creating/updating payment record: {str(e)}")
            return None
        
    def get_payment_record_by_invoice(self, invoice_number: str) -> Optional[Dict[str, Any]]:
        """Get a payment record by invoice number."""
        try:
            if not invoice_number:
                logger.warning("Cannot get payment record: No invoice_number provided")
                return None
                
            return self.payment_collection.find_one({"invoice_number": invoice_number})
        except Exception as e:
            logger.error(f"Error getting payment record: {str(e)}")
            return None
        
    def update_payment_record(self, invoice_number: str, update_data: Dict[str, Any]) -> Optional[Any]:
        """Update specific fields in a payment record."""
        try:
            if not invoice_number or not update_data:
                logger.warning("Cannot update payment record: Missing invoice_number or update_data")
                return None
                
            update_data["updated_at"] = datetime.utcnow()
            result = self.payment_collection.update_one(
                {"invoice_number": invoice_number},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated payment record for invoice {invoice_number}")
            
            return result
        except Exception as e:
            logger.error(f"Error updating payment record: {str(e)}")
            return None

    # ==== Contact Operations ====
    
    def _update_contact_info(self, original_email: str, new_contacts: List[str]) -> Optional[Any]:
        """Update contact information based on auto-reply or no-reply emails."""
        try:
            if not original_email or not new_contacts:
                return None
                
            existing = self.contact_collection.find_one({"original_email": original_email})
            
            if existing:
                # Get existing contacts and find new unique ones
                existing_contacts = existing.get("alternative_contacts", [])
                new_unique_contacts = [c for c in new_contacts if c not in existing_contacts]
                
                # Update with new contacts if any
                if new_unique_contacts:
                    result = self.contact_collection.update_one(
                        {"original_email": original_email},
                        {
                            "$addToSet": {"alternative_contacts": {"$each": new_unique_contacts}},
                            "$set": {"updated_at": datetime.utcnow(), "is_processed": False}
                        }
                    )
                    logger.info(f"Updated contact info for {original_email} with {len(new_unique_contacts)} new contacts")
                    return result
                return None
            else:
                # Create new record
                result = self.contact_collection.insert_one({
                    "original_email": original_email,
                    "alternative_contacts": new_contacts,
                    "updated_at": datetime.utcnow(),
                    "is_processed": False
                })
                logger.info(f"Created new contact record for {original_email}")
                return result
        except Exception as e:
            logger.error(f"Error updating contact info: {str(e)}")
            return None

    def get_contact_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get contact information by email address."""
        try:
            if not email:
                return None
                
            # First try as original email
            contact = self.contact_collection.find_one({"original_email": email})
            if contact:
                return contact
                
            # Then try as alternative contact
            contact = self.contact_collection.find_one({"alternative_contacts": email})
            return contact
        except Exception as e:
            logger.error(f"Error getting contact by email: {str(e)}")
            return None

    # ==== Database Synchronization ====
    
    def sync_batch_emails_to_postgres(self, batch_id: str) -> int:
        """
        Up-sert Mongo e-mails into core.account_email *without* requiring a
        UNIQUE constraint on message_id.

        Strategy:
        1. For each mail → try an UPDATE on message_id.
            • if rowcount == 0 → queue for INSERT
        2. Bulk-insert anything new with execute_batch().
        """
        if not batch_id:
            logger.warning("sync_batch_emails_to_postgres called without batch_id")
            return 0

        pg_conn = get_postgres()
        if not pg_conn:
            logger.error("PG connection unavailable – aborting sync")
            return 0

        try:
            mails = self.collection.find({"batch_id": batch_id})
            inserts: list[Tuple] = []
            processed = 0

            update_sql = """
                UPDATE core.account_email SET
                    from_email          = %s,
                    to_email            = %s,
                    email_subject       = %s,
                    email_body          = %s,
                    is_sent             = %s,
                    batch_id            = %s,
                    outlook_message_id  = %s,
                    created_at          = COALESCE(created_at, %s)   -- keep original if present
                WHERE message_id = %s
            """

            insert_sql = """
                INSERT INTO core.account_email (
                    conversation_id, receiver_type, sender_name,
                    from_email, to_email, cc, email_subject, email_body,
                    debtor_number, debtor_id, user_id, is_sent, eml_file,
                    created_at, outlook_message_id, message_id, hash, batch_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            cur = pg_conn.cursor()

            for m in mails:
                # ---------- mandatory fall-backs -------------
                to_email      = m.get("recipient") or ""
                subject       = m.get("subject") or "No Subject"
                created_at    = m.get("created_at") or datetime.utcnow()
                is_sent_flag  = bool(m.get("response_sent"))

                # ---------- first try UPDATE -----------------
                cur.execute(
                    update_sql,
                    (
                        m.get("sender"),       to_email,    subject,
                        m.get("body", m.get("text", "")),
                        is_sent_flag,          batch_id,
                        m.get("outlook_message_id"),
                        created_at,            m.get("message_id")
                    )
                )

                if cur.rowcount == 0:                      # nothing updated → queue insert
                    inserts.append((
                        m.get("conversation_id"), m.get("receiver_type"), m.get("sender"),
                        m.get("sender"),          to_email,               None,
                        subject,                  m.get("body", m.get("text", "")),
                        m.get("debtor_number"),   m.get("debtor_id"),     m.get("user_id"),
                        is_sent_flag,             None,
                        created_at,               m.get("outlook_message_id"),
                        m.get("message_id"),      None,                   batch_id
                    ))

                processed += 1

            # ---------- bulk insert newbies -----------------
            if inserts:
                execute_batch(cur, insert_sql, inserts, page_size=500)

            pg_conn.commit()
            logger.info("PG-sync complete – %s processed, %s inserted (batch %s)",
                        processed, len(inserts), batch_id)
            return processed

        except Exception as e:
            if pg_conn:
                pg_conn.rollback()
            logger.error("Error syncing emails to PG: %s", e)
            return 0
        finally:
            if pg_conn:
                PostgresConnector.return_connection(pg_conn)

    def close(self):
        """Close the MongoDB client."""
        try:
            self.client.close()
            logger.info("MongoDB connection closed successfully.")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {str(e)}")


# =====================================
# PostgreSQL Operations
# =====================================

class PostgresConnector:
    """PostgreSQL operations for batch tracking and reporting."""
    
    # Class variable to hold the connection pool
    _pool = None
    
    @classmethod
    def _get_pool(cls):
        if cls._pool is None:
            # Debug: Print all variables
            print("DB_HOST:", os.getenv("DB_HOST"))
            print("DB_PORT:", os.getenv("DB_PORT"))
            print("DB_NAME:", os.getenv("DB_NAME"))
            print("DB_USERNAME:", os.getenv("DB_USERNAME"))
            print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))

            try:
                cls._pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=os.getenv("DB_HOST"),
                    port=int(os.getenv("DB_PORT", 5432)),
                    dbname=os.getenv("DB_NAME"),
                    user=os.getenv("DB_USERNAME"),
                    password=os.getenv("DB_PASSWORD")
                )
                logger.info("PostgreSQL connection pool initialized")
            except Exception as e:
                logger.error(f"Error initializing PostgreSQL connection pool: {str(e)}")
                cls._pool = None
        return cls._pool

    
    @classmethod
    def get_connection(cls):
        """Get a connection from the pool."""
        pool = cls._get_pool()
        if pool:
            try:
                conn = pool.getconn()
                conn.autocommit = False  # For transaction control
                return conn
            except Exception as e:
                logger.error(f"Error getting connection from pool: {str(e)}")
                return None
        return None
        
    @classmethod
    def return_connection(cls, conn):
        """Return a connection to the pool."""
        if cls._pool and conn:
            try:
                cls._pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {str(e)}")

    # Replace the current PostgresConnector.ping() method with this:
    @staticmethod
    def ping() -> bool:
        """
        Quick “is the database alive?” check.

        Returns
        -------
        bool
            True  – connection succeeded and a simple `SELECT 1` ran
            False – any exception occurred (logged for debugging)
        """
        try:
            # use the built-in context-manager so the connection
            # is automatically returned to the pool
            with PostgresConnector.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception as exc:
            logger.error("PostgreSQL ping failed: %s", exc)
            return False
            
    @classmethod
    def connection(cls):
        """Context manager for PostgreSQL connections.
        
        Usage:
            with PostgresConnector.connection() as conn:
                # Use the connection here
                # It will be automatically returned to the pool
        """
        class ConnectionManager:
            def __init__(self):
                self.conn = None
                
            def __enter__(self):
                self.conn = cls.get_connection()
                return self.conn
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.conn:
                    cls.return_connection(self.conn)
        
        return ConnectionManager()

    @staticmethod
    def execute_query(query, params=None, fetch=True, commit=True):
        """Execute a SQL query with connection handling and error recovery."""
        conn = None
        cursor = None
        try:
            conn = PostgresConnector.get_connection()
            if not conn:
                raise Exception("Failed to get database connection")
                
            cursor = conn.cursor(cursor_factory=extras.DictCursor if fetch else None)
            cursor.execute(query, params or ())
            
            result = None
            if fetch:
                result = cursor.fetchall()
            
            if commit:
                conn.commit()
                
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                PostgresConnector.return_connection(conn)

    @staticmethod
    def insert_batch_run():
        """
        Create a new batch run record in PostgreSQL.
        
        Returns:
            str: The UUID of the new batch run, or None if failed
        """
        try:
            batch_id = str(uuid.uuid4())
            query = """
                INSERT INTO core.batch_runs (id, status, created_at)
                VALUES (%s, %s, NOW())
            """
            
            PostgresConnector.execute_query(query, (batch_id, "in_progress"), fetch=False)
            logger.info(f"Created new batch run in PostgreSQL with ID: {batch_id}")
            return batch_id
        except Exception as e:
            logger.error(f"Error creating batch run: {str(e)}")
            return None

    @staticmethod
    def update_batch_result(batch_id, processed_count, failed_count, status="success", draft_count=0):
        """
        Update the batch result in PostgreSQL with processed count, failed count, and status.
        
        Args:
            batch_id (str): The batch ID to update
            processed_count (int): Number of successfully processed emails
            failed_count (int): Number of failed emails
            status (str): Status of the batch (success, partial, failed)
            draft_count (int): Number of emails saved as drafts
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not batch_id:
                logger.warning("Cannot update batch result: No batch_id provided")
                return False
                
            query = """
                UPDATE core.batch_runs
                SET 
                    processed_count = %s,
                    failed_count = %s,
                    status = %s,
                    draft_count = %s,
                    processed_at = NOW()
                WHERE id = %s
            """
            
            PostgresConnector.execute_query(query, (processed_count, failed_count, status, draft_count, batch_id), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error updating batch result: {str(e)}")
            return False

    @staticmethod
    def mark_batch_response_processed(batch_id):
        """
        Mark a batch as having responses processed.
        
        Args:
            batch_id (str): The batch ID to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not batch_id:
                logger.warning("Cannot mark batch response processed: No batch_id provided")
                return False
                
            query = """
                UPDATE core.batch_runs
                SET response_processed = true
                WHERE id = %s
            """
            
            PostgresConnector.execute_query(query, (batch_id,), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error marking batch response processed: {str(e)}")
            return False

    @staticmethod
    def mark_batch_mail_sent(batch_id):
        """
        Mark a batch as having emails sent.
        
        Args:
            batch_id (str): The batch ID to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not batch_id:
                logger.warning("Cannot mark batch mail sent: No batch_id provided")
                return False
                
            query = """
                UPDATE core.batch_runs
                SET mail_sent = true
                WHERE id = %s
            """
            
            PostgresConnector.execute_query(query, (batch_id,), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error marking batch mail sent: {str(e)}")
            return False

    @staticmethod
    def get_batch_flags(batch_id):
        """
        Get the response_processed and mail_sent flags for a batch.
        
        Args:
            batch_id (str): The batch ID to query
            
        Returns:
            tuple: (response_processed, mail_sent, status) or None if batch not found
        """
        try:
            if not batch_id:
                logger.warning("Cannot get batch flags: No batch_id provided")
                return None
                
            query = """
                SELECT response_processed, mail_sent, status
                FROM core.batch_runs
                WHERE id = %s
            """
            
            result = PostgresConnector.execute_query(query, (batch_id,))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting batch flags: {str(e)}")
            return None

    @staticmethod
    def get_failed_batches(timeout_minutes=15):
        """
        Get batches that are failed or timed out, excluding permanently failed batches
        and already successfully processed batches.
        
        Returns:
            list: List of dictionaries containing batch information
        """
        try:
            query = """
                SELECT id, status, processed_count, failed_count, draft_count
                FROM core.batch_runs
                WHERE (status = 'failed' OR (status = 'in_progress' AND created_at < NOW() - INTERVAL %s))
                AND status <> 'permanently_failed'
                AND status <> 'success'
            """
            
            results = PostgresConnector.execute_query(query, (f"{timeout_minutes} minutes",))
            
            batches = []
            for row in results:
                batches.append({
                    'id': row[0],
                    'status': row[1],
                    'processed_count': row[2],
                    'failed_count': row[3],
                    'draft_count': row[4]
                })
            
            return batches
        except Exception as e:
            logger.error(f"Error getting failed batches: {str(e)}")
            return []

    @staticmethod
    def mark_batch_permanently_failed(batch_id):
        """
        Mark a batch as permanently failed.
        
        Args:
            batch_id (str): The batch ID to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not batch_id:
                logger.warning("Cannot mark batch permanently failed: No batch_id provided")
                return False
                
            query = """
                UPDATE core.batch_runs
                SET status = 'permanently_failed'
                WHERE id = %s
            """
            
            PostgresConnector.execute_query(query, (batch_id,), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error marking batch permanently failed: {str(e)}")
            return False

    @staticmethod
    def get_batch_statistics(days=7):
        """
        Get statistics about batch runs for reporting.
        
        Args:
            days (int): Number of days to look back
            
        Returns:
            dict: Dictionary containing batch statistics
        """
        try:
            query = """
                SELECT 
                    COUNT(*) as total_batches,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_batches,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_batches,
                    SUM(CASE WHEN status = 'permanently_failed' THEN 1 ELSE 0 END) as perm_failed_batches,
                    SUM(processed_count) as total_processed,
                    SUM(failed_count) as total_failed,
                    SUM(draft_count) as total_drafts
                FROM core.batch_runs
                WHERE created_at > NOW() - INTERVAL %s
            """
            
            result = PostgresConnector.execute_query(query, (f"{days} days",))
            
            if result and result[0]:
                row = result[0]
                return {
                    'total_batches': row[0] or 0,
                    'successful_batches': row[1] or 0,
                    'failed_batches': row[2] or 0,
                    'perm_failed_batches': row[3] or 0,
                    'total_processed': row[4] or 0,
                    'total_failed': row[5] or 0,
                    'total_drafts': row[6] or 0,
                    'period_days': days
                }
            return {
                'total_batches': 0,
                'successful_batches': 0,
                'failed_batches': 0,
                'perm_failed_batches': 0,
                'total_processed': 0,
                'total_failed': 0,
                'total_drafts': 0,
                'period_days': days
            }
        except Exception as e:
            logger.error(f"Error getting batch statistics: {str(e)}")
            return {
                'total_batches': 0,
                'successful_batches': 0,
                'failed_batches': 0,
                'perm_failed_batches': 0,
                'total_processed': 0,
                'total_failed': 0,
                'total_drafts': 0,
                'period_days': days,
                'error': str(e)
            }
        
    


# =====================================
# Factory Functions (Public Interface)
# =====================================

@lru_cache(maxsize=1)
def get_mongo():
    """Returns a singleton instance of MongoConnector."""
    try:
        return MongoConnector()
    except Exception as e:
        logger.error(f"Error creating MongoDB connection: {str(e)}")
        return None

def get_postgres():
    """Returns a PostgreSQL connection from the pool."""
    return PostgresConnector.get_connection()


# =====================================
# Legacy Functions for Backward Compatibility
# =====================================

# This function is kept for backward compatibility
def get_postgres_connection():
    """Returns a new PostgreSQL connection from environment variables."""
    return get_postgres()


# For backward compatibility, expose the PostgresHelper class
# as an alias to the new PostgresConnector
class PostgresHelper:
    """
    Legacy class for backward compatibility.
    All methods are static and delegate to the PostgresConnector.
    """
    
    @staticmethod
    def get_pg_connection():
        return PostgresConnector.get_connection()
        
    @staticmethod
    def update_batch_result(batch_id, processed_count, failed_count, status="success", draft_count=0):
        return PostgresConnector.update_batch_result(batch_id, processed_count, failed_count, status, draft_count)
        
    @staticmethod
    def insert_batch_run():
        return PostgresConnector.insert_batch_run()
        
    @staticmethod
    def mark_batch_response_processed(batch_id):
        return PostgresConnector.mark_batch_response_processed(batch_id)
        
    @staticmethod
    def mark_batch_mail_sent(batch_id):
        return PostgresConnector.mark_batch_mail_sent(batch_id)
        
    @staticmethod
    def get_batch_flags(batch_id):
        return PostgresConnector.get_batch_flags(batch_id)
        
    @staticmethod
    def get_failed_batches(timeout_minutes=15):
        return PostgresConnector.get_failed_batches(timeout_minutes)
        
    @staticmethod
    def mark_batch_permanently_failed(batch_id):
        return PostgresConnector.mark_batch_permanently_failed(batch_id)