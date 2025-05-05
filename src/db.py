import os
import sys
import uuid
from functools import lru_cache
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import psycopg2
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from log_config import logger

# PostgreSQL connection
def get_postgres_connection():
    """Returns a new PostgreSQL connection from environment variables."""
    try:
        return psycopg2.connect(
            dbname=os.getenv("PGDATABASE", "email_batch_test"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "0000"),
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432")
        )
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise

# Consistent list of allowed labels
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

class MongoConnector:
    def __init__(self, uri=None, db_name=None, collection_name=None):
        """Initialize MongoDB connection with database and collections."""
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.client = MongoClient(self.uri)
        self.db_name = db_name or os.getenv("MONGO_DB", "emailDB")
        self.collection_name = collection_name or os.getenv("MONGO_COLLECTION", "classified_emails")
        self.db = self.client[self.db_name]
        
        # Initialize collections
        self.collection = self.db[self.collection_name]
        self.template_collection = self.db["email_templates"]
        self.payment_collection = self.db["payments"]
        self.contact_collection = self.db["contacts"]
        self.archive_collection = self.db["archived_emails"]
        self.billing_collection = self.db["billing_records"]
        self.batch_runs_collection = self.db["batch_runs"]
        
        self._setup_indexes()
        logger.info(f"Connected to MongoDB: db={self.db_name}, collection={self.collection_name}")

    def _setup_indexes(self):
        """Create necessary indexes for efficient queries."""
        # Emails collection indexes
        index_definitions = [
            ([("message_id", ASCENDING)], {"unique": True}),
            ([("prediction", ASCENDING)], {}),
            ([("manual_review", ASCENDING)], {}),
            ([("sender", ASCENDING)], {}),
            ([("created_at", ASCENDING)], {}),
            ([("batch_id", ASCENDING)], {}),
            ([("response_process", ASCENDING)], {}),
            ([("save_as_draft", ASCENDING)], {})
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

    def _validate_and_process_email(self, email_data):
        """Validate and process email data before inserting/updating."""
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
        if "batch_id" not in email_data and hasattr(self, 'current_batch_id'):
            email_data["batch_id"] = self.current_batch_id
            
        return email_data

    def insert_email(self, email_data: dict):
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
            
            # Note: Removed the call to _update_batch_id_in_postgres
            
            return result
        except DuplicateKeyError:
            logger.warning(f"Duplicate email with message_id: {email_data.get('message_id')}")
            return None
        except Exception as e:
            logger.error(f"Error inserting email data: {str(e)}")
            return None

    # Removed _update_batch_id_in_postgres method completely
    
    # Payment record methods
    def create_or_update_payment_record(self, payment_data):
        """Create or update a payment record in the payment collection."""
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
        
    def get_payment_record_by_invoice(self, invoice_number):
        """Get a payment record by invoice number."""
        return self.payment_collection.find_one({"invoice_number": invoice_number})
        
    def update_payment_record(self, invoice_number, update_data):
        """Update specific fields in a payment record."""
        update_data["updated_at"] = datetime.utcnow()
        result = self.payment_collection.update_one(
            {"invoice_number": invoice_number},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated payment record for invoice {invoice_number}")
        
        return result
    
    # Batch operations    
    def set_batch_id(self, batch_id):
        """Set the current batch ID for subsequent operations."""
        self.current_batch_id = batch_id
        return self

    def insert_batch_run(self):
        """Create a new batch run record and return its ID."""
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
    
    def update_batch_result(self, batch_id, processed_count, failed_count, draft_count=0, status="success", error_log=None):
        """Update a batch run record with results."""
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

    def mark_batch_permanently_failed(self, batch_id, reason=None):
        """Mark a batch as permanently failed so it won't be retried."""
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
    
    def increment_batch_retry(self, batch_id):
        """Increment the retry counter for a batch."""
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
    
    def get_batch_retry_info(self, batch_id):
        """Get retry information for a batch."""
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
            return {"retry_count": 0, "permanently_failed": False, "last_retry": None, "status": "unknown"}
            
    def sync_batch_emails_to_postgres(self, batch_id):
        """
        This method is now a stub in the decoupled architecture.
        It returns 0 to maintain compatibility with existing code.
        In the decoupled system, PostgreSQL synchronization happens separately.
        """
        logger.debug(f"sync_batch_emails_to_postgres called for batch_id {batch_id} - no action needed in decoupled architecture")
        return 0
    
    # Added new method as requested
    def find_emails(self, query, *args, **kwargs):
        """Wrapper for collection.find() to get emails matching a query."""
        return list(self.collection.find(query, *args, **kwargs))
    
    # Email operations
    def email_exists(self, message_id: str, sender: str = None, subject: str = None, received_at: str = None):
        """Check if an email with the same message_id or (sender, subject, received_at) exists."""
        if message_id:
            existing_email = self.collection.find_one({"message_id": message_id})
            if existing_email:
                return existing_email.get("response_sent") is not None
        if sender and subject and received_at:
            duplicate_email = self.collection.find_one({
                "sender": sender,
                "subject": subject,
                "received_at": received_at
            })
            if duplicate_email:
                return duplicate_email.get("response_sent") is not None
        return False

    def find_pending_responses(self, limit=50):
        """Find emails that need a response but don't have one yet."""
        query = {
            "response_sent": False,
            "manual_review": False,
            "save_as_draft": False
        }
        return list(self.collection.find(query).limit(limit))
            
    def find_draft_emails(self, batch_id=None, limit=50):
        """Find emails that should be saved as drafts."""
        query = {
            "response_sent": False,
            "save_as_draft": True,
            "response_process": False  # Not yet processed into a draft
        }
        
        if batch_id:
            query["batch_id"] = batch_id
            
        return list(self.collection.find(query).limit(limit))

    def update_email_with_response(self, message_id: str, response_text: str):
        """Attach generated response to an email entry."""
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

    def fetch_unreplied_emails(self, batch_id):
        """Get all emails in a batch that haven't been replied to yet."""
        query = {
            "batch_id": batch_id,
            "response_sent": False,
            "save_as_draft": False  # Don't retry emails meant for drafts
        }
        return list(self.collection.find(query))

    def mark_email_sent(self, message_id):
        """Mark an email as having been sent a response."""
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
            
    def mark_email_draft_saved(self, message_id, draft_id=None):
        """Mark an email as having been saved as a draft."""
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
            
    def _update_contact_info(self, original_email: str, new_contacts: list):
        """Update contact information based on auto-reply or no-reply emails."""
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

    def get_contact_by_email(self, email: str):
        """Get contact information by email address."""
        if not email:
            return None
            
        # First try as original email
        contact = self.contact_collection.find_one({"original_email": email})
        if contact:
            return contact
            
        # Then try as alternative contact
        contact = self.contact_collection.find_one({"alternative_contacts": email})
        return contact

    def close(self):
        """Close the MongoDB client."""
        self.client.close()
        logger.info("MongoDB connection closed successfully.")

@lru_cache(maxsize=1)
def get_mongo():
    """Returns a singleton instance of MongoConnector."""
    return MongoConnector()

# PostgreSQL helper functions - Clean implementation
class PostgresHelper:
    @staticmethod
    @staticmethod
    def execute(query, params=None, fetch_one=False, fetch_all=False):
        """Execute a PostgreSQL query with error handling."""
        conn = None
        cur = None
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()
            cur.execute(query, params or ())
            
            if fetch_one:
                result = cur.fetchone()
            elif fetch_all:
                result = cur.fetchall()
            else:
                result = None
                
            conn.commit()
            return result
        except Exception as e:
            logger.error(f"PostgreSQL error: {str(e)}")
            if conn:
                conn.rollback()
            return None
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def insert_batch_run():
        """Create a new batch run in PostgreSQL and return its ID."""
        batch_id = str(uuid.uuid4())
        result = PostgresHelper.execute(
            "INSERT INTO core.batch_runs (id, status, created_at) VALUES (%s, %s, now()) RETURNING id",
            (batch_id, "in_progress"),
            fetch_one=True
        )
        
        if result:
            logger.info(f"Created new batch run in PostgreSQL with ID: {batch_id}")
            return batch_id
        return None

    @staticmethod
    def update_batch_result(batch_id, processed, failed, status="success", draft_count=0):
        """Update a batch run record in PostgreSQL with results."""
        result = PostgresHelper.execute(
            """UPDATE core.batch_runs
               SET status = %s, processed_at = now(), processed_count = %s,
                   failed_count = %s, draft_count = %s
               WHERE id = %s""",
            (status, processed, failed, draft_count, batch_id)
        )
        
        if result is not None:
            logger.info(f"Updated batch {batch_id} in PostgreSQL with status: {status}")
            return True
        return False

    @staticmethod
    def get_failed_batches(timeout_minutes=15):
        """Get all failed or timed-out in-progress batches from PostgreSQL."""
        rows = PostgresHelper.execute(
            """
            SELECT id, status, created_at, processed_count, failed_count, draft_count
            FROM core.batch_runs
            WHERE ((status = 'failed' AND processed_at IS NULL)
                 OR (status = 'in_progress' AND created_at < now() - interval '%s minutes'))
                 AND (status != 'permanently_failed')
            """,
            (timeout_minutes,),
            fetch_all=True
        )
        
        if not rows:
            return []
            
        # Convert to list of dictionaries
        batches = []
        for row in rows:
            batches.append({
                "id": row[0],
                "status": row[1],
                "created_at": row[2],
                "processed_count": row[3],
                "failed_count": row[4],
                "draft_count": row[5] if len(row) > 5 else 0
            })
        
        return batches

    @staticmethod
    def mark_batch_permanently_failed(batch_id):
        """Mark a batch as permanently failed in PostgreSQL."""
        result = PostgresHelper.execute(
            "UPDATE core.batch_runs SET status = 'permanently_failed' WHERE id = %s",
            (batch_id,)
        )
        
        if result is not None:
            logger.info(f"Marked batch {batch_id} as permanently failed in PostgreSQL")
            return True
        return False

    @staticmethod
    def mark_batch_response_processed(batch_id):
        """Mark a batch as having completed response generation."""
        result = PostgresHelper.execute(
            "UPDATE core.batch_runs SET response_processed = true WHERE id = %s",
            (batch_id,)
        )
        
        if result is not None:
            logger.info(f"Marked batch {batch_id} as response processed")
            return True
        return False

    @staticmethod
    def mark_batch_mail_sent(batch_id):
        """Mark a batch as having all emails sent."""
        result = PostgresHelper.execute(
            "UPDATE core.batch_runs SET mail_sent = true WHERE id = %s",
            (batch_id,)
        )
        
        if result is not None:
            logger.info(f"Marked batch {batch_id} as mail sent")
            return True
        return False

    @staticmethod
    def get_batch_flags(batch_id):
        """Get the processing flags for a batch."""
        return PostgresHelper.execute(
            "SELECT response_processed, mail_sent, status FROM core.batch_runs WHERE id = %s", 
            (batch_id,),
            fetch_one=True
        )