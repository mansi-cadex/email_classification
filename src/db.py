"""
db.py - Simplified database operations for email classification system.

Essential operations with SECURITY FIXES:
1. MongoDB for email storage and batch resumption (with TLS)
2. PostgreSQL for batch tracking (with TLS)
3. Basic synchronization
4. Issue #4: Database TLS enforcement for both MongoDB and PostgreSQL
"""

import os
import uuid
from functools import lru_cache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from typing import Dict, List, Optional, Any
from psycopg2 import pool
from psycopg2.extras import execute_batch
from src.log_config import logger
from dotenv import load_dotenv

load_dotenv()

RESPONSE_LABELS = ["invoice_request_no_info", "claims_paid_no_proof"]

class MongoConnector:
    """MongoDB operations for email classification system with TLS enforcement."""
    
    def __init__(self):
        self.uri = os.getenv("MONGO_URI")
        
        # SECURITY FIX Issue #4: Force TLS for MongoDB
        if self.uri and "tls=true" not in self.uri.lower() and "ssl=true" not in self.uri.lower():
            # Add TLS parameter to URI if not already present
            separator = "&" if "?" in self.uri else "?"
            self.uri += f"{separator}tls=true&tlsAllowInvalidCertificates=false"
            
        self.client = MongoClient(self.uri, tls=True, tlsAllowInvalidCertificates=False)
        self.db_name = os.getenv("MONGO_DB")
        self.collection_name = os.getenv("MONGO_COLLECTION")
        
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.batch_runs_collection = self.db["batch_runs"]
        
        # Basic indexes - handle existing indexes gracefully
        try:
            self.collection.create_index([("message_id", ASCENDING)], unique=True)
        except Exception as e:
            logger.info(f"MongoDB message_id index already exists or failed: {str(e)}")
        
        try:
            self.collection.create_index([("batch_id", ASCENDING)])
        except Exception as e:
            logger.info(f"MongoDB batch_id index already exists or failed: {str(e)}")
            
        try:
            self.batch_runs_collection.create_index([("id", ASCENDING)], unique=True)
        except Exception as e:
            logger.info(f"MongoDB batch_runs id index already exists or failed: {str(e)}")
        
        self.current_batch_id = None
        logger.info(f"Connected to MongoDB with TLS enforcement: {self.db_name}.{self.collection_name}")

    def set_batch_id(self, batch_id: str):
        """Set the current batch ID."""
        self.current_batch_id = batch_id
        return self

    def email_exists(self, message_id: str) -> bool:
        """Check if email exists."""
        if not message_id:
            return False
        return self.collection.count_documents({"message_id": message_id}, limit=1) > 0

    def insert_email(self, email_data: Dict[str, Any]):
        """Insert email document."""
        # Add timestamp and batch_id if missing
        if "created_at" not in email_data:
            email_data["created_at"] = datetime.utcnow()
        if "batch_id" not in email_data and self.current_batch_id:
            email_data["batch_id"] = self.current_batch_id
        
        try:
            result = self.collection.insert_one(email_data)
            logger.info(f"Email inserted: {result.inserted_id}")
            return result
        except DuplicateKeyError:
            logger.warning(f"Duplicate email: {email_data.get('message_id')}")
            return None

    def update_message_id(self, old_id: str, new_id: str) -> bool:
        """Update message ID after folder move."""
        if not old_id or not new_id:
            return False
            
        result = self.collection.update_one(
            {"message_id": old_id},
            {"$set": {"message_id": new_id, "previous_message_id": old_id}}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated message ID: {old_id} -> {new_id}")
            return True
        return False

    def mark_email_sent(self, message_id: str):
        """Mark email as sent."""
        if not message_id:
            return None
            
        result = self.collection.update_one(
            {"message_id": message_id},
            {"$set": {"response_sent": True, "response_timestamp": datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            logger.info(f"Marked email sent: {message_id}")
        return result

    def mark_email_draft_saved(self, message_id: str, draft_id: Optional[str] = None):
        """Mark email as draft saved."""
        if not message_id:
            return None
            
        update_data = {"draft_saved": True, "draft_timestamp": datetime.utcnow()}
        if draft_id:
            update_data["draft_id"] = draft_id
        
        result = self.collection.update_one(
            {"message_id": message_id},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"Marked email draft saved: {message_id}")
        return result

    def update_batch_result(self, batch_id: str, processed_count: int, failed_count: int, 
                           draft_count: int = 0, status: str = "success"):
        """Update batch result in MongoDB - matches loop.py signature."""
        if not batch_id:
            return None
            
        update_data = {
            "status": status,
            "processed_at": datetime.utcnow(),
            "processed_count": processed_count,
            "failed_count": failed_count,
            "draft_count": draft_count
        }
        
        result = self.batch_runs_collection.update_one(
            {"id": batch_id},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated MongoDB batch {batch_id}: {status}")
        return result

    def sync_batch_emails_to_postgres(self, batch_id: str) -> int:
        """Sync MongoDB emails to PostgreSQL."""
        if not batch_id:
            return 0

        pg_conn = get_postgres()
        if not pg_conn:
            return 0

        mails = self.collection.find({"batch_id": batch_id})
        inserts = []
        processed = 0

        update_sql = """
            UPDATE core.account_email SET
                conversation_id = %s, receiver_type = %s, sender_name = %s,
                from_email = %s, to_email = %s, cc = %s, email_subject = %s, email_body = %s,
                debtor_number = %s, debtor_id = %s, user_id = %s, is_sent = %s, eml_file = %s,
                created_at = COALESCE(created_at, %s), outlook_message_id = %s, hash = %s, batch_id = %s
            WHERE message_id = %s
        """

        insert_sql = """
            INSERT INTO core.account_email (
                conversation_id, receiver_type, sender_name, from_email, to_email, cc, 
                email_subject, email_body, debtor_number, debtor_id, user_id, is_sent, 
                eml_file, created_at, outlook_message_id, message_id, hash, batch_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cur = pg_conn.cursor()

        for m in mails:
            # Map MongoDB fields to PostgreSQL schema
            to_email = m.get("recipient", "")
            subject = m.get("subject", "No Subject")
            created_at = m.get("received_at") or m.get("created_at") or datetime.utcnow()
            is_sent_flag = bool(m.get("response_sent"))
            
            # Model data
            debtor_number = m.get("debtor_number", "")
            debtor_id = m.get("debtor_id")
            
            # Schema fields
            conversation_id = m.get("conversation_id", "")
            receiver_type = m.get("receiver_type", "")
            sender_name = m.get("sender_name", "")
            cc = m.get("cc")
            user_id = m.get("user_id")
            eml_file = m.get("eml_file")
            hash_value = m.get("hash")

            # Try UPDATE first
            cur.execute(update_sql, (
                conversation_id, receiver_type, sender_name, m.get("sender"), to_email, cc, subject,
                m.get("body", ""), debtor_number, debtor_id, user_id, is_sent_flag, eml_file, 
                created_at, m.get("outlook_message_id"), hash_value, batch_id, m.get("message_id")
            ))

            # If no update, queue for insert
            if cur.rowcount == 0:
                inserts.append((
                    conversation_id, receiver_type, sender_name, m.get("sender"), to_email, cc,
                    subject, m.get("body", ""), debtor_number, debtor_id, user_id, is_sent_flag, 
                    eml_file, created_at, m.get("outlook_message_id"), m.get("message_id"), 
                    hash_value, batch_id
                ))

            processed += 1

        # Bulk insert new emails
        if inserts:
            execute_batch(cur, insert_sql, inserts, page_size=500)

        pg_conn.commit()
        logger.info(f"PostgreSQL sync: {processed} processed, {len(inserts)} inserted")
        
        PostgresConnector.return_connection(pg_conn)
        return processed


class PostgresConnector:
    """PostgreSQL operations for batch tracking with TLS enforcement."""
    
    _pool = None
    
    @classmethod
    def _get_pool(cls):
        if cls._pool is None:
            cls._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 5432)),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                sslmode="require"  # SECURITY FIX Issue #4: Force TLS encryption
            )
            logger.info("PostgreSQL connection pool initialized with TLS enforcement")
        return cls._pool

    @classmethod
    def get_connection(cls):
        """Get connection from pool."""
        pool = cls._get_pool()
        if pool:
            conn = pool.getconn()
            conn.autocommit = False
            return conn
        return None
        
    @classmethod
    def return_connection(cls, conn):
        """Return connection to pool."""
        if cls._pool and conn:
            cls._pool.putconn(conn)

    @staticmethod
    def insert_batch_run():
        """Create new batch run in PostgreSQL."""
        batch_id = str(uuid.uuid4())
        query = """
            INSERT INTO core.batch_runs (id, status, created_at, response_processed, mail_sent) 
            VALUES (%s, %s, NOW(), %s, %s)
        """
        
        conn = PostgresConnector.get_connection()
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (batch_id, "in_progress", False, False))
            conn.commit()
            PostgresConnector.return_connection(conn)
            logger.info(f"Created PostgreSQL batch: {batch_id}")
            return batch_id
        return None

    @staticmethod
    def update_batch_result(batch_id, processed_count, failed_count, status="success", draft_count=0):
        """Update batch result in PostgreSQL - matches loop.py signature."""
        if not batch_id:
            return False
        
        # Determine response_processed based on draft_count
        response_processed = draft_count > 0
        mail_sent = False  # Always False for now as per your requirement
        
        query = """
            UPDATE core.batch_runs
            SET processed_count = %s, failed_count = %s, status = %s, 
                draft_count = %s, processed_at = NOW(), response_processed = %s, mail_sent = %s
            WHERE id = %s
        """
        
        conn = PostgresConnector.get_connection()
        if conn:
            with conn.cursor() as cur:
                cur.execute(query, (processed_count, failed_count, status, draft_count, response_processed, mail_sent, batch_id))
            conn.commit()
            PostgresConnector.return_connection(conn)
            logger.info(f"Updated PostgreSQL batch {batch_id}: {status}, response_processed: {response_processed}")
            return True
        return False

    @staticmethod
    def ensure_batch_record_exists(batch_id: str) -> bool:
        """Ensure batch record exists in PostgreSQL - used by loop.py."""
        if not batch_id:
            return False
        
        conn = PostgresConnector.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM core.batch_runs WHERE id = %s", (batch_id,))
                if cur.fetchone() is None:
                    # Insert if not exists - include required fields
                    cur.execute(
                        """
                        INSERT INTO core.batch_runs (id, status, created_at, response_processed, mail_sent)
                        VALUES (%s, %s, NOW(), %s, %s)
                        """,
                        (batch_id, "in_progress", False, False)
                    )
                    conn.commit()
                    logger.info(f"Created missing batch record in PostgreSQL: {batch_id}")
            return True
        finally:
            PostgresConnector.return_connection(conn)

    @staticmethod
    def update_batch_id_only(batch_id, limit=1, email_data=None):
        """Insert tracking record with batch_id - used by loop.py."""
        if not batch_id:
            return 0

        conn = PostgresConnector.get_connection()
        if not conn:
            return 0
        
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Use real email data when available
                if email_data and isinstance(email_data, dict):
                    to_email = email_data.get('to_email', email_data.get('recipient', ''))
                    from_email = email_data.get('from_email', email_data.get('sender', ''))
                    email_subject = email_data.get('subject', email_data.get('email_subject', ''))
                    is_sent = email_data.get('is_sent', False)
                    debtor_number = email_data.get('debtor_number', '')
                    debtor_id = email_data.get('debtor_id', None)
                else:
                    # Minimal values if no email_data
                    to_email = ''
                    from_email = 'system@abc-amega.com'
                    email_subject = ''
                    is_sent = False
                    debtor_number = ''
                    debtor_id = None
                
                # Insert tracking record
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
            PostgresConnector.return_connection(conn)

# Factory Functions
@lru_cache(maxsize=1)
def get_mongo():
    """Get MongoDB connection."""
    return MongoConnector()

def get_postgres():
    """Get PostgreSQL connection."""
    return PostgresConnector.get_connection()


# Functions used by loop.py - compatibility layer
def ensure_batch_record_exists(batch_id: str) -> bool:
    """Ensure batch record exists in both PostgreSQL and MongoDB."""
    if not batch_id:
        return False
    
    # Ensure PostgreSQL record
    pg_success = PostgresConnector.ensure_batch_record_exists(batch_id)
    
    # Ensure MongoDB record  
    mongo = get_mongo()
    if mongo:
        batch = mongo.batch_runs_collection.find_one({"id": batch_id})
        if not batch:
            mongo.batch_runs_collection.insert_one({
                "id": batch_id,
                "status": "in_progress",
                "created_at": datetime.utcnow(),
                "processed_count": 0,
                "failed_count": 0,
                "draft_count": 0
            })
            logger.info(f"Created missing batch record in MongoDB: {batch_id}")
    
    return pg_success

def update_batch_id_only(batch_id, limit=1, email_data=None):
    """Insert tracking record - used by loop.py."""
    return PostgresConnector.update_batch_id_only(batch_id, limit, email_data)


# Legacy Compatibility
class PostgresHelper:
    """Legacy compatibility class."""
    
    @staticmethod
    def update_batch_result(batch_id, processed_count, failed_count, status="success", draft_count=0):
        return PostgresConnector.update_batch_result(batch_id, processed_count, failed_count, status, draft_count)
        
    @staticmethod
    def insert_batch_run():
        return PostgresConnector.insert_batch_run()
