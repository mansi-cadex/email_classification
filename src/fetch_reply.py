"""
fetch_reply.py - Module for fetching, classifying, and moving emails.

Updated with UNIFIED signal handling for Issue #6:
1. Email fetching from multiple accounts
2. Model API integration with retry logic (Issue #2 fixed)
3. Email classification and folder organization
4. Clean text extraction without threads
5. Reply generation with threaded drafts (during processing)
6. Complete email data structure for MongoDB
7. Proper message ID handling after folder moves
8. UNIFIED threading-based stop mechanism
"""

import os
import time
import threading
import httpx
import msal
import requests
import re
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple
from dotenv import load_dotenv
from src.db import get_mongo, PostgresHelper
from src.log_config import logger

load_dotenv()

# Configuration
MS_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN", "abc-amega.com")
MODEL_API_URL = "http://104.197.197.76:8000"
COMPANY_NAME = os.getenv("COMPANY_NAME", "ABC/AMEGA")

# Updated list of allowed labels
ALLOWED_LABELS = [
    "no_reply_no_info",
    "no_reply_with_info", 
    "auto_reply_no_info",
    "auto_reply_with_info",
    "invoice_request_no_info",
    "claims_paid_no_proof",
    "claims_paid_with_proof",
    "manual_review",
    "uncategorised"
]
# Labels that should receive responses
RESPONSE_LABELS = [
    "invoice_request_no_info",
    "claims_paid_no_proof"
]

def validate_config():
    """Validate required environment variables."""
    required_vars = ["CLIENT_ID", "CLIENT_SECRET", "TENANT_ID", "EMAIL_ADDRESS"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    logger.info("Configuration validation passed")

class ModelAPIClient:
    """Client for model API calls - 60 second timeout with manual review fallback."""
    
    def __init__(self):
        self.base_url = MODEL_API_URL
        
    def health_check(self):
        """Quick health check with timeout."""
        try:
            response = requests.get(f"{self.base_url}/api/health", timeout=10)
            return response.status_code == 200
        except:
            return False

    def process_email_complete(self, subject, body, headers=None, sender_email=None, 
                             recipient_emails=None, has_attachments=False, had_threads=False):
        """Process email with model API - 60 second timeout, immediate response."""
        payload = {
            "subject": subject,
            "body": body,
            "headers": headers or [],
            "sender_email": sender_email,
            "recipient_emails": recipient_emails or [],
            "has_attachments": has_attachments,
            "had_threads": had_threads
        }
        
        try:
            logger.info("Calling model API (60s timeout)...")
            start_time = time.time()
            
            # Single 60-second timeout - no retries needed
            response = requests.post(
                f"{self.base_url}/api/process_email_complete", 
                json=payload, 
                timeout=180  # 60 seconds max
            )
            response.raise_for_status()
            result = response.json()
            
            # Log actual processing time
            elapsed = time.time() - start_time
            logger.info(f"Model API completed in {elapsed:.1f}s: {result.get('event_type', 'unknown')}")
            
            return result
            
        except requests.exceptions.Timeout:
            logger.warning(f"Model API timeout after 60s - classifying as manual_review")
            return self._get_manual_review_fallback()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Model API request error: {e} - classifying as manual_review")
            return self._get_manual_review_fallback()
            
        except Exception as e:
            logger.error(f"Unexpected model API error: {e} - classifying as manual_review")
            return self._get_manual_review_fallback()

    def generate_reply(self, subject, body, label, sender_name=None, entities=None):
        """Generate reply - 60 second timeout, no reply if timeout."""
        if label not in RESPONSE_LABELS:
            return ""
            
        payload = {
            "subject": subject,
            "body": body,
            "label": label,
            "sender_name": sender_name,
            "entities": entities or {}
        }
        
        try:
            logger.info(f"Generating reply for {label} (60s timeout)...")
            start_time = time.time()
            
            # Single 60-second timeout for reply generation too
            response = requests.post(
                f"{self.base_url}/api/generate_reply", 
                json=payload, 
                timeout=60  # 60 seconds max
            )
            response.raise_for_status()
            result = response.json()
            reply = result.get("reply", "")
            
            # Log actual processing time
            elapsed = time.time() - start_time
            logger.info(f"Reply generated in {elapsed:.1f}s for {label}: {len(reply)} chars")
            
            return reply
            
        except requests.exceptions.Timeout:
            logger.warning(f"Reply generation timeout after 60s - no reply will be generated")
            return ""
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Reply generation error: {e} - no reply will be generated")
            return ""
            
        except Exception as e:
            logger.error(f"Unexpected reply error: {e} - no reply will be generated")
            return ""

    def _get_manual_review_fallback(self):
        """Fallback response for failed API calls - sends to manual review."""
        logger.info("Using manual_review fallback - email will need human review")
        return {
            "debtor_number": "",
            "event_type": "manual_review",
            "target_folder": "manual_review",
            "reply_sent": "no_response",
            "new_contact_email": "",
            "new_contact_phone": "",
            "contact_status": "active",
            "cleaned_body": ""
        }

def html_to_text(html_content):
    """Convert HTML to clean text - exactly like test code."""
    if not html_content:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html_content)
    
    # Clean up HTML entities
    entities = {
        '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
        '&quot;': '"', '&#39;': "'", '\r\n': '\n', '\r': '\n'
    }
    
    for entity, replacement in entities.items():
        text = text.replace(entity, replacement)
    
    # Clean up whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    
    return text.strip()

def extract_clean_email_content(msg):
    """Extract clean email content without HTML and threads - exactly like test code."""
    clean_body = ""
    data_source = ""
    had_threads = False
    
    # Try uniqueBody first (excludes threads)
    unique_body = msg.get("uniqueBody", {})
    full_body = msg.get("body", {})
    
    # Check if email has threads
    if unique_body and unique_body.get("content") and full_body and full_body.get("content"):
        unique_content = unique_body.get("content", "").strip()
        full_content = full_body.get("content", "").strip()
        
        if len(unique_content) > 0 and len(full_content) > len(unique_content) * 1.2:
            had_threads = True
    
    if unique_body and unique_body.get("content"):
        content = unique_body.get("content", "").strip()
        content_type = unique_body.get("contentType", "").lower()
        
        if content_type == "text":
            clean_body = content
            data_source = "uniqueBody_text"
        elif content_type == "html":
            clean_body = html_to_text(content)
            data_source = "uniqueBody_html"
    
    # Fallback to full body
    if not clean_body and full_body and full_body.get("content"):
        content = full_body.get("content", "").strip()
        content_type = full_body.get("contentType", "").lower()
        
        if content_type == "text":
            clean_body = content
            data_source = "body_text"
        elif content_type == "html":
            clean_body = html_to_text(content)
            data_source = "body_html"
    
    # Last resort: bodyPreview
    if not clean_body:
        clean_body = msg.get("bodyPreview", "").strip()
        data_source = "bodyPreview"
    
    return clean_body, data_source, had_threads

class MSGraphClient:
    """Microsoft Graph API client for email operations."""
    
    def __init__(self):
        self.base_url = MS_GRAPH_BASE_URL
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        
        # Parse multiple email addresses
        email_env = EMAIL_ADDRESS or ""
        if "," in email_env:
            self.email_addresses = [email.strip() for email in email_env.split(",")]
        else:
            self.email_addresses = [email_env] if email_env else []
        
        self._token_cache = {"token": None, "expires_at": 0}
        
        logger.info(f"MSGraphClient initialized for {len(self.email_addresses)} email(s)")
        
    def get_access_token(self):
        """Get a valid access token."""
        current_time = time.time()
        if self._token_cache.get("token") and self._token_cache.get("expires_at", 0) > current_time + 60:
            return self._token_cache["token"]
        
        validate_config()
        
        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}"
        )
        
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" in result:
            self._token_cache = {
                "token": result["access_token"],
                "expires_at": current_time + result.get("expires_in", 3600)
            }
            return result["access_token"]
        else:
            raise Exception(f"Failed to acquire token: {result.get('error_description')}")

    def fetch_unread_emails_from_account(self, email_address, max_emails):
        """Fetch unread emails from a single account - OLDEST FIRST like test code."""
        params = {
            "$orderby": "receivedDateTime asc",  # OLDEST FIRST (like test code)
            "$filter": "isRead eq false and isDraft eq false",
            "$select": "id,subject,from,body,bodyPreview,uniqueBody,receivedDateTime,hasAttachments,toRecipients,ccRecipients,internetMessageHeaders,conversationId",
            "$top": max_emails
        }
        
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/users/{email_address}/mailFolders/inbox/messages"
        
        response = httpx.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        
        emails = response.json().get("value", [])
        
        # Add account info to each email
        for email in emails:
            email["source_account"] = email_address
        
        logger.info(f"Fetched {len(emails)} unread emails from {email_address}")
        return emails

    def fetch_unread_emails(self, batch_size):
        """Fetch unread emails from all accounts with simple distribution."""
        all_emails = []
        
        # Step 1: Check availability in each account
        account_availability = {}
        total_available = 0
        
        for email_address in self.email_addresses:
            access_token = self.get_access_token()
            count_headers = {
                "Authorization": f"Bearer {access_token}",
                "ConsistencyLevel": "eventual"
            }
            count_url = f"{self.base_url}/users/{email_address}/mailFolders/inbox/messages/$count"
            count_params = {"$filter": "isRead eq false and isDraft eq false"}
            
            response = httpx.get(count_url, headers=count_headers, params=count_params, timeout=60)
            available_count = int(response.text) if response.status_code == 200 else 0
            
            account_availability[email_address] = available_count
            total_available += available_count
            
            logger.info(f"Account {email_address}: {available_count} unread emails")
        
        if total_available == 0:
            logger.info("No unread emails found")
            return all_emails
        
        # Step 2: Calculate emails to process
        emails_to_process = min(batch_size, total_available)
        logger.info(f"Processing {emails_to_process} emails from {total_available} available")
        
        # Step 3: Simple distribution - priority to accounts with most emails
        sorted_accounts = sorted(account_availability.items(), key=lambda x: x[1], reverse=True)
        
        distribution_plan = {}
        remaining_to_fetch = emails_to_process
        
        for email_address, available_count in sorted_accounts:
            if remaining_to_fetch <= 0:
                distribution_plan[email_address] = 0
                continue
            
            to_fetch = min(available_count, remaining_to_fetch)
            distribution_plan[email_address] = to_fetch
            remaining_to_fetch -= to_fetch
        
        # Log the distribution plan
        logger.info("Distribution plan:")
        for email_address, to_fetch in distribution_plan.items():
            available = account_availability[email_address]
            logger.info(f"  {email_address}: {to_fetch}/{available} emails")
        
        # Step 4: Fetch emails according to the distribution plan
        total_fetched = 0
        for email_address in self.email_addresses:
            emails_to_fetch = distribution_plan.get(email_address, 0)
            
            if emails_to_fetch <= 0:
                continue
                
            emails = self.fetch_unread_emails_from_account(email_address, emails_to_fetch)
            all_emails.extend(emails)
            total_fetched += len(emails)
            
            logger.info(f"Collected {len(emails)}/{emails_to_fetch} emails from {email_address}")
        
        logger.info(f"Batch complete: {total_fetched} emails collected")
        return all_emails

    def move_email_to_folder(self, message_id, folder_id, email_address):
        """Move email to folder."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{email_address}/messages/{message_id}/move"
        payload = {"destinationId": folder_id}
        
        response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        new_id = response.json().get("id", message_id)
        logger.info(f"Moved email {message_id} to folder {folder_id}")
        return True, new_id

    def mark_email_read(self, message_id, email_address, is_read=True):
        """Mark email as read/unread."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{email_address}/messages/{message_id}"
        payload = {"isRead": is_read}
        
        response = httpx.patch(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return True

    def create_threaded_reply_draft(self, original_message_id, reply_text, from_account):
        """Create threaded reply draft using createReply method - EXACTLY like test code."""
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Add footer
        footer = f"\n\n---\nThis email was generated automatically by {COMPANY_NAME} System.\nSent on: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
        body_with_footer = reply_text + footer
        
        try:
            # STEP 1: Create reply draft using createReply (this is what worked in test!)
            create_reply_endpoint = f"{self.base_url}/users/{from_account}/messages/{original_message_id}/createReply"
            
            response = httpx.post(create_reply_endpoint, headers=headers, json={}, timeout=30)
            
            if response.status_code in [200, 201]:
                draft_data = response.json()
                draft_id = draft_data.get("id")
                
                if draft_id:
                    logger.info(f"Reply draft created: {draft_id}")
                    
                    # STEP 2: Update the draft with our content
                    update_payload = {
                        "body": {
                            "contentType": "Text",
                            "content": body_with_footer
                        }
                    }
                    
                    update_endpoint = f"{self.base_url}/users/{from_account}/messages/{draft_id}"
                    update_response = httpx.patch(update_endpoint, headers=headers, json=update_payload, timeout=30)
                    
                    if update_response.status_code in [200, 204]:
                        logger.info(f"Threaded draft body updated successfully")
                        return draft_id
                    else:
                        logger.warning(f"Draft created but body update failed: {update_response.status_code}")
                        return draft_id  # Return anyway, as draft was created
                else:
                    logger.error("No draft ID returned from createReply")
            else:
                logger.error(f"createReply failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error creating threaded reply: {e}")
        
        return None

    def ensure_classification_folders(self, email_address):
        """Ensure classification folders exist."""
        def _normalize_folder_name(name):
            return re.sub(r"\s+", " ", name.strip().lower())

        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        parent_name = "Email Classifications"
        folder_map = {}

        # Get all folders
        response = httpx.get(f"{self.base_url}/users/{email_address}/mailFolders?$top=1000",
                             headers=headers, timeout=30)
        response.raise_for_status()
        all_folders = response.json()["value"]

        # Find or create parent folder
        parent_id = None
        normalized_parent = _normalize_folder_name(parent_name)
        for folder in all_folders:
            if _normalize_folder_name(folder["displayName"]) == normalized_parent:
                parent_id = folder["id"]
                break

        if not parent_id:
            response = httpx.post(f"{self.base_url}/users/{email_address}/mailFolders",
                                  headers=headers, json={"displayName": parent_name}, timeout=30)
            response.raise_for_status()
            parent_id = response.json()["id"]
            logger.info(f"Created parent folder: {parent_name}")

        # Get child folders
        response = httpx.get(f"{self.base_url}/users/{email_address}/mailFolders/{parent_id}/childFolders",
                             headers=headers, timeout=30)
        response.raise_for_status()
        child_folders = response.json()["value"]

        # Create missing child folders
        for label in ALLOWED_LABELS:
            display_name = label.replace("_", " ").title()
            normalized_display_name = _normalize_folder_name(display_name)

            folder_id = None
            for folder in child_folders:
                if _normalize_folder_name(folder["displayName"]) == normalized_display_name:
                    folder_id = folder["id"]
                    break

            if not folder_id:
                response = httpx.post(
                    f"{self.base_url}/users/{email_address}/mailFolders/{parent_id}/childFolders",
                    headers=headers,
                    json={"displayName": display_name},
                    timeout=30
                )
                if response.status_code in [200, 201]:
                    folder_id = response.json()["id"]
                    logger.info(f"Created folder: {display_name}")

            if folder_id:
                folder_map[label] = folder_id

        logger.info(f"Folder mapping ready: {len(folder_map)} folders")
        return folder_map

class EmailProcessor:
    """Main email processing logic with UNIFIED stop signal."""
    
    def __init__(self, batch_id, stop_event=None):
        self.batch_id = batch_id
        self.stop_event = stop_event or threading.Event()
        self.mongo = get_mongo()
        self.model_api = ModelAPIClient()
        self.graph_client = MSGraphClient()
        self.folder_mappings = {}
        
        # Set batch in MongoDB
        if self.mongo:
            self.mongo.set_batch_id(batch_id)
    
    def _process_single_email(self, msg):
        """Process a single email with UNIFIED stop signal."""
        # CHECK UNIFIED STOP BEFORE PROCESSING EACH EMAIL
        if self.stop_event.is_set():
            logger.info("STOP: Stop signal detected during email processing - stopping NOW")
            return False
        
        message_id = msg.get("id", "unknown")
        source_account = msg.get("source_account", "")
        
        # Extract basic email data
        sender_info = msg.get("from", {}).get("emailAddress", {})
        sender = sender_info.get("address", "")
        sender_name = sender_info.get("name", sender)
        subject = msg.get("subject", "")
        received = msg.get("receivedDateTime", "")
        has_attachments = msg.get("hasAttachments", False)
        conversation_id = msg.get("conversationId", "")
        
        # Extract clean content
        clean_body, data_source, had_threads = extract_clean_email_content(msg)
        
        # Extract recipients  
        to_recipients = msg.get("toRecipients", [])
        recipient = ""
        recipient_emails = []
        if to_recipients:
            recipient = to_recipients[0].get("emailAddress", {}).get("address", "")
            recipient_emails = [r.get("emailAddress", {}).get("address", "") for r in to_recipients]
        
        # Extract CC recipients
        cc_recipients = msg.get("ccRecipients", [])
        cc_emails = []
        if cc_recipients:
            cc_emails = [cc.get("emailAddress", {}).get("address", "") for cc in cc_recipients]
        cc_string = ", ".join(cc_emails) if cc_emails else None
        
        # Determine receiver type
        receiver_type = "external"
        if recipient and "@abc-amega.com" in recipient.lower():
            receiver_type = "internal"
        
        logger.info(f"Processing email {message_id} from {sender} | Account: {source_account}")
        
        # Check for duplicates
        if self.mongo and self.mongo.email_exists(message_id):
            logger.info(f"Skipping duplicate email by message_id: {message_id}")
            return True
        
        # CHECK UNIFIED STOP BEFORE MODEL API CALL
        if self.stop_event.is_set():
            logger.info("STOP: Stop signal detected before model API call - stopping NOW")
            return False
        
        # Call model API
        model_response = self.model_api.process_email_complete(
            subject=subject,
            body=clean_body,
            sender_email=sender,
            recipient_emails=recipient_emails,
            has_attachments=has_attachments,
            had_threads=had_threads
        )
        
        # CHECK UNIFIED STOP AFTER MODEL API CALL
        if self.stop_event.is_set():
            logger.info("STOP: Stop signal detected after model API call - stopping NOW")
            return False
        
        # Get model fields
        debtor_number = model_response.get("debtor_number", "")
        debtor_id = None  # Always null
        event_type = model_response.get("event_type", "uncategorised") 
        target_folder = model_response.get("target_folder", "uncategorised")
        reply_sent = model_response.get("reply_sent", "no_response")
        new_contact_email = model_response.get("new_contact_email", "")
        new_contact_phone = model_response.get("new_contact_phone", "")
        contact_status = model_response.get("contact_status", "active")
        cleaned_body = model_response.get("cleaned_body", "")
        
        # Validate event type
        if event_type not in ALLOWED_LABELS:
            event_type = "uncategorised"
            target_folder = "uncategorised"
        
        logger.info(f"Model classified email as: {event_type}")
        
        # Generate threaded reply if needed
        reply_text = ""
        draft_created = False
        draft_id = None
        
        if event_type in RESPONSE_LABELS:
            # CHECK UNIFIED STOP BEFORE REPLY GENERATION
            if self.stop_event.is_set():
                logger.info("STOP: Stop signal detected before reply generation - stopping NOW")
                return False
                
            reply_text = self.model_api.generate_reply(
                subject=subject,
                body=clean_body, 
                label=event_type,
                sender_name=sender_name
            )
            
            # CHECK UNIFIED STOP AFTER REPLY GENERATION
            if self.stop_event.is_set():
                logger.info("STOP: Stop signal detected after reply generation - stopping NOW")
                return False
                
            if reply_text:
                logger.info(f"Reply generated: {len(reply_text)} chars")
                # Create threaded reply draft using original message ID
                draft_id = self.graph_client.create_threaded_reply_draft(
                    message_id, reply_text, source_account
                )
                if draft_id:
                    logger.info(f"Threaded draft saved: {draft_id}")
                    draft_created = True
                else:
                    logger.warning(f"Threaded draft save failed")
        
        # CHECK UNIFIED STOP BEFORE MONGODB STORAGE
        if self.stop_event.is_set():
            logger.info("STOP: Stop signal detected before MongoDB storage - stopping NOW")
            return False
        
        # Build email data for MongoDB
        email_data = {
            # Graph API fields
            "message_id": message_id,
            "sender": sender,
            "sender_name": sender_name,
            "recipient": recipient,
            "subject": subject,
            "body": clean_body,
            "received_at": received,
            "has_attachments": has_attachments,
            "source_account": source_account,
            "conversation_id": conversation_id,
            "receiver_type": receiver_type,
            "cc": cc_string,
            
            # Model API fields
            "debtor_number": debtor_number,
            "event_type": event_type,
            "target_folder": target_folder,
            "reply_sent": reply_sent,
            "new_contact_email": new_contact_email,
            "new_contact_phone": new_contact_phone,
            "contact_status": contact_status,
            "cleaned_body": cleaned_body,
            
            # Client fields
            "debtor_id": debtor_id,
            "classification": event_type,
            "prediction": event_type,
            "response": reply_text,
            "response_sent": False if reply_text else None,
            "draft_created": draft_created,
            "draft_id": draft_id,
            "batch_id": self.batch_id,
            "data_source": data_source,
            "had_threads": had_threads,
            "processed_at": datetime.utcnow().isoformat(),
            "batch_complete": False
        }
        
        # Store in MongoDB
        if self.mongo:
            result = self.mongo.insert_email(email_data)
            if result:
                logger.info(f"Email {message_id} stored in MongoDB")
        
        # CHECK UNIFIED STOP BEFORE FOLDER OPERATIONS
        if self.stop_event.is_set():
            logger.info("STOP: Stop signal detected before folder operations - stopping NOW")
            return False
        
        # Move to folder and track message ID properly
        folder_mapping = self.folder_mappings.get(source_account, {})
        folder_id = folder_mapping.get(event_type)
        msg_id_for_read = message_id  # Start with original ID
        
        if folder_id:
            try:
                success, new_id = self.graph_client.move_email_to_folder(message_id, folder_id, source_account)
                if success and new_id != message_id:
                    msg_id_for_read = new_id  # Use new ID for subsequent operations
                    # Update MongoDB with new ID
                    if self.mongo:
                        self.mongo.update_message_id(message_id, new_id)
            except Exception as e:
                logger.warning(f"Failed to move email: {e}")
        
        return True
            
    def process_batch(self, batch_size):
        """Process a batch of emails - handle ANY number of emails like test code."""
        # Setup folders for all accounts
        logger.info("Setting up classification folders...")
        for email_address in self.graph_client.email_addresses:
            folder_mapping = self.graph_client.ensure_classification_folders(email_address)
            if folder_mapping:
                self.folder_mappings[email_address] = folder_mapping
        
        if not self.folder_mappings:
            logger.error("Could not create folder mappings")
            return False, 0, 0
        
        # Fetch emails
        logger.info(f"Fetching up to {batch_size} emails...")
        emails = self.graph_client.fetch_unread_emails(batch_size)
        
        if not emails:
            logger.info("No emails to process - this is normal, not an error")
            # STILL sync to PostgreSQL even with 0 emails (batch tracking)
            if self.mongo:
                synced = self.mongo.sync_batch_emails_to_postgres(self.batch_id)
                logger.info(f"Synced {synced} emails to PostgreSQL")
            return True, 0, 0  # Success with 0 emails processed
        
        # Process whatever emails we have (1, 5, 30, 120 - doesn't matter)
        processed = 0
        failed = 0
        
        logger.info(f"Processing {len(emails)} emails...")
        
        for email in emails:
            # CHECK UNIFIED STOP BEFORE PROCESSING EACH EMAIL IN BATCH
            if self.stop_event.is_set():
                logger.info("STOP: Stop signal detected during batch processing - stopping NOW")
                break
                
            try:
                if self._process_single_email(email):
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Error processing email {email.get('id', 'unknown')}: {e}")
                failed += 1
                # CONTINUE PROCESSING OTHER EMAILS - like test code
                continue
        
        # ALWAYS sync to PostgreSQL (whether we have 1 email or 100)
        if self.mongo:
            synced = self.mongo.sync_batch_emails_to_postgres(self.batch_id)
            logger.info(f"Synced {synced} emails to PostgreSQL")
        
        logger.info(f"Batch {self.batch_id} complete: {processed} processed, {failed} failed")
        return True, processed, failed

def process_unread_emails(batch_id, batch_size=30, stop_event=None):
    """Process unread emails with UNIFIED stop signal."""
    processor = EmailProcessor(batch_id, stop_event)
    success, processed, failed = processor.process_batch(batch_size)
    
    return {
        "success": success,
        "emails_processed": processed,
        "emails_errored": failed,
        "batch_id": batch_id
    }

def get_failed_batches():
    """Get list of failed batch IDs that need retry."""
    mongo = get_mongo()
    if not mongo:
        return []
        
    # Find failed or incomplete batches
    failed_query = {
        "$or": [
            {"status": "failed"},
            {"status": "in_progress", "created_at": {"$lt": datetime.utcnow().replace(hour=datetime.utcnow().hour-1)}}
        ],
        "permanently_failed": {"$ne": True}
    }
    
    failed_batches = list(mongo.batch_runs_collection.find(failed_query, {"id": 1}))
    batch_ids = [batch.get("id") for batch in failed_batches if batch.get("id")]
    
    logger.info(f"Found {len(batch_ids)} failed batches to retry")
    return batch_ids

def retry_failed_batch(batch_id, batch_size=30):
    """Retry a specific failed batch."""
    logger.info(f"Retrying failed batch: {batch_id}")
    
    # Process the batch
    result = process_unread_emails(batch_id, batch_size)
    
    if result["success"]:
        # Update batch status
        mongo = get_mongo()
        if mongo:
            mongo.update_batch_result(
                batch_id,
                result["emails_processed"],
                result["emails_errored"],
                0,
                "success"
            )
        
        PostgresHelper.update_batch_result(
            batch_id,
            result["emails_processed"],
            result["emails_errored"],
            "success"
        )
        
        logger.info(f"Successfully retried batch {batch_id}")
        return True
    else:
        logger.warning(f"Retry failed for batch {batch_id}")
        return False

def generate_daily_report():
    """
    Generate daily report at 12:00 AM ET for the previous day.
    Updated per Yogesh's requirements for ET timezone and Susan as recipient.
    """
    from datetime import datetime, timedelta
    import pytz
    import httpx
    
    # Report recipients - UPDATED with Susan per Yogesh's request
    report_emails = [
        "sanskar.gawande@cadex-solutions.com",
        "yogesh.patel@cadex-solutions.com"
    ]
    
    try:
        # Get Eastern Time zone
        et_tz = pytz.timezone('US/Eastern')
        
        # Get current time in Eastern Time
        now_et = datetime.now(et_tz)
        
        # Calculate previous day in ET (report covers the day that just ended)
        yesterday_et = now_et - timedelta(days=1)
        
        # Convert to UTC for MongoDB query (MongoDB stores in UTC)
        start_date_et = yesterday_et.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_et = start_date_et + timedelta(days=1)
        
        # Convert ET times to UTC for database query
        start_date_utc = start_date_et.astimezone(pytz.UTC).replace(tzinfo=None)
        end_date_utc = end_date_et.astimezone(pytz.UTC).replace(tzinfo=None)
        
        logger.info(f"Generating daily report for: {yesterday_et.strftime('%Y-%m-%d')} ET")
        logger.info(f"UTC query range: {start_date_utc} to {end_date_utc}")
        
        # Get MongoDB connection
        mongo = get_mongo()
        if not mongo:
            logger.error("No MongoDB connection for report")
            return False
        
        # Query emails processed yesterday (in ET timezone)
        query = {
            "processed_at": {
                "$gte": start_date_utc.isoformat(),
                "$lt": end_date_utc.isoformat()
            }
        }
        
        emails = list(mongo.collection.find(query))
        
        # Enhanced counting and analysis
        total_emails = len(emails)
        replies_generated = 0
        drafts_created = 0
        label_counts = {}
        account_counts = {}
        
        # Count everything with more detail
        for email in emails:
            # Count replies
            if email.get("response", ""):
                replies_generated += 1
            
            # Count drafts created
            if email.get("draft_created", False):
                drafts_created += 1
            
            # Count by label
            label = email.get("event_type", "unknown")
            label_counts[label] = label_counts.get(label, 0) + 1
            
            # Count by source account
            account = email.get("source_account", "unknown")
            account_counts[account] = account_counts.get(account, 0) + 1
        
        # Create enhanced text report with ET timezone
        report_text = f"""Daily Email Processing Report - {yesterday_et.strftime('%Y-%m-%d')} (Eastern Time)

=== SUMMARY ===
Total Emails Processed: {total_emails}
Replies Generated: {replies_generated}
Drafts Created: {drafts_created}

=== PROCESSING BY CLASSIFICATION ===
"""
        
        # Add label counts (sorted by count, descending)
        for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_emails * 100) if total_emails > 0 else 0
            report_text += f"- {label.replace('_', ' ').title()}: {count} emails ({percentage:.1f}%)\n"
        
        report_text += f"""
=== PROCESSING BY EMAIL ACCOUNT ===
"""
        
        # Add account counts
        for account, count in sorted(account_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_emails * 100) if total_emails > 0 else 0
            report_text += f"- {account}: {count} emails ({percentage:.1f}%)\n"
        
        # Add response rate analysis
        response_rate = (replies_generated / total_emails * 100) if total_emails > 0 else 0
        draft_rate = (drafts_created / total_emails * 100) if total_emails > 0 else 0
        
        report_text += f"""
=== RESPONSE ANALYSIS ===
Response Rate: {response_rate:.1f}% ({replies_generated}/{total_emails})
Draft Creation Rate: {draft_rate:.1f}% ({drafts_created}/{total_emails})

=== REPORT INFO ===
Report Date: {yesterday_et.strftime('%Y-%m-%d')} (Eastern Time)
Report Generated: {now_et.strftime('%Y-%m-%d %I:%M:%S %p %Z')}
Coverage: 12:00 AM - 11:59 PM ET
"""
        
        logger.info(f"Report ready: {total_emails} emails, {replies_generated} replies, {drafts_created} drafts")
        
        # Send email using Graph API
        return send_simple_report_email(report_text, yesterday_et.strftime('%Y-%m-%d'), report_emails)
        
    except Exception as e:
        logger.error(f"Error generating daily report: {e}")
        return False

def send_simple_report_email(report_text, report_date, recipients):
    """Send enhanced daily report email with proper ET timezone handling."""
    try:
        # Create MSGraphClient to get access token
        graph_client = MSGraphClient()
        access_token = graph_client.get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Enhanced email formatting
        to_recipients = [{"emailAddress": {"address": email}} for email in recipients]
        subject = f"Daily Email Processing Report - {report_date} (ET)"
        
        # Add HTML formatting for better readability
        html_body = report_text.replace('\n', '<br>').replace('===', '<b>===').replace('==', '==</b>')
        
        message = {
            "subject": subject,
            "body": {
                "contentType": "HTML", 
                "content": f"<html><body style='font-family: monospace; white-space: pre-line;'>{html_body}</body></html>"
            },
            "toRecipients": to_recipients,
            "importance": "Normal"
        }
        
        payload = {"message": message, "saveToSentItems": "true"}
        endpoint = f"{MS_GRAPH_BASE_URL}/users/{EMAIL_ADDRESS.split(',')[0].strip()}/sendMail"
        
        response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
        
        if response.status_code in [200, 202]:
            logger.info(f"Daily report sent to {len(recipients)} recipients")            
            return True
        else:
            logger.error(f"Failed to send report: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending report email: {e}")
        return False