"""
fetch_reply.py - Module for fetching, classifying, and moving emails.

This module provides functionality for:
1. Fetching unread emails from Microsoft Graph API
2. Classifying emails via API
3. Moving emails to appropriate folders based on classification
4. Storing email data in MongoDB

It separates the email fetching and classification from the email sending,
which is now handled by email_sender.py.
"""

import os
import sys
import time
import httpx
import msal
import re
import requests
from datetime import datetime, timedelta
from urllib.parse import quote, urlencode
from typing import Dict, Optional, List, Any, Tuple
from dotenv import load_dotenv
from src.db import get_mongo, PostgresHelper
from src.log_config import logger

# Import the email sender functionality 
try:
    from src.email_sender import send_pending_replies, process_draft_emails
    EMAIL_SENDER_AVAILABLE = True
except ImportError:
    logger.warning("Email sender module not available. Replies will not be sent automatically.")
    EMAIL_SENDER_AVAILABLE = False

load_dotenv()

# Constants
MS_GRAPH_TIMEOUT = 30  # seconds
EMAIL_FETCH_TOP = 1000  # Maximum folders to fetch
MS_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

# Configuration from environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN", "abc-amega.com")
TIME_FILTER_HOURS = int(os.getenv("TIME_FILTER_HOURS", "24"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

# Email sending flags - these will be passed to email_sender but not used directly here
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "yes", "1"]
FORCE_DRAFTS = os.getenv("FORCE_DRAFTS", "True").lower() in ["true", "yes", "1"]

# Log configuration for transparency
if MAIL_SEND_ENABLED:
    logger.warning("🚨 EMAIL SENDING IS ENABLED - EMAILS WILL BE SENT RATHER THAN SAVED AS DRAFTS")
    logger.warning("Set MAIL_SEND_ENABLED=False to prevent sending")
else:
    logger.info("📝 Email sending is disabled - all emails will be saved as drafts")

if FORCE_DRAFTS:
    logger.info("📝 FORCE_DRAFTS is enabled - all emails will be saved as drafts regardless of other settings")
    
if MAIL_SEND_ENABLED and FORCE_DRAFTS:
    logger.warning("⚠️ CONFLICT IN CONFIGURATION: Both MAIL_SEND_ENABLED and FORCE_DRAFTS are True.")
    logger.warning("This will result in emails being saved as drafts despite mail sending being enabled.")
    logger.warning("If you want to actually send emails, set FORCE_DRAFTS=False")

# API configuration for the model server
MODEL_API_URL = os.getenv("MODEL_API_URL", "http://localhost:8000")

# Final list of allowed labels
ALLOWED_LABELS = [
    "no_reply_no_info",
    "no_reply_with_info",
    "auto_reply_no_info",
    "auto_reply_with_info",
    "invoice_request_no_info",
    "claims_paid_no_proof",
    "manual_review"
]

# Labels that should receive responses
RESPONSE_LABELS = [
    "invoice_request_no_info",
    "claims_paid_no_proof"
]


class ModelAPIClient:
    """Client for model API calls (classification and reply generation)."""
    
    def __init__(self, base_url=None):
        """Initialize with base URL from environment."""
        self.base_url = base_url or MODEL_API_URL
        
    def health_check(self) -> bool:
        """Check if API is available."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def classify_email(self, subject: str, body: str) -> Dict:
        """Classify an email."""
        try:
            # Check if API is available, use fallback if not
            if not self.health_check():
                logger.warning("Classification API not available, using fallback classification")
                return {"label": "manual_review", "confidence": 0.0, "method": "fallback"}
            
            response = requests.post(
                f"{self.base_url}/api/classify",
                json={"subject": subject, "body": body},
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            # Handle the response format
            if "status" in result and result["status"] == "success" and "results" in result:
                classification = result["results"][0]
                logger.info(f"Classification API returned: {classification}")
                return classification
            else:
                logger.warning(f"Unexpected response format from API: {result}")
                return {"label": "manual_review", "confidence": 0.0, "method": "api_error"}
        except Exception as e:
            logger.error(f"Error calling classification API: {e}")
            return {"label": "manual_review", "confidence": 0.0, "method": "api_error"}
    
    def generate_reply(self, subject: str, body: str, label: str, entities: Optional[Dict] = None) -> str:
        """Generate a reply for an email."""
        try:
            # Check if API is available, use fallback if not
            if not self.health_check():
                logger.warning("Reply generation API not available, using fallback reply")
                return self._generate_fallback_reply(label)
                
            response = requests.post(
                f"{self.base_url}/api/generate_reply",
                json={
                    "subject": subject,
                    "body": body,
                    "label": label,
                    "entities": entities or {}
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            reply = result.get("reply", "")
            logger.info(f"Reply generation API returned a response of length: {len(reply)}")
            return reply
        except Exception as e:
            logger.error(f"Error calling reply generation API: {e}")
            return self._generate_fallback_reply(label)
    
    def _generate_fallback_reply(self, label: str) -> str:
        """Generate a simple fallback reply when API is unavailable."""
        if label == "invoice_request_no_info":
            return """Dear Customer,

Thank you for your email. We would be happy to provide you with a copy of the invoice you requested.

To fulfill your request, we need the following information:
1. Invoice number(s)
2. Account number
3. Company name

Once we receive this information, we will promptly send you the requested invoice.

Thank you for your cooperation.

Best regards,
ABC Collections Team"""

        elif label == "claims_paid_no_proof":
            return """Dear Customer,

Thank you for informing us about your payment. To properly credit your account, we need verification of the payment.

Please provide one of the following:
1. Copy of the payment confirmation
2. Bank transaction reference number
3. Check number and date of payment

Once we receive this documentation, we will update your account accordingly.

Thank you for your prompt attention to this matter.

Best regards,
ABC Collections Team"""
        else:
            return """Dear Customer,

Thank you for your email. We have received your message and will process it accordingly.

If you have any further questions, please don't hesitate to contact us.

Best regards,
ABC Collections Team"""


class MSGraphClient:
    """Microsoft Graph API client for email operations."""
    
    def __init__(self):
        """Initialize with credentials from environment."""
        self.base_url = MS_GRAPH_BASE_URL
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        self.email_address = EMAIL_ADDRESS
        self._token_cache = {"token": None, "expires_at": 0}
        
    def get_access_token(self, force_refresh=False):
        """Get a valid access token, refreshing if needed."""
        try:
            # Check if we have a cached valid token
            current_time = time.time()
            if not force_refresh and self._token_cache.get("token") and self._token_cache.get("expires_at", 0) > current_time + 60:
                logger.debug("Using cached access token")
                return self._token_cache["token"]
            
            # Check if all required variables are set
            if not all([self.client_id, self.client_secret, self.tenant_id]):
                logger.error("Missing required Microsoft Graph API credentials")
                logger.error("Please set CLIENT_ID, CLIENT_SECRET, and TENANT_ID in .env file")
                raise ValueError("Missing Microsoft Graph API credentials")
                
            if not self.email_address:
                logger.error("EMAIL_ADDRESS is not set in the .env file")
                logger.error("Please set EMAIL_ADDRESS to the mailbox you want to access")
                raise ValueError("Missing EMAIL_ADDRESS in environment variables")
            
            # Log client and tenant ID for debugging (only show first 8 chars of client_id)
            logger.debug(f"Using client_id: {self.client_id[:8]}*** and tenant_id: {self.tenant_id}")
            
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            # Acquire token for application permissions
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            
            if "access_token" in result:
                # Cache the token with expiration
                self._token_cache = {
                    "token": result["access_token"],
                    "expires_at": current_time + result.get("expires_in", 3600)
                }
                logger.info("Successfully acquired access token using application permissions")
                return result["access_token"]
            else:
                error = f"{result.get('error')}: {result.get('error_description')}"
                logger.error(f"Error acquiring token: {error}")
                raise Exception(f"Failed to acquire token: {error}")
        except Exception as e:
            logger.exception(f"Error getting access token: {str(e)}")
            raise
    
    def get_all_pages(self, url, params=None):
        """Yield every item in a Graph collection, following @odata.nextLink."""
        # Add params to initial URL if provided
        if params:
            query_params = urlencode(params)
            full_url = f"{url}?{query_params}" if "?" not in url else f"{url}&{query_params}"
        else:
            full_url = url
            
        # Get headers with access token
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
            
        # Track number of items yielded for batch size limiting
        count = 0
        max_items = BATCH_SIZE
            
        while full_url:
            try:
                logger.debug(f"Requesting: {full_url}")
                r = httpx.get(full_url, headers=headers, timeout=MS_GRAPH_TIMEOUT)
                r.raise_for_status()
                data = r.json()
                for item in data.get("value", []):
                    yield item
                    count += 1
                    
                    # Check if we've reached the batch limit
                    if max_items and count >= max_items:
                        logger.info(f"Reached batch limit of {max_items} items")
                        return
                        
                full_url = data.get("@odata.nextLink")
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error during pagination: {e.response.status_code} - {e.response.text}")
                break
            except httpx.RequestError as e:
                logger.error(f"Network error during pagination: {str(e)}")
                break
            except Exception as e:
                logger.error(f"Error during pagination: {str(e)}")
                break
    
    def fetch_unread_emails(self, hours_filter=TIME_FILTER_HOURS, max_emails=BATCH_SIZE) -> List[Dict]:
        """Fetch unread emails from inbox with filtering by time."""
        # Get unread emails with filtering by time
        time_threshold = datetime.utcnow() - timedelta(hours=hours_filter)
        time_threshold_str = time_threshold.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Parameters for filtering unread emails
        params = {
            "$orderby": "receivedDateTime desc",
            "$filter": f"isRead eq false and isDraft eq false and receivedDateTime ge {time_threshold_str}",
            "$select": "id,subject,from,bodyPreview,receivedDateTime,hasAttachments,toRecipients",
            "$top": max_emails
        }
        
        # Get headers with access token
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/users/{self.email_address}/mailFolders/inbox/messages"
        
        try:
            # Count total unread for logging - add ConsistencyLevel header for count requests
            count_headers = {
                "Authorization": f"Bearer {access_token}", 
                "Content-Type": "application/json",
                "ConsistencyLevel": "eventual"
            }
            count_url = f"{self.base_url}/users/{self.email_address}/mailFolders/inbox/messages/$count"
            count_params = {"$filter": "isRead eq false"}
            
            count_response = httpx.get(count_url, headers=count_headers, params=count_params, timeout=MS_GRAPH_TIMEOUT)
            total_unread = int(count_response.text) if count_response.status_code == 200 else "unknown"
            logger.info(f"Total unread emails in inbox: {total_unread}")
            
            # Collect emails to process
            emails = list(self.get_all_pages(url=url, params=params))
            
            if not emails:
                logger.info("No unread emails to process.")
            else:
                logger.info(f"Found {len(emails)} unread emails to process")
                
            return emails
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching emails: {e.response.status_code} - {e.response.text}")
            return []
        except httpx.RequestError as e:
            logger.error(f"Network error fetching emails: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error fetching unread emails: {str(e)}")
            return []
    
    def move_email_to_folder(self, message_id, folder_id):
        """Move an email to a specific folder in Outlook."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{self.email_address}/messages/{message_id}/move"
        payload = {"destinationId": folder_id}
        
        try:
            logger.debug(f"Attempting to move email {message_id} to folder ID: {folder_id}")
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=20.0)
            response.raise_for_status()
            result = response.json()
            new_id = result.get("id", "unknown")
            logger.info(f"Email {message_id} moved to folder ID: {folder_id}, new ID: {new_id}")
            return True, new_id
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to move email {message_id} to folder {folder_id}. HTTP error: {e.response.status_code} - {e.response.text}")
            return False, None
        except httpx.RequestError as e:
            logger.warning(f"Network error moving email {message_id} to folder {folder_id}: {str(e)}")
            return False, None
        except Exception as e:
            logger.warning(f"Unexpected error moving email {message_id} to folder {folder_id}: {str(e)}")
            return False, None
    
    def mark_email_read_status(self, message_id, is_read=True):
        """Mark an email as read or unread."""
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{self.email_address}/messages/{message_id}"
        payload = {"isRead": is_read}
        
        try:
            response = httpx.patch(endpoint, headers=headers, json=payload, timeout=20.0)
            response.raise_for_status()
            logger.info(f"Email {message_id} marked as {'read' if is_read else 'unread'}")
            return True
        except Exception as e:
            logger.warning(f"Failed to mark email {message_id} as {'read' if is_read else 'unread'}: {str(e)}")
            return False
    
    def ensure_classification_folders(self) -> Dict[str, str]:
        """Ensure all classification folders exist in Outlook and return mapping."""
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        parent_name = "Email Classification"
        folder_map = {}
        
        # Helper functions
        def normalize(s):
            return ''.join(c for c in s.lower() if c.isalnum())
            
        def graph_search(name):
            """Return the first folder dict whose displayName matches `name`, or None."""
            escaped = name.replace("'", "''")
            odata = f"displayName eq '{escaped}'"
            url = f"{self.base_url}/users/{self.email_address}/mailFolders?$filter={quote(odata)}&$select=id,parentFolderId,displayName"
            
            r = httpx.get(url, headers=headers, timeout=MS_GRAPH_TIMEOUT)
            r.raise_for_status()
            
            items = r.json().get("value", [])
            return items[0] if items else None
            
        def move_to_parent(folder_id, new_parent_id):
            url = f"{self.base_url}/users/{self.email_address}/mailFolders/{folder_id}"
            httpx.patch(url, headers=headers,
                      json={"parentFolderId": new_parent_id}, timeout=20).raise_for_status()
        
        # Fetch all folders once with a high limit to reduce paging
        try:
            r = httpx.get(f"{self.base_url}/users/{self.email_address}/mailFolders?$top={EMAIL_FETCH_TOP}",
                         headers=headers, timeout=MS_GRAPH_TIMEOUT)
            r.raise_for_status()
            all_folders = r.json()["value"]
            lookup = {normalize(f["displayName"]): f["id"] for f in all_folders}
            
            # Ensure/create parent folder
            parent_id = lookup.get(normalize(parent_name))
            if not parent_id:
                r = httpx.post(f"{self.base_url}/users/{self.email_address}/mailFolders",
                              headers=headers,
                              json={"displayName": parent_name}, timeout=MS_GRAPH_TIMEOUT)
                r.raise_for_status()
                parent_id = r.json()["id"]
                logger.info(f"Created parent folder '{parent_name}' (ID: {parent_id})")
            else:
                logger.info(f"Found parent folder '{parent_name}' (ID: {parent_id})")
            
            # Fetch ALL child folders with pagination support
            child_url = f"{self.base_url}/users/{self.email_address}/mailFolders/{parent_id}/childFolders?$top=100"
            child_folders = list(self.get_all_pages(url=child_url))
            child_lookup = {normalize(f["displayName"]): f["id"] for f in child_folders}
            
            # Process each label
            for label in ALLOWED_LABELS:
                display = label.replace("_", " ").title()
                key = normalize(display)
                folder_id = child_lookup.get(key)
                
                if not folder_id:  # not under parent => search globally
                    ghost = graph_search(display)
                    if ghost:  # found elsewhere → move
                        folder_id = ghost["id"]
                        move_to_parent(folder_id, parent_id)
                        logger.info(f"Moved ghost folder '{display}' (ID: {folder_id}) under parent.")
                    else:  # truly absent → create
                        try:
                            r = httpx.post(f"{self.base_url}/users/{self.email_address}/mailFolders/{parent_id}/childFolders", 
                                          headers=headers,
                                          json={"displayName": display}, timeout=MS_GRAPH_TIMEOUT)
                            if r.status_code == 409:  # Handle conflict explicitly
                                # Re-query to get the existing folder
                                ghost = graph_search(display)
                                if ghost:
                                    folder_id = ghost["id"]
                                    logger.info(f"Folder '{display}' already exists (ID: {folder_id})")
                                else:
                                    logger.warning(f"409 Conflict for '{display}' but couldn't find it via search")
                                    continue
                            else:
                                r.raise_for_status()
                                folder_id = r.json()["id"]
                                logger.info(f"Created folder '{display}' (ID: {folder_id})")
                        except Exception as e:
                            logger.error(f"Error creating folder '{display}': {str(e)}")
                            continue
                            
                folder_map[label] = folder_id
                logger.debug(f"Mapped '{label}' → {folder_id}")
            
            logger.info(f"Folder mapping ready ({len(folder_map)} folders)")
            return folder_map
            
        except Exception as e:
            logger.error(f"Error ensuring classification folders: {str(e)}")
            return {}


class EmailProcessor:
    """Main email processing logic for fetching, classifying, and moving emails."""
    
    def __init__(self, batch_id=None):
        """Initialize the email processor with MongoDB and API connections."""
        self.mongo = get_mongo()
        self.model_api = ModelAPIClient()
        self.graph_client = MSGraphClient()
        self.folder_mapping = None
        self.batch_id = batch_id
        self.batch_size = BATCH_SIZE
        self.stop_requested = False
        self.metrics = {
            "emails_processed": 0,
            "emails_classified": 0,
            "emails_skipped": 0,
            "emails_errored": 0,
            "emails_moved": 0
        }
    
    def _prepare_batch(self):
        """Prepare the batch ID and MongoDB connection for processing."""
        # If no batch_id is provided, create one using PostgreSQL
        if not self.batch_id:
            self.batch_id = PostgresHelper.insert_batch_run()
            logger.info(f"Created new PostgreSQL batch with ID: {self.batch_id}")
            # Set batch_id in MongoDB connector
            self.mongo.set_batch_id(self.batch_id)
        
        # Update batch size from environment if set
        if "BATCH_SIZE" in os.environ:
            try:
                self.batch_size = int(os.environ["BATCH_SIZE"])
                logger.info(f"Using batch size from environment: {self.batch_size}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid BATCH_SIZE in environment. Using default: {self.batch_size}")
    
    def _process_single_email(self, msg):
        """Process a single email message - classify, store, and move to folder."""
        message_id = msg.get('id', 'unknown_id')
        
        try:
            # Extract email metadata
            sender_info = msg.get("from", {}).get("emailAddress", {})
            sender = sender_info.get("address", "")
            subject = msg.get("subject", "")
            body_preview = msg.get("bodyPreview", "")
            received = msg.get("receivedDateTime", "")
            
            # Extract recipient information (if available)
            recipient = ""
            to_recipients = msg.get("toRecipients", [])
            if to_recipients and len(to_recipients) > 0:
                recipient_info = to_recipients[0].get("emailAddress", {})
                recipient = recipient_info.get("address", "")
            
            logger.info(f"Processing email: {message_id} | From: {sender} | Subject: {subject}")
            
            # Skip if already processed
            if self.mongo.email_exists(message_id):
                logger.info(f"Skipping already processed email: {message_id}")
                self.metrics["emails_skipped"] += 1
                return
            
            # Classify email using the API
            try:
                # Call the classification API
                classification_result = self.model_api.classify_email(subject=subject, body=body_preview)
                label = classification_result.get("label", "manual_review")
                confidence = classification_result.get("confidence", 0.0)
                
                # Make sure we only use allowed labels
                if label not in ALLOWED_LABELS:
                    logger.warning(f"Classifier returned non-allowed label '{label}', using 'manual_review' instead")
                    label = "manual_review"
                
                logger.info(f"Email {message_id} classified as '{label}' with confidence {confidence:.2f}")
                self.metrics["emails_classified"] += 1
                
            except Exception as e:
                logger.exception(f"Error during classification for email {message_id}. Using manual_review:")
                label = "manual_review"
                confidence = 0.0
                classification_result = {"label": label, "confidence": confidence, "method": "api_error"}
            
            # Extract entities for response generation
            entities = classification_result.get("entities", {})
            
            # Generate response if needed - only for labels in RESPONSE_LABELS
            reply_text = ""
            
            try:
                if label in RESPONSE_LABELS:
                    logger.info(f"Generating response for {message_id} with label: {label}")
                    
                    # Call the reply generation API
                    reply_text = self.model_api.generate_reply(
                        subject=subject,
                        body=body_preview,
                        label=label,
                        entities=entities
                    )
                    
                    if not reply_text:
                        logger.warning(f"Empty reply generated for email {message_id}")
                
            except Exception as e:
                logger.exception(f"Error generating response for email {message_id}:")
                label = "manual_review"
                reply_text = ""
            
            # Determine if this needs manual review based on label
            needs_manual_review = label not in RESPONSE_LABELS
            
            # Default save_as_draft value based on classification
            save_as_draft = needs_manual_review
            
            # Force draft mode if MAIL_SEND_ENABLED is False or FORCE_DRAFTS is True
            if not MAIL_SEND_ENABLED or FORCE_DRAFTS:
                save_as_draft = True
                logger.info(f"Forcing email {message_id} to be saved as draft due to configuration")
            
            # Set the current batch_id for the MongoDB connection
            self.mongo.set_batch_id(self.batch_id)
            
            # Add batch_id to email data for tracking
            email_data = {
                "message_id": message_id,
                "sender": sender,
                "recipient": recipient,  # Added recipient field here
                "to": recipient,         # Also add as "to" for backward compatibility
                "subject": subject,
                "body": body_preview,
                "text": body_preview,
                "received_at": received,
                "classification": label,
                "prediction": label,
                "confidence": confidence,
                "method": classification_result.get("method", ""),
                "response": reply_text,
                "response_sent": False if reply_text else None,  # Always start as not sent
                "processed_at": datetime.utcnow().isoformat(),
                "batch_id": self.batch_id,
                "response_process": False,
                "save_as_draft": save_as_draft,  # Use the corrected value
                "draft_saved": False,  # Initialize as not saved
                "target_folder": label,
                # Store entities directly at top level for easier access by extract_contact_info
                "entities": entities,
                # Also store properly in metadata structure for compatibility
                "metadata": {
                    "entities": entities,
                    "sentiment": classification_result.get("sentiment", {}),
                    "confidence_score": confidence,
                    "classification_method": classification_result.get("method", ""),
                    "matching_patterns": classification_result.get("matching_patterns", [])
                }
            }
            
            # Copy OOO and left company data if available
            if "ooo_person" in entities:
                email_data["ooo_person"] = entities.get("ooo_person", {})
                email_data["ooo_contact_person"] = entities.get("ooo_contact_person", {})
                email_data["ooo_dates"] = entities.get("ooo_dates", {})
                
                # Also put in metadata
                email_data["metadata"]["out_of_office"] = {
                    "ooo_person": entities.get("ooo_person", {}),
                    "contact_person": entities.get("ooo_contact_person", {}),
                    "ooo_dates": entities.get("ooo_dates", {})
                }
            
            if "left_person" in entities:
                email_data["left_person"] = entities.get("left_person", {})
                email_data["replacement_contact"] = entities.get("replacement_contact", {})
                
                # Also put in metadata
                email_data["metadata"]["left_company"] = {
                    "left_person": entities.get("left_person", {}),
                    "replacement": entities.get("replacement_contact", {})
                }
            
            # Insert into MongoDB
            result = self.mongo.insert_email(email_data)
            logger.info(f"Email {message_id} inserted into MongoDB")
            
            # Mark as read/unread based on classification
            is_read = label not in ["manual_review"]
            mark_success = self.graph_client.mark_email_read_status(message_id, is_read)
            if mark_success:
                logger.info(f"Email {message_id} marked as {'read' if is_read else 'unread'}")
            else:
                logger.warning(f"Failed to mark email {message_id} as {'read' if is_read else 'unread'}")
            
            # Move to appropriate folder
            folder_id = self.folder_mapping.get(label)
            if folder_id:
                move_success, new_id = self.graph_client.move_email_to_folder(message_id, folder_id)
                if move_success:
                    logger.info(f"Email {message_id} successfully moved to folder for label '{label}'")
                    self.metrics["emails_moved"] += 1
                else:
                    logger.warning(f"Failed to move email {message_id} to folder for label '{label}'")
            else:
                # Log when folder mapping is missing
                logger.warning(f"No folder mapping found for label '{label}', email {message_id} will remain in inbox")
                # Print all available folder mappings for debugging
                logger.debug(f"Available folder mappings: {self.folder_mapping}")
            
            self.metrics["emails_processed"] += 1
            
        except Exception as e:
            logger.exception(f"Error processing email {message_id}:")
            self.metrics["emails_errored"] += 1
    
    def _sync_with_databases(self):
        """Synchronize processed emails with PostgreSQL and finalize batch."""
        try:
            # Ensure synchronization with PostgreSQL
            synced_count = self.mongo.sync_batch_emails_to_postgres(self.batch_id)
            logger.info(f"Synchronized {synced_count} emails to PostgreSQL for batch {self.batch_id}")
            
            # Update batch results
            PostgresHelper.update_batch_result(
                self.batch_id, 
                self.metrics["emails_processed"], 
                self.metrics["emails_errored"],
                "success", 
                0  # No draft count here as draft creation is now handled separately
            )
            logger.info(f"Updated PostgreSQL batch {self.batch_id} with processing status: success")
            
        except Exception as e:
            logger.error(f"Error synchronizing with databases: {str(e)}")
    
    def process_unread_emails(self) -> Tuple[bool, int, int, int]:
        """Process all unread emails in the inbox - fetch, classify, and move."""
        try:
            # Step 1: Prepare the batch
            self._prepare_batch()
            
            # Step 2: Ensure folders for classification
            self.folder_mapping = self.graph_client.ensure_classification_folders()
            if not self.folder_mapping:
                logger.error("Could not create folder mapping. Aborting.")
                return False, 0, 0, 0
                
            # Step 3: Fetch unread emails
            emails = self.graph_client.fetch_unread_emails()
            if not emails:
                logger.info("No emails to process.")
                return True, 0, 0, 0
                
            # Step 4: Process each email
            for email in emails:
                if self.stop_requested:
                    logger.info("Batch processor stopped by user")
                    break
                self._process_single_email(email)
                
            # Step 5: Sync with databases
            self._sync_with_databases()
            
            # Return success status and metrics for batch tracking
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]
            
        except KeyboardInterrupt:
            logger.info("Batch processor stopped by user")
            self.stop_requested = True
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]
        except Exception as e:
            logger.exception(f"Error in process_unread_emails: {str(e)}")
            return False, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]


def process_unread_emails(batch_id=None) -> Dict:
    """Public interface function to process unread emails.
    
    Returns a dictionary with processing metrics.
    """
    processor = EmailProcessor(batch_id)
    success, processed, errors, classified = processor.process_unread_emails()
    
    return {
        "success": success,
        "emails_processed": processed,
        "emails_classified": classified,
        "emails_errored": errors,
        "emails_moved": processor.metrics["emails_moved"],
        "batch_id": processor.batch_id
    }


def main():
    """Main function to run the email processor."""
    logger.info("Starting fetch_reply.py")
    logger.info(f"Using Model API URL: {MODEL_API_URL}")
    
    # Log email sending configuration
    if MAIL_SEND_ENABLED:
        logger.warning("🚨 EMAIL SENDING IS ENABLED - RUNNING WITH MAIL_SEND_ENABLED=True")
    else:
        logger.info("📝 Email sending is disabled - MAIL_SEND_ENABLED=False")
        
    if FORCE_DRAFTS:
        logger.info("📝 FORCE_DRAFTS is enabled - all emails will be saved as drafts")
    
    processor = EmailProcessor()
    processor.stop_requested = False
    
    def signal_handler(sig, frame):
        logger.info("Received interrupt signal, stopping gracefully...")
        processor.stop_requested = True
    
    # Register signal handlers
    try:
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except (ImportError, AttributeError):
        # Windows or other platforms may not support all signals
        pass
        
    success, processed, errors, classified = processor.process_unread_emails()
    
    # Log the outcome
    logger.info("Email Processing Summary:")
    logger.info(f"- Emails processed: {processed}")
    logger.info(f"- Emails classified: {classified}")
    logger.info(f"- Emails moved: {processor.metrics['emails_moved']}")
    logger.info(f"- Emails errored: {errors}")
    logger.info(f"- Emails skipped: {processor.metrics['emails_skipped']}")
    
    # Process drafts and send replies if email_sender is available
    if EMAIL_SENDER_AVAILABLE and processor.batch_id:
        logger.info("Starting email sending/draft creation phase...")
        
        try:
            # First run the draft emails processor
            draft_success, draft_failed = process_draft_emails(processor.batch_id)
            logger.info(f"Draft processing complete: {draft_success} created, {draft_failed} failed")
            
            # Then process pending emails
            sent_success, sent_failed = send_pending_replies(processor.batch_id)
            logger.info(f"Email sending complete: {sent_success} sent, {sent_failed} failed")
            
            logger.info("Email sending/draft creation phase complete")
        except Exception as e:
            logger.error(f"Error in email sending/draft creation phase: {str(e)}")
    
    if success:
        logger.info(f"fetch_reply.py execution completed successfully")
        sys.exit(0)
    else:
        logger.error(f"fetch_reply.py execution completed with errors")
        sys.exit(1)
        
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Unhandled exception in main:")
        sys.exit(1)