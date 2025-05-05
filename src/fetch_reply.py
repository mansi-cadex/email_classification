import os
import sys
import webbrowser
import time
import httpx
import msal
import re
import requests
from datetime import datetime, timedelta
from urllib.parse import quote, urlencode
from typing import Dict, Optional
from dotenv import load_dotenv
# Import from the correct src package paths - updated for PostgresHelper
from src.db import get_mongo, PostgresHelper
# No longer import classifier or reply_generator directly
from src.log_config import logger

# Import the email sender functionality
try:
    from src.email_sender import send_pending_replies, process_draft_emails
    EMAIL_SENDER_AVAILABLE = True
except ImportError:
    logger.warning("Email sender module not available. Replies will not be sent automatically.")
    EMAIL_SENDER_AVAILABLE = False

load_dotenv()

# Constants - moved from code to top level
REFRESH_TOKEN_PATH = "client_side/refresh_token.txt"
MS_GRAPH_TIMEOUT = 30  # seconds
AUTH_RETRY_ATTEMPTS = 3
EMAIL_FETCH_TOP = 1000  # Maximum folders to fetch
MS_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

# Configuration from environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AUTHORITY = os.getenv("AUTHORITY", "https://login.microsoftonline.com/common")
SCOPES = ["User.Read", "Mail.ReadWrite", "Mail.Send"]
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN", "yourdomain.com")
TIME_FILTER_HOURS = int(os.getenv("TIME_FILTER_HOURS", "24"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
SEND_REPLIES = os.getenv("SEND_REPLIES", "true").lower() in ["true", "yes", "1"]

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

# API functions for classification and reply generation
def classify_email_via_api(subject: str, body: str) -> Dict:
    """Call the API to classify email"""
    try:
        response = requests.post(
            f"{MODEL_API_URL}/api/classify",
            json={"subject": subject, "body": body},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Classification API returned: {result}")
        return result
    except Exception as e:
        logger.error(f"Error calling classification API: {e}")
        return {"label": "manual_review", "confidence": 0.0, "method": "api_error"}

def generate_reply_via_api(subject: str, body: str, label: str, entities: Optional[Dict] = None) -> str:
    """Call the API to generate reply"""
    try:
        response = requests.post(
            f"{MODEL_API_URL}/api/generate_reply",
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
        return ""

class EmailProcessor:
    def __init__(self):
        """Initialize the email processor with MongoDB and API connections."""
        self.mongo = get_mongo()
        # No longer initialize classifier or responder directly
        self.folder_mapping = None
        self.batch_size = BATCH_SIZE
        self.batch_id = None
        self.metrics = {
            "emails_processed": 0,
            "emails_skipped": 0,
            "emails_errored": 0,
            "drafts_created": 0
        }
    
    def get_access_token(self):
        """Acquire an access token for Microsoft Graph API."""
        client = msal.ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=CLIENT_SECRET,
            authority=AUTHORITY,
        )
        # Try silent token acquisition first
        silent_result = client.acquire_token_silent(scopes=SCOPES, account=None)
        if silent_result and "access_token" in silent_result:
            logger.info("Successfully acquired access token via MSAL cache.")
            return silent_result["access_token"]
        # Fall back to refresh token from file
        if os.path.exists(REFRESH_TOKEN_PATH):
            try:
                with open(REFRESH_TOKEN_PATH, "r") as file:
                    refresh_token = file.read().strip()
                    if refresh_token:
                        logger.info("Attempting token acquisition by refresh token.")
                        token_response = client.acquire_token_by_refresh_token(refresh_token, scopes=SCOPES)
                        if "access_token" in token_response:
                            logger.info("Successfully acquired access token via refresh token.")
                            if "refresh_token" in token_response:
                                with open(REFRESH_TOKEN_PATH, "w") as file:
                                    file.write(token_response["refresh_token"])
                                logger.info("Updated refresh token.")
                            return token_response["access_token"]
            except Exception as e:
                logger.warning(f"Error using refresh token: {str(e)}. Will try interactive flow.")
        # Interactive authentication as last resort
        logger.info("No valid refresh token found; initiating interactive auth flow.")
        auth_url = client.get_authorization_request_url(SCOPES, extra_query_parameters={"prompt": "consent"})
        webbrowser.open(auth_url)
        logger.info("Browser opened for authentication. Please login and authorize the app.")
        
        for attempt in range(AUTH_RETRY_ATTEMPTS):
            try:
                authorization_code = input("Enter the authorization code: ")
                if not authorization_code:
                    logger.warning(f"Empty authorization code (attempt {attempt+1}/{AUTH_RETRY_ATTEMPTS})")
                    continue
                    
                token_response = client.acquire_token_by_authorization_code(code=authorization_code, scopes=SCOPES)
                
                if "access_token" in token_response:
                    logger.info("Successfully acquired access token via authorization code.")
                    if "refresh_token" in token_response:
                        with open(REFRESH_TOKEN_PATH, "w") as file:
                            file.write(token_response["refresh_token"])
                        logger.info("Stored new refresh token.")
                    return token_response["access_token"]
                else:
                    error_message = token_response.get("error_description", "Unknown error")
                    logger.error(f"Failed to acquire token: {error_message}")
                    
                    if "invalid_grant" in str(error_message).lower():
                        logger.info(f"Invalid grant error (attempt {attempt+1}/{AUTH_RETRY_ATTEMPTS}). Try again.")
                        continue
                    
                    raise Exception(f"Failed to acquire token: {error_message}")
                
            except Exception as e:
                logger.error(f"Authentication error (attempt {attempt+1}/{AUTH_RETRY_ATTEMPTS}): {str(e)}")
                if attempt < AUTH_RETRY_ATTEMPTS-1:
                    logger.info("Retrying authentication...")
                    time.sleep(2)
                else:
                    raise Exception(f"Failed to authenticate after {AUTH_RETRY_ATTEMPTS} attempts: {str(e)}")
        
        raise Exception("Authentication failed after multiple attempts.")

    def _get_all_pages(self, url, headers, params=None):
        """Yield every item in a Graph collection, following @odata.nextLink."""
        # Add params to initial URL if provided
        if params:
            query_params = urlencode(params)
            full_url = f"{url}?{query_params}" if "?" not in url else f"{url}&{query_params}"
        else:
            full_url = url
            
        # Track number of items yielded for batch size limiting
        count = 0
        max_items = getattr(self, 'batch_size', BATCH_SIZE)
            
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
                        logger.info(f"Reached batch limit of {max_items} emails")
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

    def ensure_classification_folders(self, access_token):
        """Ensure all classification folders exist in Outlook."""
        # Use our defined allowed labels
        LABELS = ALLOWED_LABELS
        
        # Return existing mapping if already created
        if self.folder_mapping:
            return self.folder_mapping
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
            url = f"{MS_GRAPH_BASE_URL}/me/mailFolders?$filter={quote(odata)}&$select=id,parentFolderId,displayName"
            
            r = httpx.get(url, headers=headers, timeout=MS_GRAPH_TIMEOUT)
            r.raise_for_status()
            
            items = r.json().get("value", [])
            return items[0] if items else None
        def move_to_parent(folder_id, new_parent_id):
            url = f"{MS_GRAPH_BASE_URL}/me/mailFolders/{folder_id}"
            httpx.patch(url, headers=headers,
                        json={"parentFolderId": new_parent_id}, timeout=20).raise_for_status()
        # Fetch all folders once with a high limit to reduce paging
        r = httpx.get(f"{MS_GRAPH_BASE_URL}/me/mailFolders?$top={EMAIL_FETCH_TOP}",
                      headers=headers, timeout=MS_GRAPH_TIMEOUT)
        r.raise_for_status()
        all_folders = r.json()["value"]
        lookup = {normalize(f["displayName"]): f["id"] for f in all_folders}
        # Ensure/create parent folder
        parent_id = lookup.get(normalize(parent_name))
        if not parent_id:
            r = httpx.post(f"{MS_GRAPH_BASE_URL}/me/mailFolders",
                          headers=headers,
                          json={"displayName": parent_name}, timeout=MS_GRAPH_TIMEOUT)
            r.raise_for_status()
            parent_id = r.json()["id"]
            logger.info("Created parent folder '%s' (ID: %s)", parent_name, parent_id)
        else:
            logger.info("Found parent folder '%s' (ID: %s)", parent_name, parent_id)
        # Fetch ALL child folders with pagination support
        child_url = f"{MS_GRAPH_BASE_URL}/me/mailFolders/{parent_id}/childFolders?$top=100"
        child_lookup = {normalize(f["displayName"]): f["id"] 
                        for f in self._get_all_pages(child_url, headers)}
        # Process each label
        for label in LABELS:
            display = label.replace("_", " ").title()
            key = normalize(display)
            folder_id = child_lookup.get(key)
            if not folder_id:  # not under parent => search globally
                ghost = graph_search(display)
                if ghost:  # found elsewhere → move
                    folder_id = ghost["id"]
                    move_to_parent(folder_id, parent_id)
                    logger.info("Moved ghost folder '%s' (ID: %s) under parent.", display, folder_id)
                else:  # truly absent → create
                    try:
                        r = httpx.post(f"{MS_GRAPH_BASE_URL}/me/mailFolders/{parent_id}/childFolders", 
                                      headers=headers,
                                      json={"displayName": display}, timeout=MS_GRAPH_TIMEOUT)
                        if r.status_code == 409:  # Handle conflict explicitly
                            # Re-query to get the existing folder
                            ghost = graph_search(display)
                            if ghost:
                                folder_id = ghost["id"]
                                logger.info("Folder '%s' already exists (ID: %s)", display, folder_id)
                            else:
                                logger.warning(f"409 Conflict for '{display}' but couldn't find it via search")
                                continue
                        else:
                            r.raise_for_status()
                            folder_id = r.json()["id"]
                            logger.info("Created folder '%s' (ID: %s)", display, folder_id)
                    except Exception as e:
                        logger.error(f"Error creating folder '{display}': {str(e)}")
                        continue
            folder_map[label] = folder_id
            logger.debug("Mapped '%s' → %s", label, folder_id)
        logger.info("Folder mapping ready (%d folders)", len(folder_map))
        self.folder_mapping = folder_map
        return folder_map

    def move_email_to_folder(self, message_id, folder_id, access_token):
        """Move an email to a specific folder in Outlook."""
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{MS_GRAPH_BASE_URL}/me/messages/{message_id}/move"
        payload = {"destinationId": folder_id}
        try:
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=20.0)
            response.raise_for_status()
            logger.info(f"Email {message_id} moved to folder ID: {folder_id}")
            return True
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to move email {message_id} to folder {folder_id}. HTTP error: {e.response.status_code} - {e.response.text}")
            return False
        except httpx.RequestError as e:
            logger.warning(f"Network error moving email {message_id} to folder {folder_id}: {str(e)}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error moving email {message_id} to folder {folder_id}: {str(e)}")
            return False

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

    def _fetch_unread_emails(self, access_token):
        """Fetch unread emails from Microsoft Graph API."""
        # Get unread emails with filtering by time
        time_threshold = datetime.utcnow() - timedelta(hours=TIME_FILTER_HOURS)
        time_threshold_str = time_threshold.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Parameters for filtering unread emails
        params = {
            "$orderby": "receivedDateTime desc",
            "$filter": f"isRead eq false and isDraft eq false and receivedDateTime ge {time_threshold_str}",
            "$select": "id,subject,from,bodyPreview,receivedDateTime,hasAttachments",
            "$top": self.batch_size
        }
        
        # Get unread emails with pagination support
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        url = f"{MS_GRAPH_BASE_URL}/me/mailFolders/inbox/messages"
        
        try:
            # Count total unread for logging - add ConsistencyLevel header for count requests
            count_headers = {
                "Authorization": f"Bearer {access_token}", 
                "Content-Type": "application/json",
                "ConsistencyLevel": "eventual"
            }
            count_url = f"{MS_GRAPH_BASE_URL}/me/mailFolders/inbox/messages/$count"
            count_params = {"$filter": "isRead eq false"}
            
            count_response = httpx.get(count_url, headers=count_headers, params=count_params, timeout=MS_GRAPH_TIMEOUT)
            total_unread = int(count_response.text) if count_response.status_code == 200 else "unknown"
            logger.info(f"Total unread emails in inbox: {total_unread}")
            
            # Collect emails to process
            emails = list(self._get_all_pages(url=url, headers=headers, params=params))
            
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

    def _process_single_email(self, msg, access_token):
        """Process a single email message."""
        message_id = msg.get('id', 'unknown_id')
        
        try:
            # Extract email metadata
            sender_info = msg.get("from", {}).get("emailAddress", {})
            sender = sender_info.get("address", "")
            subject = msg.get("subject", "")
            body_preview = msg.get("bodyPreview", "")
            received = msg.get("receivedDateTime", "")
            
            logger.info(f"Processing email: {message_id} | From: {sender} | Subject: {subject}")
            # Skip if own domain or auto-generated
            if sender.lower().endswith(f"@{YOUR_DOMAIN}") or any(x in sender.lower() for x in ["no-reply", "auto-reply", "noreply", "donotreply"]):
                logger.info(f"Skipping email from {sender}")
                self.metrics["emails_skipped"] += 1
                return
            # Skip if already processed
            if self.mongo.email_exists(message_id):
                logger.info(f"Skipping already processed email: {message_id}")
                self.metrics["emails_skipped"] += 1
                return
            
            # Classify email using the API
            try:
                # Call the classification API
                classification_result = classify_email_via_api(subject=subject, body=body_preview)
                label = classification_result.get("label", "manual_review")
                confidence = classification_result.get("confidence", 0.0)
                
                # Make sure we only use allowed labels
                if label not in ALLOWED_LABELS:
                    logger.warning(f"Classifier returned non-allowed label '{label}', using 'manual_review' instead")
                    label = "manual_review"
                
                logger.info(f"Email {message_id} classified as '{label}' with confidence {confidence:.2f}")
                
            except Exception as e:
                logger.exception(f"Error during classification for email {message_id}. Using manual_review:")
                label = "manual_review"
                confidence = 0.0
            
            # Extract entities for response generation
            entities = {}
            try:
                # We could make another API call to extract entities, but for now we'll rely
                # on the classification API to return them or the reply generation API to handle it
                entities = classification_result.get("entities", {})
            except Exception as e:
                logger.warning(f"Error extracting entities: {str(e)}")
            
            # Generate response if needed - only for labels in RESPONSE_LABELS
            reply_text = ""
            
            try:
                if label in RESPONSE_LABELS:
                    logger.info(f"Generating response for {message_id} with label: {label}")
                    
                    # Call the reply generation API
                    reply_text = generate_reply_via_api(
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
            
            # Set response_process and save_as_draft flags
            needs_manual_review = label not in RESPONSE_LABELS
            
            # Set the current batch_id for the MongoDB connection
            self.mongo.set_batch_id(self.batch_id)
            
            # Add batch_id to email data for tracking
            email_data = {
                "message_id": message_id,
                "sender": sender,
                "subject": subject,
                "body": body_preview,
                "text": body_preview,
                "received_at": received,
                "classification": label,
                "prediction": label,
                "confidence": confidence,
                "response": reply_text,
                "response_sent": False if reply_text else None,
                "processed_at": datetime.utcnow().isoformat(),
                "batch_id": self.batch_id,
                "response_process": False,
                "save_as_draft": needs_manual_review,
                "target_folder": label  # Add target folder for easier tracking
            }
            
            # Insert into MongoDB
            self.mongo.insert_email(email_data)
            
            if needs_manual_review:
                self.metrics["drafts_created"] += 1
                logger.info(f"Email {message_id} flagged for manual review and will be saved as draft")
            
            # Mark as read/unread based on classification
            headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
            mark_url = f"{MS_GRAPH_BASE_URL}/me/messages/{message_id}"
            is_read = label not in ["manual_review"]
            httpx.patch(mark_url, headers=headers, json={"isRead": is_read}, timeout=20.0)
            
            # Move to appropriate folder
            folder_id = self.folder_mapping.get(label)
            if folder_id:
                self.move_email_to_folder(message_id, folder_id, access_token)
            
            self.metrics["emails_processed"] += 1
            
        except Exception as e:
            logger.exception(f"Error processing email {message_id}:")
            self.metrics["emails_errored"] += 1

    def _finalize_batch(self):
        """Handle draft creation and batch finalization after processing emails."""
        # Process draft emails (for manual review items)
        draft_count = 0
        draft_failed = 0
        try:
            if EMAIL_SENDER_AVAILABLE:
                draft_success, draft_failed = process_draft_emails(self.batch_id)
                draft_count = draft_success
                logger.info(f"Draft processing complete: {draft_count} saved as drafts")
        except Exception as e:
            logger.error(f"Error processing draft emails: {str(e)}")
            
        # Ensure synchronization with PostgreSQL
        synced_count = self.mongo.sync_batch_emails_to_postgres(self.batch_id)
        logger.info(f"Synchronized {synced_count} emails to PostgreSQL for batch {self.batch_id}")
        
        # Update batch results with draft count
        PostgresHelper.update_batch_result(
            self.batch_id, 
            self.metrics["emails_processed"], 
            0,  # No failed emails to count yet
            "success", 
            draft_count
        )
        
        # Log summary
        if self.metrics["emails_processed"] > 0 or self.metrics["emails_skipped"] > 0:
            logger.info(f"Email processing summary:")
            logger.info(f"- Emails processed: {self.metrics['emails_processed']}")
            logger.info(f"- Emails skipped: {self.metrics['emails_skipped']}")
            logger.info(f"- Emails errored: {self.metrics['emails_errored']}")
            logger.info(f"- Emails saved as drafts: {draft_count}")
        else:
            logger.info("No unread emails were processed.")

    def process_unread_emails(self):
        """Process all unread emails in the inbox."""
        try:
            # Step 1: Prepare the batch
            self._prepare_batch()
            
            # Step 2: Get access token and ensure folders
            access_token = self.get_access_token()
            self.folder_mapping = self.ensure_classification_folders(access_token)
            if not self.folder_mapping:
                logger.error("Could not create folder mapping. Aborting.")
                return False, 0, 0, 0
                
            # Step 3: Fetch unread emails
            emails = self._fetch_unread_emails(access_token)
            if not emails:
                logger.info("No emails to process.")
                return True, 0, 0, 0
                
            # Step 4: Process each email
            for email in emails:
                if getattr(self, 'stop_requested', False):
                    logger.info("Batch processor stopped by user")
                    break
                self._process_single_email(email, access_token)
                
            # Step 5: Finalize the batch
            self._finalize_batch()
            
            # Return success status and metrics for batch tracking
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["drafts_created"]
            
        except KeyboardInterrupt:
            logger.info("Batch processor stopped by user")
            self.stop_requested = True
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["drafts_created"]
        except Exception as e:
            logger.exception(f"Error in process_unread_emails: {str(e)}")
            return False, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["drafts_created"]

def main():
    """Main function to run the email processor."""
    logger.info("Starting fetch_reply.py")
    logger.info(f"Using Model API URL: {MODEL_API_URL}")
    
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
        
    success, processed, failed, drafts = processor.process_unread_emails()
    
    if success:
        logger.info(f"fetch_reply.py execution completed successfully: {processed} processed, {drafts} drafts, {failed} failed")
        sys.exit(0)
    else:
        logger.error(f"fetch_reply.py execution completed with errors: {processed} processed, {drafts} drafts, {failed} failed")
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