"""
fetch_reply.py - Module for fetching, classifying, and moving emails.
"""

import os
import sys
import time
import httpx
import msal
import requests
import random
import logging
import re
from functools import wraps
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
MS_GRAPH_TIMEOUT = int(os.getenv("MS_GRAPH_TIMEOUT", "60"))  
EMAIL_FETCH_TOP = 1000  
MS_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

# Configuration from environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN", "abc-amega.com")
TIME_FILTER_HOURS = 24
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

# Email sending flags - these will be passed to email_sender but not used directly here
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "1", "yes"]
FORCE_DRAFTS = os.getenv("FORCE_DRAFTS", "True").lower() in ["true", "1", "yes"]

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

# API configuration for the model
MODEL_API_URL = os.getenv("MODEL_API_URL","http://34.72.113.155:8000")

# Updated list of allowed labels
ALLOWED_LABELS = [
    "no_reply_no_info",
    "no_reply_with_info", 
    "auto_reply_no_info",
    "auto_reply_with_info",
    "invoice_request_no_info",
    "claims_paid_no_proof",
    "claims_paid_with_proof",  # NEW
    "manual_review",
    "uncategorised"            # NEW (replaces fallback)
]

# Labels that should receive responses
RESPONSE_LABELS = [
    "invoice_request_no_info",
    "claims_paid_no_proof"
]

def validate_config():
    """Validate all required environment variables"""
    required_vars = ["CLIENT_ID", "CLIENT_SECRET", "TENANT_ID", "EMAIL_ADDRESS"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")

def retry_with_backoff(max_retries=3, initial_backoff=1.5):
    """Retry decorator with exponential backoff for HTTP requests."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            backoff = initial_backoff
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except httpx.RequestError as e:
                    last_exception = e
                    # Check if we should retry based on error type
                    status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    
                    # Don't retry client errors except 429 (rate limit) and 408 (timeout)
                    if status_code and 400 <= status_code < 500 and status_code not in (429, 408):
                        logger.warning(f"Client error {status_code}, not retrying: {str(e)}")
                        raise
                        
                    # Only retry on request errors or server errors
                    logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {str(e)}")
                    
                    # Check if this is the last attempt
                    if attempt == max_retries - 1:
                        logger.error(f"Max retries reached, giving up: {str(e)}")
                        raise
                    
                    # Calculate backoff with jitter
                    sleep_time = backoff * (1.0 + 0.1 * random.random())
                    logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    
                    # Increase backoff for next attempt
                    backoff *= 2
            
            # If we got here, raise the last exception (if any)
            if last_exception:
                raise last_exception
            raise RuntimeError("retry_with_backoff gave up but no exception captured")
        return wrapper
    return decorator

class ModelAPIClient:
    """Client for model API calls (classification and reply generation)."""
    
    def __init__(self, base_url=None):
        """Initialize with base URL from environment."""
        self.base_url = base_url or MODEL_API_URL
        
    def health_check(self) -> bool:
        """Check if API is available."""
        try:
            response = requests.get(f"{self.base_url}", timeout=10)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def classify_email(self, subject: str, body: str, headers: List[Dict] = None, 
                      sender_email: str = None, recipient_emails: List[str] = None, 
                      has_attachments: bool = False, had_threads: bool = False) -> Dict:
        """Classify an email with full context including headers and thread information."""
        try:
            # Check if API is available, use fallback if not
            if not self.health_check():
                logger.warning("Classification API not available, using fallback classification")
                return {"label": "uncategorised", "confidence": 0.0, "method": "api_unavailable"}
            
            # Prepare request payload with all available data
            payload = {
                "subject": subject,
                "body": body,
                "headers": headers or [],
                "sender_email": sender_email,
                "recipient_emails": recipient_emails or [],
                "has_attachments": has_attachments,
                "had_threads": had_threads  # NEW: Send thread information to model
            }
            
            response = requests.post(
                f"{self.base_url}/api/classify",
                json=payload,
                timeout=420
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
                return {"label": "uncategorised", "confidence": 0.0, "method": "api_error"}
        except Exception as e:
            logger.error(f"Error calling classification API: {e}")
            return {"label": "uncategorised", "confidence": 0.0, "method": "api_error"}

    def generate_reply(self, subject: str, body: str, label: str, entities: Optional[Dict] = None) -> str:
        """Generate a reply for an email."""
        try:
            # Only generate replies for specific labels
            if label not in RESPONSE_LABELS:
                return ""
                
            # Check if API is available
            if not self.health_check():
                logger.warning("Reply generation API not available")
                return ""
                
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
            return ""

class MSGraphClient:
    """Microsoft Graph API client for email operations with clean text extraction."""
    
    def __init__(self, batch_size=BATCH_SIZE, email_address=None):
        """Initialize with credentials from environment."""
        self.base_url = MS_GRAPH_BASE_URL
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        
        # UPDATED: Support for multiple email addresses
        if email_address:
            # Single email address provided
            self.email_addresses = [email_address]
        else:
            # Parse multiple email addresses from environment
            email_env = EMAIL_ADDRESS or ""
            if "," in email_env:
                self.email_addresses = [email.strip() for email in email_env.split(",")]
            else:
                self.email_addresses = [email_env] if email_env else []
        
        # For backward compatibility, keep single email_address
        self.email_address = self.email_addresses[0] if self.email_addresses else ""
        
        self._token_cache = {"token": None, "expires_at": 0}
        self.batch_size = batch_size
        self.timeout = httpx.Timeout(MS_GRAPH_TIMEOUT)
        
        # Log which email addresses are being used
        logger.info(f"MSGraphClient initialized for {len(self.email_addresses)} email(s): {self.email_addresses}")
        
    def get_access_token(self, force_refresh=False):
        """Get a valid access token, refreshing if needed."""
        try:
            # Check if we have a cached valid token
            current_time = time.time()
            if not force_refresh and self._token_cache.get("token") and self._token_cache.get("expires_at", 0) > current_time + 60:
                logger.debug("Using cached access token")
                return self._token_cache["token"]
            
            # Validate configuration
            validate_config()
            
            # Log client and tenant ID for debugging (only show first 8 chars of client_id)
            logger.debug(f"Using client_id: {self.client_id[:8]}*** and tenant_id: {self.tenant_id}")
            
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            # Acquire token for application permissions
            logger.info("Attempting token acquisition...")
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
                
                # Log full error details for debugging
                logger.error(f"Full error result: {result}")
                
                raise Exception(f"Failed to acquire token: {error}")
        except Exception as e:
            logger.exception(f"Error getting access token: {str(e)}")
            raise
        
    def get_all_pages(self, url, params=None, max_items=None):
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
        max_items = max_items or self.batch_size  # Use instance batch_size if none provided
            
        while full_url:
            try:
                logger.debug(f"Requesting: {full_url}")
                r = httpx.get(full_url, headers=headers, timeout=self.timeout)
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
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to clean text (simple but effective)"""
        try:
            if not html_content:
                return ""
            
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', html_content)
            
            # Clean up common HTML entities
            html_entities = {
                '&nbsp;': ' ',
                '&amp;': '&',
                '&lt;': '<',
                '&gt;': '>',
                '&quot;': '"',
                '&#39;': "'",
                '&apos;': "'",
                '\r\n': '\n',
                '\r': '\n'
            }
            
            for entity, replacement in html_entities.items():
                text = text.replace(entity, replacement)
            
            # Clean up multiple whitespaces and newlines
            text = re.sub(r'\n\s*\n', '\n\n', text)  # Max 2 consecutive newlines
            text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single space
            
            return text.strip()
            
        except Exception as e:
            logger.warning(f"Error converting HTML to text: {str(e)}")
            return html_content  # Return original if conversion fails

    def _extract_clean_email_content(self, msg: Dict) -> Tuple[str, str, bool]:
        """Extract clean email content without HTML and threads"""
        try:
            clean_body = ""
            data_source = ""
            had_threads = False  # NEW: Track if email had threads
            
            # 1. Try uniqueBody first (excludes threads) - COMPLETE content
            unique_body = msg.get("uniqueBody", {})
            full_body = msg.get("body", {})
            
            # Check if email has threads by comparing uniqueBody with full body
            if unique_body and unique_body.get("content") and full_body and full_body.get("content"):
                unique_content = unique_body.get("content", "").strip()
                full_content = full_body.get("content", "").strip()
                
                # If uniqueBody is significantly shorter than full body, it likely had threads
                if len(unique_content) > 0 and len(full_content) > len(unique_content) * 1.2:
                    had_threads = True
                    logger.debug(f"Thread detected: uniqueBody={len(unique_content)} chars, fullBody={len(full_content)} chars")
            
            if unique_body and unique_body.get("content"):
                unique_content = unique_body.get("content", "").strip()
                content_type = unique_body.get("contentType", "").lower()
                
                if content_type == "text":
                    # Perfect: uniqueBody in text format (no HTML, no threads)
                    clean_body = unique_content
                    data_source = "uniqueBody_text"
                elif content_type == "html" and unique_content:
                    # Convert HTML to text for uniqueBody (still no threads)
                    clean_body = self._html_to_text(unique_content)
                    data_source = "uniqueBody_html_converted"
            
            # 2. If uniqueBody not available, use full body content
            if not clean_body:
                if full_body and full_body.get("content"):
                    body_content = full_body.get("content", "").strip()
                    content_type = full_body.get("contentType", "").lower()
                    
                    if content_type == "text":
                        # Full body in text format
                        clean_body = body_content
                        data_source = "body_text"
                    elif content_type == "html" and body_content:
                        # Convert HTML to text for full body
                        clean_body = self._html_to_text(body_content)
                        data_source = "body_html_converted"
            
            # 3. Last resort: bodyPreview (but this is limited to ~160 chars)
            if not clean_body:
                clean_body = msg.get("bodyPreview", "").strip()
                data_source = "bodyPreview_fallback"
            
            return clean_body, data_source, had_threads
            
        except Exception as e:
            logger.warning(f"Error extracting clean email content: {str(e)}")
            return msg.get("bodyPreview", ""), "error_fallback", False

    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def fetch_unread_emails_from_account(self, email_address: str, max_emails: int) -> List[Dict]:
        """Fetch unread emails from a single email account."""
        # Updated parameters to fetch complete email data including headers and clean body
        params = {
            "$orderby": "receivedDateTime desc",
            "$filter": "isRead eq false and isDraft eq false",
            "$select": "id,subject,from,body,bodyPreview,uniqueBody,receivedDateTime,hasAttachments,toRecipients,ccRecipients,internetMessageHeaders,conversationId",
            "$top": max_emails
        }
        
        # Get headers with access token
        access_token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/users/{email_address}/mailFolders/inbox/messages"
        
        try:
            # Count total unread for logging - add ConsistencyLevel header for count requests
            count_headers = {
                "Authorization": f"Bearer {access_token}", 
                "Content-Type": "application/json",
                "ConsistencyLevel": "eventual"
            }
            count_url = f"{self.base_url}/users/{email_address}/mailFolders/inbox/messages/$count"
            count_params = {"$filter": "isRead eq false"}
            
            count_response = httpx.get(count_url, headers=count_headers, params=count_params, timeout=self.timeout)
            total_unread = int(count_response.text) if count_response.status_code == 200 else "unknown"
            logger.info(f"Total unread emails in inbox for {email_address}: {total_unread}")
            
            # Collect emails to process
            emails = list(self.get_all_pages(url=url, params=params, max_items=max_emails))
            
            # Add account info to each email
            for email in emails:
                email["source_account"] = email_address
            
            if not emails:
                logger.info(f"No unread emails to process for {email_address}.")
            else:
                logger.info(f"Found {len(emails)} unread emails to process for {email_address} with clean text extraction")
                
            return emails
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching emails for {email_address}: {e.response.status_code} - {e.response.text}")
            return []
        except httpx.RequestError as e:
            logger.error(f"Network error fetching emails for {email_address}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error fetching unread emails for {email_address}: {str(e)}")
            return []

    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def fetch_unread_emails(self, emails_per_account=None) -> List[Dict]:
        """    
        Args:
            emails_per_account: If provided, uses fixed distribution (backward compatibility)
            
        Returns:
            List[Dict]: List of email messages up to batch_size total
        """
        all_emails = []
        
        # If emails_per_account is explicitly provided, use legacy fixed distribution
        if emails_per_account is not None:
            logger.info(f"Using fixed distribution: {emails_per_account} emails per account")
            for email_address in self.email_addresses:
                try:
                    logger.info(f"Fetching emails from: {email_address}")
                    emails = self.fetch_unread_emails_from_account(email_address, emails_per_account)
                    all_emails.extend(emails)
                    logger.info(f"Collected {len(emails)} emails from {email_address}")
                except Exception as e:
                    logger.error(f"Error fetching emails from {email_address}: {str(e)}")
                    continue
            
            logger.info(f"Total emails collected from all accounts: {len(all_emails)}")
            return all_emails
        
        # DYNAMIC DISTRIBUTION LOGIC
        logger.info(f"Using dynamic batch distribution (batch_size: {self.batch_size})")
        
        # Step 1: Check email availability in each account
        account_availability = {}
        total_available = 0
        
        for email_address in self.email_addresses:
            try:
                # Get email count without fetching emails
                access_token = self.get_access_token()
                count_headers = {
                    "Authorization": f"Bearer {access_token}", 
                    "Content-Type": "application/json",
                    "ConsistencyLevel": "eventual"
                }
                count_url = f"{self.base_url}/users/{email_address}/mailFolders/inbox/messages/$count"
                count_params = {"$filter": "isRead eq false and isDraft eq false"}
                
                count_response = httpx.get(count_url, headers=count_headers, params=count_params, timeout=self.timeout)
                available_count = int(count_response.text) if count_response.status_code == 200 else 0
                
                account_availability[email_address] = available_count
                total_available += available_count
                
                logger.info(f"Account {email_address}: {available_count} unread emails available")
                
            except Exception as e:
                logger.warning(f"Could not check email count for {email_address}: {str(e)}")
                account_availability[email_address] = 0
        
        logger.info(f"Total unread emails across all accounts: {total_available}")
        
        # Step 2: Calculate dynamic distribution
        if total_available == 0:
            logger.info("No unread emails found in any account.")
            return all_emails
        
        # Determine how many emails to process (up to batch_size)
        emails_to_process = min(self.batch_size, total_available)
        logger.info(f"Will process {emails_to_process} emails (batch_size: {self.batch_size}, available: {total_available})")
        
        # Step 3: Sort accounts by email count (descending) for priority processing
        sorted_accounts = sorted(
            account_availability.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # Step 4: Distribute emails_to_process across accounts based on availability
        distribution_plan = {}
        remaining_to_fetch = emails_to_process
        
        for email_address, available_count in sorted_accounts:
            if remaining_to_fetch <= 0 or available_count <= 0:
                distribution_plan[email_address] = 0
                continue
                
            # Calculate fair share based on availability ratio
            if total_available > 0:
                proportional_share = int((available_count / total_available) * emails_to_process)
                # Take minimum of proportional share, available emails, and remaining to fetch
                to_fetch = min(proportional_share, available_count, remaining_to_fetch)
                
                # Ensure we don't leave small amounts unfetched if this is the last account with emails
                if remaining_to_fetch <= 5 and available_count >= remaining_to_fetch:
                    to_fetch = remaining_to_fetch
                    
            else:
                to_fetch = 0
                
            distribution_plan[email_address] = to_fetch
            remaining_to_fetch -= to_fetch
        
        # Step 5: If we still have remaining capacity, distribute to accounts with most emails
        while remaining_to_fetch > 0:
            allocated = False
            for email_address, available_count in sorted_accounts:
                current_allocation = distribution_plan[email_address]
                if current_allocation < available_count and remaining_to_fetch > 0:
                    distribution_plan[email_address] += 1
                    remaining_to_fetch -= 1
                    allocated = True
                    
            # Prevent infinite loop if no more emails can be allocated
            if not allocated:
                break
        
        # Log the distribution plan
        logger.info("Dynamic distribution plan:")
        for email_address, to_fetch in distribution_plan.items():
            available = account_availability[email_address]
            logger.info(f"  {email_address}: {to_fetch}/{available} emails")
        
        # Step 6: Fetch emails according to the distribution plan
        total_fetched = 0
        for email_address in self.email_addresses:
            emails_to_fetch = distribution_plan.get(email_address, 0)
            
            if emails_to_fetch <= 0:
                logger.info(f"Skipping {email_address}: no emails to fetch")
                continue
                
            try:
                logger.info(f"Fetching {emails_to_fetch} emails from: {email_address}")
                emails = self.fetch_unread_emails_from_account(email_address, emails_to_fetch)
                all_emails.extend(emails)
                total_fetched += len(emails)
                
                logger.info(f"Collected {len(emails)}/{emails_to_fetch} emails from {email_address}")
                
            except Exception as e:
                logger.error(f"Error fetching emails from {email_address}: {str(e)}")
                continue
        
        logger.info(f"Dynamic batch complete: {total_fetched} emails collected from {len(self.email_addresses)} accounts")
        return all_emails
    
    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def move_email_to_folder(self, message_id, folder_id, email_address=None):
        """Move an email to a specific folder in Outlook."""
        # Use provided email address or fall back to first configured address
        target_email = email_address or self.email_address
        
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{target_email}/messages/{message_id}/move"
        payload = {"destinationId": folder_id}
        
        try:
            logger.debug(f"Attempting to move email {message_id} to folder ID: {folder_id} for {target_email}")
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            new_id = result.get("id", "unknown")
            logger.info(f"Email {message_id} moved to folder ID: {folder_id}, new ID: {new_id} for {target_email}")
            return True, new_id
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to move email {message_id} to folder {folder_id} for {target_email}. HTTP error: {e.response.status_code} - {e.response.text}")
            return False, None
        except httpx.RequestError as e:
            logger.warning(f"Network error moving email {message_id} to folder {folder_id} for {target_email}: {str(e)}")
            return False, None
        except Exception as e:
            logger.warning(f"Unexpected error moving email {message_id} to folder {folder_id} for {target_email}: {str(e)}")
            return False, None
    
    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def mark_email_read_status(self, message_id, is_read=True, email_address=None):
        """Mark an email as read or unread."""
        # Use provided email address or fall back to first configured address
        target_email = email_address or self.email_address
        
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        endpoint = f"{self.base_url}/users/{target_email}/messages/{message_id}"
        payload = {"isRead": is_read}
        
        try:
            response = httpx.patch(endpoint, headers=headers, json=payload, timeout=self.timeout)
            response.raise_for_status()
            logger.info(f"Email {message_id} marked as {'read' if is_read else 'unread'} for {target_email}")
            return True
        except Exception as e:
            logger.warning(f"Failed to mark email {message_id} as {'read' if is_read else 'unread'} for {target_email}: {str(e)}")
            return False
    
    def ensure_classification_folders(self, email_address=None) -> Dict[str, str]:
        """Ensure all classification folders exist in Outlook and return mapping."""
        # Use provided email address or fall back to first configured address
        target_email = email_address or self.email_address
        
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
            url = f"{self.base_url}/users/{target_email}/mailFolders?$filter={quote(odata)}&$select=id,parentFolderId,displayName"
            
            r = httpx.get(url, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            
            items = r.json().get("value", [])
            return items[0] if items else None
            
        def move_to_parent(folder_id, new_parent_id):
            url = f"{self.base_url}/users/{target_email}/mailFolders/{folder_id}"
            httpx.patch(url, headers=headers,
                      json={"parentFolderId": new_parent_id}, timeout=self.timeout).raise_for_status()
        
        # Fetch all folders once with a high limit to reduce paging
        try:
            r = httpx.get(f"{self.base_url}/users/{target_email}/mailFolders?$top={EMAIL_FETCH_TOP}",
                         headers=headers, timeout=self.timeout)
            r.raise_for_status()
            all_folders = r.json()["value"]
            lookup = {normalize(f["displayName"]): f["id"] for f in all_folders}
            
            # Ensure/create parent folder
            parent_id = lookup.get(normalize(parent_name))
            if not parent_id:
                r = httpx.post(f"{self.base_url}/users/{target_email}/mailFolders",
                              headers=headers,
                              json={"displayName": parent_name}, timeout=self.timeout)
                r.raise_for_status()
                parent_id = r.json()["id"]
                logger.info(f"Created parent folder '{parent_name}' (ID: {parent_id}) for {target_email}")
            else:
                logger.info(f"Found parent folder '{parent_name}' (ID: {parent_id}) for {target_email}")
            
            # Fetch ALL child folders with pagination support
            child_url = f"{self.base_url}/users/{target_email}/mailFolders/{parent_id}/childFolders?$top=100"
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
                        logger.info(f"Moved ghost folder '{display}' (ID: {folder_id}) under parent for {target_email}.")
                    else:  # truly absent → create
                        try:
                            r = httpx.post(f"{self.base_url}/users/{target_email}/mailFolders/{parent_id}/childFolders", 
                                          headers=headers,
                                          json={"displayName": display}, timeout=self.timeout)
                            if r.status_code == 409:  # Handle conflict explicitly
                                # Re-query to get the existing folder
                                ghost = graph_search(display)
                                if ghost:
                                    folder_id = ghost["id"]
                                    logger.info(f"Folder '{display}' already exists (ID: {folder_id}) for {target_email}")
                                else:
                                    logger.warning(f"409 Conflict for '{display}' but couldn't find it via search for {target_email}")
                                    continue
                            else:
                                r.raise_for_status()
                                folder_id = r.json()["id"]
                                logger.info(f"Created folder '{display}' (ID: {folder_id}) for {target_email}")
                        except Exception as e:
                            logger.error(f"Error creating folder '{display}' for {target_email}: {str(e)}")
                            continue
                            
                folder_map[label] = folder_id
                logger.debug(f"Mapped '{label}' → {folder_id} for {target_email}")
            
            logger.info(f"Folder mapping ready ({len(folder_map)} folders) for {target_email}")
            return folder_map
            
        except Exception as e:
            logger.error(f"Error ensuring classification folders for {target_email}: {str(e)}")
            return {}

class EmailProcessor:
    """Main email processing logic for fetching, classifying, and moving emails."""
    
    def __init__(self, batch_id=None):
        """Initialize the email processor with MongoDB and API connections."""
        self.mongo = get_mongo()
        self.model_api = ModelAPIClient()
        self.graph_client = MSGraphClient()
        self.folder_mappings = {}  # Store folder mappings per account
        self.batch_id = batch_id
        self.batch_size = BATCH_SIZE
        self.stop_requested = False
        self.metrics = {
            "emails_processed": 0,
            "emails_classified": 0,
            "emails_skipped": 0,
            "emails_errored": 0,
            "emails_moved": 0,
            "clean_text_extracted": 0
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
        """Process a single email message – classify, store, and move to folder with clean text."""
        message_id = msg.get("id", "unknown_id")
        source_account = msg.get("source_account", "")  # NEW: Get source account
        
        try:
            # ── 1 ▸ extract ALL metadata including headers ──────────────────────
            sender_info = msg.get("from", {}).get("emailAddress", {})
            sender = sender_info.get("address", "")
            subject = msg.get("subject", "")
            received = msg.get("receivedDateTime", "")
            has_attachments = msg.get("hasAttachments", False)
            conversation_id = msg.get("conversationId", "")
            
            # Extract clean body content (no HTML, no threads) + thread detection
            clean_body, data_source, had_threads = self.graph_client._extract_clean_email_content(msg)
            
            # Extract headers
            headers = msg.get("internetMessageHeaders", [])
            
            # Extract all recipients
            to_recipients = msg.get("toRecipients", [])
            cc_recipients = msg.get("ccRecipients", [])
            recipient_emails = []
            
            # Primary recipient
            recipient = ""
            if to_recipients:
                recipient_info = to_recipients[0].get("emailAddress", {})
                recipient = recipient_info.get("address", "")
                recipient_emails.extend([r.get("emailAddress", {}).get("address", "") for r in to_recipients])
            
            # Add CC recipients to the list
            if cc_recipients:
                recipient_emails.extend([r.get("emailAddress", {}).get("address", "") for r in cc_recipients])

            logger.info("Processing email %s | From: %s | Subject: %s | Account: %s | Data Source: %s | Clean Text Length: %d | Had Threads: %s",
                        message_id, sender, subject, source_account, data_source, len(clean_body), had_threads)

            # Track clean text extraction
            if data_source.startswith("uniqueBody"):
                self.metrics["clean_text_extracted"] += 1

            # ── 2 ▸ skip duplicates ─────────────────────────────────────────────
            if self.mongo.email_exists(message_id):
                logger.info("Skipping already-processed email: %s", message_id)
                self.metrics["emails_skipped"] += 1
                return

            # ── 3 ▸ classify with FULL context including thread info ────────────
            try:
                classification_result = self.model_api.classify_email(
                    subject=subject,
                    body=clean_body,  # Use clean text for classification
                    headers=headers,
                    sender_email=sender,
                    recipient_emails=recipient_emails,
                    has_attachments=has_attachments,
                    had_threads=had_threads  # NEW: Pass thread information to model
                )
                label = classification_result.get("label", "uncategorised")
                confidence = classification_result.get("confidence", 0.0)

                if label not in ALLOWED_LABELS:
                    logger.warning("Classifier returned non-allowed label '%s'; using 'uncategorised'", label)
                    label = "uncategorised"

                logger.info("Email %s classified as '%s' (%.2f) | Had Threads: %s",
                            message_id, label, confidence, had_threads)
                self.metrics["emails_classified"] += 1

            except Exception:
                logger.exception("Error during classification for email %s; defaulting to uncategorised",
                                message_id)
                label = "uncategorised"
                confidence = 0.0
                classification_result = {"label": label, "confidence": confidence, "method": "api_error"}

            entities = classification_result.get("entities", {})

            # ── 3.5 ▸ extract and process file numbers for debtor contact matching ──
            file_numbers = entities.get("file_numbers", [])
            primary_file_number = file_numbers[0] if file_numbers else None
            
            # Log file number extraction for debugging
            if file_numbers:
                logger.info("Email %s contains file numbers: %s | Primary: %s", 
                        message_id, file_numbers, primary_file_number)
            else:
                logger.debug("Email %s contains no file numbers - will use email-based matching", message_id)

            # ── 4 ▸ generate a reply if needed ──────────────────────────────────
            reply_text = ""
            try:
                if label in RESPONSE_LABELS:
                    logger.info("Generating response for %s with label '%s'", message_id, label)
                    reply_text = self.model_api.generate_reply(
                        subject=subject,
                        body=clean_body,  # Use clean text for reply generation
                        label=label,
                        entities=entities,
                    )
                    if not reply_text:
                        logger.warning("Empty reply generated for email %s", message_id)
            except Exception:
                logger.exception("Error generating response for email %s", message_id)
                label = "uncategorised"
                reply_text = ""

            # ── 5 ▸ flags for drafts / manual review ────────────────────────────
            needs_manual_review = label in ["manual_review", "uncategorised"]
            save_as_draft = needs_manual_review
            if not MAIL_SEND_ENABLED or FORCE_DRAFTS:
                save_as_draft = True
                logger.info("Forcing email %s to draft due to configuration", message_id)

            # ── 6 ▸ assemble document for Mongo with all metadata including file numbers ────
            self.mongo.set_batch_id(self.batch_id)          # associate batch

            email_data = {
                "message_id":        message_id,
                "sender":            sender,
                "recipient":         recipient,
                "to":                recipient,             # back-compat
                "subject":           subject,
                "body":              clean_body,            # Store clean text
                "text":              clean_body,            # back-compat
                "received_at":       received,
                "classification":    label,
                "prediction":        label,
                "confidence":        confidence,
                "method":            classification_result.get("method", ""),
                "response":          reply_text,
                "response_sent":     False if reply_text else None,
                "processed_at":      datetime.utcnow().isoformat(),
                "batch_id":          self.batch_id,
                "response_process":  False,
                "save_as_draft":     save_as_draft,
                "draft_saved":       False,
                "target_folder":     label,
                "entities":          entities,
                "conversation_id":   conversation_id,
                "has_attachments":   has_attachments,
                "data_source":       data_source,  # Track clean text source
                "had_threads":       had_threads,  # NEW: Store thread information
                "source_account":    source_account,  # NEW: Store source account
                # ── NEW: File number fields for debtor contact matching ──
                "file_numbers":      file_numbers,           # All extracted file numbers
                "primary_file_number": primary_file_number,  # Primary file number for matching
                "headers": {
                    "internet_headers": headers,
                    "to_recipients": to_recipients,
                    "cc_recipients": cc_recipients,
                    "all_recipients": recipient_emails
                },
                "metadata": {
                    "entities":              entities,
                    "confidence_score":      confidence,
                    "classification_method": classification_result.get("method", ""),
                    "matching_patterns":     classification_result.get("matching_patterns", []),
                    "conversation_id":       conversation_id,
                    "has_attachments":       has_attachments,
                    "headers_count":         len(headers),
                    "recipients_count":      len(recipient_emails),
                    "body_length":          len(clean_body),
                    "clean_text_source":    data_source,  # Track source of clean text
                    "had_threads":          had_threads,  # NEW: Store in metadata too
                    "source_account":       source_account,  # NEW: Store in metadata too
                    # ── NEW: File number metadata for tracking and debugging ──
                    "file_numbers_count":   len(file_numbers),
                    "has_file_numbers":     bool(file_numbers),
                    "primary_file_number":  primary_file_number,
                    "all_file_numbers":     file_numbers,
                    # ── NEW: Debtor contact matching strategy ──
                    "debtor_matching_strategy": "file_number_primary" if primary_file_number else "email_fallback",
                    "debtor_matching_key":      primary_file_number or sender,
                },
            }

            # ▸ copy OOO / left-company info if present (rest of the code remains same)
            if "ooo_person" in entities:
                email_data["ooo_person"]         = entities.get("ooo_person", {})
                email_data["ooo_contact_person"] = entities.get("ooo_contact_person", {})
                email_data["ooo_dates"]          = entities.get("ooo_dates", {})
                email_data["metadata"]["out_of_office"] = {
                    "ooo_person":     email_data["ooo_person"],
                    "contact_person": email_data["ooo_contact_person"],
                    "ooo_dates":      email_data["ooo_dates"],
                }

            if "left_person" in entities:
                email_data["left_person"]        = entities.get("left_person", {})
                email_data["replacement_contact"] = entities.get("replacement_contact", {})
                email_data["metadata"]["left_company"] = {
                    "left_person": email_data["left_person"],
                    "replacement": email_data["replacement_contact"],
                }

            # ── NEW: Enhanced file number info in metadata for debugging ──
            if file_numbers:
                email_data["metadata"]["file_number_extraction"] = {
                    "extracted_count": len(file_numbers),
                    "all_numbers": file_numbers,
                    "primary_number": primary_file_number,
                    "extraction_source": "entities_api_response",
                    "will_use_for_matching": True
                }
            else:
                email_data["metadata"]["file_number_extraction"] = {
                    "extracted_count": 0,
                    "all_numbers": [],
                    "primary_number": None,
                    "extraction_source": "none_found",
                    "will_use_for_matching": False,
                    "fallback_to": "email_address"
                }

            # ── 7 ▸ insert in Mongo ─────────────────────────────────────────────
            result = self.mongo.insert_email(email_data)
            if result and hasattr(result, 'inserted_id'):
                self.mongo.last_inserted_id = result.inserted_id
            logger.info("Email %s inserted into MongoDB with clean text (source: %s, had_threads: %s, file_numbers: %s)", 
                    message_id, data_source, had_threads, len(file_numbers))

            # ── 8 ▸ move to folder first ────────────────────────────────────────
            # Get folder mapping for this specific account
            folder_mapping = self.folder_mappings.get(source_account, {})
            folder_id = folder_mapping.get(label)
            logger.debug("Folder mapping for '%s' → %s (account: %s)", label, folder_id, source_account)
            
            old_id = message_id  # Store original ID before move
            
            if folder_id:
                move_success, new_id = self.graph_client.move_email_to_folder(
                    message_id, folder_id, email_address=source_account
                )
                if move_success and new_id:
                    logger.info("Email %s moved to folder for label '%s' in account %s", message_id, label, source_account)
                    self.mongo.update_message_id(old_id, new_id)
                    message_id = new_id  # Update message_id with the new ID
                    self.metrics["emails_moved"] += 1
                else:
                    logger.warning("Move failed for email %s (label '%s') in account %s", message_id, label, source_account)
            else:
                logger.warning("No folder mapping for label '%s' in account %s; email %s left in Inbox",
                            label, source_account, message_id)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Available folder-mapping keys for %s: %s", source_account, list(folder_mapping.keys()))

            # ── 9 ▸ then mark read/unread in Outlook ────────────────────────────
            is_read = label not in ["manual_review", "uncategorised"]
            self.graph_client.mark_email_read_status(message_id, is_read, email_address=source_account)  # Using the new ID if moved

            self.metrics["emails_processed"] += 1

        except Exception:
            logger.exception("Unhandled error processing email %s", message_id)
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
    
    def process_unread_emails(self, emails_per_account=None) -> Tuple[bool, int, int, int]:
        """Process all unread emails in the inbox - fetch, classify, and move with clean text."""
        try:
            # Step 1: Prepare the batch
            self._prepare_batch()
            
            # Step 2: Ensure folders for classification FOR EACH ACCOUNT
            logger.info("Setting up classification folders for all email accounts...")
            for email_address in self.graph_client.email_addresses:
                try:
                    folder_mapping = self.graph_client.ensure_classification_folders(email_address)
                    if folder_mapping:
                        self.folder_mappings[email_address] = folder_mapping
                        logger.info(f"Folder mapping ready for {email_address}: {len(folder_mapping)} folders")
                    else:
                        logger.error(f"Could not create folder mapping for {email_address}")
                except Exception as e:
                    logger.error(f"Error setting up folders for {email_address}: {str(e)}")
            
            if not self.folder_mappings:
                logger.error("Could not create folder mappings for any account. Aborting.")
                return False, 0, 0, 0
                
            # Step 3: Fetch unread emails with clean text from all accounts
            logger.info("Fetching unread emails with clean text extraction from all accounts...")
            emails = self.graph_client.fetch_unread_emails(emails_per_account=emails_per_account)
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
            
            # Log clean text extraction metrics
            logger.info(f"Clean text extraction summary: {self.metrics['clean_text_extracted']} emails used uniqueBody (thread-free)")
            
            # Return success status and metrics for batch tracking
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]
            
        except KeyboardInterrupt:
            logger.info("Batch processor stopped by user")
            self.stop_requested = True
            return True, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]
        except Exception as e:
            logger.exception(f"Error in process_unread_emails: {str(e)}")
            return False, self.metrics["emails_processed"], self.metrics["emails_errored"], self.metrics["emails_classified"]


def process_unread_emails(batch_id=None, emails_per_account=None) -> Dict:
   """Public interface function to process unread emails with clean text extraction.
   
   Returns a dictionary with processing metrics.
   """
   processor = EmailProcessor(batch_id)
   success, processed, errors, classified = processor.process_unread_emails(emails_per_account)
   
   return {
       "success": success,
       "emails_processed": processed,
       "emails_classified": classified,
       "emails_errored": errors,
       "emails_moved": processor.metrics["emails_moved"],
       "clean_text_extracted": processor.metrics["clean_text_extracted"],
       "batch_id": processor.batch_id
   }