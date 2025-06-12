"""
email_sender.py - Module for handling all email reply creation and sending operations.

This module provides a centralized interface for:
1. Creating draft emails
2. Sending emails directly
3. Processing batches of draft emails
4. Sending pending replies

It handles Microsoft Graph API integration and database updates.
"""

import os
import sys
import base64
import httpx
import msal
import time
import random
from functools import wraps
from datetime import datetime
from dotenv import load_dotenv
from src.db import get_mongo, PostgresHelper
from src.log_config import logger

# Load environment variables
load_dotenv()

# Global configuration flags - read directly from environment
MAIL_SEND_ENABLED = os.getenv("MAIL_SEND_ENABLED", "False").lower() in ["true", "1", "yes"]
FORCE_DRAFTS = os.getenv("FORCE_DRAFTS", "True").lower() in ["true", "1", "yes"]


# Log email sending configuration
if MAIL_SEND_ENABLED:
    logger.warning("🚨 MAIL_SEND_ENABLED is TRUE - Emails will be SENT rather than saved as drafts")
    logger.warning("Set MAIL_SEND_ENABLED=False in .env to prevent sending emails")
else:
    logger.info("📝 Email sending is disabled - all emails will be saved as drafts")

if FORCE_DRAFTS:
    logger.info("📝 FORCE_DRAFTS is enabled - all emails will be saved as drafts regardless of other settings")


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
            
            # This should never happen, but just in case
            if last_exception:
                raise last_exception
            return None
        return wrapper
    return decorator

class Config:
    """Configuration class for email processing system."""
    
    def __init__(self):
        # Microsoft Graph API configuration
        self.ms_graph_base_url = "https://graph.microsoft.com/v1.0"
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.tenant_id = os.getenv("TENANT_ID")
        self.email_address = os.getenv("EMAIL_ADDRESS")
        self.scopes = ["https://graph.microsoft.com/.default"]

        # Domain configuration
        self.your_domain = "ABC-AMEGA.COM"
        self.company_name = "ABC/AMEGA"
        
        # Email label configurations
        self.allowed_labels = [
            "no_reply_no_info",
            "no_reply_with_info",
            "auto_reply_no_info",
            "auto_reply_with_info",
            "invoice_request_no_info",
            "claims_paid_no_proof",
            "manual_review"
        ]
        
        self.no_response_labels = [
            "no_reply_no_info",
            "no_reply_with_info",
            "auto_reply_no_info", 
            "auto_reply_with_info",
            "manual_review"
        ]
        
        self.response_labels = [
            "invoice_request_no_info",
            "claims_paid_no_proof"
        ]
        
        # Email retry configuration
        self.max_retries = 2
        self.retry_delay = 3  # seconds
        
        # Email footer settings
        self.add_footer = os.getenv("ADD_EMAIL_FOOTER", "true").lower() in ["true", "yes", "1"]
        self.footer_template = (
            "\n\n---\n"
            "This email was generated automatically by {company_name} System.\n"
            "Sent on: {sent_datetime}"
        )


class MSGraphAuth:
    """Microsoft Graph API authentication handler using application permissions."""
    
    def __init__(self, config=None):
        self.config = config or Config()
        self._token_cache = {}  # Cache tokens in memory
    
    def get_access_token(self, force_refresh=False):
        """Get an access token using client credentials flow."""
        try:
            # Check if we have a cached valid token
            current_time = time.time()
            if not force_refresh and self._token_cache.get("token") and self._token_cache.get("expires_at", 0) > current_time + 60:
                logger.debug("Using cached access token")
                return self._token_cache["token"]
            
            # Log client and tenant ID for debugging
            logger.debug(f"Using client_id: {self.config.client_id[:8]}*** and tenant_id: {self.config.tenant_id}")
            
            app = msal.ConfidentialClientApplication(
                client_id=self.config.client_id,
                client_credential=self.config.client_secret,
                authority=f"https://login.microsoftonline.com/{self.config.tenant_id}"
            )
            
            # Acquire token for application permissions
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            
            if "access_token" in result:
                # Cache the token with expiration
                self._token_cache = {
                    "token": result["access_token"],
                    "expires_at": current_time + result.get("expires_in", 3600)
                }
                logger.debug(f"Successfully acquired access token using application permissions")
                return result["access_token"]
            else:
                logger.error(f"Error acquiring token: {result.get('error')}: {result.get('error_description')}")
                raise Exception(f"Failed to acquire token: {result.get('error_description')}")
                
        except Exception as e:
            logger.exception(f"Error getting access token: {str(e)}")
            raise


class EmailValidator:
    """Validates email responses and determines processing logic."""
    
    def __init__(self, config=None):
        self.config = config or Config()
    
    def is_valid_reply(self, reply):
        """Check if the generated reply is valid."""
        if not reply or not isinstance(reply, str):
            return False
        reply = reply.strip()
        if not reply or reply.startswith("(Reply generation failed") or reply.startswith("[Error generating reply"):
            return False
        return True
    
    def should_skip_email(self, email):
        """Determine if an email should be skipped."""
        # Check if the email has already been processed
        if email.get("draft_saved") is True:
            logger.info(f"Skipping already saved draft: {email.get('message_id')}")
            return True
            
        if email.get("response_sent") is True:
            logger.info(f"Skipping already sent email: {email.get('message_id')}")
            return True
        
        # Process if it has a response and should be saved as draft
        if email.get("save_as_draft", False) is True and email.get("response"):
            return False
            
        # Process if it has a response and a response label
        label = email.get("prediction", "")
        if label in self.config.response_labels and email.get("response"):
            return False
        
        # Skip if email has a no-response label and no response
        if label in self.config.no_response_labels and not email.get("response"):
            return True
            
        # Skip if label is not in our response labels and not marked for draft
        if label not in self.config.response_labels and not email.get("save_as_draft"):
            return True
            
        # Skip if response is marked as None (explicitly set not to respond)
        if email.get("response_sent") is None:
            return True
            
        return False


class EmailSender:
    """Handles sending emails and saving drafts."""
    
    def __init__(self, config=None, auth_manager=None):
        self.config = config or Config()
        self.auth_manager = auth_manager or MSGraphAuth(self.config)
    
    def _add_footer(self, body):
        """Add a standardized footer to the email body."""
        if not self.config.add_footer:
            return body
            
        footer = self.config.footer_template.format(
            company_name=self.config.company_name,
            sent_datetime=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        )
        return body + footer
    
    def _handle_http_error(self, response, operation, retry_attempt=0):
        """Handle common HTTP errors with appropriate actions."""
        status_code = response.status_code
        
        # Handle token expiration
        if status_code == 401:
            logger.warning(f"Access token expired during {operation}. Refreshing token...")
            try:
                # Force token refresh
                _ = self.auth_manager.get_access_token(force_refresh=True)
                return True  # Signal retry
            except Exception as e:
                logger.error(f"Failed to refresh token: {str(e)}")
                return False
                
        # Handle rate limiting
        elif status_code == 429:
            retry_after = response.headers.get('Retry-After')
            wait_time = int(retry_after) if retry_after else (2 + retry_attempt)
            logger.warning(f"Rate limit hit during {operation}. Waiting for {wait_time}s before retry...")
            time.sleep(wait_time)
            return True  # Signal retry
            
        # Handle server errors that might be temporary
        elif 500 <= status_code < 600:
            if retry_attempt < self.config.max_retries:
                wait_time = self.config.retry_delay * (retry_attempt + 1)
                logger.warning(f"Server error {status_code} during {operation}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return True  # Signal retry
                
        # Handle other errors
        logger.error(f"HTTP error during {operation}: {status_code} - {response.text}")
        return False
    
    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def save_as_draft(self, to_address, subject, body, message_id=None, batch_id=None, retry_attempt=0):
        """Save an email as draft instead of sending it."""
        try:
            logger.info(f"Attempting to save draft to {to_address} with subject: '{subject[:30]}...'")
            
            # Check if email address is configured
            if not self.config.email_address:
                logger.error("EMAIL_ADDRESS environment variable is not set or is empty")
                return None
                
            logger.info(f"Using email address: {self.config.email_address}")
            
            # Get access token
            access_token = self.auth_manager.get_access_token()
            logger.debug("Successfully got access token for draft creation")
        except Exception as e:
            logger.error(f"Failed to get access token: {str(e)}")
            return None
        
        # Add footer if configured
        body = self._add_footer(body)
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Build message payload
        message_payload = {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to_address
                    }
                }
            ],
            "importance": "Normal",
            "replyTo": [
                {
                    "emailAddress": {
                        "address": f"support@{self.config.your_domain}"
                    }
                }
            ]
        }
        
        # Create draft endpoint - updated to use users/{email} instead of me
        endpoint = f"{self.config.ms_graph_base_url}/users/{self.config.email_address}/messages"
        logger.info(f"Creating draft using endpoint: {endpoint}")
        
        try:
            # Log request details for debugging
            logger.debug(f"Draft request headers: {headers}")
            logger.debug(f"Draft request payload: {message_payload}")
            
            response = httpx.post(endpoint, headers=headers, json=message_payload, timeout=30.0)
            
            # Log detailed response for debugging
            logger.info(f"Draft creation response status: {response.status_code}")
            
            if response.status_code in [200, 201]:
                # If successful, the response will contain the created draft
                draft_data = response.json()
                draft_id = draft_data.get("id")
                logger.info(f"Email saved as draft for {to_address} with draft ID: {draft_id}")
                
                # Log response details for successful drafts
                logger.debug(f"Draft creation successful response: {draft_data}")
                
                # Update MongoDB if message_id is provided
                if message_id:
                    mongo = get_mongo()
                    mongo.mark_email_draft_saved(message_id, draft_id)
                
                return draft_id
            
            # Log detailed error information
            logger.error(f"Failed to save draft. Status code: {response.status_code}")
            logger.error(f"Response body: {response.text}")
            
            # Handle errors with retry logic
            if self._handle_http_error(response, "draft creation", retry_attempt):
                # FIX: Always increment retry_attempt on retry
                next_retry = retry_attempt + 1
                if next_retry < self.config.max_retries:
                    logger.info(f"Retrying draft creation after error (attempt {next_retry+1})")
                    return self.save_as_draft(to_address, subject, body, message_id, batch_id, next_retry)
            
            return None
                
        except httpx.HTTPError as e:
            logger.error(f"HTTP error saving draft: {str(e)}")
            if retry_attempt < self.config.max_retries:
                logger.info(f"Retrying draft creation ({retry_attempt + 1}/{self.config.max_retries})...")
                time.sleep(self.config.retry_delay)
                return self.save_as_draft(to_address, subject, body, message_id, batch_id, retry_attempt + 1)
            return None
        except Exception as e:
            logger.exception(f"Error saving draft: {str(e)}")
            return None

    @retry_with_backoff(max_retries=3, initial_backoff=1.5)
    def send_email(self, to_address, subject, body, message_id=None, batch_id=None, retry_attempt=0):
        """Send an email directly."""
        # Safety check - never send if mail sending is disabled or force drafts is enabled
        if not MAIL_SEND_ENABLED or FORCE_DRAFTS:
            logger.warning(f"⚠️ ATTEMPTED TO SEND EMAIL TO {to_address}, BUT MAIL_SEND_ENABLED={MAIL_SEND_ENABLED} OR FORCE_DRAFTS={FORCE_DRAFTS}")
            logger.info("Saving as draft instead of sending due to environment configuration")
            return self.save_as_draft(to_address, subject, body, message_id, batch_id, retry_attempt)
        
        try:
            logger.info(f"Attempting to send email to {to_address} with subject: '{subject[:30]}...'")
            
            # Check if email address is configured
            if not self.config.email_address:
                logger.error("EMAIL_ADDRESS environment variable is not set or is empty")
                return False
                
            logger.info(f"Using email address: {self.config.email_address}")
            
            access_token = self.auth_manager.get_access_token()
            logger.debug("Successfully got access token for email sending")
        except Exception as e:
            logger.error(f"Failed to get access token: {str(e)}")
            return False
        
        # Add footer if configured
        body = self._add_footer(body)
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Build message payload
        message_payload = {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to_address
                    }
                }
            ],
            "importance": "Normal",
            "replyTo": [
                {
                    "emailAddress": {
                        "address": f"support@{self.config.your_domain}"
                    }
                }
            ]
        }
        
        payload = {
            "message": message_payload,
            "saveToSentItems": "true"
        }
        
        # Updated to use users/{email} instead of me
        endpoint = f"{self.config.ms_graph_base_url}/users/{self.config.email_address}/sendMail"
        logger.info(f"Sending email using endpoint: {endpoint}")
        
        try:
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=30.0)
            
            # Log response details
            logger.info(f"Email sending response status: {response.status_code}")
            
            # Check if successful
            if response.status_code in [200, 202]:
                logger.info(f"Email sent successfully to {to_address}")
                
                # Update MongoDB if message_id is provided
                if message_id:
                    mongo = get_mongo()
                    mongo.mark_email_sent(message_id)
                
                return True
            
            # Log detailed error information
            logger.error(f"Failed to send email. Status code: {response.status_code}")
            logger.error(f"Response body: {response.text}")
            
            # Handle errors with retry logic
            if self._handle_http_error(response, "email sending", retry_attempt):
                # FIX: Always increment retry_attempt on retry
                next_retry = retry_attempt + 1
                if next_retry < self.config.max_retries:
                    logger.info(f"Retrying email sending after error (attempt {next_retry+1})")
                    return self.send_email(to_address, subject, body, message_id, batch_id, next_retry)
            
            return False
                
        except httpx.HTTPError as e:
            logger.error(f"HTTP error sending email: {str(e)}")
            if retry_attempt < self.config.max_retries:
                logger.info(f"Retrying email send ({retry_attempt + 1}/{self.config.max_retries})...")
                time.sleep(self.config.retry_delay)
                return self.send_email(to_address, subject, body, message_id, batch_id, retry_attempt + 1)
            return False
        except Exception as e:
            logger.exception(f"Error sending email: {str(e)}")
            return False


class EmailProcessor:
    """Main email processing logic for batches of emails."""
    
    def __init__(self, config=None, email_validator=None, email_sender=None):
        self.config = config or Config()
        self.email_validator = email_validator or EmailValidator(self.config)
        self.email_sender = email_sender or EmailSender(self.config)
        self.already_warned_no_pending = False
        self.mongo = get_mongo()  # Get MongoDB connector
        self.current_batch_stats = {
            "drafts_created": 0,
            "drafts_failed": 0,
            "emails_sent": 0,
            "emails_failed": 0,
            "emails_skipped": 0
        }
    
    def _update_postgres_batch(self, batch_id, action_type):
        """Update PostgreSQL batch run record with latest stats."""
        if not batch_id:
            return
            
        try:
            status = "success"
            if action_type == "draft" and self.current_batch_stats["drafts_failed"] > 0:
                status = "partial"
            elif action_type == "send" and self.current_batch_stats["emails_failed"] > 0:
                status = "partial"
                
            PostgresHelper.update_batch_result(
                batch_id=batch_id,
                processed_count=(self.current_batch_stats["emails_sent"] + self.current_batch_stats["drafts_created"]),
                failed_count=(self.current_batch_stats["emails_failed"] + self.current_batch_stats["drafts_failed"]),
                status=status,
                draft_count=self.current_batch_stats["drafts_created"]
            )
            logger.info(f"Updated PostgreSQL batch {batch_id} with {action_type} status: {status}")
            
        except Exception as e:
            logger.error(f"Failed to update PostgreSQL batch {batch_id}: {str(e)}")
    
    def process_email(self, email):
        """Process an email - either send it or save as draft based on flag."""
        message_id = email.get("message_id", "unknown")
        logger.info(f"Processing email with message_id: {message_id}")
        
        try:
            # Check for duplicate processing - skip if already processed
            if email.get("draft_saved") is True:
                logger.info(f"Email {message_id} already saved as draft - skipping")
                self.current_batch_stats["emails_skipped"] += 1
                return True, True
                
            if email.get("response_sent") is True:
                logger.info(f"Email {message_id} already sent - skipping")
                self.current_batch_stats["emails_skipped"] += 1
                return True, False
            
            # Skip emails that shouldn't receive responses
            if self.email_validator.should_skip_email(email):
                logger.info(f"Skipping email {message_id} - should not receive a response")
                # Do NOT mark as sent; leave it un-sent so we can see it later during testing
                # Instead mark it as "draft saved" so dashboards see it as handled
                self.mongo.mark_email_draft_saved(email.get("message_id"))
                self.current_batch_stats["emails_skipped"] += 1
                return True, False  # Success, not a draft
            
            # Determine if email should be saved as draft
            # Use environment variables to override if needed
            original_save_as_draft = email.get("save_as_draft", False)
            save_draft = True if (FORCE_DRAFTS or not MAIL_SEND_ENABLED) else original_save_as_draft
            
            if save_draft != original_save_as_draft:
                logger.info(f"Email {message_id} save_as_draft flag was {original_save_as_draft}, overridden to {save_draft} by environment settings")
            else:
                logger.info(f"Email {message_id} save_as_draft flag: {save_draft}")
            
            # Get and validate response text
            response_text = str(email.get("response", "")).strip()
            
            # Skip invalid replies
            if not self.email_validator.is_valid_reply(response_text):
                logger.info(f"Email {message_id} has invalid reply text")
                if save_draft:
                    self.mongo.mark_email_draft_saved(email.get("message_id"))
                    self.current_batch_stats["drafts_created"] += 1
                else:
                    self.mongo.mark_email_sent(email.get("message_id"))
                    self.current_batch_stats["emails_skipped"] += 1
                return True, save_draft
                
            # Get recipient address
            to_address = str(email.get("sender", "")).strip()
            
            # Skip if no valid recipient
            if not to_address:
                logger.warning(f"Email {message_id} has no valid recipient address")
                if save_draft:
                    self.mongo.mark_email_draft_saved(email.get("message_id"))
                    self.current_batch_stats["drafts_created"] += 1
                else:
                    self.mongo.mark_email_sent(email.get("message_id"))
                    self.current_batch_stats["emails_skipped"] += 1
                return True, save_draft
                
            # Prepare email content
            subject = f"Re: {email.get('subject', 'No Subject')}"
            body = response_text
            
            if save_draft:
                # Save as draft instead of sending
                logger.info(f"Saving email {message_id} as draft to {to_address}")
                draft_id = self.email_sender.save_as_draft(
                    to_address=to_address,
                    subject=subject,
                    body=body,
                    message_id=email.get("message_id"),
                    batch_id=email.get("batch_id")
                )
                
                if draft_id:
                    logger.info(f"Email saved as draft for review: {email.get('message_id')}")
                    self.current_batch_stats["drafts_created"] += 1
                    return True, True  # Success, is a draft
                else:
                    logger.warning(f"Failed to save draft for: {email.get('message_id')}")
                    self.current_batch_stats["drafts_failed"] += 1
                    return False, True  # Failed, is a draft
            else:
                # Send the email directly
                logger.info(f"Sending email {message_id} to {to_address}")
                send_success = self.email_sender.send_email(
                    to_address=to_address,
                    subject=subject,
                    body=body,
                    message_id=email.get("message_id"),
                    batch_id=email.get("batch_id")
                )
                
                if send_success:
                    logger.info(f"Email {message_id} sent successfully")
                    self.current_batch_stats["emails_sent"] += 1
                    return True, False  # Success, not a draft
                
                logger.warning(f"Failed to send email {message_id}")
                self.current_batch_stats["emails_failed"] += 1
                return False, False  # Failed, not a draft
            
        except Exception as e:
            logger.exception(f"Error processing email {message_id}: {str(e)}")
            if email.get("save_as_draft", False):
                self.current_batch_stats["drafts_failed"] += 1
            else:
                self.current_batch_stats["emails_failed"] += 1
            return False, email.get("save_as_draft", False)
        
    def process_draft_emails(self, batch_id=None):
        """Process emails that should be saved as drafts."""
        try:
            # Reset batch stats
            self.current_batch_stats = {
                "drafts_created": 0,
                "drafts_failed": 0,
                "emails_sent": 0,
                "emails_failed": 0,
                "emails_skipped": 0
            }
            
            # Get emails that should be saved as drafts
            drafts = self.mongo.find_draft_emails(batch_id)
            
            if not drafts:
                logger.info("No emails to save as drafts.")
                
                # Debug check to find emails by response labels
                response_emails = list(self.mongo.collection.find({
                    "prediction": {"$in": self.config.response_labels},
                    "response": {"$exists": True, "$ne": ""},
                    "response_sent": False
                }).limit(10))
                
                if response_emails:
                    logger.info(f"Found {len(response_emails)} emails with response labels that need responses")
                    for email in response_emails:
                        logger.info(f"Email ID: {email.get('message_id')}, Label: {email.get('prediction')}, Has Response: {bool(email.get('response'))}, Save as Draft: {email.get('save_as_draft')}")
                
                return 0, 0
                    
            logger.info(f"Found {len(drafts)} emails to save as drafts")
            
            # Log details about the found drafts
            for i, email in enumerate(drafts[:5]):  # Log details for first 5 drafts
                logger.info(f"Draft {i+1}: ID={email.get('message_id')}, Label={email.get('prediction')}, Has Response={bool(email.get('response'))}")
            
            # Process each draft email
            for email in drafts:
                success, is_draft = self.process_email(email)
            
            # Update PostgreSQL batch if provided
            if batch_id:
                self._update_postgres_batch(batch_id, "draft")
                
            logger.info(f"Draft processing complete: {self.current_batch_stats['drafts_created']} saved as drafts, "
                    f"{self.current_batch_stats['drafts_failed']} failed")
                    
            return self.current_batch_stats["drafts_created"], self.current_batch_stats["drafts_failed"]
                
        except Exception as e:
            logger.exception(f"Error processing draft emails: {str(e)}")
            return 0, 0
    
    def send_pending_replies(self, batch_id=None):
        """Send all pending email replies with valid responses."""
        # If mail sending is disabled or forcing drafts, mark all pending emails as draft
        if not MAIL_SEND_ENABLED or FORCE_DRAFTS:
            logger.info(f"Mail sending is disabled (MAIL_SEND_ENABLED={MAIL_SEND_ENABLED}) or forced to drafts (FORCE_DRAFTS={FORCE_DRAFTS})")
            logger.info("Converting all pending replies to drafts instead of sending...")
            
            try:
                # Update all pending emails to be saved as drafts
                update_result = self.mongo.collection.update_many(
                    {
                        "response": {"$exists": True, "$ne": None},
                        "response_sent": {"$ne": True},
                        "save_as_draft": {"$ne": True},
                        "prediction": {"$in": self.config.response_labels}
                    },
                    {"$set": {"save_as_draft": True}}
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"Updated {update_result.modified_count} emails to be saved as drafts")
                    # Now process them as drafts 
                    return self.process_draft_emails(batch_id)
                else:
                    logger.info("No emails needed to be updated to draft mode")
                    return 0, 0
            except Exception as e:
                logger.error(f"Error updating emails to draft mode: {str(e)}")
                return 0, 0
        
        try:
            # Reset batch stats for sending
            self.current_batch_stats = {
                "drafts_created": 0,
                "drafts_failed": 0,
                "emails_sent": 0,
                "emails_failed": 0,
                "emails_skipped": 0
            }
            
            logger.info(f"Checking for pending replies... (current time: {datetime.utcnow().isoformat()})")
            
            # Find emails with responses that haven't been sent yet
            query = {
                "response": {"$exists": True, "$ne": None},  # Has a response
                "response_sent": {"$ne": True},              # Not already sent
                "save_as_draft": {"$ne": True},              # Not meant to be saved as draft
                "prediction": {"$in": self.config.response_labels}  # Only include emails with response labels
            }
            
            # Add batch_id filter if provided
            if batch_id:
                query["batch_id"] = batch_id
                
            pending = list(self.mongo.collection.find(query))
            # If we have no pending replies
            if not pending:
                if not self.already_warned_no_pending:
                    logger.info("No pending replies found.")
                    self.already_warned_no_pending = True
                return 0, 0
            
            self.already_warned_no_pending = False
            logger.info(f"Found {len(pending)} pending emails to send.")
            # Process each pending email
            for email in pending:
                try:
                    success, is_draft = self.process_email(email)
                    
                except Exception as e:
                    logger.exception(f"Error processing email id={email.get('_id')}: {str(e)}")
                    self.current_batch_stats["emails_failed"] += 1
            
            # Update PostgreSQL batch if provided
            if batch_id:
                self._update_postgres_batch(batch_id, "send")
            
            # Log summary
            logger.info(f"Email processing summary: {self.current_batch_stats['emails_sent']} sent, "
                        f"{self.current_batch_stats['emails_skipped']} skipped, "
                        f"{self.current_batch_stats['emails_failed']} failed")
                        
            return self.current_batch_stats["emails_sent"], self.current_batch_stats["emails_failed"]
        except Exception as e:
            logger.exception(f"Error in send_pending_replies: {str(e)}")
            return 0, 0
    
    def get_email_stats(self):
        """Return current email stats dictionary."""
        return self.current_batch_stats


# ========================
# Public interface functions for other modules to use
# ========================

def get_email_sender():
    """Get a configured email sender instance."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    return EmailSender(config, auth_manager)


def save_as_draft(to_address, subject, body, message_id=None, batch_id=None, retry_attempt=0):
    """Save an email as draft and update databases."""
    email_sender = get_email_sender()
    return email_sender.save_as_draft(to_address, subject, body, message_id, batch_id, retry_attempt)


def send_email(to_address, subject, body, message_id=None, batch_id=None, retry_attempt=0):
    """Send an email directly and update databases."""
    # Safety check - never send if mail sending is disabled or force drafts is enabled
    if not MAIL_SEND_ENABLED or FORCE_DRAFTS:
        logger.warning(f"⚠️ ATTEMPTED TO SEND EMAIL TO {to_address}, BUT MAIL_SEND_ENABLED={MAIL_SEND_ENABLED} OR FORCE_DRAFTS={FORCE_DRAFTS}")
        logger.info("Saving as draft instead of sending due to environment configuration")
        return save_as_draft(to_address, subject, body, message_id, batch_id, retry_attempt)
    
    email_sender = get_email_sender()
    return email_sender.send_email(to_address, subject, body, message_id, batch_id, retry_attempt)


def process_draft_emails(batch_id=None):
    """Process all emails marked for draft in a batch."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    email_validator = EmailValidator(config)
    email_sender = EmailSender(config, auth_manager)
    email_processor = EmailProcessor(config, email_validator, email_sender)
    return email_processor.process_draft_emails(batch_id)


def send_pending_replies(batch_id=None):
    """Send all pending replies in a batch."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    email_validator = EmailValidator(config)
    email_sender = EmailSender(config, auth_manager)
    email_processor = EmailProcessor(config, email_validator, email_sender)
    return email_processor.send_pending_replies(batch_id)


def process_emails_for_batch(batch_id):
    """Process all emails for a specific batch - both drafts and pending."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    email_validator = EmailValidator(config)
    email_sender = EmailSender(config, auth_manager)
    email_processor = EmailProcessor(config, email_validator, email_sender)
    
    # First process drafts
    draft_success, draft_failed = email_processor.process_draft_emails(batch_id)
    
    # Then process pending replies
    sent_success, sent_failed = email_processor.send_pending_replies(batch_id)
    
    return {
        "drafts_created": draft_success,
        "drafts_failed": draft_failed,
        "emails_sent": sent_success,
        "emails_failed": sent_failed,
        "emails_skipped": email_processor.current_batch_stats["emails_skipped"]
    }


# Main execution (when run directly)
if __name__ == "__main__":
    try:
        # Parse batch_id from command line if provided
        if len(sys.argv) > 1:
            batch_id = sys.argv[1]
            logger.info(f"Processing emails for batch ID: {batch_id}")
            stats = process_emails_for_batch(batch_id)
            
            # Generate a summary table in logs
            logger.info("   EMAIL PROCESSING SUMMARY")
            logger.info(f"Emails Sent:       {stats['emails_sent']}")
            logger.info(f"Drafts Created:    {stats['drafts_created']}")
            logger.info(f"Emails Failed:     {stats['emails_failed']}")
            logger.info(f"Drafts Failed:     {stats['drafts_failed']}")
            logger.info(f"Emails Skipped:    {stats['emails_skipped']}")
            
            # Exit with appropriate code
            if stats['emails_failed'] > 0 or stats['drafts_failed'] > 0:
                sys.exit(1)
            else:
                sys.exit(0)
        else:
            logger.error("No batch ID provided. Usage: python email_sender.py <batch_id>")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)