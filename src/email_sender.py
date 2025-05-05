import os
import sys
import base64
import httpx
import msal
import time
from datetime import datetime
from dotenv import load_dotenv
# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Import both MongoDB and PostgreSQL helpers
from db import get_mongo, PostgresHelper
from log_config import logger
# Load environment variables
load_dotenv()
class Config:
    """Configuration class for email processing system."""
    
    def __init__(self):
        # Microsoft Graph API configuration
        self.ms_graph_base_url = "https://graph.microsoft.com/v1.0"
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.authority = os.getenv("AUTHORITY", "https://login.microsoftonline.com/common")
        self.scopes = ["User.Read", "Mail.ReadWrite", "Mail.Send"]

        # API configuration for the model server
        self.model_api_url = os.getenv("MODEL_API_URL", "http://localhost:8000")
        
        # Domain configuration
        self.your_domain = os.getenv("YOUR_DOMAIN", "yourdomain.com")
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.company_name = os.getenv("COMPANY_NAME", "Your Company")
        
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
    """Microsoft Graph API authentication handler."""
    
    def __init__(self, config):
        self.config = config
        self.refresh_token_path = "refresh_token.txt"
        self._token_cache = {}  # Cache tokens in memory
    
    def get_access_token(self, force_refresh=False):
        """Fetch a valid access token using a refresh token."""
        try:
            # Check if we have a cached valid token
            current_time = time.time()
            if not force_refresh and self._token_cache.get("token") and self._token_cache.get("expires_at", 0) > current_time + 60:
                logger.debug("Using cached access token")
                return self._token_cache["token"]
            
            client = msal.ConfidentialClientApplication(
                client_id=self.config.client_id,
                client_credential=self.config.client_secret,
                authority=self.config.authority,
            )
            
            # Check if refresh token file exists
            if not os.path.exists(self.refresh_token_path):
                logger.error("Refresh token not found. Run fetch_reply.py first to initialize it.")
                raise Exception("Refresh token not found. Run fetch_reply.py first to initialize it.")
            
            # Read refresh token
            try:
                with open(self.refresh_token_path, "r") as file:
                    refresh_token = file.read().strip()
                    
                if not refresh_token:
                    logger.error("Refresh token file exists but is empty")
                    raise Exception("Empty refresh token found. Run fetch_reply.py to reinitialize.")
            except IOError as e:
                logger.error(f"Error reading refresh token file: {str(e)}")
                raise Exception(f"Could not read refresh token: {str(e)}")
            
            # Acquire token with error handling
            token_response = client.acquire_token_by_refresh_token(refresh_token, scopes=self.config.scopes)
            
            # Check for errors in response
            if "error" in token_response:
                error_code = token_response.get("error")
                error_message = token_response.get("error_description", "Unknown error")
                logger.error(f"Token acquisition failed: {error_code} - {error_message}")
                
                # Handle specific error cases
                if "invalid_grant" in error_message.lower() or error_code == "invalid_grant":
                    logger.warning("Invalid grant error - refresh token may have expired or been revoked")
                    # Optional: delete invalid refresh token file
                    # os.remove(self.refresh_token_path)
                    raise Exception("Refresh token has expired. Please run fetch_reply.py to re-authenticate.")
                
                raise Exception(f"Failed to acquire token: {error_code} - {error_message}")
            
            # Check for access token
            if "access_token" in token_response:
                # Cache the token with expiration
                if "expires_in" in token_response:
                    self._token_cache = {
                        "token": token_response["access_token"],
                        "expires_at": current_time + token_response["expires_in"]
                    }
                    logger.debug(f"Token cached, expires in {token_response['expires_in']} seconds")
                else:
                    # Default expiration: 1 hour
                    self._token_cache = {
                        "token": token_response["access_token"],
                        "expires_at": current_time + 3600
                    }
                    logger.debug("Token cached with default 1-hour expiration")
                
                # Save refresh token if provided
                if "refresh_token" in token_response:
                    try:
                        with open(self.refresh_token_path, "w") as file:
                            file.write(token_response["refresh_token"])
                        logger.debug("Saved new refresh token")
                    except IOError as e:
                        logger.warning(f"Could not save new refresh token: {str(e)}")
                        # Continue even if we can't save the new token
                
                return token_response["access_token"]
            else:
                # This should be unreachable if we properly check for errors above
                # But kept as a fallback
                logger.error(f"No access token in response: {token_response}")
                raise Exception("No access token returned. Authentication failed.")
        except Exception as e:
            logger.exception(f"Error getting access token: {str(e)}")
            raise

class EmailValidator:
    """Validates email responses and determines processing logic."""
    
    def __init__(self, config):
        self.config = config
    
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
        # Skip if email has a no-response label
        label = email.get("prediction", "")
        if label in self.config.no_response_labels:
            return True
            
        # Skip if label is not in our response labels
        if label not in self.config.response_labels:
            return True
            
        # Skip if response is marked as None (explicitly set not to respond)
        if email.get("response_sent") is None:
            return True
            
        return False
class EmailSender:
    """Handles sending emails and saving drafts."""
    
    def __init__(self, config, auth_manager):
        self.config = config
        self.auth_manager = auth_manager
    
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
    
    def save_as_draft(self, to_address, subject, body, retry_attempt=0):
        """Save an email as draft instead of sending it."""
        try:
            access_token = self.auth_manager.get_access_token()
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
        
        # Create draft endpoint
        endpoint = f"{self.config.ms_graph_base_url}/me/messages"
        
        try:
            response = httpx.post(endpoint, headers=headers, json=message_payload, timeout=30.0)
            
            if response.status_code in [200, 201]:
                # If successful, the response will contain the created draft
                draft_data = response.json()
                draft_id = draft_data.get("id")
                logger.info(f"Email saved as draft for {to_address} with draft ID: {draft_id}")
                return draft_id
            
            # Handle errors with retry logic
            if self._handle_http_error(response, "draft creation", retry_attempt):
                # Only increment retry_attempt if token refresh was triggered
                next_retry = retry_attempt + 1 if response.status_code == 401 else retry_attempt
                if next_retry < self.config.max_retries:
                    return self.save_as_draft(to_address, subject, body, next_retry)
            
            logger.error(f"Failed to save draft. Status code: {response.status_code}, Response: {response.text}")
            return None
                
        except httpx.HTTPError as e:
            logger.error(f"HTTP error saving draft: {str(e)}")
            if retry_attempt < self.config.max_retries:
                logger.info(f"Retrying draft creation ({retry_attempt + 1}/{self.config.max_retries})...")
                time.sleep(self.config.retry_delay)
                return self.save_as_draft(to_address, subject, body, retry_attempt + 1)
            return None
        except Exception as e:
            logger.exception(f"Error saving draft: {str(e)}")
            return None
    
    def send_email(self, to_address, subject, body, retry_attempt=0):
        """Send an email directly."""
        try:
            access_token = self.auth_manager.get_access_token()
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
        
        endpoint = f"{self.config.ms_graph_base_url}/me/sendMail"
        
        try:
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=30.0)
            
            # Check if successful
            if response.status_code in [200, 202]:
                logger.info(f"Email sent successfully to {to_address}")
                return True
            
            # Handle errors with retry logic
            if self._handle_http_error(response, "email sending", retry_attempt):
                # Only increment retry_attempt if token refresh was triggered
                next_retry = retry_attempt + 1 if response.status_code == 401 else retry_attempt
                if next_retry < self.config.max_retries:
                    return self.send_email(to_address, subject, body, next_retry)
            
            logger.error(f"Failed to send email. Status code: {response.status_code}, Response: {response.text}")
            return False
                
        except httpx.HTTPError as e:
            logger.error(f"HTTP error sending email: {str(e)}")
            if retry_attempt < self.config.max_retries:
                logger.info(f"Retrying email send ({retry_attempt + 1}/{self.config.max_retries})...")
                time.sleep(self.config.retry_delay)
                return self.send_email(to_address, subject, body, retry_attempt + 1)
            return False
        except Exception as e:
            logger.exception(f"Error sending email: {str(e)}")
            return False
class EmailProcessor:
    """Main email processing logic."""
    
    def __init__(self, config, email_validator, email_sender):
        self.config = config
        self.email_validator = email_validator
        self.email_sender = email_sender
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
        try:
            # Skip emails that shouldn't receive responses
            if self.email_validator.should_skip_email(email):
                # Still mark as sent for these since we're deliberately skipping
                self.mongo.mark_email_sent(email.get("message_id"))
                self.current_batch_stats["emails_skipped"] += 1
                return True, False  # Success, not a draft
            
            # Check if email should be saved as draft
            save_draft = email.get("save_as_draft", False)
            
            # Get and validate response text
            response_text = str(email.get("response", "")).strip()
            
            # Skip invalid replies
            if not self.email_validator.is_valid_reply(response_text):
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
                draft_id = self.email_sender.save_as_draft(
                    to_address=to_address,
                    subject=subject,
                    body=body
                )
                
                if draft_id:
                    # Mark as saved to draft
                    self.mongo.mark_email_draft_saved(email.get("message_id"), draft_id)
                    logger.info(f"Email saved as draft for review: {email.get('message_id')}")
                    self.current_batch_stats["drafts_created"] += 1
                    return True, True  # Success, is a draft
                else:
                    logger.warning(f"Failed to save draft for: {email.get('message_id')}")
                    self.current_batch_stats["drafts_failed"] += 1
                    return False, True  # Failed, is a draft
            else:
                # Send the email directly
                send_success = self.email_sender.send_email(
                    to_address=to_address,
                    subject=subject,
                    body=body
                )
                
                if send_success:
                    # Mark as responded
                    self.mongo.mark_email_sent(email.get("message_id"))
                    self.current_batch_stats["emails_sent"] += 1
                    return True, False  # Success, not a draft
                
                self.current_batch_stats["emails_failed"] += 1
                return False, False  # Failed, not a draft
                
        except Exception as e:
            logger.exception(f"Error processing email: {str(e)}")
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
                return 0, 0
                
            logger.info(f"Found {len(drafts)} emails to save as drafts")
            
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
class EmailProcessingSystem:
    """Main class that orchestrates the email processing system."""
    
    def __init__(self):
        self.config = Config()
        self.auth_manager = MSGraphAuth(self.config)
        self.email_validator = EmailValidator(self.config)
        self.email_sender = EmailSender(self.config, self.auth_manager)
        self.email_processor = EmailProcessor(self.config, self.email_validator, self.email_sender)
    
    def run(self, batch_id=None):
        """Run the email processing system."""
        try:
            logger.info("Running email processing system...")
            
            # First process draft emails
            draft_success, draft_failed = self.email_processor.process_draft_emails(batch_id)
            
            # Then send regular emails
            sent_success, sent_failed = self.email_processor.send_pending_replies(batch_id)
            
            logger.info(f"Email processing complete: {sent_success} sent, {draft_success} saved as drafts")
            logger.info("Completed email processing system execution.")
            
            # Get complete stats
            stats = self.email_processor.get_email_stats()
            
            return draft_success, draft_failed, sent_success, sent_failed, stats
            
        except Exception as e:
            logger.error(f"Fatal error in email processing system: {str(e)}")
            raise
# Define standalone functions that are exported to other modules
def send_email(to_address, subject, body):
    """Standalone function for sending an email."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    email_sender = EmailSender(config, auth_manager)
    return email_sender.send_email(to_address, subject, body)
def save_as_draft(to_address, subject, body):
    """Standalone function for saving an email as draft."""
    config = Config()
    auth_manager = MSGraphAuth(config)
    email_sender = EmailSender(config, auth_manager)
    return email_sender.save_as_draft(to_address, subject, body)
def process_draft_emails(batch_id=None):
    """Standalone function for processing draft emails."""
    system = EmailProcessingSystem()
    return system.email_processor.process_draft_emails(batch_id)
def send_pending_replies(batch_id=None):
    """Standalone function for sending pending replies."""
    system = EmailProcessingSystem()
    return system.email_processor.send_pending_replies(batch_id)
# Main execution
if __name__ == "__main__":
    try:
        # Parse batch_id from command line if provided
        batch_id = sys.argv[1] if len(sys.argv) > 1 else None
        if batch_id:
            logger.info(f"Processing emails for batch ID: {batch_id}")
        
        # Create and run the email processing system
        email_system = EmailProcessingSystem()
        draft_success, draft_failed, sent_success, sent_failed, stats = email_system.run(batch_id)
        
        # Generate a summary table in logs
        logger.info("   EMAIL PROCESSING SUMMARY")
        logger.info(f"Emails Sent:       {sent_success}")
        logger.info(f"Drafts Created:    {draft_success}")
        logger.info(f"Emails Failed:     {sent_failed}")
        logger.info(f"Drafts Failed:     {draft_failed}")
        logger.info(f"Emails Skipped:    {stats['emails_skipped']}")
        
        # Exit with appropriate code
        if sent_failed > 0 or draft_failed > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)