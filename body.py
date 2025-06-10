
import os
import time
import httpx
import msal
from datetime import datetime
from dotenv import load_dotenv
from src.log_config import logger

load_dotenv()

# Credentials (same as your classifier)
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET") 
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

class EmailMonitor:
    """Monitor emails coming into the classifier - shows exactly what classifier sees."""
    
    def __init__(self):
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.token = None
        
    def get_token(self):
        """Get access token."""
        app = msal.ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=CLIENT_SECRET,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}"
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        self.token = result["access_token"]
        logger.info("Connected to Microsoft Graph API")
        
    def get_headers(self):
        """Get API headers."""
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
    
    def fetch_unread_emails_like_classifier(self):
        """Fetch emails EXACTLY like the classifier does."""
        # EXACT same parameters as your classifier
        params = {
            "$orderby": "receivedDateTime desc",
            "$filter": "isRead eq false and isDraft eq false",  # Only unread
            "$select": "id,subject,from,bodyPreview,receivedDateTime,hasAttachments,toRecipients",
            "$top": 50  # Limit for monitoring
        }
        
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders/inbox/messages"
        
        try:
            response = httpx.get(url, headers=self.get_headers(), params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("value", [])
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []
    
    def get_full_body(self, message_id):
        """Get the full email body (what classifier SHOULD see for threads)."""
        try:
            url = f"{self.base_url}/users/{EMAIL_ADDRESS}/messages/{message_id}"
            response = httpx.get(url, headers=self.get_headers(), timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("body", {}).get("content", "")
        except Exception as e:
            logger.warning(f"Could not get full body: {e}")
            return ""
    
    def display_email_details(self, email, index):
        """Display detailed info about what classifier sees."""
        message_id = email.get("id", "")
        subject = email.get("subject", "")
        body_preview = email.get("bodyPreview", "")
        sender_info = email.get("from", {}).get("emailAddress", {})
        sender = sender_info.get("address", "")
        received = email.get("receivedDateTime", "")
        has_attachments = email.get("hasAttachments", False)
        
        print(f"\n{'='*80}")
        print(f"📧 EMAIL #{index}")
        print(f"{'='*80}")
        print(f"📨 From: {sender}")
        print(f"📅 Received: {received}")
        print(f"📎 Has Attachments: {has_attachments}")
        print(f"🏷️  Subject: {subject}")
        print(f"\n📄 Body Preview (what classifier sees):")
        print(f"   Length: {len(body_preview)} characters")
        print(f"   Content: {body_preview[:200]}{'...' if len(body_preview) > 200 else ''}")
        
        # Show full body for comparison
        print(f"\n🔍 Getting full body for thread analysis...")
        full_body = self.get_full_body(message_id)
        if full_body:
            print(f"📄 Full Body:")
            print(f"   Length: {len(full_body)} characters")
            print(f"   Preview: {full_body[:300]}{'...' if len(full_body) > 300 else ''}")
            
            # Check for thread indicators
            thread_indicators = [
                "From:", "Sent:", "On .* wrote:", "-----Original Message-----",
                "Subject: RE:", "Subject: FW:", ">", "forwarded message"
            ]
            
            found_indicators = []
            for indicator in thread_indicators:
                if indicator.lower() in full_body.lower():
                    found_indicators.append(indicator)
            
            if found_indicators:
                print(f"🧵 Thread Indicators Found: {', '.join(found_indicators)}")
            else:
                print(f"🧵 No Thread Indicators Found")
        
        print(f"{'='*80}")
    
    def monitor_emails(self):
        """Monitor emails in real-time."""
        print("🔍 EMAIL CLASSIFIER MONITOR STARTED")
        print("Shows exactly what emails your classifier receives")
        print("Press Ctrl+C to stop")
        print(f"Monitoring mailbox: {EMAIL_ADDRESS}")
        
        seen_emails = set()
        
        try:
            while True:
                emails = self.fetch_unread_emails_like_classifier()
                
                new_emails = []
                for email in emails:
                    email_id = email.get("id", "")
                    if email_id not in seen_emails:
                        new_emails.append(email)
                        seen_emails.add(email_id)
                
                if new_emails:
                    print(f"\n🆕 Found {len(new_emails)} new unread emails")
                    for i, email in enumerate(new_emails, 1):
                        self.display_email_details(email, i)
                else:
                    current_time = datetime.now().strftime("%H:%M:%S")
                    total_unread = len(emails)
                    print(f"⏰ {current_time} - Monitoring... ({total_unread} total unread emails)")
                
                # Wait 10 seconds before next check
                time.sleep(10)
                
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped by user")
    
    def show_current_unread(self):
        """Show all current unread emails (one-time check)."""
        print("📧 CURRENT UNREAD EMAILS IN CLASSIFIER")
        print("="*80)
        
        emails = self.fetch_unread_emails_like_classifier()
        
        if not emails:
            print("✅ No unread emails found")
            return
        
        print(f"Found {len(emails)} unread emails:")
        
        for i, email in enumerate(emails, 1):
            self.display_email_details(email, i)
            
            # Pause between emails for readability
            if i < len(emails):
                input("\nPress Enter to see next email...")

def main():
    print("🔍 EMAIL CLASSIFIER MONITOR")
    print("This shows exactly what emails your classifier sees")
    print()
    print("Choose mode:")
    print("1. Show current unread emails (one-time)")
    print("2. Monitor for new emails (real-time)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    monitor = EmailMonitor()
    monitor.get_token()
    
    if choice == "1":
        monitor.show_current_unread()
    elif choice == "2":
        monitor.monitor_emails()
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Program stopped by user")
    except Exception as e:
        logger.exception(f"Error: {e}")