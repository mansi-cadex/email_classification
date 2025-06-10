"""
Clean Email Fetcher - Get emails with clean text (no HTML, no threads)
Uses Microsoft Graph API's uniqueBody and text format for optimal data quality
"""

import os
import httpx
import msal
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional

load_dotenv()

# Configuration
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

class CleanEmailFetcher:
    def __init__(self):
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.token = None
        self.timeout = httpx.Timeout(60.0)  # Increased timeout for reliability
    
    def get_token(self) -> str:
        """Get access token with error handling"""
        if self.token:
            return self.token
            
        try:
            app = msal.ConfidentialClientApplication(
                client_id=CLIENT_ID,
                client_credential=CLIENT_SECRET,
                authority=f"https://login.microsoftonline.com/{TENANT_ID}"
            )
            
            result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if "access_token" not in result:
                error_msg = result.get('error_description', result.get('error', 'Unknown authentication error'))
                raise Exception(f"Authentication failed: {error_msg}")
                
            self.token = result["access_token"]
            print("✅ Authentication successful")
            return self.token
            
        except Exception as e:
            print(f"❌ Authentication error: {str(e)}")
            raise
    
    def get_inbox_folder_id(self) -> Optional[str]:
        """Get Inbox folder ID"""
        headers = {"Authorization": f"Bearer {self.get_token()}"}
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders"
        
        try:
            response = httpx.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            folders = response.json()["value"]
            
            # Find Inbox folder (case-insensitive)
            for folder in folders:
                if folder["displayName"].lower() == "inbox":
                    print(f"✅ Found Inbox folder: {folder['id']}")
                    return folder["id"]
            
            print("❌ Inbox folder not found")
            return None
            
        except httpx.HTTPError as e:
            print(f"❌ Error getting inbox folder: {str(e)}")
            return None
    
    def fetch_clean_emails(self, limit: int = 150) -> List[Dict]:
        """
        Fetch emails with COMPLETE clean text (no HTML, no thread content)
        Uses uniqueBody for thread-free content and full body for complete content
        """
        inbox_id = self.get_inbox_folder_id()
        if not inbox_id:
            return []
        
        headers = {"Authorization": f"Bearer {self.get_token()}"}
        
        # Request COMPLETE email data without HTML and threads
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders/{inbox_id}/messages"
        params = {
            # Select ALL body content fields for complete email
            "$select": "id,subject,from,body,bodyPreview,uniqueBody,receivedDateTime,hasAttachments,internetMessageId",
            "$filter": "isDraft eq false",  # Exclude drafts
            "$orderby": "receivedDateTime desc",  # Latest first
            "$top": min(limit, 1000)  # Graph API limit
        }
        
        print(f"📧 Fetching {limit} COMPLETE emails from Inbox...")
        print("🧹 Using uniqueBody (no threads) + full body content (no length limits)")
        
        emails = []
        emails_collected = 0
        page_count = 0
        
        try:
            while url and emails_collected < limit:
                page_count += 1
                print(f"📄 Processing page {page_count}...")
                
                response = httpx.get(url, headers=headers, params=params if params else None, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                for email in data.get("value", []):
                    if emails_collected >= limit:
                        break
                    
                    # Extract clean email data
                    email_data = self._extract_clean_email_data(email)
                    if email_data:  # Only add if we got valid data
                        emails.append(email_data)
                        emails_collected += 1
                
                # Get next page if needed
                if emails_collected < limit:
                    url = data.get("@odata.nextLink")
                    params = None  # Only use params for first request
                else:
                    break
                    
        except httpx.HTTPError as e:
            print(f"❌ Error fetching emails: {str(e)}")
            if emails:
                print(f"⚠️  Returning {len(emails)} emails that were successfully fetched")
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            return emails
        
        print(f"✅ Successfully fetched {len(emails)} clean emails")
        return emails
    
    def _extract_clean_email_data(self, email: Dict) -> Optional[Dict]:
        """Extract COMPLETE clean text data from email response (no length limits)"""
        try:
            # Get subject
            subject = email.get("subject", "").strip()
            
            # Get sender
            sender_info = email.get("from", {}).get("emailAddress", {})
            sender = sender_info.get("address", "")
            
            # Get COMPLETE clean body text (priority order for full content)
            clean_body = ""
            data_source = ""
            had_threads = False  # NEW: Track if email had threads
            
            # Check for thread detection by comparing uniqueBody with full body
            unique_body = email.get("uniqueBody", {})
            full_body = email.get("body", {})
            
            if unique_body and unique_body.get("content") and full_body and full_body.get("content"):
                unique_content = unique_body.get("content", "").strip()
                full_content = full_body.get("content", "").strip()
                
                # If uniqueBody is significantly shorter than full body, it likely had threads
                if len(unique_content) > 0 and len(full_content) > len(unique_content) * 1.2:
                    had_threads = True
            
            # 1. Try uniqueBody first (excludes threads) - COMPLETE content
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
                clean_body = email.get("bodyPreview", "").strip()
                data_source = "bodyPreview_fallback"
            
            # Don't skip based on length - get ALL content
            if not clean_body:
                return None
            
            # Additional metadata
            received_date = email.get("receivedDateTime", "")
            has_attachments = email.get("hasAttachments", False)
            message_id = email.get("internetMessageId", email.get("id", ""))
            
            return {
                "subject": subject,
                "sender": sender,
                "body": clean_body,  # COMPLETE clean text, no HTML, no threads
                "received_date": received_date,
                "has_attachments": has_attachments,
                "message_id": message_id,
                "body_length": len(clean_body),
                "data_source": data_source,
                "had_threads": had_threads  # NEW: Thread detection flag
            }
            
        except Exception as e:
            print(f"⚠️  Error processing email: {str(e)}")
            return None
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to clean text (simple but effective)"""
        try:
            import re
            
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
            print(f"⚠️  Error converting HTML to text: {str(e)}")
            return html_content  # Return original if conversion fails
    
    def save_to_csv(self, emails: List[Dict]) -> None:
        """Save clean emails to CSV with quality metrics"""
        if not emails:
            print("❌ No emails to save")
            return
            
        df = pd.DataFrame(emails)
        
        # Remove duplicates based on content
        original_count = len(df)
        df = df.drop_duplicates(subset=["subject", "body"], keep="first")
        dedup_count = len(df)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"clean_emails_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        
        # Quality metrics
        print(f"\n📊 DATASET QUALITY REPORT")
        print(f"=" * 40)
        print(f"📁 Saved to: {filename}")
        print(f"📧 Total emails: {dedup_count}")
        print(f"🔄 Duplicates removed: {original_count - dedup_count}")
        print(f"📏 Average body length: {df['body_length'].mean():.0f} chars")
        print(f"📎 With attachments: {df['has_attachments'].sum()}")
        
        # Data source breakdown
        source_counts = df['data_source'].value_counts()
        print(f"\n🔍 Data Source Quality:")
        for source, count in source_counts.items():
            percentage = (count / len(df)) * 100
            print(f"   {source}: {count} emails ({percentage:.1f}%)")
        
        # Length distribution
        print(f"\n📏 Content Length Distribution:")
        print(f"   Short (< 100 chars): {len(df[df['body_length'] < 100])}")
        print(f"   Medium (100-500): {len(df[(df['body_length'] >= 100) & (df['body_length'] < 500)])}")
        print(f"   Long (500+ chars): {len(df[df['body_length'] >= 500])}")
        
        # Sample preview
        print(f"\n📝 Sample Clean Text (first email):")
        if len(df) > 0:
            sample_body = df.iloc[0]['body'][:200]
            print(f"   '{sample_body}{'...' if len(df.iloc[0]['body']) > 200 else ''}'")
        
        print(f"\n✅ Clean dataset ready for ML training!")

def main():
    print("🧹 Clean Email Fetcher - No HTML, No Threads")
    print("=" * 50)
    
    # Validate environment
    missing_vars = []
    for var in ["CLIENT_ID", "CLIENT_SECRET", "TENANT_ID", "EMAIL_ADDRESS"]:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file")
        return
    
    print(f"🔑 Using email: {EMAIL_ADDRESS}")
    
    try:
        fetcher = CleanEmailFetcher()
        
        # Fetch clean emails (you can adjust the limit)
        emails = fetcher.fetch_clean_emails(limit=500)
        
        # Save high-quality dataset
        if emails:
            fetcher.save_to_csv(emails)
        else:
            print("❌ No emails found or fetched")
            
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")

if __name__ == "__main__":
    main()