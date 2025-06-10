"""
Clean Email Fetcher - Build dataset with clean body, thread flag, no HTML
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

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

class CleanEmailFetcher:
    def __init__(self):
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.token = None
        self.timeout = httpx.Timeout(60.0)

    def get_token(self) -> str:
        if self.token:
            return self.token
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
        print("Authentication successful")
        return self.token

    def get_target_folder_id(self) -> Optional[str]:
        headers = {"Authorization": f"Bearer {self.get_token()}"}
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders"
        response = httpx.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        folders = response.json()["value"]
        
        print("Available root folders:")
        for folder in folders:
            print(f"  - {folder['displayName']}")
        
        email_classification_id = None
        for folder in folders:
            if folder["displayName"] == "Email Classification":
                email_classification_id = folder["id"]
                break
        
        if not email_classification_id:
            print("Email Classification folder not found")
            return None
        
        print("Found Email Classification folder, checking subfolders...")
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders/{email_classification_id}/childFolders"
        response = httpx.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        child_folders = response.json()["value"]
        
        print("Available subfolders in Email Classification:")
        for folder in child_folders:
            print(f"  - {folder['displayName']}")
        
        for folder in child_folders:
            if folder["displayName"] == "Auto Reply With Info":
                print(f"Found target folder: {folder['id']}")
                return folder["id"]
        
        print("Claims Paid No Proof folder not found")
        return None

    def fetch_clean_emails(self, limit: int = 150) -> List[Dict]:
        folder_id = self.get_target_folder_id()
        if not folder_id:
            return []
        headers = {"Authorization": f"Bearer {self.get_token()}"}
        url = f"{self.base_url}/users/{EMAIL_ADDRESS}/mailFolders/{folder_id}/messages"
        params = {
            "$select": "id,subject,from,body,bodyPreview,uniqueBody,receivedDateTime,hasAttachments,internetMessageId",
            "$filter": "isDraft eq false",
            "$orderby": "receivedDateTime desc",
            "$top": min(limit, 100)
        }
        emails = []
        emails_collected = 0
        page_count = 0
        while url and emails_collected < limit:
            page_count += 1
            print(f"Processing page {page_count}...")
            response = httpx.get(url, headers=headers, params=params if params else None, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            for email in data.get("value", []):
                if emails_collected >= limit:
                    break
                email_data = self._extract_clean_email_data(email)
                if email_data:
                    emails.append(email_data)
                    emails_collected += 1
            url = data.get("@odata.nextLink") if emails_collected < limit else None
            params = None
        print(f"Successfully fetched {len(emails)} clean emails")
        return emails

    def _extract_clean_email_data(self, email: Dict) -> Optional[Dict]:
        import re

        def html_to_text(html_content):
            if not html_content:
                return ""
            text = re.sub(r'<[^>]+>', '', html_content)
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
            text = re.sub(r'\n\s*\n', '\n\n', text)
            text = re.sub(r'[ \t]+', ' ', text)
            return text.strip()

        def to_text(content, content_type):
            if content_type == "html":
                return html_to_text(content)
            return content.strip()

        try:
            subject = email.get("subject", "").strip()
            sender_info = email.get("from", {}).get("emailAddress", {})
            sender = sender_info.get("address", "")
            unique_body_dict = email.get("uniqueBody", {})
            full_body_dict = email.get("body", {})

            unique_body_text = to_text(unique_body_dict.get("content", ""), unique_body_dict.get("contentType", "").lower())
            full_body_text = to_text(full_body_dict.get("content", ""), full_body_dict.get("contentType", "").lower())

            had_threads = False
            try:
                if (
                    unique_body_text and full_body_text and
                    len(full_body_text) > len(unique_body_text) * 1.25 and
                    (len(full_body_text) - len(unique_body_text)) > 200 and
                    len(full_body_text) > 700
                ):
                    had_threads = True
            except Exception as ee:
                pass

            clean_body = unique_body_text

            received_date = email.get("receivedDateTime", "")
            has_attachments = email.get("hasAttachments", False)
            message_id = email.get("internetMessageId", email.get("id", ""))

            if not clean_body:
                return None

            return {
                "subject": subject,
                "sender": sender,
                "body": clean_body,
                "received_date": received_date,
                "has_attachments": has_attachments,
                "message_id": message_id,
                "body_length": len(clean_body),
                "had_threads": had_threads
            }
        except Exception as e:
            print(f"Error processing email: {str(e)}")
            return None

    def save_to_csv(self, emails: List[Dict]) -> None:
        if not emails:
            print("No emails to save")
            return
        df = pd.DataFrame(emails)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"clean_emails_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nDATASET QUALITY REPORT")
        print(f"=" * 40)
        print(f"Saved to: {filename}")
        print(f"Total emails: {len(df)}")
        print(f"Average body length: {df['body_length'].mean():.0f} chars")
        print(f"With attachments: {df['has_attachments'].sum()}")
        print(f"Thread mails: {df['had_threads'].sum()}")
        if len(df) > 0:
            print(f"Sample Clean Text:\n'{df.iloc[0]['body'][:200]}...'")
        print(f"\nClean dataset ready for ML training!")

def main():
    print("Clean Email Fetcher - No HTML, With Accurate Thread Flag")
    print("=" * 50)
    missing_vars = [var for var in ["CLIENT_ID", "CLIENT_SECRET", "TENANT_ID", "EMAIL_ADDRESS"] if not os.getenv(var)]
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file")
        return
    print(f"Using email: {EMAIL_ADDRESS}")
    try:
        fetcher = CleanEmailFetcher()
        emails = fetcher.fetch_clean_emails(limit=300)
        if emails:
            fetcher.save_to_csv(emails)
        else:
            print("No emails found or fetched")
    except Exception as e:
        print(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()