# Email Classification System

A modular email processing system that automatically classifies incoming emails, generates appropriate responses, and manages email organization through Microsoft Graph API.

## Overview

The Email Classification System provides an automated workflow for:
1. Fetching unread emails from a Microsoft Outlook inbox
2. Classifying emails using a machine learning model
3. Moving emails to appropriate folders based on classification
4. Generating appropriate responses based on email content
5. Either saving responses as drafts or sending them directly
6. Exporting processed data to Excel reports

The system operates in batch mode, processing emails at regular intervals with comprehensive error handling and recovery mechanisms.

## System Architecture

```
email_classification/
├── src/
│   ├── fetch_reply.py        # Email fetching and classification
│   ├── db.py                 # Database connections (MongoDB and PostgreSQL)
│   ├── log_config.py         # Logging configuration
├── loop.py                   # Batch processing and orchestration
├── main.py                   # Application entry point
```

### Component Responsibilities

1. **main.py**: Application entry point with command-line interface
   - Initializes logging.
   - Parses command-line arguments.
   - Starts the appropriate processing mode
   - Handles graceful shutdown and error reporting
   - 
2. **src/fetch_reply.py**: Email fetching and classification
   - Connects to Microsoft Graph API for email access
   - Fetches unread emails from the inbox
   - Classifies emails using the model API
   - Moves emails to appropriate folders based on classification
   - Stores email data in MongoDB

3. **src/db.py**: Database operations
   - Provides interfaces for MongoDB operations
   - Provides interfaces for PostgreSQL operations
   - Handles data synchronization between databases
   - Manages batch tracking and status updates

4. **loop.py**: Batch processing and orchestration
   - Manages batch lifecycle (create, process, finalize)
   - Retries failed batches with exponential backoff
   - Exports data to Excel reports
   - Uploads reports to SFTP
   - Schedules email processing at regular intervals

## Installation

### Prerequisites

- Python 3.8 or higher
- MongoDB
- PostgreSQL
- SFTP server (optional, for reports)
- Microsoft Graph API credentials

### Setup

HEAD
1. **Clone the Repository**
   ```
   git clone https://github.com/SanskarCadex/email_classification.git
   cd email_classification

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/email-classification-system.git
   cd email-classification-system
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with the following environment variables:
   ```
   # Microsoft Graph API
   CLIENT_ID=your_client_id
   CLIENT_SECRET=your_client_secret
   TENANT_ID=your_tenant_id
   EMAIL_ADDRESS=your_email@example.com
   
   # MongoDB
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=emailDB
   MONGO_COLLECTION=classified_emails
   
   # PostgreSQL
   PGHOST=localhost
   PGPORT=5432
   PGDATABASE=email_system
   PGUSER=postgres
   PGPASSWORD=your_password
   
   # Email Configuration
   MAIL_SEND_ENABLED=False
   FORCE_DRAFTS=True
   ADD_EMAIL_FOOTER=True
   
   # Batch Configuration
   BATCH_SIZE=125
   BATCH_INTERVAL=600
   MAX_RETRIES=3
   RETRY_DELAY=60
   BATCH_TIMEOUT=900
   
   # SFTP Configuration
   SFTP_ENABLED=False
   SFTP_HOST=your_sftp_host
   SFTP_PORT=22
   SFTP_USERNAME=your_username
   SFTP_PASSWORD=your_password
   
   # Model API
   MODEL_API_URL=http://localhost:8000
   
   # Logging
   LOG_LEVEL=INFO
   LOG_DIR=logs
   ```

4. Set up the database schema:
   ```bash
   psql -U your_username -d email_system -f schema.sql
   ```

## Usage

### Running the Application

The system can be run in different modes:

1. **Daemon Mode** (continuous operation):
   ```bash
   python main.py --mode daemon
   ```

2. **Single Batch Mode** (process a single batch):
   ```bash
   python main.py --mode single
   ```

3. **Retry Mode** (retry failed batches):
   ```bash
   python main.py --mode retry
   ```

4. **Cleanup Mode** (mark failed batches as permanently failed):
   ```bash
   python main.py --mode cleanup
   ```

### Command-Line Options

```
usage: main.py [-h] [--mode {daemon,single,retry,cleanup}] [--batch-id BATCH_ID] [--batch-size BATCH_SIZE] [--interval INTERVAL] [--send-mail] [--force-drafts]

Email Classification System

optional arguments:
  -h, --help            show this help message and exit
  --mode {daemon,single,retry,cleanup}, -m {daemon,single,retry,cleanup}
                        Operation mode - daemon (continuous), single (one batch), retry (failed batches), cleanup (mark failed batches)
  --batch-id BATCH_ID, -b BATCH_ID
                        Process a specific batch ID (only used with single mode)
  --batch-size BATCH_SIZE, -s BATCH_SIZE
                        Override batch size from environment variable
  --interval INTERVAL, -i INTERVAL
                        Override batch interval in seconds from environment variable
  --send-mail           Enable mail sending regardless of environment setting
  --force-drafts        Force all emails to be saved as drafts regardless of environment setting
```

## Email Classification Categories

The system classifies emails into the following categories:

1. **no_reply_no_info**: No response needed, no information to extract
2. **no_reply_with_info**: No response needed, but contains information to extract
3. **auto_reply_no_info**: Auto-reply detected, no information to extract
4. **auto_reply_with_info**: Auto-reply detected with information to extract
5. **invoice_request_no_info**: Invoice request without specific invoice information
6. **claims_paid_no_proof**: Payment claim without proof of payment
7. **manual_review**: Requires manual review by a human operator

## Batch Processing Flow

1. A new batch is created with a unique ID
2. Unread emails are fetched from the inbox
3. Each email is classified and stored in MongoDB
4. Emails are moved to appropriate folders based on classification
5. Responses are generated for emails that require replies
6. Responses are either saved as drafts or sent directly
7. Batch status is updated in both MongoDB and PostgreSQL
8. Excel report is generated and uploaded to SFTP (if enabled)
9. System waits for the next interval to process a new batch

## Error Handling and Recovery

The system includes comprehensive error handling:

1. **Automatic Retries**: Failed operations are retried with exponential backoff
2. **Batch Recovery**: Failed batches are retried automatically
3. **Transaction Safety**: Database operations use transactions where appropriate
4. **Graceful Degradation**: System continues operating even when components fail
5. **Detailed Logging**: All operations are logged with appropriate detail

## Monitoring and Reporting

1. **Logs**: Detailed logs are stored in the configured log directory
2. **Excel Reports**: Each batch generates an Excel report with processing details
3. **Database Records**: All operations are recorded in MongoDB and PostgreSQL
4. **Status Tracking**: Batch status is tracked throughout the processing lifecycle



## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
