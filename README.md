
```markdown
# Email Processing System - Client Side

## Overview

This email processing system is designed to automatically fetch, classify, and respond to emails based on their content. The system uses a decoupled architecture with two main components:

1. **Client Side** (this repository): Handles email fetching, processing, and sending
2. **Model Server**: Handles AI/ML-based classification and response generation (in a separate repository)

## Architecture

### Client-Side Components

- **Email Fetching**: Connects to Microsoft Graph API to retrieve unread emails
- **Batch Processing**: Manages batches of emails for efficient processing
- **Email Sending**: Sends responses or saves drafts based on classification
- **Database Integration**: Stores processed emails in MongoDB and PostgreSQL
- **SFTP Export**: Uploads processed data as Excel files to SFTP server (optional)

### Key Files

email_classification/
├── src/
│   ├── fetch_reply.py        # Email fetching and processing
│   ├── email_sender.py       # Email sending and draft creation
│   ├── db.py                 # Database connections (MongoDB and PostgreSQL)
│   ├── log_config.py         # Logging configuration
├── loop.py                   # Batch processing and orchestration
├── main.py                   # Application entry point


## Prerequisites

- Python 3.9+
- MongoDB
- PostgreSQL
- Microsoft Graph API credentials
- Model Server running and accessible

## Setup

1. **Clone the Repository**
   ```
   git clone https://github.com/SanskarCadex/email_classification.git
   cd email_classification
   ```

2. **Install Dependencies**
   ```
   pip install -r requirements.txt
   ```

3. **Set Up MongoDB**
   - Install MongoDB
   - Create a database named `emailDB` (or your preferred name, configurable in .env)

4. **Set Up PostgreSQL**
   - Install PostgreSQL
   - Create a database (name provided by your organization)
   - Ensure the database has a schema named `core` with these tables:
     - `batch_runs`
     - `account_email`

5. **Environment Variables**
   Create a `.env` file with the following variables:
   ```
   # Microsoft Graph API credentials
   CLIENT_ID=your_client_id
   CLIENT_SECRET=your_client_secret
   AUTHORITY=https://login.microsoftonline.com/common
   
   # Email configuration
   SENDER_EMAIL=your_email@domain.com
   YOUR_DOMAIN=yourdomain.com
   COMPANY_NAME=Your Company
   
   # Model Server configuration
   MODEL_API_URL=http://localhost:8000
   
   # Database configuration
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=emailDB
   PGHOST=your_postgres_host
   PGPORT=5432
   PGDATABASE=your_postgres_database
   PGUSER=your_postgres_user
   PGPASSWORD=your_postgres_password
   
   # Batch processing
   BATCH_SIZE=125
   BATCH_INTERVAL=600
   TIME_FILTER_HOURS=24
   
   # SFTP configuration (if needed)
   SFTP_ENABLED=True
   SFTP_HOST=sftp.example.com
   SFTP_PORT=22
   SFTP_USERNAME=username
   SFTP_PASSWORD=password
   
   # Email sending
   MAIL_SEND_ENABLED=False  # Set to True to enable actual email sending
   ```

## Usage

### Running the Application

1. **Start the Model Server First** (from the model server repository)
   ```
   cd model_server
   python main.py
   ```

2. **Run the Client Application**
   ```
   python main.py
   ```

This will start the email processing system, which will:
1. Fetch unread emails from Microsoft Graph API
2. Send emails to the model server for classification
3. Generate appropriate responses
4. Send responses or save as drafts based on classification
5. Export processed data to SFTP (if enabled)

### Batch Processing

The system processes emails in batches for efficiency. You can configure:
- `BATCH_SIZE`: Number of emails to process in each batch
- `BATCH_INTERVAL`: Time between batch processing (in seconds)
- `TIME_FILTER_HOURS`: Only process emails received within this many hours

### Email Classification

Emails are classified into the following categories:
- `no_reply_no_info`: Informational emails that don't need a response
- `no_reply_with_info`: Informational emails with useful contact information
- `auto_reply_no_info`: Automatic replies without alternative contact details
- `auto_reply_with_info`: Automatic replies with alternative contact information
- `invoice_request_no_info`: Invoice requests without specific details
- `claims_paid_no_proof`: Payment claims without evidence or attachments
- `manual_review`: Complex emails that need human review

### Logging

Logs are stored in the `logs` directory by default. Configure the log location in `src/log_config.py`.

## Security Notes

- Store sensitive credentials in environment variables, not in the code
- Ensure proper access controls for the database
- Use strong passwords for all services
- Consider encrypting sensitive data in the database

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Check your Microsoft Graph API credentials
   - Run `fetch_reply.py` manually to initialize authentication

2. **Database Connection Issues**
   - Verify MongoDB and PostgreSQL are running
   - Check connection strings and credentials

3. **Model Server Connection Issues**
   - Ensure the model server is running
   - Check the `MODEL_API_URL` environment variable

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Create a new Pull Request
```

