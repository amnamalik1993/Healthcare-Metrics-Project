import boto3
import json
import io

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload



# ---------------------------------------
# Configuration
# ---------------------------------------

SECRET_NAME = "google-drive/service-account"
AWS_REGION = "us-east-1"

GOOGLE_DRIVE_FOLDER_ID = (
    "19ZvwHkHMNvZZYh4mtoUdysn6nJ6btLaO"
)

S3_BUCKET = "heathcare-source-data-bucket"
S3_PREFIX = "raw/"


# ---------------------------------------
# AWS Clients
# ---------------------------------------

secrets_client = boto3.client(
    "secretsmanager",
    region_name=AWS_REGION
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ---------------------------------------
# Get Secret from AWS Secrets Manager
# ---------------------------------------

response = secrets_client.get_secret_value(
    SecretId=SECRET_NAME
)

service_account_info = json.loads(
    response["SecretString"]
)


# ---------------------------------------
# Authenticate with Google Drive
# ---------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly"
]

credentials = (
    service_account.Credentials
    .from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )
)

drive_service = build(
    "drive",
    "v3",
    credentials=credentials
)


# ---------------------------------------
# List CSV Files in Google Drive Folder
# ---------------------------------------

query = f"""
'{GOOGLE_DRIVE_FOLDER_ID}' in parents
and trashed = false
and mimeType = 'text/csv'
"""

files = []
page_token = None

while True:

    response = drive_service.files().list(
        q=query,
        fields="nextPageToken, files(id, name, mimeType)",
        pageSize=1000,
        pageToken=page_token
    ).execute()

    files.extend(response.get("files", []))

    page_token = response.get("nextPageToken")

    if not page_token:
        break


print(f"Number of files found: {len(files)}")


# ---------------------------------------
# Download Files from Google Drive
# and Upload to S3
# ---------------------------------------

for file in files:

    file_id = file["id"]
    file_name = file["name"]

    print(f"Processing file: {file_name}")
    print(f"Google Drive File ID: {file_id}")

    # Download file content from Google Drive
    request = drive_service.files().get_media(
        fileId=file_id
    )

    file_content = io.BytesIO()

    downloader = MediaIoBaseDownload(
        file_content,
        request
    )

    done = False

    while not done:

        status, done = downloader.next_chunk()

        if status:

            print(
                f"Download progress: "
                f"{int(status.progress() * 100)}%"
            )

    # Reset file pointer
    file_content.seek(0)

    # S3 destination
    s3_key = f"{S3_PREFIX}{file_name}"

    # Upload to S3
    s3_client.upload_fileobj(
        file_content,
        S3_BUCKET,
        s3_key
    )

    print(
        f"Uploaded successfully to: "
        f"s3://{S3_BUCKET}/{s3_key}"
    )

    print("---------------------------")


print("All files processed successfully!")