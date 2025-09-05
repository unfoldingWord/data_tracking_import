import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from functions import get_logger

#########################################################################################################
#                       Scraping the Google Folder for the Case Study and White Papers
#########################################################################################################
# --- Load environment variables from .env file ---
load_dotenv()

# --- Configuration ---
# Path to your downloaded service account JSON key file
SERVICE_ACCOUNT_FILE = os.getenv("JSON_KEY_PATH")  # <<<

# IMPORTANT: Update this path!
# The scope for Google Drive API. Read-only is usually sufficient for counting.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

SHARED_DRIVE_ID = os.getenv("SHARED_DRIVE_ID") # <<< IMPORTANT: Update this!

folder_ids_raw = os.getenv("GOOGLE_FOLDER_ID")
FOLDER_ID = folder_ids_raw.split(",") if folder_ids_raw else []

folder_name_raw = os.getenv("GOOGLE_FOLDER_NAME")
FOLDER_NAME = folder_name_raw.split(",") if folder_name_raw else []

mylogger = get_logger()

def get_drive_service_account():
    """Authenticates with Google Drive API using a service account."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        mylogger.info("Successfully authenticated with Google Drive API using service account.")
        return service
    except Exception as e:
        mylogger.exception(f"Error authenticating with service account: {e}")
        return None

def count_items_in_shared_drive_folder(service, shared_drive_id, folder_id, folder_name):
    """
    Counts the number of documents/objects directly within a specific folder
    on a Google Shared Drive. Excludes subfolders and trashed items.
    """
    if not service:
        mylogger.critical("Drive service not available. Cannot count items.")
        return None

    item_count = 0
    page_token = None

    try:
        #print(f"Counting items in folder '{folder_name}' within Shared Drive '{shared_drive_id}'...")
        while True:
            # Construct the query to list files in the specified folder, excluding trashed items and folders themselves
            # 'trashed = false' ensures we only count active files
            # 'mimeType != "application/vnd.google-apps.folder"' excludes subfolders from the count
            query = (
                f"'{folder_id}' in parents and trashed = false "
                "and mimeType != 'application/vnd.google-apps.folder'"
            )

            results = service.files().list(
                q=query,
                corpora='drive',                  # Important for Shared Drives
                driveId=shared_drive_id,          # ID of the Shared Drive
                includeItemsFromAllDrives=True,   # Required for Shared Drives
                supportsAllDrives=True,           # Required for Shared Drives
                fields="nextPageToken, files(id)", # Requesting only 'id' for efficient counting
                pageToken=page_token
            ).execute()

            files = results.get('files', [])
            item_count += len(files)

            page_token = results.get('nextPageToken', None)
            if not page_token:
                break # No more pages

        #print(f"\nTotal documents/objects found in folder {folder_name}: {item_count}")
        return item_count

    except HttpError as error:
        mylogger.exception(f'An HTTP error occurred: {error}')
        # Common errors:
        # 403: "User does not have sufficient permissions for this file." - Service account needs access.
        # 404: "File not found." - Check folder_id or shared_drive_id.
        return None
    except Exception as e:
        mylogger.exception(f"An unexpected error occurred: {e}")
        return None


if __name__ == '__main__':
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        mylogger.critical(f"Service account key file not found at {SERVICE_ACCOUNT_FILE}")
        mylogger.critical("Please download your service account JSON key and update the SERVICE_ACCOUNT_FILE path.")
    else:
        drive_service = get_drive_service_account()
        if drive_service:
            # Create an empty dictionary to store the counts
            folder_counts = {}

            for folder_id_single, folder_name_single in zip(FOLDER_ID, FOLDER_NAME):
                count = count_items_in_shared_drive_folder(drive_service, SHARED_DRIVE_ID, folder_id_single,
                                                           folder_name_single)
                if count is not None:  # Only store if the counting was successful
                    folder_counts[folder_name_single] = count

            mylogger.info("\nSuccessfully pulled data from Google Drive")
            mylogger.debug("\n--- Summary of Folder Counts ---")

            for name, count in folder_counts.items():
                mylogger.debug(f"Folder '{name}': {count} documents")

            # Now you can access the counts like this:
            case_study_count = folder_counts.get('Case Study')  # Use .get() to avoid KeyError if name not found
            white_papers_count = folder_counts.get('White Papers')

def collect_metrics():
    """
    Runs the scraper and returns the case study + white papers counts.
    Returns a dict of the form:
        {"case_study_count": int, "white_papers_count": int}
    """
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account key file not found at {SERVICE_ACCOUNT_FILE}")

    drive_service = get_drive_service_account()
    if not drive_service:
        raise RuntimeError("Drive service not available")

    folder_counts = {}
    for folder_id_single, folder_name_single in zip(FOLDER_ID, FOLDER_NAME):
        count = count_items_in_shared_drive_folder(drive_service, SHARED_DRIVE_ID,
                                                   folder_id_single, folder_name_single)
        if count is not None:
            folder_counts[folder_name_single] = count

    case_study_count = folder_counts.get('Case Study')
    white_papers_count = folder_counts.get('White Papers')

    return {
        "case_study_count": case_study_count,
        "white_papers_count": white_papers_count
    }