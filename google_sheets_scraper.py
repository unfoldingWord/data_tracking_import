import gspread
import os
from gspread_dataframe import get_as_dataframe
from google.oauth2 import service_account
from functions import get_logger

from dotenv import load_dotenv # Import load_dotenv

mylogger = get_logger()

#########################################################################################################
#                       Scraping Several Google Sheets to Pull the data together
#########################################################################################################
# --- Load environment variables from .env file ---
load_dotenv() # This line loads the variables

# --- Configuration ---
# Path to your downloaded service account JSON key file
SERVICE_ACCOUNT_FILE =  os.getenv("JSON_KEY_PATH")

# IMPORTANT: Update this path!
# The scope for Google Drive API. Read-only is usually sufficient for counting.
SCOPE = ['https://www.googleapis.com/auth/drive.readonly']
# Name of your Google Spreadsheet
SPREADSHEET_NAME = os.getenv('OPEN_RESOURCE_SPREADSHEET_NAME')  # Replace with your spreadsheet's name

# Name or index (0-based) of the worksheet you want to read
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME') # Replace with your worksheet's name, or use index (e.g., 0 for the first sheet)

# --- Authentication ---

try:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPE,  # same scopes you used before
    )
    client = gspread.authorize(creds)
    mylogger.info("Authentication successful!")
except Exception as e:
    mylogger.critical(f"Authentication failed: {e}")
    mylogger.critical("Please ensure your SERVICE_ACCOUNT_FILE path is correct and the JSON file is valid.")
    mylogger.critical("Also, check if the Google Sheets API and Google Drive API are enabled in your Google Cloud Project.")
    exit()

# --- Accessing and Reading Data ---
try:
    spreadsheet = client.open(SPREADSHEET_NAME)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

    # To get all records as a list of dictionaries (useful for debugging)
    # records = worksheet.get_all_records()
    # print(records)

    # Get the data directly into a Pandas DataFrame
    # get_as_dataframe handles headers and empty rows/columns nicely.
    df = get_as_dataframe(worksheet)

    mylogger.info(f"Successfully pulled data from '{SPREADSHEET_NAME}' spreadsheet: with {df.shape[0]} rows pulled.")

except gspread.exceptions.SpreadsheetNotFound:
    mylogger.error(f"Error: Spreadsheet '{SPREADSHEET_NAME}' not found. Check the name and sharing permissions.")
except gspread.exceptions.WorksheetNotFound:
    mylogger.error(f"Error: Worksheet '{WORKSHEET_NAME}' not found in '{SPREADSHEET_NAME}'. Check the name.")
except Exception as e:
    mylogger.exception(f"An unexpected error occurred: {e}")

def collect_metrics() -> dict:
    """
    Connects to the Google Sheet and returns the number of open resource partners.
    Returns a dict of the form: {"open_resource_partners": <int>}
    """
    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        df = get_as_dataframe(worksheet)
        open_resource_partners = len(df)

        return {"open_resource_partners": open_resource_partners}
    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e),
            "open_resource_partners": None
        }
