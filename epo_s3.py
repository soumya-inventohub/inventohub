import os
import time
import boto3
import shutil
import zipfile
import tarfile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- S3 Configuration ---
BUCKET_NAME = "epo.inventohub"
# This is the base folder for the raw archives, e.g., '2025'
# The script will derive the year from the downloaded filename.
ARCHIVE_BASE_FOLDER = "2025" 

# --- Download Configuration ---
URL = "https://publication-bdds.apps.epo.org/raw-data/products/public/product/32"
DOWNLOAD_TIMEOUT_SECONDS = 1800  # 30 minutes

# --- Local Temporary Directory ---
# All temporary files will be stored here and cleaned up afterwards.
TEMP_DIR = os.path.join(os.getcwd(), "temp_epo_downloads")
os.makedirs(TEMP_DIR, exist_ok=True)

# --- Boto3 S3 Client ---
s3 = boto3.client("s3")

def s3_file_exists(bucket, key):
    """Checks if a file exists in an S3 bucket."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Propagate other errors (like permissions)
            raise

def upload_to_s3(local_file_path, bucket, s3_key):
    """Uploads a single file to S3."""
    print(f"⬆️ Uploading {os.path.basename(local_file_path)} to s3://{bucket}/{s3_key}")
    s3.upload_file(local_file_path, bucket, s3_key)
    print(f"✅ Upload complete.")

def is_valid_xml(filename):
    """Checks if a file is a valid XML to be processed (ends with .xml, not TOC.xml)."""
    return filename.endswith(".xml") and not filename.endswith("TOC.xml")

def extract_and_upload_xmls(local_archive_path, dest_s3_folder):
    """
    Extracts an archive, finds inner zip files, and uploads valid XMLs to a specified S3 folder.

    Args:
        local_archive_path (str): The path to the local .zip or .tar archive.
        dest_s3_folder (str): The destination S3 folder for the extracted XMLs
                              (e.g., "2025/epo-xmls/2025_01").
    """
    archive_name = os.path.basename(local_archive_path)
    # Use a unique temporary directory for extraction
    extract_dir = os.path.join(TEMP_DIR, f"extracted_{os.path.splitext(archive_name)[0]}")
    os.makedirs(extract_dir, exist_ok=True)

    try:
        # 1. Extract top-level archive (.zip or .tar)
        print(f"📦 Extracting top-level archive: {archive_name}")
        if local_archive_path.endswith(".zip"):
            with zipfile.ZipFile(local_archive_path, 'r') as z:
                z.extractall(extract_dir)
        elif local_archive_path.endswith(".tar"):
            with tarfile.open(local_archive_path, 'r') as t:
                t.extractall(extract_dir)
        else:
            print(f"⚠️ Unsupported archive format for extraction: {archive_name}")
            return

        # 2. Walk through extracted files to find inner .zip archives
        xml_upload_count = 0
        for root, _, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)

                if zipfile.is_zipfile(file_path):
                    with zipfile.ZipFile(file_path, 'r') as inner_zip:
                        for inner_file_name in inner_zip.namelist():
                            if is_valid_xml(inner_file_name):
                                # 3. Upload valid XMLs directly from the inner zip to S3
                                with inner_zip.open(inner_file_name) as f:
                                    xml_filename = os.path.basename(inner_file_name)
                                    dest_key = f"{dest_s3_folder}/{xml_filename}"
                                    s3.upload_fileobj(f, BUCKET_NAME, dest_key)
                                    xml_upload_count += 1
        
        print(f"✅ Successfully uploaded {xml_upload_count} XML files to s3://{BUCKET_NAME}/{dest_s3_folder}/")

    except Exception as e:
        print(f"❌ Error during extraction and XML upload: {e}")
    finally:
        # 4. Clean up local extraction directory
        if os.path.exists(extract_dir):
            print(f"🧹 Cleaning up local extraction directory: {extract_dir}")
            shutil.rmtree(extract_dir)

def download_and_process_latest_file(url, download_path):
    """
    Orchestrates the entire process: download, upload raw archive, extract, upload XMLs, and clean up.
    """
    print("--- Starting Download and Process Workflow ---")

    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # --- PART 1: Find and Download the Latest File ---
        print("1. Navigating to the URL...")
        driver.get(url)

        wait = WebDriverWait(driver, 30)
        all_blocks = wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'div[data-epo="epo-data-table-30x"]'))
        )

        print(f"2. Found {len(all_blocks)} file blocks. Searching for latest archive...")

        latest_file_info = None
        latest_datetime = None
        valid_extensions = ['.zip', '.tar'] # Only process archives

        for block in all_blocks:
            try:
                file_name_element = block.find_element(By.CSS_SELECTOR, "p.text-semibold")
                file_name = file_name_element.text
                if not any(file_name.lower().endswith(ext) for ext in valid_extensions):
                    continue

                published_at_str = block.find_element(By.CSS_SELECTOR, "td:nth-child(3) p").text
                current_datetime = datetime.strptime(published_at_str, "%d.%m.%Y %H:%M")

                if latest_datetime is None or current_datetime > latest_datetime:
                    latest_datetime = current_datetime
                    initial_button = block.find_element(By.CSS_SELECTOR, "button")
                    latest_file_info = {
                        "file_name": file_name,
                        "initial_button": initial_button
                    }
            except NoSuchElementException:
                continue

        if not latest_file_info:
            print("No matching archive files (.zip, .tar) found on the page.")
            return

        filename = latest_file_info['file_name']
        print(f"\n✅ Found latest archive file: {filename}")

        # Derive S3 paths from filename (e.g., EPRTBJV2025000001001001.zip)
        try:
            year = filename[7:11]
            week = filename[15:17]
            archive_s3_key = f"{ARCHIVE_BASE_FOLDER}/{filename}"
            xml_dest_s3_folder = f"{year}/epo-xmls/{year}_{week}"
        except IndexError:
            print(f"❌ ERROR: Could not parse year/week from filename '{filename}'. Skipping.")
            return
        
        if s3_file_exists(BUCKET_NAME, archive_s3_key):
            print(f"SKIPPING: Raw archive already exists in S3: s3://{BUCKET_NAME}/{archive_s3_key}")
            return
            
        print("3. Clicking download button...")
        latest_file_info['initial_button'].click()

        final_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="download-item-button"]'))
        )
        final_button.click()

        local_filepath = os.path.join(download_path, filename)
        temp_path = local_filepath + ".crdownload"

        print("4. Waiting for download to finish...")
        seconds_waited = 0
        while True:
            if os.path.exists(local_filepath) and not os.path.exists(temp_path):
                print(f"✅ Download complete: {filename}")
                break
            time.sleep(1)
            seconds_waited += 1
            if seconds_waited > DOWNLOAD_TIMEOUT_SECONDS:
                raise TimeoutException(f"Download of '{filename}' timed out.")
            if seconds_waited % 60 == 0:
                print(f"... still downloading ({seconds_waited}s elapsed)")

        # --- PART 2: Upload Raw Archive to S3 ---
        upload_to_s3(local_filepath, BUCKET_NAME, archive_s3_key)

        # --- PART 3: Extract and Upload XMLs ---
        print("\n--- Starting Extraction and XML Upload Process ---")
        extract_and_upload_xmls(local_filepath, xml_dest_s3_folder)

        # --- PART 4: Final Cleanup ---
        print("\n--- Starting Final Cleanup ---")
        os.remove(local_filepath)
        print(f"🧹 Local archive deleted: {local_filepath}")

    except TimeoutException as e:
        print(f"ERROR: A process timed out -> {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred -> {e}")
    finally:
        driver.quit()
        # Final check to remove the temp directory if it still exists
        if os.path.exists(TEMP_DIR):
            # shutil.rmtree(TEMP_DIR) # Uncomment if you want to delete the main temp folder too
            pass
        print("\n--- Workflow Finished ---")

# --- Main Execution ---
if __name__ == "__main__":
    download_and_process_latest_file(URL, TEMP_DIR)