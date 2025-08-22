import os
import time
import datetime
import boto3 # Import boto3 for S3 interaction
import shutil # For removing directories if needed
import zipfile # For handling zip files
import re      # For regex in filename parsing and XML splitting
import xml.etree.ElementTree as ET # For XML parsing
import pandas as pd # For DataFrame and Parquet
import tempfile # For temporary directories
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys # Import Keys for TAB
from botocore.exceptions import ClientError # For S3 specific errors


# --- S3 Configuration ---
BUCKET_NAME = "uspto.inventohub"
# Ensure your AWS credentials are configured (e.g., via environment variables, ~/.aws/credentials, or IAM role)
# Setting region explicitly, match your bucket's region
s3 = boto3.client("s3", region_name='ap-south-1') 

# --- Set your download directory ---
# This directory will hold the ZIP files temporarily after Selenium downloads them.
# They will be deleted after being processed and uploaded to S3.
download_dir = os.path.abspath("./ptgr_downloads")
os.makedirs(download_dir, exist_ok=True)

# --- Chrome configuration ---
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
})

chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_argument("--disable-blink-features=AutomationControlled")  ## ANTI DETECTION MEASURE
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
chrome_options.add_argument(f"user-agent={user_agent}")

# Run in headless mode for server environments
chrome_options.add_argument("--headless=new") # Use new headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu') # Needed for some headless environments
chrome_options.add_argument("--window-size=1920,1080") # Set window size for headless

# --- Function to get the latest USPTO patent grant date (Tuesday) ---
def get_latest_uspto_patent_grant_date():
    """
    Calculates the date of the most recent Tuesday, as USPTO Patent Grant data
    (PTGRXML) is typically issued weekly on Tuesdays.
    Returns date in MM-DD-YYYY format, matching website's expected input.
    """
    today = datetime.date.today()
    # Monday=0, Tuesday=1, ..., Sunday=6
    # If today is Tuesday (1), days_to_subtract is 0
    # If today is Monday (0), days_to_subtract is (0 - 1 + 7) % 7 = 6
    # If today is Wednesday (2), days_to_subtract is (2 - 1 + 7) % 7 = 1
    days_to_subtract = (today.weekday() - 1 + 7) % 7
    latest_tuesday = today - datetime.timedelta(days=days_to_subtract)
    return latest_tuesday.strftime("%m-%d-%Y")

# --- Helper function to wait for a file download to complete ---
def wait_for_download_completion(file_path, timeout=300):
    """
    Waits for a file to appear and complete downloading by monitoring its size.
    Returns True if download completes, False otherwise.
    """
    print(f"[INFO] Waiting for download to complete. Expected file: {file_path}")
    start_time = time.time()
    download_complete = False
    initial_size = -1

    while time.time() - start_time < timeout:
        # Check for .crdownload or .tmp extension for ongoing downloads
        if os.path.exists(file_path + ".crdownload") or os.path.exists(file_path + ".tmp"):
            time.sleep(1) # Still downloading, wait
            continue

        if os.path.exists(file_path):
            current_size = os.path.getsize(file_path)
            if initial_size == -1: # First time we detect the file
                initial_size = current_size
                print(f"[INFO] File '{os.path.basename(file_path)}' detected. Current size: {current_size} bytes. Waiting for completion...")
                time.sleep(5) # Give it some time to start downloading and show initial growth
            elif current_size > initial_size: # File is actively growing
                initial_size = current_size
                time.sleep(1) # Check frequently
            elif current_size == initial_size and current_size > 0: # Size hasn't changed and it's not 0, assume complete
                print(f"[✓] Download complete: {os.path.basename(file_path)} (Final size: {current_size} bytes)")
                download_complete = True
                break
        else: # File not yet created or detected
            time.sleep(1)
    
    if not download_complete:
        print(f"❌ Download failed or timed out for '{os.path.basename(file_path)}'. File not found or did not complete within timeout.")
        if os.path.exists(file_path):
            print(f"Partial download might exist at: {file_path}")
    return download_complete

# --- Helper function to check if a file exists in S3 ---
def s3_file_exists(bucket, key):
    """Checks if a file exists in an S3 bucket."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Propagate other errors (like permissions)
            raise

# --- Helper function to upload to S3 ---
def upload_to_s3(local_path, bucket, s3_key):
    """Uploads a single file to S3."""
    print(f"⬆️ Uploading {os.path.basename(local_path)} to s3://{bucket}/{s3_key}")
    try:
        s3.upload_file(local_path, bucket, s3_key)
        print(f"✅ Upload complete for {os.path.basename(local_path)}.")
        return True
    except ClientError as e:
        print(f"❌ Error uploading {os.path.basename(local_path)} to S3: {e}")
        return False

# --- XML Extraction and Parquet Conversion Logic ---
def split_patent_documents(xml_content):
    """
    Splits a single XML file content into individual patent XML strings.
    """
    # This regex looks for the DOCTYPE and the root element, which is the start of a new patent document.
    # It assumes each patent document is self-contained with its own XML declaration and DOCTYPE.
    pattern = re.compile(
        r"(<\?xml[^>]+>\s*<!DOCTYPE[^>]+>\s*<us-patent-[a-z-]+[^>]*>.*?</us-patent-[a-z-]+>)",
        re.DOTALL
    )
    return pattern.findall(xml_content)

def extract_data(xml_string):
    """
    Extracts relevant data points from a single patent XML string.
    """
    root = ET.fromstring(xml_string)
    output = {}

    # Patent Title
    output["title"] = root.findtext(".//invention-title")

    # CPC Classifications
    classifications = []
    classification_versions = []
    for classification in root.findall(".//classification-cpc"):
        version = classification.findtext("cpc-version-indicator/date")
        section = classification.findtext("section")
        class_ = classification.findtext("class")
        subclass = classification.findtext("subclass")
        main_group = classification.findtext("main-group")
        subgroup = classification.findtext("subgroup")
        # Ensure all parts exist before formatting to avoid NoneType errors
        if all(v is not None for v in [section, class_, subclass, main_group, subgroup]):
            formatted_class = f"{section}{class_}{subclass} {main_group}/{subgroup}"
            classifications.append(formatted_class)
            classification_versions.append(version)

    output["classifications"] = classifications
    output["classification_versions"] = classification_versions

    # Abstract Text
    abstract_element = root.find(".//abstract")
    output["abstract_text"] = " ".join(p.text for p in abstract_element.findall(".//p") if p.text).strip() if abstract_element is not None else None

    # Publication and Application References
    for ref_type, prefix in [("publication-reference", "pub_ref"), ("application-reference", "app_ref")]:
        doc_id = root.find(f".//{ref_type}/document-id")
        if doc_id is not None:
            output[f"{prefix}_country"] = doc_id.findtext("country")
            output[f"{prefix}_doc_number"] = doc_id.findtext("doc-number")
            output[f"{prefix}_kind"] = doc_id.findtext("kind")
            output[f"{prefix}_date"] = doc_id.findtext("date")

    # Assignees
    output["assignees_orgnames"] = []
    output["assignees_cities"] = []
    output["assignees_countries"] = []
    for ab in root.findall(".//assignee/addressbook"):
        output["assignees_orgnames"].append(ab.findtext("orgname"))
        addr = ab.find("address")
        output["assignees_cities"].append(addr.findtext("city") if addr is not None else None)
        output["assignees_countries"].append(addr.findtext("country") if addr is not None else None)

    # Inventors
    output["inventors_last_names"] = []
    output["inventors_first_names"] = []
    output["inventors_cities"] = []
    output["inventors_countries"] = []
    for ab in root.findall(".//inventor/addressbook"):
        output["inventors_last_names"].append(ab.findtext("last-name"))
        output["inventors_first_names"].append(ab.findtext("first-name"))
        addr = ab.find("address")
        output["inventors_cities"].append(addr.findtext("city") if addr is not None else None)
        output["inventors_countries"].append(addr.findtext("country") if addr is not None else None)

    # Description Text
    desc_text = []
    for desc in root.findall(".//description//p"):
        desc_text.append(" ".join(desc.itertext()))
    # Clean up multiple spaces and newlines/tabs
    output["description_text"] = re.sub(' +', ' ', " ".join(desc_text).replace("\n", " ").replace("\t", " ")).strip()

    return output

# --- MODIFIED: process_uspto_zip_to_parquet now accepts an optional local_zip_path_to_process ---
def process_uspto_zip_to_parquet(s3_zip_key, local_zip_path_to_process=None):
    """
    Downloads a USPTO zip from S3 (or uses a provided local path), extracts XMLs,
    processes them into a DataFrame, and uploads a consolidated Parquet file to S3.
    """
    print(f"\n[INFO] Starting XML extraction and Parquet conversion for S3 Key: {s3_zip_key}")
    
    # Extract year and base filename from the S3 key for output path
    # Expected s3_zip_key format: "{year}/zipped/{filename.zip}"
    parts = s3_zip_key.split('/')
    if len(parts) < 3 or parts[-2] != 'zipped': 
        print(f"[Error] Invalid S3 key format for processing: {s3_zip_key}. Expected 'year/zipped/filename.zip'")
        return False
    
    year_folder = parts[0] # e.g., "2024"
    zip_base = parts[-1]   # e.g., "ipg240101.zip"

    # Extract date_part for parquet filename (e.g., '240101' from 'ipg240101.zip')
    # This new pattern captures the 6 digits but allows for other characters before .zip
    date_match = re.search(r'ipg(\d{6}).*?\.zip', zip_base, re.IGNORECASE)
    if not date_match:
        print(f"[Error] Could not extract date_part from filename: {zip_base}. Skipping Parquet conversion.")
        return False
    date_part = date_match.group(1)

    # Define S3 output key for the Parquet file
    parquet_s3_key = f"{year_folder}/xmls/{date_part}.parquet"

    # Check if parquet already exists in S3 to avoid reprocessing
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=parquet_s3_key)
        print(f"[✓] Skipping Parquet generation for {zip_base} → Already processed as {parquet_s3_key}")
        return True # Already processed
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"[Error] Checking S3 for existing Parquet {parquet_s3_key}: {e}")
            return False # Other S3 error, stop processing this file

    # Use a temporary directory for all local operations (download, extract, create parquet)
    with tempfile.TemporaryDirectory() as temp_dir:
        actual_zip_path = ""
        if local_zip_path_to_process and os.path.exists(local_zip_path_to_process):
            # If a local path is provided and exists, use it directly
            actual_zip_path = local_zip_path_to_process
            print(f"[INFO] Using existing local file for processing: {actual_zip_path}")
        else:
            # Otherwise, download the ZIP file from S3 to the temp directory
            actual_zip_path = os.path.join(temp_dir, zip_base)
            try:
                print(f"[INFO] Downloading {s3_zip_key} from S3 for processing...")
                s3.download_file(BUCKET_NAME, s3_zip_key, actual_zip_path)
                print(f"[INFO] Downloaded {zip_base} from S3.")
            except ClientError as e:
                print(f"❌ Error downloading {s3_zip_key} from S3: {e}")
                return False


        extract_path = os.path.join(temp_dir, "extracted_xmls")
        os.makedirs(extract_path, exist_ok=True)

        try:
            with zipfile.ZipFile(actual_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print(f"[Extracted] ZIP: {zip_base} to {extract_path}")
        except zipfile.BadZipFile:
            print(f"[Error] {zip_base} is a bad or corrupted zip file. Skipping processing.")
            return False
        except Exception as e:
            print(f"[Error] Extracting {zip_base}: {e}")
            return False

        xml_files = [f for f in os.listdir(extract_path) if f.endswith('.xml')]
        if not xml_files:
            print(f"[!] No XML files found inside {zip_base}. Skipping Parquet creation.")
            return False

        print(f"[Info] Found {len(xml_files)} XML files inside {zip_base}")

        consolidated_records = []
        for xml_file in xml_files:
            local_xml_file_path = os.path.join(extract_path, xml_file)
            try:
                # Read raw data, try UTF-8 first, then ISO-8859-1
                with open(local_xml_file_path, 'rb') as f:
                    raw_data = f.read()
                try:
                    xml_content = raw_data.decode("utf-8")
                except UnicodeDecodeError:
                    xml_content = raw_data.decode("ISO-8859-1")

                # Split into individual patent documents within the XML file
                patents = split_patent_documents(xml_content)
                print(f" └─ {xml_file}: {len(patents)} patent records found")

                for patent_xml_string in patents:
                    try:
                        record = extract_data(patent_xml_string)
                        consolidated_records.append(record)
                    except ET.ParseError as pe:
                        print(f"[Error] XML ParseError in {xml_file} for a patent record: {pe}")
                    except Exception as e:
                        print(f"[Error] Extracting patent data from a record in {xml_file}: {e}")

            except Exception as e:
                print(f"[Error] Reading or processing XML file {xml_file}: {e}")

        if consolidated_records:
            df = pd.DataFrame(consolidated_records)
            # Use 'pub_ref_doc_number' for deduplication if it's consistently available and unique
            if "pub_ref_doc_number" in df.columns:
                df.drop_duplicates(subset=["pub_ref_doc_number"], inplace=True)
                print(f"[Info] Deduplicated to {len(df)} unique records.")
            else:
                print("WARNING: 'pub_ref_doc_number' not found for deduplication. Skipping deduplication.")

            parquet_output_path = os.path.join(temp_dir, f"{date_part}.parquet")
            df.to_parquet(parquet_output_path, index=False)
            
            # Upload Parquet to S3
            if upload_to_s3(parquet_output_path, BUCKET_NAME, parquet_s3_key):
                print(f"[✓] Successfully processed and uploaded {len(df)} records from {zip_base} to {parquet_s3_key}.")
                return True
            else:
                print(f"❌ Failed to upload Parquet file for {zip_base}.")
                return False # Upload failed
        else:
            print(f"[!] No valid patent records were extracted from {zip_base}. No Parquet created.")
            return False
    # temp_dir and its contents are automatically cleaned up when exiting 'with' block

# --- Function to set rows per page ---
def set_rows_per_page(driver, wait, rows_per_page="75"):
    """
    Clicks the 'Rows per page' dropdown and selects the specified number of rows.
    """
    print(f"[INFO] Attempting to set 'Rows per page' to {rows_per_page}...")
    try:
        # 1. Click the dropdown to open it
        rows_per_page_dropdown = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[@aria-label='Rows per page' and contains(@class, 'p-select-label')]"))
        )
        rows_per_page_dropdown.click()
        print("[INFO] 'Rows per page' dropdown clicked.")

        # 2. Wait for the options to appear and click the desired option
        rows_per_page_option_xpath = f"//li[@role='option']/span[text()='{rows_per_page}']"
        option_to_select = wait.until(
            EC.element_to_be_clickable((By.XPATH, rows_per_page_option_xpath))
        )
        option_to_select.click()
        print(f"[INFO] Selected '{rows_per_page}' rows per page.")
        time.sleep(5) # Give time for the page to update/re-render the table with new rows

        return True

    except TimeoutException:
        print(f"❌ Timeout trying to set 'Rows per page' to {rows_per_page}. Element not found or clickable.")
        driver.save_screenshot(f"set_rows_per_page_timeout_error.png")
        return False
    except NoSuchElementException:
        print(f"❌ 'Rows per page' dropdown or '{rows_per_page}' option not found.")
        driver.save_screenshot(f"set_rows_per_page_not_found_error.png")
        return False
    except ElementNotInteractableException:
        print(f"❌ 'Rows per page' dropdown or option not interactable.")
        driver.save_screenshot(f"set_rows_per_page_not_interactable_error.png")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred while setting 'Rows per page': {e}")
        driver.save_screenshot(f"set_rows_per_page_general_error.png")
        return False


# --- Main Automation Pipeline ---
if __name__ == "__main__":
    driver = None
    wait = None
    try:
        # Initialize Chrome browser
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        wait = WebDriverWait(driver, 60) # Increased wait time for page elements

        # Inject JavaScript to mask navigator.webdriver and other detection points.
        # This needs to be done *before* navigating to the page.
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """
        })

        # --- Dynamically get the latest end date ---
        latest_date_str = get_latest_uspto_patent_grant_date()
        # The 'from' date defines the start of the data range. Set as needed.
        # Using an earlier date to ensure multiple files are available for demonstration.
        # For testing, ensure this range contains files you expect.
        from_date_str = "01-01-2024" 

        # --- URL Construction ---
        base_ptgrxml_url = "https://data.uspto.gov/bulkdata/datasets/ptgrxml"

        print(f"[INFO] Navigating to the USPTO PTGRXML dataset page: {base_ptgrxml_url}")
        driver.get(base_ptgrxml_url)

        print("[INFO] Waiting for initial page content to load (10 seconds)...")
        time.sleep(10) # Give sufficient time for anti-bot mechanisms and page JS to execute

        current_url = driver.current_url
        if "uspto.gov/home" in current_url and current_url != base_ptgrxml_url:
            print("❌ Redirection to USPTO home page detected immediately after initial navigation.")
            print("This indicates strong bot detection. Consider running with headless=False for visual debugging.")
            driver.save_screenshot("initial_redirect_error.png")
            raise Exception("Bot detection suspected: Redirected to home page.")

        print(f"[INFO] Page loaded: {current_url}")

        # --- Step 1: Locate and set the 'Start Date' input field using JavaScript ---
        print(f"[INFO] Setting 'Start Date' to: {from_date_str} using JavaScript.")
        from_date_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@aria-labelledby='from']")))
        driver.execute_script(f"arguments[0].value = '{from_date_str}';", from_date_input)
        # Trigger a blur event to make the date picker acknowledge the change
        driver.execute_script("arguments[0].dispatchEvent(new Event('blur'));", from_date_input)
        # Also try sending TAB, just in case a native event is needed
        from_date_input.send_keys(Keys.TAB)
        time.sleep(1)

        # --- Step 2: Locate and set the 'End Date' input field using JavaScript ---
        print(f"[INFO] Setting 'End Date' to: {latest_date_str} using JavaScript.")
        to_date_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@aria-labelledby='to']")))
        driver.execute_script(f"arguments[0].value = '{latest_date_str}';", to_date_input)
        # Trigger a blur event
        driver.execute_script("arguments[0].dispatchEvent(new Event('blur'));", to_date_input)
        # Also try sending TAB
        to_date_input.send_keys(Keys.TAB)
        time.sleep(1)

        # --- Step 3: Click the "Filter" button ---
        print("[INFO] Clicking the 'Filter' button...")
        filter_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Filter')]")))
        filter_button.click()
        print("[INFO] 'Filter' button clicked. Waiting for results table to update (5 seconds)...")
        time.sleep(5) # Give time for the page to process the filter and update the results table

        # --- Set Rows per Page to 75 ---
        set_rows_per_page(driver, wait, "75")
        time.sleep(5) # Give extra time for page to settle after changing rows per page

        print(f"\n[INFO] Processing the current page (after filter and rows per page setting)...")
        # --- Find ALL download links in the updated results table ---
        print("[INFO] Looking for all .zip download links in the current results table...")
        
        all_download_links = []
        try:
            # Wait for at least one download link to be present, indicating table loaded
            all_download_links = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), '.zip') and @href='#']")))
            
            if not all_download_links:
                print("❌ No .zip download links found on this page. Ending script.")
            else:
                print(f"[INFO] Found {len(all_download_links)} .zip files to download and process on the current page.")

                # Iterate and download each file on the current page
                for i in range(len(all_download_links)):
                    try:
                        # Re-locate the current link to avoid StaleElementReferenceException
                        current_page_links = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), '.zip') and @href='#']")))
                        link = current_page_links[i]
                        filename = link.text.strip()
                    except IndexError:
                        print(f"WARNING: Link at index {i} no longer exists on the page. Moving to next available link.")
                        continue 
                    except StaleElementReferenceException:
                        print(f"WARNING: Stale element reference for link at index {i}. Re-locating and retrying.")
                        all_download_links = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), '.zip') and @href='#']")))
                        try:
                            link = all_download_links[i]
                            filename = link.text.strip()
                        except IndexError: 
                            print(f"WARNING: Still unable to access link at index {i} after re-location. Skipping.")
                            continue

                    if not filename.lower().endswith(".zip"):
                        print(f"WARNING: Extracted filename '{filename}' does not end with .zip. Skipping.")
                        continue 
                    
                    # --- Extract the year based on 'ipgYYMMDD.zip' format ---
                    try:
                        short_year = filename[3:5] 
                        year = f"20{short_year}" 
                        raw_s3_key = f"{year}/zipped/{filename}" 
                    except IndexError:
                        print(f"WARNING: Could not parse year from filename '{filename}'. Skipping this file.")
                        continue

                    # --- NEW LOGIC for skipping re-download and processing ---
                    date_match = re.search(r'ipg(\d{6})\.zip', filename, re.IGNORECASE)
                    date_part = date_match.group(1) if date_match else None
                    parquet_s3_check_key = f"{year}/xmls/{date_part}.parquet" if date_part else None

                    if raw_s3_key and s3_file_exists(BUCKET_NAME, raw_s3_key):
                        if parquet_s3_check_key and s3_file_exists(BUCKET_NAME, parquet_s3_check_key):
                            print(f"SKIPPING: Both Raw ZIP '{filename}' and Parquet already exist on S3. Moving to next file.")
                            continue 
                        else:
                            print(f"INFO: Raw ZIP '{filename}' exists on S3, but Parquet does not. Attempting to process existing raw ZIP from S3.")
                            process_uspto_zip_to_parquet(raw_s3_key) 
                            continue 

                    # Proceed with download if not skipped
                    print(f"[Clicking] Download link {i+1}/{len(all_download_links)} for: {filename}")
                    try:
                        link.click() # Attempt standard click
                    except Exception as e: # Catch any exception during click
                        print(f"❌ Failed to click {filename}: {e}. Retrying with JavaScript click.")
                        driver.execute_script("arguments[0].click();", link) # Fallback to JavaScript click
                        print(f"[INFO] Successfully clicked {filename} using JavaScript.")
                    
                    local_download_path = os.path.join(download_dir, filename)
                    
                    # Wait for download to complete
                    download_succeeded = wait_for_download_completion(local_download_path) 

                    if download_succeeded:
                        # --- Step 5: Upload Raw ZIP to S3 ---
                        upload_succeeded = upload_to_s3(local_download_path, BUCKET_NAME, raw_s3_key)
                        
                        if upload_succeeded:
                            # --- Step 6: Process the locally downloaded and newly uploaded ZIP into Parquet ---
                            process_uspto_zip_to_parquet(raw_s3_key, local_zip_path_to_process=local_download_path)

                            # Clean up local raw ZIP file after successful upload and processing
                            try:
                                os.remove(local_download_path)
                                print(f"🧹 Local raw ZIP file deleted: {local_download_path}")
                            except OSError as e:
                                print(f"❌ Error deleting local raw ZIP file {local_download_path}: {e}")
                        else:
                            print(f"Skipping Parquet processing for {filename} due to failed S3 upload of raw ZIP.")
                    else:
                        print(f"Skipping S3 upload and Parquet processing for {filename} due to failed download.")
                    
                    time.sleep(2) # Short pause between downloads/uploads to prevent overwhelming the browser/server

        except TimeoutException:
            print("❌ Could not find any .zip download links on this page within timeout. Ending script.")
        
    except TimeoutException as e:
        print(f"A timeout occurred: {e}")
        print("This often means an element was not found or the page didn't load in time.")
        if driver: driver.save_screenshot("timeout_error_screenshot.png")
        print("Screenshot saved as timeout_error_screenshot.png for debugging.")
    except NoSuchElementException as e:
        print(f"An element was not found: {e}")
        print("The XPath or selector might be incorrect, or the page structure has changed.")
        if driver: driver.save_screenshot("element_not_found_error.png")
        print("Screenshot saved as element_not_found_error.png for debugging.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Ensure driver is initialized before trying to save screenshot
        if driver:
            driver.save_screenshot("general_error_screenshot.png")
            print("Screenshot saved as general_error_screenshot.png for debugging.")
        else:
            print("Driver was not initialized, cannot save screenshot.")

    finally:
        if driver:
            print("[INFO] Quitting browser.")
            driver.quit()
        # Clean up the temporary download directory if it's empty or you want to ensure it's removed
        if os.path.exists(download_dir) and not os.listdir(download_dir):
            try:
                os.rmdir(download_dir)
                print(f"🧹 Cleaned up empty download directory: {download_dir}")
            except OSError as e:
                print(f"❌ Error removing empty download directory {download_dir}: {e}")