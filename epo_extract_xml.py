import os
import zipfile
import tarfile
import boto3
import shutil

# --- Configuration ---
s3 = boto3.client("s3")
BUCKET = "epo.inventohub"
YEARS_TO_PROCESS = ["2025"]
TEMP_BASE_DIR = "/mnt/epodata"


def is_valid_xml(filename):
    return filename.endswith(".xml") and not filename.endswith("TOC.xml")


def extract_top_archive(archive_path, extract_to):
    print(f"📦 Extracting top-level archive: {archive_path}")
    if archive_path.endswith(".zip"):
        with zipfile.ZipFile(archive_path, 'r') as z:
            z.extractall(extract_to)
    elif archive_path.endswith(".tar"):
        with tarfile.open(archive_path, 'r') as t:
            t.extractall(extract_to)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")


def extract_and_upload(archive_key, dest_s3_folder):
    archive_name = os.path.basename(archive_key)
    tmpdir = os.path.join(TEMP_BASE_DIR, os.path.splitext(archive_name)[0])
    os.makedirs(tmpdir, exist_ok=True)

    try:
        archive_path = os.path.join(tmpdir, archive_name)
        print(f"⬇️ Downloading {archive_key} to {archive_path}")
        s3.download_file(BUCKET, archive_key, archive_path)

        extract_top_archive(archive_path, tmpdir)

        doc_path = os.path.join(tmpdir, "DOC")
        if not os.path.exists(doc_path):
            print(f"⚠️ 'DOC' directory not found in {archive_key}. Searching entire temp directory.")
            doc_path = tmpdir

        for root, _, files in os.walk(doc_path):
            for file in files:
                file_path = os.path.join(root, file)
                if zipfile.is_zipfile(file_path):
                    with zipfile.ZipFile(file_path, 'r') as inner_zip:
                        for inner_file_name in inner_zip.namelist():
                            if is_valid_xml(inner_file_name):
                                xml_filename = os.path.basename(inner_file_name)
                                dest_key = f"{dest_s3_folder}/{xml_filename}"

                                # Check if XML already exists to avoid duplicate uploads
                                try:
                                    s3.head_object(Bucket=BUCKET, Key=dest_key)
                                    print(f"⚠️ Skipping already uploaded: {dest_key}")
                                    continue
                                except s3.exceptions.ClientError as e:
                                    if e.response["Error"]["Code"] != "404":
                                        raise

                                with inner_zip.open(inner_file_name) as f:
                                    print(f"⬆️ Uploading {xml_filename} to {dest_key}")
                                    s3.upload_fileobj(f, BUCKET, dest_key)

        print(f"✅ Successfully processed {archive_key}")
        print(f"✅ Original archive {archive_key} is kept in S3.")

    except Exception as e:
        print(f"❌ Error processing {archive_key}: {e}")
    finally:
        if os.path.exists(tmpdir):
            print(f"🧹 Cleaning up local directory: {tmpdir}")
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    print("🚀 Starting EPO XML extraction process...")

    for year in YEARS_TO_PROCESS:
        print(f"\n--- Processing Year: {year} ---")
        source_prefix = str(year)
        dest_base_prefix = f"{year}/epo-xmls"

        for week in range(1, 53):
            week_str = str(week).zfill(2)
            dest_s3_folder = f"{dest_base_prefix}/{year}_{week_str}"
            archive_name_base = f"EPRTBJV{year}0000{week_str}001001"

            for ext in [".zip", ".tar"]:
                archive_key = f"{source_prefix}/{archive_name_base}{ext}"
                try:
                    s3.head_object(Bucket=BUCKET, Key=archive_key)
                    print(f"🔎 Found archive: {archive_key}")

                    # 🧠 Check if destination already has XMLs
                    existing = s3.list_objects_v2(Bucket=BUCKET, Prefix=dest_s3_folder + "/")
                    already_processed = any(obj["Key"].endswith(".xml") for obj in existing.get("Contents", []))

                    if already_processed:
                        print(f"⏭️ Skipping archive {archive_key} — XMLs already exist in {dest_s3_folder}")
                        break

                    extract_and_upload(archive_key, dest_s3_folder)
                    break

                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        continue
                    else:
                        print(f"❌ Boto3 client error on {archive_key}: {e}")
                        raise

    print("\n🏁 All years processed.")
