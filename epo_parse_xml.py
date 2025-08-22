import os
import re
import io
import time
import boto3
import pandas as pd
import psycopg2
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_batch
import pprint

# ==============================================================================
# --- 1. CONFIGURATION ---
# ==============================================================================


# --- S3 Client ---
BUCKET = "epo.inventohub"
s3 = boto3.client("s3")

# --- PostgreSQL Database Configuration ---
db_config = {
    "database": "postgres",
    "user": "postgres",
    "password": "Invento!12345!",
    "host": "db-epo-serverless-cluster.cluster-ro-cneqa26q6wpx.ap-south-1.rds.amazonaws.com",
    "port": 5432
}

# ==============================================================================
# --- 2. HELPER FUNCTIONS ---
# ==============================================================================

def get_db_connection():
    print("🔌 Connecting to Aurora database...")
    return psycopg2.connect(**db_config)

def parse_xml(s3_client, key):
    try:
        response = s3_client.get_object(Bucket=BUCKET, Key=key)
        content = response['Body'].read()
        root = ET.fromstring(content)
        doc_id = root.attrib.get("id", "").strip()
        doc_number_str = root.attrib.get("doc-number", "").strip()
        if not doc_id or not doc_number_str or not doc_number_str.isdigit():
            print(f"⚠️ Skipping due to missing doc_id/doc_number: {doc_id}, {doc_number_str}")
            return None
        doc_number = int(doc_number_str)

        def get_texts(parent, tags):
            if parent is None: return ""
            return ' '.join(
                el.text.strip() for tag in tags for el in parent.findall(f'.//{tag}')
                if el is not None and el.text
            )

        def get_ordered_texts(elem):
            if elem is None: return ""
            texts = []
            for child in elem.iter():
                tag = child.tag.split('}', 1)[-1]
                if tag in ['p', 'ul', 'li', 'heading']:
                    text = ''.join(child.itertext()).strip()
                    if text:
                        texts.append(text)
            return '\n'.join(texts)

        abstract_el = root.find(".//abstract[@id='abst']")
        abstract_text = get_texts(abstract_el, ['p']) if abstract_el is not None else ""
        desc_el = root.find(".//description[@id='desc']")
        desc_text = get_ordered_texts(desc_el) if desc_el is not None else ""
        claims_el = root.find(".//claims[@id='claims01']")
        claims_text = get_texts(claims_el, ['claim-text']) if claims_el is not None else ""
        ipc_elements = root.findall(".//B500/B510EP/classification-ipcr/text")
        ipc_list = [el.text.strip() for el in ipc_elements if el is not None and el.text]
        ipc_classifications = '; '.join(ipc_list)
        cpc_elements = root.findall(".//B520EP/classifications-cpc/classification-cpc/text")
        cpc_list = [el.text.strip() for el in cpc_elements if el is not None and el.text]
        cpc_classifications = '; '.join(cpc_list)
        int_appl_num_el = root.find(".//B860/B861/dnum/anum")
        int_application_number = int_appl_num_el.text.strip() if int_appl_num_el is not None else ""
        applicant_elements = root.findall(".//B700/B710/B711/snm")
        applicants = '; '.join(el.text.strip() for el in applicant_elements if el is not None and el.text)
        inventor_elements = root.findall(".//B720/B721/snm")
        inventors = '; '.join(el.text.strip() for el in inventor_elements if el is not None and el.text)
        titles = {'title_en': '', 'title_de': '', 'title_fr': ''}
        b540 = root.find(".//B540")
        if b540 is not None:
            langs = b540.findall("B541")
            texts = b540.findall("B542")
            for lang_tag, title_tag in zip(langs, texts):
                lang = lang_tag.text.strip() if lang_tag.text else ""
                text = title_tag.text.strip() if title_tag.text else ""
                if lang.lower() == 'en': titles['title_en'] = text
                elif lang.lower() == 'de': titles['title_de'] = text
                elif lang.lower() == 'fr': titles['title_fr'] = text
        int_class_main = root.findtext(".//B510/B511", "").strip()
        int_class_subs = [el.text.strip() for el in root.findall(".//B510/B512") if el.text]
        int_classification = '; '.join(filter(None, [int_class_main] + int_class_subs))
        date_pub_el = root.find(".//B400/B405/date")
        date_publication = date_pub_el.text.strip() if date_pub_el is not None else ""
        year_publication = date_publication[:4]
        date_filing_el = root.find(".//B200/B220/date")
        date_filing = date_filing_el.text.strip() if date_filing_el is not None else ""
        year_filing = date_filing[:4]
        priority_number = root.findtext(".//B300/B310", "").strip()
        priority_date = root.findtext(".//B300/B320/date", "").strip()

        return {
            
            "doc_id": doc_id,
            **titles,
            "doc_number": doc_number,
            'lang': root.get('lang'),
            'country': root.get('country'),
            'abstract': abstract_text,
            'description': desc_text,
            'claims': claims_text,
            'ipc_classifications': ipc_classifications,
            'cpc_classifications': cpc_classifications,
            'international_application_number': int_application_number,
            'applicants': applicants,
            'inventors': inventors,
            'int_classifications': int_classification,
            'date_publication': date_publication,
            'year_publication':year_publication,
            'date_filing': date_filing,
            'year_filing':year_filing,
            "priority_number": priority_number,
            "priority_date" : priority_date,
            "representatives": "; ".join(
                f"{el.findtext('snm', '').strip()}, {el.findtext('adr/city', '').strip()}, {el.findtext('adr/ctry', '').strip()}".strip(", ")
                for el in root.findall(".//B700/B740/B741")
            ),
            "correction_code": root.findtext(".//B150/B151", "").strip(),
            "correction_description": next(
                (
                    t.text.strip()
                    for b155 in root.findall(".//B150/B155")
                    for l, t in zip(b155.findall("B1551"), b155.findall("B1552"))
                    if l is not None and t is not None and l.text == "en" and t.text
                ),
                ""
            ),
            "references_cited": "; ".join(el.text.strip() for el in root.findall(".//B560/B561/text") if el.text),
            "proprietors": "; ".join(
                f"{el.findtext('snm', '').strip()}, {el.findtext('adr/city', '').strip()}, {el.findtext('adr/ctry', '').strip()}".strip(", ")
                for el in root.findall(".//B700/B730/B731")
            ),
        }
    except Exception as e:
        print(f"❌ Error parsing {key}: {e}")
        return None

# ==============================================================================
# --- 3. MAIN PROCESSING LOGIC (Modified to Insert to DB) ---
# ==============================================================================

def process_year(year):
    print(f"\n📇 Processing year: {year}")
    prefix = f"{year}/epo-xmls"
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    total_records_inserted = 0
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO epo_embed14k (
        doc_id, title_en, title_de, title_fr, doc_number, lang, country, abstract,
        description, claims, date_publ, ipc_classifications, cpc_classifications,
        international_application_number, applicants, inventors, int_classifications,
        date_publication, date_filing
    ) VALUES (
        %(doc_id)s, %(title_en)s, %(title_de)s, %(title_fr)s, %(doc_number)s, %(lang)s, %(country)s, %(abstract)s,
        %(description)s, %(claims)s, %(date_publ)s, %(ipc_classifications)s, %(cpc_classifications)s,
        %(international_application_number)s, %(applicants)s, %(inventors)s, %(int_classifications)s,
        %(date_publication)s, %(date_filing)s

    )
    ON CONFLICT (doc_id) DO NOTHING

    """

    batch_size = 500
    batch = []

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".xml") and not key.endswith("TOC.xml"):
                data = parse_xml(s3, key)
                if data:
                    batch.append(data)
                    if len(batch) >= batch_size:
                        try:
                            execute_batch(cursor, insert_query, batch)
                            conn.commit()
                            print(f"  ✅ Inserted {len(batch)} records.")
                            total_records_inserted += len(batch)
                            batch = []
                        except Exception as e:
                            print(f"❌ Insert error: {e}")
                            conn.rollback()

    if batch:
        try:
            execute_batch(cursor, insert_query, batch)
            conn.commit()
            print(f"  ✅ Inserted final {len(batch)} records.")
            total_records_inserted += len(batch)
        except Exception as e:
            print(f"❌ Final insert error: {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    print(f"🏁 Finished processing {year}. Total inserted: {total_records_inserted}")

# ==============================================================================
# --- 4. DEBUGGING EXECUTION ---
# ==============================================================================

if __name__ == "__main__":
    YEAR_TO_TEST = "2025"
    WEEK_TO_TEST = "2025_27"
    FILES_TO_TEST = 1
    #process_year(2025)
    print(f"🚀 Testing XML parse for {FILES_TO_TEST} files...")
    prefix = f"{YEAR_TO_TEST}/epo-xmls/{WEEK_TO_TEST}/"

    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

        if 'Contents' not in response:
            print(f"❌ No files found at {prefix}")
        else:
            files_processed = 0
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(".xml") and not key.endswith("TOC.xml"):
                    print("\n" + "="*50)
                    print(f"🔎 Testing file: {key}")
                    print("="*50)
                    parsed_data = parse_xml(s3, key)
                    if parsed_data:
                        pprint.pprint(parsed_data)
                    else:
                        print("❌ Parsing failed.")
                    files_processed += 1
                    if files_processed >= FILES_TO_TEST:
                        break

            if files_processed == 0:
                print("⚠️ No XML files found in folder.")

    except Exception as e:
        print(f"❌ Error: {e}")

    print("✅ Script finished.")
