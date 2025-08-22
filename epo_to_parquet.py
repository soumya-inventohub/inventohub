#!/usr/bin/env python3
import os
import re
import io
import boto3
import pandas as pd
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# Embedding deps
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sentence_transformers import SentenceTransformer
import tiktoken
import torch





# ───────────────── Config ─────────────────
BUCKET = "epo.inventohub"
YEAR = "2025"
WEEK = "2025_01"
PREFIX = f"{YEAR}/epo-xmls/{WEEK}/"
OUTPUT_KEY = f"{PREFIX}{WEEK}.parquet"
MAX_WORKERS = 8  # adjust based on Vast.ai GPU/CPU

# ───────────────── S3 ─────────────────
s3 = boto3.client("s3")

# ───────────────── NLTK ─────────────────
nltk.download('stopwords')
nltk.download('wordnet')
EN_STOPWORDS = set(stopwords.words('english'))
LEMMATIZER = WordNetLemmatizer()

# ───────────────── Embedding setup ─────────────────
CHUNK_TOKENS = 2000
OVERLAP_TOKENS = 200
TARGET_DIM = 1024

tokenizer = tiktoken.get_encoding("cl100k_base")
device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = SentenceTransformer('intfloat/e5-large-v2', device=device)

def preprocess_text(text):
    if pd.isna(text):
        return "No text provided"
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    tokens = re.findall(r'\b\w+\b', text)
    tokens = [w for w in tokens if w not in EN_STOPWORDS]
    return ' '.join([LEMMATIZER.lemmatize(w) for w in tokens])

def chunk_text(text: str, max_tokens=CHUNK_TOKENS, overlap=OVERLAP_TOKENS):
    tokens = tokenizer.encode(text)
    chunks, start = [], 0
    while start < len(tokens):
        chunk = tokens[start:start + max_tokens]
        chunks.append(tokenizer.decode(chunk))
        start += max_tokens - overlap
    return chunks

def embed_document(text: str) -> list[float]:
    try:
        processed_text = preprocess_text(text)
        chunks = chunk_text(processed_text)
        vectors = model.encode(chunks, convert_to_tensor=True).tolist()
        if vectors:
            # average pooling
            return [sum(x) / len(x) for x in zip(*vectors)]
        return [0.0] * TARGET_DIM
    except Exception as e:
        print(f"[Embedding error] {e}")
        return [0.0] * TARGET_DIM

# ───────────────── parse_xml ─────────────────
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
            'year_publication': year_publication,
            'date_filing': date_filing,
            'year_filing': year_filing,
            "priority_number": priority_number,
            "priority_date": priority_date,
        }
    except Exception as e:
        print(f"❌ Error parsing {key}: {e}")
        return None

# ───────────────── Processing function ─────────────────
def process_key(key):
    data = parse_xml(s3, key)
    if not data:
        return None
    full_text = "\n".join([
        f"Title: {data.get('title_en', '')}",
        f"Abstract: {data.get('abstract', '')}",
        f"Description: {data.get('description', '')}",
        f"Applicants: {data.get('applicants', '')}",
        f"Inventors: {data.get('inventors', '')}",
    ])
    data["embedding"] = embed_document(full_text)
    return data

# ───────────────── Main ─────────────────
def main():
    print(f"🔎 Listing XMLs under s3://{BUCKET}/{PREFIX}")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj['Key']
        for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)
        for obj in page.get('Contents', [])
        if obj['Key'].endswith(".xml") and not obj['Key'].endswith("TOC.xml")
    ]
    print(f"📂 Found {len(keys)} XML files")

    records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_key, key): key for key in keys}
        for i, future in enumerate(as_completed(futures), 1):
            key = futures[future]
            try:
                result = future.result()
                if result:
                    records.append(result)
            except Exception as e:
                print(f"❌ Error processing {key}: {e}")
            if i % 100 == 0:
                print(f"✅ Processed {i}/{len(keys)} files")

    if not records:
        print("⚠️ No parsed records found. Exiting.")
        return

    df = pd.DataFrame(records)
    print(f"✅ Parsed rows: {len(df)}; columns in parquet = parsed fields + 'embedding'")

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=buf.getvalue())
    print(f"📦 Saved with embeddings → s3://{BUCKET}/{OUTPUT_KEY}")

if __name__ == "__main__":
    main()
