import re
import unicodedata
from html import unescape
import pandas as pd
from pathlib import Path
import os
import json
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
import time
from io import BytesIO

EXCEL_URL = "https://www.federalbank.co.in/documents/10180/8267740/Housing%2BProjects%2BFinanced.xlsx/363365c1-bc84-47ec-acc1-315d1a4e93a5?t=1479363143811"

# --- normalizers -------------------------------------------------------------

ZW_REGEX = re.compile(r"[\u200B-\u200D\u2060\uFEFF]")  # zero-width chars
NBSP_REGEXES = [
    (re.compile(r"&nbsp;?", flags=re.IGNORECASE), " "),
    (re.compile(r"\u00A0"), " "),                     # Unicode NBSP
    (re.compile(r"\bNBSP\b", flags=re.IGNORECASE), " "),
]

QUOTE_TABLE = str.maketrans({
    "“": '"', "”": '"', "„": '"', "‟": '"',
    "‘": "'", "’": "'", "‚": "'", "‛": "'",
    "`": "'", "´": "'",
})

def clean_generic(text: str) -> str:
    """Clean general text fields (builder/project). Preserve case."""
    if pd.isna(text):
        return ""
    s = str(text)

    # HTML decode then Unicode normalize
    s = unescape(s)
    s = unicodedata.normalize("NFKC", s)

    # Replace NBSP variants and remove zero-width chars
    for rx, rep in NBSP_REGEXES:
        s = rx.sub(rep, s)
    s = ZW_REGEX.sub("", s)

    # Normalize quotes/backticks
    s = s.translate(QUOTE_TABLE)

    # Remove stray outer quotes/backticks
    s = s.strip().strip('"').strip("'")

    # Collapse repeated quotes inside like ""Shree""
    s = re.sub(r'"+', '"', s)
    s = re.sub(r"'+", "'", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_city_phrase(location: str) -> str:
    """Return FULL phrase after the last comma; tidy & title-case."""
    if pd.isna(location):
        return ""
    s = clean_generic(location)

    # After last comma (if any), else whole string
    part = s.rsplit(",", 1)[-1] if "," in s else s

    # Drop any parenthetical notes
    part = re.sub(r"\([^)]*\)", " ", part)

    # Keep letters, spaces, and hyphens, then collapse spaces
    part = re.sub(r"[^A-Za-z\s-]", " ", part)
    part = re.sub(r"\s+", " ", part).strip()

    # Title case for city/state names (keeps multi-word e.g., "Tamil Nadu")
    return part.title()

# --- pipeline ----------------------------------------------------------------

def fetch_excel_with_retries(url: str, attempts: int = 5, timeout: int = 30) -> BytesIO:
    last_err = None
    for i in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout, allow_redirects=True)
            if 200 <= resp.status_code < 300 and resp.content:
                return BytesIO(resp.content)
            last_err = RuntimeError(f"HTTP {resp.status_code}")
        except Exception as e:
            last_err = e
        sleep_s = min(2 ** i, 30)
        print(f"Download attempt {i}/{attempts} failed: {last_err}. Retrying in {sleep_s}s...")
        time.sleep(sleep_s)
    raise last_err or RuntimeError("Failed to download Excel file")

# Read workbook (row 2 has real headers, header=1) with retries to avoid HTTP 5xx
excel_bytes = fetch_excel_with_retries(EXCEL_URL)
df = pd.read_excel(excel_bytes, header=1, engine="openpyxl")

# Keep needed columns, clean, and build city
df = df[["Name of the Builder/Developer", "Project Name", "Location"]].dropna(how="all")

df["builder"] = df["Name of the Builder/Developer"].apply(clean_generic)
df["project"] = df["Project Name"].apply(clean_generic)
df["city"] = df["Location"].apply(extract_city_phrase)

# Final order (no raw Location in output)
out_df = df[["city", "builder", "project"]]

# Drop rows where city ended up empty after cleaning
out_df = out_df[out_df["city"].astype(bool)]

# Save
OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
csv_path = OUT_DIR / "federalbank_apf_data.csv"
out_df.to_csv(csv_path, index=False, encoding="utf-8")
print(f"Saved {len(out_df)} rows to {csv_path}")


def data_processing():
    try:
        # prepare grouped json by city
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "federalbank_apf_data.csv"
        columns = ["city", "builder", "project"]
        df = pd.read_csv(CSV_PATH, names=columns, header=0)

        grouped_data = {}
        for _, row in df.iterrows():
            city = row.get("city", "Unknown")
            builder_name = row.get("builder", "Unknown")
            project_name = row.get("project", "Unknown")
            grouped_data.setdefault(city, []).append({
                "builderName": builder_name,
                "projectName": project_name
            })

        # load env and validate
        load_dotenv()
        bucket = os.getenv("S3_BUCKET_NAME")
        key_prefix = os.getenv("S3_KEY")
        if not bucket:
            raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
        if not key_prefix:
            raise ValueError("S3_KEY is not set. Please set it in environment or .env")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/federalbank_data_{timestamp}.json"

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(grouped_data, indent=4).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"Data uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"Error: {str(e)}")


# process and upload
data_processing()
