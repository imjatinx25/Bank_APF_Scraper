import io
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import os
import json

import requests
import pandas as pd
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

PDF_URL = "https://www.icicihfc.com/content/dam/new-icicihfc-assets/doc17/List%20of%20PAN%20India%20APF%20Projects.pdf"
USE_TABULA = False  # Disable tabula-py to avoid Java/jpype and encoding errors
OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
PDF_PATH = OUT_DIR / "icici_hfc_apf.pdf"
CSV_PATH = OUT_DIR / "icici_hfc_apf_data.csv"

TARGET_COLS = ["City", "Builder Group", "Project Name"]

# -------------------- download --------------------

def download_pdf(url: str, path: Path):
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=60)
        r.raise_for_status()
        if not r.content.startswith(b"%PDF") and "pdf" not in r.headers.get("Content-Type","").lower():
            raise ValueError(f"Non-PDF response: {r.headers.get('Content-Type')}")
        path.write_bytes(r.content)
    except Exception as e:
        print("ERROR:", str(e))
        print("ISSUE: PDF download failed.")
        print("FIXES: Verify URL/network; retry; ensure server returns PDF bytes.")
        print("CAUSE: Network/CORS/CDN or non-PDF content.")
        sys.exit(1)

# -------------------- cleaners --------------------

_re_single_letters = re.compile(r"^(?:[A-Za-z]\s+){3,}[A-Za-z]\.?$")
_re_single_digits  = re.compile(r"^(?:\d\s+){2,}\d$")

def despace_letters_digits(s: str) -> str:
    if not s:
        return s
    t = str(s).strip()
    t = re.sub(r"\s*\/\s*", "/", t)
    t = re.sub(r"\s*-\s*", "-", t)
    t = re.sub(r"\s*,\s*", ", ", t)
    if _re_single_letters.match(t) or _re_single_digits.match(t):
        return re.sub(r"\s+", "", t)
    toks = t.split()
    if toks and sum(1 for k in toks if len(k) == 1) >= max(4, int(0.6*len(toks))):
        return "".join(toks)
    return t

def canon(s: str) -> str:
    if s is None: return ""
    s = despace_letters_digits(str(s))
    return re.sub(r"[^a-z]", "", s.lower())

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = (
            df[c].astype(str)
                 .map(despace_letters_digits)
                 .map(lambda x: " ".join(x.split()))
        )
    return df

def drop_header_like_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    def is_header_row(row) -> bool:
        j = " ".join(canon(v) for v in row.tolist())
        return ("city" in j and "project" in j) or j in {"citybuildergroupprojectname"}
    return df[~df.apply(is_header_row, axis=1)].reset_index(drop=True)

# -------------------- tabula first (multi-page) --------------------

def try_tabula(pdf_path: Path) -> Optional[pd.DataFrame]:
    if not USE_TABULA:
        return None
    try:
        import tabula  # requires Java
    except Exception:
        return None
    try:
        # Try lattice then stream; capture ALL pages
        dfs = tabula.read_pdf(str(pdf_path), pages="all", lattice=True, pandas_options={"dtype": str})
        if not dfs:
            dfs = tabula.read_pdf(str(pdf_path), pages="all", stream=True, guess=True, pandas_options={"dtype": str})
        if not dfs:
            return None

        frames: List[pd.DataFrame] = []
        for t in dfs:
            if t is None or t.empty:
                continue

            # Map columns by header names (handle merged variants)
            colmap: Dict[str, str] = {}
            for col in t.columns:
                c = canon(col)
                if not c: continue
                if c == "city":
                    colmap[col] = "City"
                elif c in ("buildergroup","builder","builderdeveloper","buildername"):
                    colmap[col] = "Builder Group"
                elif c in ("projectname","project"):
                    colmap[col] = "Project Name"
                elif c == "citybuildergroup":
                    colmap[col] = "City|Builder Group"

            # Sometimes first row is header text, not DataFrame header
            if not colmap and len(t) > 0:
                first = [str(x) for x in t.iloc[0].tolist()]
                tmp = {}
                for i, v in enumerate(first):
                    c = canon(v)
                    if c == "city": tmp[i] = "City"
                    elif c in ("buildergroup","builder","builderdeveloper","buildername"): tmp[i] = "Builder Group"
                    elif c in ("projectname","project"): tmp[i] = "Project Name"
                    elif c == "citybuildergroup": tmp[i] = "City|Builder Group"
                if tmp:
                    t = t.iloc[1:].reset_index(drop=True)
                    t.columns = [tmp.get(i, str(old)) for i, old in enumerate(t.columns)]
                    # rebuild colmap from new columns
                    colmap = {}
                    for col in t.columns:
                        c = canon(col)
                        if c == "city": colmap[col] = "City"
                        elif c in ("buildergroup","builder","builderdeveloper","buildername"): colmap[col] = "Builder Group"
                        elif c in ("projectname","project"): colmap[col] = "Project Name"
                        elif c == "citybuildergroup": colmap[col] = "City|Builder Group"

            # Rename direct
            for old, new in list(colmap.items()):
                if new != "City|Builder Group" and old in t.columns:
                    t = t.rename(columns={old:new})

            # Split merged City|Builder column if present
            merged = [c for c, v in colmap.items() if v == "City|Builder Group" and c in t.columns]
            if merged:
                sr = t[merged[0]].astype(str).map(despace_letters_digits).map(lambda x: " ".join(x.split()))
                city, builder = [], []
                for v in sr.tolist():
                    parts = re.split(r"\s{2,}", v)
                    if len(parts) >= 2:
                        city.append(parts[0].strip())
                        builder.append(" ".join(parts[1:]).strip())
                    else:
                        m = re.match(r"^(.+?)([A-Z].+)$", v)
                        if m:
                            city.append(m.group(1).strip()); builder.append(m.group(2).strip())
                        else:
                            toks = v.split()
                            city.append(toks[0] if toks else "")
                            builder.append(" ".join(toks[1:]) if len(toks)>1 else "")
                t = pd.concat([t.drop(columns=merged), pd.DataFrame({"City":city,"Builder Group":builder})], axis=1)

            keep = [c for c in ["City","Builder Group","Project Name"] if c in t.columns]
            if not keep:
                continue
            t = t[keep].copy()
            # ensure all targets present
            for c in TARGET_COLS:
                if c not in t.columns:
                    t[c] = ""
            t = t[TARGET_COLS]
            t = normalize_df(t)
            t = drop_header_like_rows(t)
            t = t[~t.apply(lambda r: all(v == "" for v in r), axis=1)]
            if not t.empty:
                frames.append(t)

        if not frames:
            return None
        out = pd.concat(frames, ignore_index=True)
        out = out.drop_duplicates(subset=TARGET_COLS, keep="first").reset_index(drop=True)
        return out

    except Exception as e:
        print("ERROR:", str(e))
        print("ISSUE: tabula-py extraction failed.")
        print("FIXES: Ensure Java is installed/on PATH; try lattice/stream; update tabula-py.")
        print("CAUSE: Missing Java or table detection failure.")
        return None

# -------------------- pdfplumber (header persists across pages) --------------------

def try_pdfplumber(pdf_path: Path) -> Optional[pd.DataFrame]:
    try:
        import pdfplumber
    except Exception as e:
        print("ERROR:", str(e))
        print("ISSUE: pdfplumber not installed.")
        print("FIXES: pip install pdfplumber.")
        print("CAUSE: Missing dependency.")
        return None

    def tables_by_lines(page) -> List[List[List[Optional[str]]]]:
        settings = {
            "vertical_strategy":   "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 40,
            "intersection_tolerance": 5,
            "text_x_tolerance": 1,
            "text_y_tolerance": 2,
        }
        tbls = page.extract_tables(table_settings=settings) or []
        if tbls:
            return tbls
        settings2 = {
            "vertical_strategy":   "text",
            "horizontal_strategy": "text",
            "text_x_tolerance": 1,
            "text_y_tolerance": 2,
            "keep_blank_chars": False,
        }
        return page.extract_tables(table_settings=settings2) or []

    def map_header_indices(header_row: List[Optional[str]]) -> Dict[str, str]:
        idx: Dict[str, str] = {}
        for i, cell in enumerate(header_row):
            c = canon(cell or "")
            if not c: continue
            if c == "city": idx["City"] = i
            elif c in ("buildergroup","builder","builderdeveloper","buildername"): idx["Builder Group"] = i
            elif c in ("projectname","project"): idx["Project Name"] = i
            elif c == "citybuildergroup": idx["City|Builder Group"] = i
        return idx  # values are integer indices, keyed by logical name

    def split_merged(val: str) -> Tuple[str, str]:
        v = " ".join(despace_letters_digits(val or "").split())
        if not v: return "",""
        parts = re.split(r"\s{2,}", v)
        if len(parts) >= 2: return parts[0].strip(), " ".join(parts[1:]).strip()
        m = re.match(r"^(.+?)([A-Z].+)$", v)
        if m: return m.group(1).strip(), m.group(2).strip()
        toks = v.split()
        if len(toks)>=2: return toks[0], " ".join(toks[1:])
        return v, ""

    rows: List[Dict[str, str]] = []
    header_map_prev: Optional[Dict[str, int]] = None  # persist across pages/tables

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                tables = tables_by_lines(page)
                for tbl in tables:
                    if not tbl or all(not any(r) for r in tbl):
                        continue

                    # Find header row within first few rows; otherwise reuse previous header map
                    header_row_index = None
                    header_map_now: Optional[Dict[str, int]] = None
                    for i in range(min(3, len(tbl))):
                        m = map_header_indices(tbl[i] or [])
                        if m:
                            header_row_index = i
                            header_map_now = {k:int(v) for k,v in m.items()}
                            break

                    if header_map_now:
                        header_map_prev = header_map_now
                    if not header_map_prev:
                        # no header anywhere -> skip this table
                        continue

                    start_i = header_row_index + 1 if header_row_index is not None else 0

                    for r in tbl[start_i:]:
                        if not r: continue
                        city = builder = pname = ""
                        hm = header_map_prev

                        if "City|Builder Group" in hm and hm["City|Builder Group"] < len(r):
                            city, builder = split_merged(r[hm["City|Builder Group"]])
                        else:
                            if "City" in hm and hm["City"] < len(r):
                                city = r[hm["City"]] or ""
                            if "Builder Group" in hm and hm["Builder Group"] < len(r):
                                builder = r[hm["Builder Group"]] or ""

                        if "Project Name" in hm and hm["Project Name"] < len(r):
                            pname = r[hm["Project Name"]] or ""

                        rec = {
                            "City": despace_letters_digits(city),
                            "Builder Group": despace_letters_digits(builder),
                            "Project Name": despace_letters_digits(pname),
                        }
                        if any(rec.values()):
                            rows.append(rec)

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=TARGET_COLS)
        df = normalize_df(df)
        df = drop_header_like_rows(df)
        df = df[~df.apply(lambda r: all(v == "" for v in r), axis=1)]
        df = df.drop_duplicates(subset=TARGET_COLS, keep="first").reset_index(drop=True)
        return df if not df.empty else None

    except Exception as e:
        print("ERROR:", str(e))
        print("ISSUE: pdfplumber extraction failed.")
        print("FIXES: Adjust strategies (lines/text), tolerances; upgrade pdfplumber.")
        print("CAUSE: Table grid/text layout variance.")
        return None

# -------------------- delete helper --------------------

def remove_file_with_retries(path: Path, attempts: int = 5, base_delay: float = 0.3):
    if not path.exists():
        return
    last_err = None
    for i in range(attempts):
        try:
            path.unlink()
            return
        except Exception as e:
            last_err = e
            time.sleep(base_delay * (i + 1))  # backoff
    print("ERROR:", str(last_err))
    print("ISSUE: Could not remove the downloaded PDF.")
    print("FIXES: Close any program using the file; disable antivirus file lock; check permissions; retry.")
    print("CAUSE: File lock or permission issue.")

# -------------------- run --------------------

def main():
    try:
        download_pdf(PDF_URL, PDF_PATH)

        # Prefer tabula; fallback to pdfplumber
        df = try_tabula(PDF_PATH)
        if df is None or df.empty:
            df = try_pdfplumber(PDF_PATH)

        if df is None or df.empty:
            print("ERROR: No structured rows extracted.")
            print("ISSUE: Extraction returned empty or malformed data.")
            print("FIXES: Ensure Java for tabula; tweak pdfplumber tolerances; if scanned, use OCR (pytesseract) pipeline.")
            print("CAUSE: Table structure/encoding changed or PDF is image-only.")
            sys.exit(1)

        try:
            df = df.reindex(columns=TARGET_COLS)
            df.to_csv(CSV_PATH, index=False, encoding="utf-8")
            print(f"Wrote: {CSV_PATH} ({len(df)} rows)")
            # After CSV written, group and upload to S3
            try:
                load_dotenv()
                bucket = os.getenv('S3_BUCKET_NAME')
                key_prefix = os.getenv('S3_KEY')
                if not bucket:
                    raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
                if not key_prefix:
                    raise ValueError("S3_KEY is not set. Please set it in environment or .env")

                df2 = pd.read_csv(CSV_PATH, header=0)
                grouped = {}
                for _, r in df2.iterrows():
                    city = r.get("City","Unknown")
                    grouped.setdefault(city, []).append({
                        "builderName": r.get("Builder Group","Unknown"),
                        "projectName": r.get("Project Name","Unknown")
                    })

                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                s3_key = f"{key_prefix.rstrip('/')}/icici_hfc_data_{timestamp}.json"
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=json.dumps(grouped, indent=4).encode("utf-8"),
                    ContentType="application/json"
                )
                print(f"Data uploaded to S3: {s3_key}")
            except Exception as e:
                print("Error:", str(e))
        except Exception as e:
            print("ERROR:", str(e))
            print("ISSUE: CSV write failed.")
            print("FIXES: Check permissions/disk; ensure 'output' dir exists; close open file.")
            print("CAUSE: Filesystem permission or locked file.")
            sys.exit(1)
    finally:
        remove_file_with_retries(PDF_PATH)
        CSV_PATH.unlink()

if __name__ == "__main__":
    main()
