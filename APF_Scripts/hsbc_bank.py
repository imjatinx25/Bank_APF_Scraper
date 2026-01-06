# hsbc_projects_minimal.py
# pip install playwright
# python -m playwright install

from pathlib import Path
from urllib.parse import urljoin
import csv
from playwright.sync_api import sync_playwright, Playwright
import os
import json
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

BASE = "https://www.hsbc.co.in"
URL  = f"{BASE}/home-loans/list-of-projects/"
OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
CSV_PATH = OUT_DIR / "hsbc_apf_data.csv"
FIELDNAMES = ["city", "builder", "project"]

def tidy(s: str) -> str:
    return " ".join((s or "").split())

def accept_consent_if_any(page):
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button[aria-label*='Accept']",
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
    ]:
        btn = page.locator(sel)
        if btn.count() and btn.first.is_visible():
            btn.first.click()
            page.wait_for_timeout(200)
            break

def collect_cities(page):
    page.goto(URL, wait_until="domcontentloaded", timeout=40000)
    accept_consent_if_any(page)
    page.wait_for_selector("a.A-LNKC28L-RW-ALL", timeout=30000)
    anchors = page.locator("a.A-LNKC28L-RW-ALL:visible")
    cities = []
    for i in range(anchors.count()):
        a = anchors.nth(i)
        name = tidy(a.text_content())
        href = a.get_attribute("href")
        if name and href:
            cities.append({"name": name, "url": urljoin(BASE, href)})
    return cities

def find_table(page):
    # pick the first visible table with rows
    page.wait_for_selector("table:has(tr):visible", timeout=15000)
    return page.locator("table:has(tr):visible").first

def scrape_city_table(browser, city):
    p = browser.new_page()
    p.goto(city["url"], wait_until="domcontentloaded", timeout=40000)
    accept_consent_if_any(p)

    table = find_table(p)
    rows = table.locator("tr")
    out = []
    for i in range(rows.count()):
        cells = rows.nth(i).locator(":scope > th, :scope > td")
        if cells.count() < 2:
            continue

        # Header rows contain "Project name" etc → skip
        joined = " ".join((cells.nth(j).inner_text() or "").lower() for j in range(cells.count()))
        if "project" in joined and "builder" in joined:
            continue

        project = tidy(cells.nth(0).inner_text())
        builder = tidy(cells.nth(1).inner_text())
        if project and builder:
            out.append({"city": city["name"], "builder": builder, "project": project})
    p.close()
    return out

def append_rows_to_csv(rows, csv_path: Path, fieldnames):
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        w.writerows(rows)

def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        viewport={"width": 1366, "height": 768},
    )
    page = ctx.new_page()

    cities = collect_cities(page)
    print(f"[+] Cities found: {len(cities)}")

    for city in cities:
        print(f"[>] Scraping {city['name']}")
        try:
            rows = scrape_city_table(browser, city)
            print(f"    Rows: {len(rows)}")
            if rows:
                append_rows_to_csv(rows, CSV_PATH, FIELDNAMES)
                print(f"    [OK] Appended {len(rows)} rows to {CSV_PATH.name}")
            else:
                print("    [!] No rows found")
        except Exception as e:
            print(f"    [!] Failed on {city['name']}: {e}")

    browser.close()

if __name__ == "__main__":
    with sync_playwright() as p:
        run(p)
    # process and upload to S3
    try:
        load_dotenv()
        bucket = os.getenv('S3_BUCKET_NAME')
        key_prefix = os.getenv('S3_KEY')
        if not bucket:
            raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
        if not key_prefix:
            raise ValueError("S3_KEY is not set. Please set it in environment or .env")

        # read CSV and group by city
        import pandas as pd
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "hsbc_apf_data.csv"
        cols = ["city","builder","project"]
        df = pd.read_csv(CSV_PATH, names=cols, header=0)
        grouped = {}
        for _, r in df.iterrows():
            grouped.setdefault(r.get("city","Unknown"), []).append({
                "builderName": r.get("builder","Unknown"),
                "projectName": r.get("project","Unknown")
            })

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/hsbc_bank_data_{timestamp}.json"
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(grouped, indent=4).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"Data uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        CSV_PATH.unlink()
