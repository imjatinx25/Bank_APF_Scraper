from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import os
import json
import boto3
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict


def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def scrape_pnsbank_apf():
    url = "https://punjabandsind.bank.in/Housing/index.php"
    driver = initialize_driver()
    driver.get(url)

    all_data_rows = []

    try:
        # Get all city names before the loop
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "city")))
        city_select = Select(driver.find_element(By.NAME, "city"))
        
        cities = [opt.text.strip() for opt in city_select.options if "select" not in opt.text.lower()]
        print(f"Found {len(cities)} cities in dropdown")

        for city in cities:
            print(f"City: {city}")

            # Re-select dropdown fresh after reload
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "city")))
            city_select = Select(driver.find_element(By.NAME, "city"))
            city_select.select_by_visible_text(city)

            time.sleep(1)

            try:
                # wait until submit button exists in DOM
                submit_btn = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "submit")))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", submit_btn)
            except Exception as e:
                print("Click intercepted, using JavaScript executor instead.", e)
                driver.execute_script("arguments[0].click();", submit_btn)

            # wait until result table appears (new page loaded)
            table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "housingTable")))
            rows = table.find_elements(By.XPATH, ".//tbody/tr")

            # Wait for table // removed code because header is different now
            # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "housingTable")))
            # rows = driver.find_elements(By.XPATH, '//*[@id="housingTable"]/tbody/tr')[1:]  # skip header

            city_data = []
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 6:
                    city_data.append({
                        "City": city,
                        "Project Name": cols[2].text.strip(),
                        "Builder Name": cols[3].text.strip(),
                    })

            if city_data:
                df = pd.DataFrame(city_data)

                OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
                CSV_PATH = OUT_DIR / "pnsbank_apf_data.csv"
                write_header = not CSV_PATH.exists()
                df.to_csv(CSV_PATH, index=False, mode='a', header=write_header)

                all_data_rows.extend(city_data)

            # Navigate back to start
            driver.get(url)

    finally:
        driver.quit()
    
    return all_data_rows

def data_processing():
    try:
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "pnsbank_apf_data.csv"
        
        if not CSV_PATH.exists():
            print(f"CSV file not found: {CSV_PATH}")
            return

        columns = ['City', 'Project Name', 'Builder Name']
        df = pd.read_csv(CSV_PATH)

        # group by city
        grouped_data = defaultdict(list)
        for _, row in df.iterrows():
            city = row.get("City", "Unknown")
            grouped_data[city].append({
                "builderName": row.get("Builder Name", "Unknown"),
                "projectName": row.get("Project Name", "Unknown")
            })
        
        # upload to s3 using .env
        load_dotenv()
        bucket = os.getenv('S3_BUCKET_NAME')
        key_prefix = os.getenv('S3_KEY')
        if not bucket:
            raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
        if not key_prefix:
            raise ValueError("S3_KEY is not set. Please set it in environment or .env")
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/pnsbank_data_{timestamp}.json"
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(grouped_data, indent=4).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"Data uploaded to S3: {s3_key}")
        
        # Optional: delete CSV after upload
        CSV_PATH.unlink()
        print(f"Temporary CSV file {CSV_PATH} deleted.")
        
    except Exception as e:
        print(f"Error in data processing: {str(e)}")

if __name__ == "__main__":
    print("Starting PNS Bank APF scraper...")
    scraped_data = scrape_pnsbank_apf()
    print("Processing data...")
    data_processing()
    print("PNS Bank APF scraper completed.")
