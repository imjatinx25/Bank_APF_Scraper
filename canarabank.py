from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import time
import pandas as pd
import os
from datetime import datetime, timezone
from collections import defaultdict
import boto3
import json
from pathlib import Path
from dotenv import load_dotenv

def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def wait_for_table(driver, timeout=10):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.ID, "tbllogdata"))
    )
    time.sleep(2)

def is_valid_data_row(columns):
    return len(columns) >= 4 and all(col.text.strip() for col in columns[1:4])

def scrape_canara_apf():
    print(" Starting Canara Bank APF scraper...")
    url = "https://canarabank.com/housingprojects"
    driver = initialize_driver()
    driver.get(url)

    all_data_rows = []

    try:
        city_dropdown = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "CityName"))
        ))
        total_cities = len(city_dropdown.options)
        print(f"Found {total_cities} cities in dropdown")

        for index in range(1, total_cities):
            retry_count = 0
            max_retries = 5
            success = False

            while retry_count < max_retries and not success:
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "CityName")))
                    city_dropdown = Select(driver.find_element(By.ID, "CityName"))

                    city_option = city_dropdown.options[index]
                    city_name = city_option.text.strip()
                    print(f"\n Scraping city: {city_name}")

                    city_dropdown.select_by_index(index)

                    submit_btn = driver.find_element(By.ID, "BtnSubmit")
                    driver.execute_script("arguments[0].scrollIntoView(true);", submit_btn)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", submit_btn)

                    wait_for_table(driver)

                    table = driver.find_element(By.ID, "tbllogdata")
                    rows = table.find_elements(By.TAG_NAME, "tr")

                    city_data = []

                    for row in rows[1:]:  # skip header
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if is_valid_data_row(cols):
                            city_data.append({
                                "City": cols[1].text.strip(),
                                "Project Name": cols[2].text.strip(),
                                "Builder Name": cols[3].text.strip()
                            })

                    if city_data:
                        df = pd.DataFrame(city_data)
                        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
                        CSV_PATH = OUT_DIR / "canara_apf_data.csv"
                        write_header = not CSV_PATH.exists()
                        df.to_csv(CSV_PATH, index=False, mode='a', header=write_header)
                        all_data_rows.extend(city_data)

                    success = True
                except StaleElementReferenceException:
                    retry_count += 1
                    print(f" Retry {retry_count}/{max_retries} for {city_name}")
                    time.sleep(2)
                except Exception as e:
                    print(f" Error in {city_name}: {e}")
                    break

            if not success:
                print(f" Skipped city index {index} after retries.")

    finally:
        driver.quit()
        print(" Scraping completed.")

    return all_data_rows

def data_processing():
    try:
        columns = ['city', 'projectName', 'builderName']
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "canara_apf_data.csv"
        df = pd.read_csv(CSV_PATH, names=columns, header=0)

        # group by city
        grouped_data = defaultdict(list)
        for _, row in df.iterrows():
            city = row.get("city", "Unknown")
            grouped_data[city].append({
                "builderName": row.get("builderName", "Unknown"),
                "projectName": row.get("projectName", "Unknown")
            })
        
        # upload to s3
        # ensure .env variables are loaded
        load_dotenv()
        bucket = os.getenv('S3_BUCKET_NAME')
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key_prefix = os.getenv('S3_KEY')
        if not bucket:
            raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
        if not s3_key_prefix:
            raise ValueError("S3_KEY is not set. Please set it in environment or .env")
        s3_key = f"{s3_key_prefix.rstrip('/')}/canarabank_data_{timestamp}.json"
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

if __name__ == "__main__":
    print("Starting Canara Bank APF scraper...")
    scraped_data = scrape_canara_apf()

    # ask for confirmation
    # confirm = input("Do you want to upload the data to S3? (y/n): ")
    # if confirm.lower() == "y":
    print("Processing and uploading data to S3...")
    try:
        data_processing()
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Data processing and uploading to S3 failed.")
        print("Canara Bank APF scraper completed.")
        exit(1)
