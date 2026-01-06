from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
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
    options.add_argument("--headless=new") # Keeping it non-headless for now as per user's preference or debugging
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    
    # Extra stealth: remove the webdriver property
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })
    
    return driver


def scrape_yesbank_apf():
    # Using the direct URL to avoid unnecessary redirects
    url = "https://www.yesbank.in/approved-projects"
    driver = initialize_driver()
    
    all_data_rows = []
    
    try:
        driver.get(url)
        
        # Robust wait for the main content to load
        wait = WebDriverWait(driver, 20)
        try:
            wait.until(EC.presence_of_element_located((By.ID, "state_drop")))
        except Exception:
            print("Initial load failed or timed out. Refreshing...")
            driver.refresh()
            wait.until(EC.presence_of_element_located((By.ID, "state_drop")))

        time.sleep(3)  # Extra buffer for dynamic JS
        
        state_dropdown = driver.find_element(By.ID, "state_drop")
        state_select = Select(state_dropdown)
        
        # Ensure options are populated
        WebDriverWait(driver, 10).until(lambda d: len(Select(d.find_element(By.ID, "state_drop")).options) > 1)
        
        # Refresh state dropdown reference after wait
        state_dropdown = driver.find_element(By.ID, "state_drop")
        state_select = Select(state_dropdown)
        states = state_select.options[1:]  # Skip "Select State"

        # Get state names first to avoid stale elements
        state_names = [opt.text.strip() for opt in states if opt.text.strip()]

        for state in state_names:
            print(f"\nState: {state}")
            
            # Re-find state dropdown to avoid stale element
            state_dropdown = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "state_drop")))
            state_select = Select(state_dropdown)
            state_select.select_by_visible_text(state)
            time.sleep(2)

            # Wait for city dropdown to populate
            WebDriverWait(driver, 15).until(lambda d: len(Select(d.find_element(By.ID, "city_drop")).options) > 1)
            
            city_dropdown = driver.find_element(By.ID, "city_drop")
            city_select = Select(city_dropdown)
            city_names = [opt.text.strip() for opt in city_select.options if "Choose" not in opt.text and opt.text.strip().lower() != "select"]

            for city in city_names:
                print(f"  City: {city}")
                
                try:
                    # Re-find city dropdown
                    city_dropdown = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "city_drop")))
                    city_select = Select(city_dropdown)
                    city_select.select_by_visible_text(city)
                    time.sleep(1)

                    # Click Submit
                    submit_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "approvebtn")))
                    driver.execute_script("arguments[0].click();", submit_btn)

                    # Wait for the result table or 'No results' indicator
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.ID, "projectDataTable"))
                        )
                        rows = driver.find_elements(By.XPATH, "//table[@id='projectDataTable']//tr")[1:]  # Skip header

                        city_data = []
                        for row in rows:
                            cols = row.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 3:
                                builder = cols[0].text.strip()
                                project = cols[1].text.strip()
                                
                                # Skip placeholder rows if any
                                if builder.lower() == "select" or project.lower() == "select":
                                    continue
                                    
                                city_data.append({
                                    "City": city,
                                    "Builder Name": builder,
                                    "Project Name": project,
                                })

                        if city_data:
                            df = pd.DataFrame(city_data)
                            OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
                            CSV_PATH = OUT_DIR / "yesbank_apf_data.csv"
                            write_header = not CSV_PATH.exists()
                            df.to_csv(CSV_PATH, index=False, mode='a', header=write_header)
                            all_data_rows.extend(city_data)
                            print(f"    Saved {len(city_data)} projects.")

                    except Exception as table_err:
                        print(f"    No table found for city: {city}")
                
                except Exception as city_err:
                    print(f"    Error processing city {city}: {city_err}")
                
                time.sleep(1)
    except Exception as e:
        print(f"Error in scrape_yesbank_apf: {str(e)}")
    finally:
        driver.quit()

    return all_data_rows

def data_processing():
    try:
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "yesbank_apf_data.csv"

        if not CSV_PATH.exists():
            print(f"CSV file not found: {CSV_PATH}")
            return

        columns = ['City', 'Builder Name', 'Project Name']
        df = pd.read_csv(CSV_PATH)

        grouped_data = defaultdict(list)
        for _, row in df.iterrows():
            city = row.get("City", "Unknown")
            grouped_data[city].append({
                "builderName": row.get("Builder Name", "Unknown"),
                "projectName": row.get("Project Name", "Unknown"),
            })

        load_dotenv()
        bucket = os.getenv('S3_BUCKET_NAME')
        key_prefix = os.getenv('S3_KEY')
        if not bucket:
            raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
        if not key_prefix:
            raise ValueError("S3_KEY is not set. Please set it in environment or .env")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/yesbank_data_{timestamp}.json"
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(grouped_data, indent=4).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"Data uploaded to S3: {s3_key}")
        
        CSV_PATH.unlink()
        print(f"Temporary CSV file {CSV_PATH} deleted.")
        
    except Exception as e:
        print(f"Error in data_processing: {str(e)}")

if __name__ == "__main__":
    print("Starting Yes Bank APF scraper...")
    scraped_data = scrape_yesbank_apf()

    print("Processing and uploading data to S3...")
    data_processing()
    print("Data processing and uploading to S3 completed.")
    print("Yes Bank APF scraper completed.")
