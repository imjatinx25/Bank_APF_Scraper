from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
import time
import pandas as pd
import re
import os
import pandas as pd
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
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def wait_for_table(driver, timeout=15):
    """Wait for table to be present and contain data rows"""
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.ID, "gvApprovedList"))
    )
    # Wait for table to have actual data rows (not just header)
    WebDriverWait(driver, timeout).until(
        lambda d: len(d.find_elements(By.XPATH, "//table[@id='gvApprovedList']//tr")) > 1
    )

def is_valid_data_row(columns):
    if len(columns) < 4:
        return False
    first_col = columns[0].text.strip().upper()
    if re.fullmatch(r"[0-9. ]+", first_col):
        return False
    if first_col == '...' or first_col.isdigit():
        return False
    return not ("PROJECT" in first_col or first_col.replace(" ", "").isdigit())

def wait_for_table_refresh(driver, previous_content, timeout=15):
    """Wait for table content to change after city selection"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.find_element(By.ID, "gvApprovedList").get_attribute("innerHTML") != previous_content
        )
        # Additional wait for data to be fully loaded
        WebDriverWait(driver, 5).until(
            lambda d: len(d.find_elements(By.XPATH, "//table[@id='gvApprovedList']//tr")) > 1
        )
        return True
    except:
        return False

def get_actual_current_page(driver):
    try:
        span = driver.find_element(By.XPATH, "//table[@id='gvApprovedList']//tr[last()]//span")
        return int(span.text.strip())
    except:
        return None

def _pagination_links(driver):
    try:
        return driver.find_elements(By.XPATH, "//table[@id='gvApprovedList']//tr[last()]//a")
    except Exception:
        return []

def go_to_next_unvisited_page(driver, visited_pages, timeout=15):
    """Advance pagination robustly, including '...' windows. Returns True if moved."""
    current_page = get_actual_current_page(driver)

    def get_numbers():
        nums = []
        for a in _pagination_links(driver):
            t = (a.text or "").strip()
            if t.isdigit():
                try:
                    nums.append(int(t))
                except Exception:
                    pass
        return sorted(set(nums))

    # First try any visible number not visited
    numbers = get_numbers()
    candidates = [n for n in numbers if n not in visited_pages and n != current_page]
    # Prefer the smallest page greater than current; else any not visited
    greater = sorted([n for n in candidates if n > (current_page or 0)])
    target = (greater[0] if greater else (sorted(candidates)[0] if candidates else None))

    if target is not None:
        prev = current_page
        for a in _pagination_links(driver):
            if (a.text or "").strip() == str(target):
                a.click()
                try:
                    WebDriverWait(driver, timeout).until(lambda d: get_actual_current_page(d) != prev)
                except Exception:
                    pass
                return get_actual_current_page(driver) != prev

    # If no visible candidates, try last '...' to reveal next window
    links = _pagination_links(driver)
    prev_texts = [ (a.text or "").strip() for a in links ]
    dots = [a for a in links if (a.text or "").strip() == "..." ]
    if dots:
        try:
            dots[-1].click()
            WebDriverWait(driver, timeout).until(
                lambda d: [ (x.text or "").strip() for x in _pagination_links(d) ] != prev_texts
            )
        except Exception:
            pass
        # re-evaluate numbers after window shift
        numbers = get_numbers()
        candidates = [n for n in numbers if n not in visited_pages and n != current_page]
        greater = sorted([n for n in candidates if n > (current_page or 0)])
        target = (greater[0] if greater else (sorted(candidates)[0] if candidates else None))
        if target is not None:
            prev = current_page
            for a in _pagination_links(driver):
                if (a.text or "").strip() == str(target):
                    a.click()
                    try:
                        WebDriverWait(driver, timeout).until(lambda d: get_actual_current_page(d) != prev)
                    except Exception:
                        pass
                    return get_actual_current_page(driver) != prev

    return False

def scrape_axis_apf():
    print("[START] Running Axis Bank APF scraper...")
    url = "https://application.axisbank.co.in/webforms/ApprovedProjectList/homeloans_request.aspx"
    driver = initialize_driver()
    driver.get(url)
    data_rows = []

    try:
        # Ensure the city dropdown is present before locating it the first time
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "ddlCity")))
        city_dropdown = Select(driver.find_element(By.ID, "ddlCity"))
        total_cities = len(city_dropdown.options)
        print(f"[INFO] Found {total_cities} cities to process.")

        for index in range(1, total_cities):
            retry_count = 0
            max_retries = 5
            success = False

            while retry_count < max_retries and not success:
                try:
                    driver.get(url)
                    wait_for_table(driver)
                    city_dropdown = Select(driver.find_element(By.ID, "ddlCity"))

                    city_option = city_dropdown.options[index]
                    city_name = city_option.text.strip()
                    print(f"\n[CITY] ===> Processing city: {city_name}")

                    # Get current table content before selection
                    current_table = driver.find_element(By.ID, "gvApprovedList")
                    previous_content = current_table.get_attribute("innerHTML")
                    
                    city_dropdown.select_by_index(index)
                    
                    # Wait for table to refresh with new city data
                    if wait_for_table_refresh(driver, previous_content):
                        print(f"[INFO] Table refreshed for {city_name}")
                    else:
                        print(f"[WARNING] Table refresh timeout for {city_name}, proceeding anyway")
                        wait_for_table(driver)

                    visited_pages = set()
                    city_data_rows = []

                    while True:
                        actual_page = get_actual_current_page(driver)
                        print(f"[DEBUG] Detected actual current page: {actual_page}")
                        visited_pages.add(actual_page)

                        print(f"[DEBUG] Scraping Page {actual_page} for {city_name}")
                        table = driver.find_element(By.ID, "gvApprovedList")
                        rows = table.find_elements(By.TAG_NAME, "tr")

                        for i, row in enumerate(rows[1:], start=1):
                            cols = row.find_elements(By.TAG_NAME, "td")
                            if is_valid_data_row(cols):
                                data = {
                                    "City": city_name,
                                    "Project Code": cols[1].text.strip(),
                                    "Project Name": cols[2].text.strip(),
                                    "Builder Name": cols[3].text.strip()
                                }
                                print("[DATA]", data)
                                city_data_rows.append(data)
                            else:
                                print(f"[DEBUG] Skipped row {i + 1}: not a valid data row")

                        # Pagination advance (supports '...')
                        if go_to_next_unvisited_page(driver, visited_pages):
                            continue
                        print("[DEBUG] No more pages to visit.")
                        break

                    # Only append to CSV after full city scrape
                    if city_data_rows:
                        df = pd.DataFrame(city_data_rows)
                        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
                        CSV_PATH = OUT_DIR / "axis_apf_projects.csv"
                        write_header = not CSV_PATH.exists()
                        df.to_csv(CSV_PATH, index=False, mode='a', header=write_header)
                        data_rows.extend(city_data_rows)

                    success = True
                except StaleElementReferenceException:
                    retry_count += 1
                    print(f"[WARN] Stale element at city index {index}. Retrying... ({retry_count})")
                    time.sleep(2)
                except Exception as e:
                    print(f"[ERROR] Failed for city index {index}: {str(e)}")
                    break

            if not success:
                print(f"[FAIL] City index {index} failed after 3 retries.")
    except Exception as e:
        print(f"[Error] {str(e)}")
    finally:
        driver.quit()

    return data_rows

def data_processing():
    try:
        OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
        CSV_PATH = OUT_DIR / "axis_apf_projects.csv"
        columns = ['city', 'projectCode', 'projectName', 'builderName']
        df = pd.read_csv(CSV_PATH, names=columns, header=None)

        # drop project code column
        df = df.drop(columns=['projectCode'])

        # group by city
        grouped_data = defaultdict(list)
        for _, row in df.iterrows():
            city = row.get("city", "Unknown")
            grouped_data[city].append({
                "builderName": row.get("builderName", "Unknown"),
                "projectName": row.get("projectName", "Unknown")
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
        s3_key = f"{key_prefix.rstrip('/')}/axisbank_data_{timestamp}.json"
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
    print("Starting Axis Bank APF scraper...")
    scraped_data = scrape_axis_apf()
    print("Processing data...")
    data_processing()
    print("Data processing completed.")
    print("Axis Bank APF scraper completed.")


