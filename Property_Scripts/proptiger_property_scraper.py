import csv
import time
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import re
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv


# Use root output folder (same as app.py), not Property_Scripts/output
BASE_DIR = Path(__file__).parent.parent  # Go up to root directory
OUT_DIR = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

CSV_FILENAME = str(OUT_DIR / "proptiger_properties.csv")
CSV_FIELDNAMES = [
    'project_name',
    'city',
    'location',
    'price',
    'property_type',
    'area',
    'possession_status',
    'builder',
    'rera_id',
    'property_url',
    'source',
    'scraped_at'
]

CARD_SELECTOR = ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']"

# In-memory dedupe across the whole run (helps prevent duplicates on retries)
SEEN_KEYS = set()


def upload_csv_to_s3():
    """Upload Proptiger CSV file to S3 (best-effort)."""
    try:
        csv_path = CSV_FILENAME

        if not os.path.exists(csv_path):
            print(f"[WARNING] CSV file not found: {csv_path}, skipping S3 upload")
            return
        if os.path.getsize(csv_path) == 0:
            print("[WARNING] CSV file is empty, skipping S3 upload")
            return

        load_dotenv()
        bucket = os.getenv("S3_BUCKET_NAME")
        if not bucket:
            print("[ERROR] S3_BUCKET_NAME not set, skipping S3 upload")
            return

        key_prefix = os.getenv("S3_KEY")

        with open(csv_path, "rb") as f:
            csv_content = f.read()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/proptiger_properties_{timestamp}.csv"

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content,
            ContentType="text/csv",
        )
        print(f"[OK] CSV uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"[ERROR] Error uploading CSV to S3: {str(e)}")


def _make_property_key(row: dict) -> str:
    """Create a stable-ish key to dedupe rows across retries/runs."""
    url = (row.get("property_url") or "").strip()
    if url:
        return f"url::{url.lower()}"
    name = (row.get("project_name") or "").strip().lower()
    loc = (row.get("location") or "").strip().lower()
    city = (row.get("city") or "").strip().lower()
    return f"nlc::{name}::{loc}::{city}"


def load_seen_keys_from_csv(filename: str = CSV_FILENAME):
    """Load existing keys from CSV to avoid duplicates when appending."""
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return
    try:
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                SEEN_KEYS.add(_make_property_key(row))
    except Exception:
        # If CSV is malformed, don't block scraping
        return


def ensure_csv_header(filename: str = CSV_FILENAME):
    """Ensure CSV exists and has header."""
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        return
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

# List of cities to search (type directly in search bar)
cities = [
    # Popular Cities
    "Delhi", "Noida", "Gurgaon", "Mumbai", "Bangalore", "Hyderabad",
    "Chennai", "Pune", "Kolkata", "Ahmedabad", "Chandigarh",
    "Lucknow", "Jaipur",

    # Other Cities
    "Agra", "Alapuzzha", "Allahabad", "Ambala", "Amritsar", "Anand",
    "Aurangabad", "Belgaum", "Bharuch", "Bhopal", "Bhubaneswar",
    "Calicut", "Coimbatore", "Daman / Diu", "Darjeeling", "Dehradun",
    "Dharwad", "Durgapur", "Goa", "Guntur", "Guwahati", "Gwalior",
    "Haridwar", "Hubli", "Indore", "Jabalpur", "Jalandar", "Kanpur",
    "Karnal", "Kochi", "Kolhapur", "Kota", "Ludhiana", "Madurai",
    "Mangalore", "Mathura", "Meerut", "Mysore", "Nadiad", "Nagpur",
    "Nasik", "Pallakad", "Panipat", "Patiala", "Patna", "Pondicherry",
    "Raipur", "Rajkot", "Ranchi", "Ratnagiri", "Rohtak", "Salem",
    "Shimla", "Siliguri", "Solan", "Sonipat", "Surat", "Trichy",
    "Trivandrum", "Thrissur", "Udaipur", "Vadodara", "Valsad", "Vapi",
    "Varanasi", "Vijayawada", "Visakhapatnam"
]

# Initialize Chrome driver with anti-detection settings
chrome_options = Options()
chrome_options.add_argument('--headless')  # Uncomment to run in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--disable-web-security')
chrome_options.add_argument('--disable-features=IsolateOrigins,site-per-process')
chrome_options.add_argument('--start-maximized')
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Block all permission prompts (location, notifications, etc.)
prefs = {
    "profile.default_content_setting_values.notifications": 2,  # Block notifications
    "profile.default_content_setting_values.geolocation": 2,  # Block location
    "profile.default_content_setting_values.media_stream": 2,  # Block media access
    "credentials_enable_service": False,
    "profile.password_manager_enabled": False
}
chrome_options.add_experimental_option("prefs", prefs)

driver = None


def start_new_browser():
    """Start a fresh Chrome session (used once per city / retry)."""
    global driver

    # Always close any previous driver first (safety)
    stop_browser()

    print("Starting Chrome browser...")
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()

    # Set longer timeouts to handle large pages
    driver.set_page_load_timeout(90)  # 90 seconds for page load
    driver.set_script_timeout(60)  # 60 seconds for script execution

    # Hide webdriver property to avoid detection
    try:
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            '''
        })
        print("[OK] Anti-detection measures applied")
    except Exception as e:
        print(f"[WARN] Could not apply some anti-detection measures: {e}")

    return driver


def stop_browser():
    """Stop current browser session safely."""
    global driver
    try:
        if driver:
            driver.quit()
    except Exception:
        pass
    finally:
        driver = None


def close_popup_if_exists():
    """Close any popup/overlay that might appear including enquiry form"""
    try:
        if not driver:
            return
        # Store current URL to detect if closing popup causes navigation
        current_url = driver.current_url
        
        # Check if popup actually exists before trying to close it
        popup_exists = False
        try:
            popup_container = driver.find_element(By.ID, "popup-container")
            if popup_container.is_displayed():
                popup_exists = True
        except:
            pass
        
        if not popup_exists:
            try:
                modal = driver.find_element(By.CSS_SELECTOR, ".modalboxv2.showPopup, .js-modalboxv2.showPopup")
                if modal.is_displayed():
                    popup_exists = True
            except:
                pass
        
        if not popup_exists:
            # No popup found, return silently
            return
        
        print("  [INFO] Popup detected, attempting to close...")
        
        # Method 1: Try to hide the popup directly via JavaScript (safest, no events triggered)
        try:
            result = driver.execute_script("""
                var closed = false;
                var popupContainer = document.getElementById('popup-container');
                if (popupContainer && popupContainer.offsetParent !== null) {
                    popupContainer.style.display = 'none';
                    popupContainer.classList.remove('showPopup');
                    closed = true;
                }
                var overlay = document.querySelector('.popup-overlay');
                if (overlay && overlay.offsetParent !== null) {
                    overlay.style.display = 'none';
                    closed = true;
                }
                var modal = document.querySelector('.modalboxv2.showPopup, .js-modalboxv2.showPopup');
                if (modal) {
                    modal.style.display = 'none';
                    modal.classList.remove('showPopup');
                    closed = true;
                }
                return closed;
            """)
            if result:
                print("  [OK] Closed popup via JavaScript")
                time.sleep(0.3)
                # Check if page reloaded
                new_url = driver.current_url
                if new_url != current_url:
                    print(f"  [WARN] Page navigated after closing popup: {current_url} -> {new_url}")
                return
        except Exception as e:
            print(f"  [WARN] JavaScript close failed: {e}")
        
        # Method 2: Try ESC key (might trigger events)
        try:
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.3)
            print("  [OK] Sent ESC key")
            return
        except:
            pass
        
        # Method 3: Try clicking close button (last resort)
        try:
            close_btn = driver.find_element(By.CSS_SELECTOR, "div[js-lead-close], .close-cross")
            if close_btn.is_displayed():
                driver.execute_script("arguments[0].click();", close_btn)
                print("  [OK] Clicked close button")
                time.sleep(0.3)
                return
        except:
            pass
        
    except Exception as e:
        # Silently ignore errors
        pass


def search_city(city_name, retry_attempt=0):
    """Search for properties in a specific city"""
    try:
        retry_msg = f" (Attempt {retry_attempt + 1})" if retry_attempt > 0 else ""
        print(f"\n{'='*60}")
        print(f"Searching for properties in: {city_name}{retry_msg}")
        print(f"{'='*60}")
        
        # Go to homepage
        try:
            driver.get("https://www.proptiger.com/")
            print("  [OK] Loading homepage...")
            
            # Wait longer for JavaScript to load
            time.sleep(5)
            
            # Check if page is loaded by checking for body content
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                if not body_text or len(body_text) < 100:
                    print(f"  [WARN] Page seems blank (body text length: {len(body_text)})")
                    print("  [INFO] Waiting additional time for JavaScript to load...")
                    time.sleep(5)
            except:
                pass
            
            # Check page title
            page_title = driver.title
            print(f"  Page title: {page_title}")
            
            # Check if we can see any content
            try:
                page_source_length = len(driver.page_source)
                print(f"  Page source length: {page_source_length} characters")
                if page_source_length < 1000:
                    print("  [WARN] Page source is very small, might be blank")
            except:
                pass
                
        except Exception as e:
            print(f"  [ERROR] Error loading homepage: {e}")
            return False
        
        # Wait for page to be fully loaded - check for the search wrapper FIRST (before closing popups)
        print("  Waiting for search bar to load...")
        try:
            search_wrapper = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "js-header-desktop-search-wrapper"))
            )
            print("  [OK] Search wrapper found")
        except TimeoutException:
            print("  [WARN] Search wrapper not found, page may not have loaded")
            # Try to continue anyway
        
        # Now close any popups AFTER confirming the page loaded
        print("  [INFO] Checking for popups...")
        close_popup_if_exists()
        time.sleep(1)
        
        # Verify page is still loaded after closing popup
        try:
            # Check if search wrapper is still there
            search_wrapper_check = driver.find_element(By.ID, "js-header-desktop-search-wrapper")
            if not search_wrapper_check.is_displayed():
                print("  [WARN] Page went blank after closing popup, reloading...")
                driver.refresh()
                time.sleep(5)
                # Wait for search wrapper again
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, "js-header-desktop-search-wrapper"))
                )
                print("  [OK] Page reloaded successfully")
        except:
            print("  [WARN] Page verification failed, continuing anyway...")
        
        # Find the main search input field and type the city name
        try:
            # Wait for the search input field to be VISIBLE
            print("  [INFO] Looking for search input field...")
            search_input = WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".js-search-input, .search-input"))
            )
            
            # Scroll to element to make sure it's in view
            driver.execute_script("arguments[0].scrollIntoView(true);", search_input)
            time.sleep(0.5)
            
            # Click on the input field to focus it
            driver.execute_script("arguments[0].click();", search_input)
            time.sleep(0.3)
            
            # Clear any existing text and type the city name
            search_input.clear()
            search_input.send_keys(city_name)
            print(f"  [OK] Typed city name: {city_name}")
            time.sleep(2)  # Wait for any autocomplete suggestions to appear
            
        except TimeoutException:
            print("  [ERROR] Search input field not found - page may not have loaded properly")
            print(f"  Current URL: {driver.current_url}")
            print("  [WARN] Trying fallback: direct URL navigation...")
            
            # Fallback: Navigate directly to search results
            city_slug = city_name.lower().replace(" ", "-")
            search_url = f"https://www.proptiger.com/property-for-sale-in-{city_slug}"
            
            try:
                driver.get(search_url)
                time.sleep(5)
                print(f"  [OK] Navigated directly to: {search_url}")
                close_popup_if_exists()
                return True
            except Exception as e:
                print(f"  [ERROR] Fallback also failed: {e}")
                return False
        except Exception as e:
            print(f"  [WARN] Could not type in search field: {e}")
            return False
        
        # Now click the search button
        try:
            # Find the search button wrapper
            search_button_wrapper = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".js-search-button"))
            )
            
            # Click the actual search button inside
            search_button = search_button_wrapper.find_element(By.CSS_SELECTOR, ".srch-btn")
            
            print("  [OK] Clicking search button...")
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)  # Wait for navigation
            
        except Exception as e:
            print(f"  [WARN] Could not click search button: {e}")
            return False
        
        # Wait for results page to load
        print("  [OK] Waiting for results page to load...")
        time.sleep(3)
        
        # Close any popups that appear on results page
        close_popup_if_exists()
        time.sleep(1)
        
        # Close again to make sure
        close_popup_if_exists()
        time.sleep(1)
        
        # Verify we're on results page by checking URL
        current_url = driver.current_url
        print(f"  [OK] Current URL: {current_url}")
        
        # Check if we actually navigated away from homepage
        if current_url == "https://www.proptiger.com/" or "#homeloanform" in current_url:
            print("  [WARN] Still on homepage or home loan form, search may not have worked")
            # Try to close popup and check again
            close_popup_if_exists()
            time.sleep(2)
            current_url = driver.current_url
            print(f"  [OK] URL after closing popup: {current_url}")
            
            # If still on homepage, return False to trigger retry
            if current_url == "https://www.proptiger.com/" or "#homeloanform" in current_url:
                print("  [WARN] Still on homepage after closing popup - search failed")
                return False
        
        print("  [OK] Search completed, ready to scrape")
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error searching for city {city_name}: {e}")
        return False


def extract_property_card(card):
    """Extract data from a property card based on cards.html structure"""
    property_data = {}
    
    try:
        # Extract project/property name (span with itemprop="name")
        try:
            name_elem = card.find_element(By.CSS_SELECTOR, "[itemprop='name']")
            property_data['project_name'] = name_elem.text.strip()
        except:
            # Fallback to .proj-name or .projectLink
            try:
                name_elem = card.find_element(By.CSS_SELECTOR, ".proj-name, .projectLink")
                property_data['project_name'] = name_elem.text.strip()
            except:
                property_data['project_name'] = ""
        
        # Extract location (span with itemprop="address")
        try:
            location_elem = card.find_element(By.CSS_SELECTOR, "[itemprop='address']")
            property_data['location'] = location_elem.get_attribute('title') or location_elem.text.strip()
        except:
            # Fallback to .loc class
            try:
                location_elem = card.find_element(By.CSS_SELECTOR, ".loc")
                property_data['location'] = location_elem.text.strip()
            except:
                property_data['location'] = ""
        
        # Extract builder/developer name (.projectBuilder)
        try:
            builder_elem = card.find_element(By.CSS_SELECTOR, ".projectBuilder")
            property_data['builder'] = builder_elem.text.strip()
        except:
            property_data['builder'] = ""
        
        # Extract possession status (.possession-wrap)
        try:
            possession_elem = card.find_element(By.CSS_SELECTOR, ".possession-wrap")
            property_data['possession_status'] = possession_elem.text.strip()
        except:
            property_data['possession_status'] = ""
        
        # Extract property configurations and prices from table
        try:
            # Get all property rows that are not hidden
            property_rows = card.find_elements(By.CSS_SELECTOR, ".showProperties")
            configs = []
            sizes = []
            prices = []
            
            for row in property_rows:
                try:
                    # Extract configuration (2BHK, 3BHK, etc.)
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 3:
                        config = cells[0].text.strip()
                        size = cells[1].text.strip()
                        price = cells[2].text.strip()
                        
                        if config:
                            configs.append(config)
                        if size:
                            sizes.append(size)
                        if price:
                            prices.append(price)
                except:
                    continue
            
            property_data['property_type'] = ", ".join(configs) if configs else ""
            property_data['area'] = ", ".join(sizes) if sizes else ""
            property_data['price'] = ", ".join(prices) if prices else ""
        except:
            property_data['property_type'] = ""
            property_data['area'] = ""
            property_data['price'] = ""
        
        # Extract property URL (a with class="projectLink")
        try:
            link_elem = card.find_element(By.CSS_SELECTOR, ".projectLink, a[data-type='cluster-project-link']")
            href = link_elem.get_attribute('href')
            if href:
                # Make sure URL is absolute
                if href.startswith('http'):
                    property_data['property_url'] = href
                else:
                    property_data['property_url'] = f"https://www.proptiger.com{href}"
            else:
                property_data['property_url'] = ""
        except:
            property_data['property_url'] = ""
        
        # Extract RERA ID
        try:
            rera_elem = card.find_element(By.CSS_SELECTOR, ".rera-id")
            property_data['rera_id'] = rera_elem.text.strip().replace("RERA ID: ", "")
        except:
            property_data['rera_id'] = ""
        
        # Extract city from the address or from data
        try:
            # Try to get from proj-address
            address_elem = card.find_element(By.CSS_SELECTOR, ".proj-address")
            address_text = address_elem.text.strip()
            # City is usually after the comma
            if "," in address_text:
                parts = address_text.split(",")
                property_data['city_display'] = parts[-1].strip()
            else:
                property_data['city_display'] = ""
        except:
            property_data['city_display'] = ""
        
    except Exception as e:
        print(f"    [WARN] Error extracting card data: {e}")
    
    return property_data


def scroll_and_load_all_cards():
    """Scroll through page to load all cards via infinite scroll (similar to magicbricks)"""
    print(f"  Scrolling to load all cards...")
    
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
        last_card_count = len(driver.find_elements(By.CSS_SELECTOR, ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']"))
        no_change_count = 0
        max_no_change = 5  # Stop after 5 consecutive scrolls with no new content
        iteration = 0
        
        while no_change_count < max_no_change:
            iteration += 1
            
            # Get current cards to scroll to the last one (avoid hitting footer)
            current_cards = driver.find_elements(By.CSS_SELECTOR, ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']")
            
            if current_cards and len(current_cards) > 0:
                # Scroll to the last visible card (not all the way to bottom)
                # This keeps the lazy loading triggered without hitting the footer
                last_card = current_cards[-1]
                try:
                    # Scroll to bring the last card into view, with some offset to trigger loading
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", last_card)
                    time.sleep(2)
                    
                    # Scroll down a bit more to trigger lazy loading (but not to footer)
                    driver.execute_script("window.scrollBy({top: 500, behavior: 'smooth'});")
                except:
                    # Fallback to regular scroll if scrollIntoView fails
                    driver.execute_script("window.scrollTo({top: document.body.scrollHeight - 1000, behavior: 'smooth'});")
            else:
                # If no cards found yet, scroll normally but not all the way to bottom
                driver.execute_script("window.scrollTo({top: document.body.scrollHeight - 1000, behavior: 'smooth'});")
            
            time.sleep(2.5)
            
            # Close any enquiry popups that might appear during scrolling
            close_popup_if_exists()
            
            # Wait longer for new content to load (especially for lazy loading)
            time.sleep(2.5)
            
            # Check if new content loaded
            new_height = driver.execute_script("return document.body.scrollHeight")
            current_cards = driver.find_elements(By.CSS_SELECTOR, ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']")
            current_card_count = len(current_cards)
            
            # Check if both height and card count changed
            height_changed = new_height != last_height
            cards_changed = current_card_count != last_card_count
            
            if height_changed or cards_changed:
                no_change_count = 0
                last_height = new_height
                last_card_count = current_card_count
                print(f"    Loaded {current_card_count} cards so far... (iteration {iteration})")
            else:
                no_change_count += 1
                if no_change_count < max_no_change:
                    print(f"    No new content ({no_change_count}/{max_no_change}), waiting longer...")
                    # Wait even longer when no new content detected to give lazy loading more time
                    time.sleep(3)
        
        print(f"  [OK] Completed scrolling after {iteration} iterations")
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Close any enquiry popups
        close_popup_if_exists()
        
        # Get final cards
        final_cards = driver.find_elements(By.CSS_SELECTOR, ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']")
        print(f"  [OK] Total cards loaded: {len(final_cards)}")
        return final_cards
        
    except Exception as e:
        print(f"  [ERROR] Error during scrolling: {e}")
        return []


def scroll_extract_and_save_cards(city_name: str, flush_every: int = 25, fsync_every: int = 200):
    """
    Real-time scraping: scroll, extract newly loaded cards, append to CSV continuously.
    This avoids data loss if the script/network/browser dies mid-run.
    """
    ensure_csv_header(CSV_FILENAME)

    saved = 0
    extracted = 0
    failed = 0

    processed_count = 0
    no_change_count = 0
    max_no_change = 5
    iteration = 0

    print("  Streaming mode: extracting + saving while scrolling...")

    # Open CSV once per city for efficiency
    with open(CSV_FILENAME, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)

        while no_change_count < max_no_change:
            iteration += 1

            # Grab current cards
            cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)

            # If new cards appeared, process only the new tail
            if len(cards) > processed_count:
                new_cards = cards[processed_count:]
                processed_count = len(cards)
                no_change_count = 0

                for card in new_cards:
                    try:
                        property_data = extract_property_card(card)
                        if property_data.get('project_name') or property_data.get('location'):
                            property_data['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            property_data['source'] = "proptiger"
                            property_data['city'] = city_name.lower()

                            key = _make_property_key(property_data)
                            if key in SEEN_KEYS:
                                continue
                            SEEN_KEYS.add(key)

                            # Write immediately
                            row = {k: property_data.get(k, '') for k in CSV_FIELDNAMES}
                            writer.writerow(row)
                            saved += 1
                            extracted += 1

                            # Flush periodically so data is actually on disk
                            if saved % flush_every == 0:
                                f.flush()
                            if saved % fsync_every == 0:
                                try:
                                    os.fsync(f.fileno())
                                except Exception:
                                    pass
                        else:
                            failed += 1
                    except Exception:
                        failed += 1
                        continue

                if extracted % 200 == 0 or extracted < 50:
                    print(f"    Saved {saved} rows so far... (cards loaded: {processed_count}, iteration {iteration})")
            else:
                no_change_count += 1
                print(f"    No new cards ({no_change_count}/{max_no_change}), continuing scroll...")

            # Close any popups that might block scrolling
            close_popup_if_exists()

            # Scroll strategy: avoid footer by scrolling to last card
            try:
                cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
                if cards:
                    last_card = cards[-1]
                    driver.execute_script(
                        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                        last_card
                    )
                    time.sleep(1.8)
                    driver.execute_script("window.scrollBy({top: 500, behavior: 'smooth'});")
                else:
                    driver.execute_script("window.scrollTo({top: document.body.scrollHeight - 1000, behavior: 'smooth'});")
                time.sleep(2.0)
            except Exception:
                # Best-effort scrolling fallback
                try:
                    driver.execute_script("window.scrollTo({top: document.body.scrollHeight - 1000, behavior: 'smooth'});")
                    time.sleep(2.0)
                except Exception:
                    pass

        # Final flush/fsync at end of city
        try:
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            pass

    print(f"  [OK] Streaming complete: saved={saved}, failed={failed}, cards_seen={processed_count}, iterations={iteration}")
    return saved


def extract_property_cards():
    """Extract all property cards from the current page"""
    properties = []
    
    try:
        # Wait for property cards to load
        time.sleep(3)
        
        # Close any enquiry popup that appears
        close_popup_if_exists()
        
        # Scroll to load all cards (infinite scroll)
        all_cards = scroll_and_load_all_cards()
        
        if not all_cards:
            print("  [WARN] No property cards found")
            return []
        
        print(f"  Extracting data from {len(all_cards)} cards...")
        
        # Re-find all cards right before extraction to avoid stale elements
        try:
            print("  Re-finding cards to avoid stale elements...")
            all_cards = driver.find_elements(By.CSS_SELECTOR, ".project-card-main-wrapper, section[itemtype*='ApartmentComplex']")
            print(f"  Found {len(all_cards)} cards for extraction")
        except Exception as e:
            print(f"  [WARN] Error re-finding cards: {e}")
        
        # Extract data from each card
        extracted_count = 0
        failed_count = 0
        
        for idx, card in enumerate(all_cards, 1):
            try:
                # Close popup if it appears during extraction
                if idx % 10 == 0:
                    close_popup_if_exists()
                
                property_data = extract_property_card(card)
                
                # Check if we got any meaningful data
                if property_data.get('project_name') or property_data.get('location'):
                    # Add scraping metadata
                    property_data['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    property_data['source'] = "proptiger"
                    
                    properties.append(property_data)
                    extracted_count += 1
                    
                    if idx % 10 == 0 or idx == len(all_cards):
                        print(f"    [{idx}/{len(all_cards)}] Extracted: {property_data.get('project_name', 'N/A')} - {property_data.get('price', 'N/A')}")
                else:
                    failed_count += 1
            
            except Exception as e:
                failed_count += 1
                if failed_count <= 5:  # Show first few errors for debugging
                    print(f"    [WARN] Error extracting card {idx}: {e}")
                continue
        
        print(f"  [OK] Extraction complete: {extracted_count} extracted, {failed_count} failed out of {len(all_cards)} cards")
        return properties
        
    except Exception as e:
        print(f"  [ERROR] Error extracting property cards: {e}")
        return []


def scrape_city_properties(city_name):
    """Scrape all properties for a specific city (fresh browser per city / retry)"""
    max_retries = 3
    last_error = None

    for attempt in range(max_retries):
        # NEW browser per attempt (and thus per city)
        start_new_browser()
        try:
            # Search for the city (pass attempt for logging)
            search_success = search_city(city_name, attempt)

            if not search_success:
                print(f"\n  [RETRY] Search failed for {city_name} on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    print("  [RETRY] Will retry with a NEW browser session...")
                continue

            # Real-time extraction + saving while scrolling
            print("\n  [OK] Search successful! Starting streaming extraction...")
            try:
                saved_count = scroll_extract_and_save_cards(city_name)
            except Exception as e:
                print(f"  [ERROR] Error during streaming extraction: {e}")
                saved_count = 0

            print(f"\n  [OK] City done: {city_name} (saved {saved_count} rows in real-time)")
            return saved_count

        except Exception as e:
            last_error = e
            print(f"\n  [WARN] Error while processing {city_name} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print("  [RETRY] Will retry with a NEW browser session...")
        finally:
            stop_browser()

    print(f"\n  [ERROR] Failed to process {city_name} after {max_retries} fresh-browser attempts")
    if last_error:
        print(f"  [WARN] Last error: {last_error}")
    return 0


def save_to_csv(properties, filename="output/proptiger_properties.csv"):
    """Save all properties to CSV file"""
    if not properties:
        print("\n[WARN] No properties to save")
        return
    
    # Define CSV columns
    fieldnames = [
        'project_name',
        'city',
        'location',
        'price',
        'property_type',
        'area',
        'possession_status',
        'builder',
        'rera_id',
        'property_url',
        'source',
        'scraped_at'
    ]
    
    # Write to CSV
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(properties)
    
    print(f"\n[OK] Saved {len(properties)} properties to {filename}")


def main():
    """Main function to scrape properties from multiple cities"""
    total_saved = 0
    
    print("\n" + "="*60)
    print("PROPTIGER PROPERTY SCRAPER")
    print("="*60)
    print(f"Cities to scrape: {len(cities)} cities")
    print("="*60)
    
    try:
        # Load existing keys once so we don't duplicate rows when appending
        load_seen_keys_from_csv(CSV_FILENAME)
        ensure_csv_header(CSV_FILENAME)

        for city in cities:
            try:
                saved_count = scrape_city_properties(city)
                total_saved += int(saved_count or 0)
                
                print(f"\n  [PROGRESS] saved {saved_count} rows for {city} (Total saved: {total_saved})")

                # Upload snapshot to S3 after each city completes (best-effort)
                print(f"  [INFO] Uploading CSV to S3 after completing {city}...")
                upload_csv_to_s3()
                time.sleep(3)  # Delay between cities
            except Exception as e:
                print(f"\n  [WARN] Error scraping {city}: {e}")
                print("  [INFO] Continuing with next city...")
                continue
        
        print("\n" + "="*60)
        print("[OK] SCRAPING COMPLETED!")
        print(f"Total rows saved (this run): {total_saved}")
        print(f"Saved to: {CSV_FILENAME}")
        print("="*60)

        # Final upload snapshot
        print("[INFO] Uploading final CSV snapshot to S3...")
        upload_csv_to_s3()
        
    except KeyboardInterrupt:
        print("\n\n[WARN] Scraping interrupted by user")
        print(f"Data already saved per city. Total rows saved so far: {total_saved}")
        print("[INFO] Uploading CSV snapshot to S3...")
        upload_csv_to_s3()
    
    except Exception as e:
        print(f"\n[ERROR] Error in main execution: {e}")
        print(f"Data already saved per city. Total rows saved so far: {total_saved}")
        print("[INFO] Uploading CSV snapshot to S3...")
        upload_csv_to_s3()
    
    finally:
        stop_browser()
        print("\n[INFO] Browser closed")


if __name__ == "__main__":
    main()

