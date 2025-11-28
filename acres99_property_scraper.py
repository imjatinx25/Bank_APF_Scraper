import csv
import time
import os
import sys
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
from datetime import datetime, timezone
from dotenv import load_dotenv

# Safe print function to handle Unicode characters
def safe_print(*args, **kwargs):
    """Print function that safely handles Unicode characters"""
    try:
        # Try normal print first
        print(*args, **kwargs)
    except UnicodeEncodeError:
        # If it fails, encode to ASCII with replacement
        safe_args = []
        for arg in args:
            if isinstance(arg, str):
                safe_args.append(arg.encode('ascii', 'replace').decode('ascii'))
            else:
                safe_args.append(str(arg).encode('ascii', 'replace').decode('ascii'))
        print(*safe_args, **kwargs)

# List of cities to search
CITIES_TO_SEARCH = [
    # Popular Cities
    "Delhi NCR", "Noida", "Gurgaon", "Mumbai", "Bangalore", "Hyderabad",
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


# Maximum number of pages to scrape per city (set to None for unlimited)
MAX_PAGES_PER_CITY = None  # Set to a number like 8 to limit, or None for all pages

# Initialize Chrome driver with anti-detection settings
chrome_options = Options()
chrome_options.add_argument('--headless')  # Uncomment to run in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)

# Hide webdriver property to avoid detection
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")


def close_popup_if_exists():
    """Close any popup/overlay that might appear"""
    popup_closed = False
    
    # Try multiple popup close methods
    popup_selectors = [
        '[data-label="RERA_DISCLAIMER.OK_GOT_IT"]',
        '.modal-close',
        '.close-button',
        '[aria-label="Close"]',
        'button[class*="close"]',
        '.overlay-close',
        '#close-popup',
    ]
    
    for selector in popup_selectors:
        try:
            popup = driver.find_element(By.CSS_SELECTOR, selector)
            if popup.is_displayed():
                driver.execute_script("arguments[0].click();", popup)
                print("  [OK] Closed popup")
                popup_closed = True
                time.sleep(0.5)
                break
        except:
            continue
    
    # Try pressing Escape key to close any modal
    if not popup_closed:
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            body.send_keys(Keys.ESCAPE)
            time.sleep(0.5)
        except:
            pass


def search_city(city_name):
    """Search for properties in a specific city"""
    try:
        print(f"\n{'='*60}")
        print(f"Searching for properties in: {city_name}")
        print(f"{'='*60}")
        
        # Go to homepage - always start fresh for each city
        driver.get("https://www.99acres.com/")
        
        # Wait for page to load
        time.sleep(3)
        
        # Close any popups/overlays multiple times to ensure they're gone
        for _ in range(3):
            close_popup_if_exists()
            time.sleep(1)
        
        # Wait for search input to be available and interactable - try multiple selectors
        search_input = None
        wait = WebDriverWait(driver, 20)
        
        # Try different selectors for the search input - first try presence, then clickable
        selectors = [
            (By.ID, "keyword2"),
            (By.CSS_SELECTOR, "input[id='keyword2']"),
            (By.CSS_SELECTOR, "#keyword2"),
            (By.CSS_SELECTOR, "input[placeholder*='Search']"),
            (By.CSS_SELECTOR, "input[placeholder*='search']"),
            (By.CSS_SELECTOR, "input[type='text'][name*='keyword']"),
            (By.CSS_SELECTOR, "#searchform input[type='text']"),
            (By.CSS_SELECTOR, "form#searchform input"),
            (By.CSS_SELECTOR, ".search-input"),
            (By.CSS_SELECTOR, "input.autocomplete-input"),
        ]
        
        # First, try to find element by presence (faster)
        for selector_type, selector_value in selectors:
            try:
                search_input = wait.until(EC.presence_of_element_located((selector_type, selector_value)))
                print(f"  Found search input (presence) using: {selector_type} = {selector_value}")
                break
            except TimeoutException:
                continue
        
        # If found by presence, wait for it to be clickable
        if search_input:
            try:
                wait.until(EC.element_to_be_clickable(search_input))
            except TimeoutException:
                # If not clickable, try to make it clickable by removing overlays
                driver.execute_script("arguments[0].style.zIndex = '9999';", search_input)
                close_popup_if_exists()
        
        # If still not found, try finding in search form
        if not search_input:
            try:
                search_form = wait.until(EC.presence_of_element_located((By.ID, "searchform")))
                # Try to find input within the form
                inputs = search_form.find_elements(By.TAG_NAME, "input")
                for inp in inputs:
                    if inp.get_attribute("type") in ["text", "search", None]:
                        search_input = inp
                        print(f"  Found search input in search form")
                        break
            except Exception as e:
                print(f"  [DEBUG] Search form not found: {e}")
        
        # Last resort: find any text input on the page
        if not search_input:
            try:
                all_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'], input[type='search']")
                for inp in all_inputs:
                    placeholder = inp.get_attribute("placeholder") or ""
                    if "search" in placeholder.lower() or "city" in placeholder.lower() or "location" in placeholder.lower():
                        search_input = inp
                        print(f"  Found search input by placeholder: {placeholder}")
                        break
            except Exception as e:
                print(f"  [DEBUG] Could not find any text input: {e}")
        
        if not search_input:
            # Take a screenshot for debugging
            try:
                driver.save_screenshot("output/search_input_error.png")
                print(f"  [DEBUG] Screenshot saved to output/search_input_error.png")
            except:
                pass
            raise Exception("Could not locate search input element. Website structure may have changed.")
        
        # Scroll to element and ensure it's visible
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_input)
        time.sleep(1)
        
        # Close popups again after scrolling (in case they reappeared)
        close_popup_if_exists()
        time.sleep(0.5)
        
        # Try to clear using JavaScript if regular clear doesn't work
        try:
            search_input.clear()
        except:
            driver.execute_script("arguments[0].value = '';", search_input)
        time.sleep(0.5)
        
        # Click on the input first to ensure it's focused
        try:
            search_input.click()
        except:
            driver.execute_script("arguments[0].click();", search_input)
        time.sleep(0.5)
        
        # Type city name - try regular typing first, fallback to JavaScript if needed
        try:
            # Type city name character by character
            for char in city_name:
                search_input.send_keys(char)
                time.sleep(0.05)
        except Exception as e:
            # If regular typing fails, use JavaScript
            print(f"  Regular typing failed, using JavaScript: {e}")
            driver.execute_script("arguments[0].value = arguments[1];", search_input, city_name)
            # Trigger input event to show suggestions
            driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", search_input)
        
        # Wait for dropdown suggestions
        time.sleep(2)
        
        # Try to click first suggestion if it's a city/locality
        try:
            suggestions = driver.find_elements(By.CSS_SELECTOR, "#suggestions_custom li, .component__inPageAutoSuggSlide li")
            if suggestions and len(suggestions) > 0:
                first_suggestion = suggestions[0]
                print(f"  Clicking suggestion: {first_suggestion.text}")
                first_suggestion.click()
                time.sleep(1)
        except Exception as e:
            print(f"  No suggestions clicked, proceeding with direct search")
        
        # Find and click search button - try multiple selectors
        search_button = None
        button_selectors = [
            (By.ID, "searchform_search_btn"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "#searchform button"),
            (By.CSS_SELECTOR, "input[type='submit']"),
        ]
        
        for selector_type, selector_value in button_selectors:
            try:
                search_button = driver.find_element(selector_type, selector_value)
                print(f"  Found search button using: {selector_type} = {selector_value}")
                break
            except:
                continue
        
        if not search_button:
            raise Exception("Could not locate search button element.")
        
        driver.execute_script("arguments[0].click();", search_button)
        time.sleep(3)
        
        # Close any popup after search
        close_popup_if_exists()
        
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error searching for city {city_name}: {e}")
        return False


def extract_regular_card(card):
    """Extract data from regular property card (tupleNew__contentWrap)"""
    property_data = {}
    
    # Extract project/property name
    try:
        project_name_elem = card.find_element(By.CLASS_NAME, "tupleNew__locationName")
        property_data['project_name'] = project_name_elem.text.strip()
    except:
        property_data['project_name'] = ""
    
    # Extract property heading (BHK type and location)
    try:
        prop_heading = card.find_element(By.CLASS_NAME, "tupleNew__propType")
        property_data['property_heading'] = prop_heading.text.strip()
    except:
        property_data['property_heading'] = ""
    
    # Extract property type and location from heading
    heading_text = property_data.get('property_heading', '')
    if ' in ' in heading_text:
        parts = heading_text.split(' in ', 1)
        property_data['property_type'] = parts[0].strip()
        property_data['location'] = parts[1].strip()
    else:
        property_data['property_type'] = heading_text
        property_data['location'] = ""
    
    # Extract price
    try:
        price_elem = card.find_element(By.CSS_SELECTOR, ".tupleNew__priceValWrap span")
        property_data['price'] = price_elem.text.strip()
    except:
        property_data['price'] = ""
    
    # Extract price per sqft
    try:
        price_sqft_elem = card.find_element(By.CSS_SELECTOR, ".tupleNew__priceAndPerSqftWrap .tupleNew__perSqftWrap")
        property_data['price_per_sqft'] = price_sqft_elem.text.strip()
    except:
        property_data['price_per_sqft'] = ""
    
    # Extract area
    try:
        area_elem = card.find_element(By.CLASS_NAME, "tupleNew__area1Type")
        property_data['area'] = area_elem.text.strip()
    except:
        property_data['area'] = ""
    
    # Extract BHK configuration
    try:
        bhk_elems = card.find_elements(By.CLASS_NAME, "tupleNew__area1Type")
        for elem in bhk_elems:
            if "BHK" in elem.text or "RK" in elem.text:
                property_data['bhk_config'] = elem.text.strip()
                break
        if 'bhk_config' not in property_data:
            property_data['bhk_config'] = ""
    except:
        property_data['bhk_config'] = ""
    
    # Extract possession status
    try:
        possession_elem = card.find_element(By.CLASS_NAME, "tupleNew__possessionBy")
        property_data['possession_status'] = possession_elem.text.strip()
    except:
        property_data['possession_status'] = ""
    
    # Extract property URL
    try:
        link_elem = card.find_element(By.CSS_SELECTOR, ".tupleNew__propertyHeading")
        property_data['property_url'] = link_elem.get_attribute('href')
    except:
        property_data['property_url'] = ""
    
    # Extract RERA status
    try:
        rera_elem = card.find_element(By.CLASS_NAME, "tupleNew__reraTags")
        property_data['rera_status'] = "Yes" if rera_elem else "No"
    except:
        property_data['rera_status'] = "No"
    
    # Extract property tag (RESALE, NEW, etc.)
    try:
        ribbon_elem = card.find_element(By.CLASS_NAME, "tupleNew__ribbon")
        property_data['property_tag'] = ribbon_elem.text.strip()
    except:
        property_data['property_tag'] = ""
    
    # Extract highlights
    try:
        highlights = []
        highlight_elems = card.find_elements(By.CLASS_NAME, "tupleNew__unitHighlightTxt")
        for h in highlight_elems:
            highlights.append(h.text.strip())
        property_data['highlights'] = ", ".join(highlights)
    except:
        property_data['highlights'] = ""

    # Extract description
    try:
        desc_elem = card.find_element(By.CLASS_NAME, "tupleNew__descText")
        property_data['description'] = desc_elem.text.strip()
    except:
        property_data['description'] = ""
    
    return property_data


def extract_project_card(card):
    """Extract data from new project card (PseudoTupleRevamp__tupleWrapProject)"""
    property_data = {}
    
    # Extract project name
    try:
        project_name_elem = card.find_element(By.CSS_SELECTOR, ".PseudoTupleRevamp__headNrating a")
        property_data['project_name'] = project_name_elem.text.strip()
    except:
        property_data['project_name'] = ""
    
    # Extract property heading
    try:
        prop_heading = card.find_element(By.CLASS_NAME, "PseudoTupleRevamp__subHeading")
        property_data['property_heading'] = prop_heading.text.strip()
    except:
        property_data['property_heading'] = ""
    
    # Extract property type and location from heading
    heading_text = property_data.get('property_heading', '')
    if ' in ' in heading_text:
        parts = heading_text.split(' in ', 1)
        property_data['property_type'] = parts[0].strip()
        property_data['location'] = parts[1].strip()
    else:
        property_data['property_type'] = heading_text
        property_data['location'] = ""
    
    # Extract price (from configuration card)
    try:
        price_elem = card.find_element(By.CLASS_NAME, "configs__ccl2")
        property_data['price'] = price_elem.text.strip()
    except:
        property_data['price'] = ""
    
    # No price per sqft in project cards
    property_data['price_per_sqft'] = ""
    
    # Extract BHK config
    try:
        bhk_elem = card.find_element(By.CLASS_NAME, "configs__ccl1")
        property_data['bhk_config'] = bhk_elem.text.strip()
        property_data['area'] = bhk_elem.text.strip()
    except:
        property_data['bhk_config'] = ""
        property_data['area'] = ""
    
    # Extract possession status (from bottom text)
    try:
        possession_elem = card.find_element(By.CSS_SELECTOR, ".ImgItem__fomoWrap span")
        property_data['possession_status'] = possession_elem.text.strip()
    except:
        property_data['possession_status'] = ""
    
    # Extract property URL
    try:
        link_elem = card.find_element(By.CSS_SELECTOR, ".PseudoTupleRevamp__headNrating a")
        property_data['property_url'] = link_elem.get_attribute('href')
    except:
        property_data['property_url'] = ""
    
    # RERA status - usually yes for projects
    property_data['rera_status'] = "Yes"
    
    # Extract property tag (NEW BOOKING, etc.)
    try:
        ribbon_elem = card.find_element(By.CLASS_NAME, "PseudoTupleRevamp__ribbon")
        property_data['property_tag'] = ribbon_elem.text.strip()
    except:
        property_data['property_tag'] = ""
    
    # Extract nearby/highlights
    try:
        highlights = []
        highlight_elems = card.find_elements(By.CLASS_NAME, "tupleNew__unitHighlightTxt")
        for h in highlight_elems:
            highlights.append(h.text.strip())
        property_data['highlights'] = ", ".join(highlights)
    except:
        property_data['highlights'] = ""

    # Extract description
    try:
        desc_elem = card.find_element(By.CLASS_NAME, "tupleNew__descText")
        property_data['description'] = desc_elem.text.strip()
    except:
        property_data['description'] = ""
    
    return property_data


def extract_property_cards():
    """Extract all property cards from the current page"""
    properties = []
    
    try:
        # Wait for property cards to load
        time.sleep(3)
        
        # Scroll through page to load all cards
        print(f"  Scrolling to load all cards...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        viewport = driver.execute_script("return window.innerHeight")
        current_pos = 0
        
        while current_pos < last_height:
            current_pos += viewport
            driver.execute_script(f"window.scrollTo(0, {current_pos});")
            time.sleep(1)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height > last_height:
                last_height = new_height
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Find ALL property cards using the OUTER wrapper - catches everything!
        # Both card types have an outer wrapper ending in __outerTupleWrap
        all_cards = driver.find_elements(By.XPATH, 
            "//div[contains(@class, 'outerTupleWrap')]")
        
        print(f"  Found {len(all_cards)} property cards total")
        
        # Extract all cards in one loop
        for idx, card in enumerate(all_cards, 1):
            try:
                card_class = card.get_attribute("class")
                property_data = None
                card_type = "unknown"
                
                # Detect card type by looking inside the outer wrapper
                # Try to find project card inside
                try:
                    project_card = card.find_element(By.CLASS_NAME, "PseudoTupleRevamp__tupleWrapProject")
                    property_data = extract_project_card(project_card)
                    card_type = "project"
                except:
                    pass
                
                # If not project, try premium/topaz card
                if not property_data:
                    try:
                        topaz_card = card.find_element(By.CLASS_NAME, "tupleNew__tupleWrapTopaz")
                        # Premium cards have contentWrap inside
                        content_wrap = topaz_card.find_element(By.CLASS_NAME, "tupleNew__contentWrap")
                        property_data = extract_regular_card(content_wrap)
                        card_type = "premium"
                    except:
                        pass
                
                # If neither, try regular card
                if not property_data:
                    try:
                        content_wrap = card.find_element(By.CLASS_NAME, "tupleNew__contentWrap")
                        property_data = extract_regular_card(content_wrap)
                        card_type = "regular"
                    except:
                        pass
                
                if property_data:
                    # Try to extract description from outer wrapper if not already found
                    if not property_data.get('description'):
                        try:
                            desc_elem = card.find_element(By.CLASS_NAME, "tupleNew__descText")
                            property_data['description'] = desc_elem.text.strip()
                        except:
                            pass
                    
                    # Add scraping metadata
                    property_data['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    property_data['source'] = "99acres"
                    property_data['card_type'] = card_type
                    
                    # Only add if we have at least a project name or property type
                    if property_data.get('project_name') or property_data.get('property_type'):
                        properties.append(property_data)
                        # Safe print to handle Unicode characters like ₹
                        try:
                            project_name = str(property_data.get('project_name', 'N/A'))[:50]
                            price = str(property_data.get('price', 'N/A'))[:40]
                            # Try to print normally first
                            print(f"    [{idx}] {card_type.upper()}: {project_name} - {price}")
                        except UnicodeEncodeError:
                            # If it fails, encode Unicode characters safely
                            project_name_safe = project_name.encode('ascii', 'replace').decode('ascii')
                            price_safe = price.encode('ascii', 'replace').decode('ascii')
                            print(f"    [{idx}] {card_type.upper()}: {project_name_safe} - {price_safe}")
                
            except Exception as e:
                # Safe error message - encode Unicode characters before printing
                # This is critical because the error message itself might contain Unicode (like ₹)
                try:
                    error_str = repr(e)  # Use repr() to get a safe representation
                    # Try to encode to see if it contains problematic Unicode
                    try:
                        error_str.encode('ascii', 'strict')
                        # Safe to print
                        print(f"    [WARNING] Error extracting card {idx}: {error_str}")
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        # Contains Unicode - encode it safely
                        safe_error = error_str.encode('ascii', 'replace').decode('ascii')
                        print(f"    [WARNING] Error extracting card {idx}: {safe_error}")
                except Exception:
                    # Last resort - use generic message
                    print(f"    [WARNING] Error extracting card {idx}: [Encoding error - check logs]")
                continue
        
        return properties
        
    except Exception as e:
        print(f"  [ERROR] Error extracting property cards: {e}")
        return []


def check_and_go_to_next_page():
    """Check if next page exists and navigate to it"""
    try:
        # Scroll to bottom to ensure pagination is visible
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # Find pagination container
        try:
            pagination = driver.find_element(By.CLASS_NAME, "Pagination__srpPagination")
        except:
            print(f"  [INFO] No pagination found (might be last page)")
            return False
        
        # Find "Next Page >" link within pagination
        next_page_href = None
        try:
            # Get all links in pagination
            links = pagination.find_elements(By.TAG_NAME, "a")
            
            # Find the "Next Page >" link (the last div's link)
            for link in links:
                link_text = link.text.strip()
                if "Next Page" in link_text and ">" in link_text:
                    next_page_href = link.get_attribute("href")
                    print(f"  Found 'Next Page >' link")
                    break
        except Exception as e:
            print(f"  Error finding Next Page link: {e}")
            return False
        
        if next_page_href:
            print(f"  [INFO] Navigating to: {next_page_href}")
            
            # Navigate directly to the next page URL
            driver.get(next_page_href)
            
            # Wait for page to load
            time.sleep(4)
            
            # Close any popups
            close_popup_if_exists()
            
            print(f"  [OK] Successfully navigated to next page")
            return True
        else:
            print(f"  [INFO] No 'Next Page' link found (reached end)")
            return False
        
    except Exception as e:
        print(f"  [WARNING] Error navigating to next page: {e}")
        return False


def scrape_city_properties(city_name):
    """Scrape all properties for a specific city"""
    all_properties = []
    
    # Search for the city
    if not search_city(city_name):
        return all_properties
    
    # Scrape multiple pages
    page_num = 1
    while True:
        # Check if we've reached the page limit (if set)
        if MAX_PAGES_PER_CITY is not None and page_num > MAX_PAGES_PER_CITY:
            print(f"\n  [INFO] Reached MAX_PAGES_PER_CITY ({MAX_PAGES_PER_CITY}), stopping")
            break
        
        print(f"\n  Page {page_num}:")
        
        # Extract properties from current page
        properties = extract_property_cards()
        
        # Add city name to each property
        for prop in properties:
            prop['city'] = city_name.lower()
        
        all_properties.extend(properties)
        
        # Save to CSV after each page
        if properties:
            # Read existing data
            existing_properties = []
            try:
                with open("output/99acres_properties.csv", 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    existing_properties = list(reader)
            except FileNotFoundError:
                pass  # File doesn't exist yet
            
            # Append new properties
            existing_properties.extend(properties)
            
            # Save all data
            save_to_csv(existing_properties, "output/99acres_properties.csv")
            print(f"  [SAVED] Saved {len(properties)} properties from this page (Total: {len(existing_properties)})")
        
        # Increment page counter
        page_num += 1
        
        # Try to go to next page
        if not check_and_go_to_next_page():
            print(f"\n  [INFO] No more pages available, stopping")
            break
        
        time.sleep(2)  # Delay between pages
    
    print(f"\n  [OK] Total properties found in {city_name}: {len(all_properties)}")
    return all_properties


def save_to_csv(properties, filename="output/99acres_properties.csv"):
    """Save all properties to CSV file"""
    if not properties:
        print("\n[WARNING] No properties to save")
        return
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else "output", exist_ok=True)
    
    # Define CSV columns - full structure with all extracted data
    fieldnames = [
        'project_name',
        'property_heading',
        'property_type',
        'city',
        'location',
        'price',
        'price_per_sqft',
        'area',
        'bhk_config',
        'possession_status',
        'rera_status',
        'property_tag',
        'highlights',
        'description',
        'property_url',
        'card_type',
        'scraped_at',
        'source'
    ]
    
    # Write to CSV
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(properties)
    
    print(f"\n[OK] Saved {len(properties)} properties to {filename}")


def upload_csv_to_s3():
    """Upload CSV file to S3"""
    try:
        csv_path = "output/99acres_properties.csv"
        
        # Check if CSV file exists
        if not os.path.exists(csv_path):
            print(f"[WARNING] CSV file not found: {csv_path}, skipping S3 upload")
            return
        
        # Check if file is empty
        if os.path.getsize(csv_path) == 0:
            print(f"[WARNING] CSV file is empty, skipping S3 upload")
            return
        
        load_dotenv()
        bucket = os.getenv('S3_BUCKET_NAME')
        if not bucket:
            print(f"[ERROR] S3_BUCKET_NAME not set in environment, skipping S3 upload")
            return
        
        # key_prefix = os.getenv('S3_KEY')
        key_prefix = "test_apf_apis/"
        
        with open(csv_path, 'rb') as f:
            csv_content = f.read()
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{key_prefix.rstrip('/')}/99acres_properties_{timestamp}.csv"
        
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content,
            ContentType="text/csv"
        )
        
        print(f"[OK] CSV uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"[ERROR] Error uploading CSV to S3: {str(e)}")


def main():
    """Main function to scrape properties from multiple cities"""
    all_properties = []
    
    print("\n" + "="*60)
    print("99ACRES PROPERTY SCRAPER")
    print("="*60)
    print(f"Cities to scrape: {', '.join(CITIES_TO_SEARCH)}")
    print(f"Max pages per city: {'Unlimited (All pages)' if MAX_PAGES_PER_CITY is None else MAX_PAGES_PER_CITY}")
    print("="*60)
    
    try:
        for city in CITIES_TO_SEARCH:
            properties = scrape_city_properties(city)
            all_properties.extend(properties)
            
            print(f"\n  [PROGRESS] {len(properties)} properties from {city} (Total: {len(all_properties)})")
            
            # Upload CSV to S3 after each city completes
            print(f"\n  [INFO] Uploading CSV to S3 after completing {city}...")
            upload_csv_to_s3()
            
            time.sleep(3)  # Delay between cities
        
        print("\n" + "="*60)
        print(f"[COMPLETED] SCRAPING COMPLETED!")
        print(f"Total properties scraped: {len(all_properties)}")
        print(f"Saved to: output/99acres_properties.csv")
        print("="*60)
        
        upload_csv_to_s3()
        
    except KeyboardInterrupt:
        print("\n\n[WARNING] Scraping interrupted by user")
        print(f"Data already saved per page. {len(all_properties)} properties were collected.")
        upload_csv_to_s3()
    
    except Exception as e:
        print(f"\n[ERROR] Error in main execution: {e}")
        print(f"Data already saved per page. {len(all_properties)} properties were collected.")
        upload_csv_to_s3()
    
    finally:
        driver.quit()
        print("\n[INFO] Browser closed")


if __name__ == "__main__":
    main()
