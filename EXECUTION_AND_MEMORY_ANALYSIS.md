# Execution Model & Memory Optimization Analysis

## 🔄 Current Execution Model

### **How Scrapers Run When API is Hit**

When you hit `POST /scrape-property/{property}` or `/scrape-apf/{bank}`:

1. **API receives request** → FastAPI endpoint handler
2. **Subprocess spawns** → `subprocess.Popen()` launches Python script as **background process**
3. **Non-blocking** → API immediately returns response (doesn't wait for scraper to finish)
4. **Process tracking** → API tracks process in `_active_processes` dictionary
5. **Logging** → Real-time logs written to `output/run_{scraper}_{timestamp}.log`
6. **Independent execution** → Scraper runs in separate process, can run for hours/days

**Key Code (app.py lines 166-174):**
```python
proc = subprocess.Popen(
    [sys.executable, "-u", str(script_path)],  # Background subprocess
    cwd=str(BASE_DIR),
    stdout=log_file_handle,
    stderr=subprocess.STDOUT,
    bufsize=1,
    universal_newlines=True,
    env=env,
)
```

### **Is it a Background Task?**

✅ **Yes, it's a background subprocess**, but:
- ❌ **Not a proper task queue** (like Celery/Redis)
- ❌ **No task persistence** (if API crashes, task tracking is lost)
- ❌ **No retry mechanism** at API level
- ❌ **No priority queue** or rate limiting

---

## 💾 Memory Usage Analysis

### **Current Memory Issues**

#### **1. acres99_property_scraper.py - CRITICAL MEMORY LEAK**

**Problem:**
```python
# Line 70: Global driver created ONCE at module level
driver = webdriver.Chrome(options=chrome_options)

# This driver stays alive for ALL 80+ cities!
# Never closed until script ends (could be days)
```

**Memory Impact:**
- Chrome browser process: **~200-500 MB per instance**
- DOM cache accumulates over time
- JavaScript heap grows with each page
- **Total for 80 cities: Could reach 1-2 GB+**

**Current Flow:**
```
Start → Create driver → Scrape City 1 → Scrape City 2 → ... → Scrape City 80 → Close driver
         (200MB)        (250MB)         (300MB)              (1.5GB+)        (finally!)
```

#### **2. proptiger_property_scraper.py - BETTER (but can improve)**

**Current:**
```python
# Creates new browser per city
def scrape_city_properties(city_name):
    for attempt in range(max_retries):
        start_new_browser()  # New browser
        # ... scrape city ...
        stop_browser()       # Close after city
```

**Memory Impact:**
- ✅ Browser closed after each city (good!)
- ⚠️ But browser stays open for entire city scraping (could be hours)
- ⚠️ No memory limits on Chrome
- ⚠️ No cache clearing between cities

#### **3. Data Accumulation**

**acres99:**
- ❌ Loads ALL properties into memory list
- ❌ Saves entire list to CSV at end of each page
- ❌ Re-reads entire CSV to append (inefficient)

**proptiger:**
- ✅ Streams data to CSV (better!)
- ✅ Writes incrementally
- ✅ Doesn't accumulate in memory

---

## 🚀 Memory Optimization Strategies

### **Strategy 1: Browser Lifecycle Management (HIGH PRIORITY)**

#### **Fix acres99 - Close Browser After Each City**

**Current (BAD):**
```python
# Global driver - stays alive forever
driver = webdriver.Chrome(options=chrome_options)

def scrape_city_properties(city_name):
    # Uses global driver for all cities
    # Never closes until script ends
```

**Fixed (GOOD):**
```python
driver = None  # No global driver

def scrape_city_properties(city_name):
    global driver
    try:
        # Create fresh browser per city
        if driver is None:
            driver = create_browser()
        
        # Scrape city...
        properties = extract_property_cards()
        
    finally:
        # ALWAYS close browser after each city
        if driver:
            driver.quit()
            driver = None
        # Force garbage collection
        import gc
        gc.collect()
```

#### **Add Memory Limits to Chrome**

```python
chrome_options.add_argument('--max-old-space-size=512')  # Limit JS heap
chrome_options.add_argument('--memory-pressure-off')     # Disable memory pressure
chrome_options.add_argument('--disable-dev-shm-usage')   # Already have this
chrome_options.add_argument('--disable-gpu')             # Save GPU memory
chrome_options.add_argument('--disable-software-rasterizer')
```

#### **Clear Browser Cache Periodically**

```python
def clear_browser_cache(driver):
    """Clear browser cache to free memory"""
    try:
        driver.execute_cdp_cmd('Network.clearBrowserCache', {})
        driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
        # Clear JavaScript heap
        driver.execute_script("if (window.gc) { window.gc(); }")
    except:
        pass
```

### **Strategy 2: Streaming Data Processing (MEDIUM PRIORITY)**

#### **Fix acres99 - Use Streaming CSV Writes**

**Current (BAD):**
```python
# Loads ALL properties into memory
all_properties = []
for city in CITIES_TO_SEARCH:
    properties = scrape_city_properties(city)
    all_properties.extend(properties)  # Accumulates in memory

# Saves all at once
save_to_csv(all_properties)
```

**Fixed (GOOD):**
```python
# Stream to CSV incrementally
with open(CSV_FILENAME, 'a', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    
    for city in CITIES_TO_SEARCH:
        properties = scrape_city_properties(city)
        
        # Write immediately, don't accumulate
        for prop in properties:
            writer.writerow(prop)
            f.flush()  # Ensure data is written
        
        # Close browser after each city
        if driver:
            driver.quit()
            driver = None
```

### **Strategy 3: Process-Level Memory Management**

#### **Add Memory Monitoring**

```python
import psutil
import os

def check_memory_usage():
    """Check current memory usage"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return {
        "rss_mb": mem_info.rss / 1024 / 1024,  # Resident Set Size
        "vms_mb": mem_info.vms / 1024 / 1024,  # Virtual Memory Size
        "percent": process.memory_percent()
    }

def log_memory_usage(context=""):
    mem = check_memory_usage()
    print(f"[MEMORY] {context}: RSS={mem['rss_mb']:.1f}MB, Percent={mem['percent']:.1f}%")
```

#### **Add Memory Threshold Checks**

```python
MAX_MEMORY_MB = 2048  # 2GB limit

def check_memory_and_restart_if_needed():
    mem = check_memory_usage()
    if mem['rss_mb'] > MAX_MEMORY_MB:
        print(f"[WARN] Memory usage high ({mem['rss_mb']:.1f}MB), restarting browser...")
        if driver:
            driver.quit()
            driver = None
        import gc
        gc.collect()
        time.sleep(2)
        return True
    return False
```

### **Strategy 4: Use Proper Task Queue (LONG TERM)**

#### **Option A: Celery with Redis**

**Benefits:**
- ✅ Task persistence (survives API restarts)
- ✅ Automatic retries
- ✅ Priority queues
- ✅ Rate limiting
- ✅ Better memory isolation
- ✅ Distributed execution

**Implementation:**
```python
# tasks.py
from celery import Celery

celery_app = Celery('scrapers', broker='redis://localhost:6379')

@celery_app.task(bind=True, max_retries=3)
def scrape_property_task(self, property_name):
    # Scraper code here
    # Each task runs in separate worker process
    pass

# app.py
@app.post("/scrape-property/{property}")
def start_property_scraper(property):
    task = scrape_property_task.delay(property)
    return {"task_id": task.id, "status": "queued"}
```

#### **Option B: Background Tasks (FastAPI built-in)**

**Simpler, but less features:**
```python
from fastapi import BackgroundTasks

@app.post("/scrape-property/{property}")
def start_property_scraper(property, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_scraper, property)
    return {"status": "started"}
```

---

## 📊 Memory Optimization Recommendations

### **Immediate Fixes (Do Now)**

1. **Fix acres99 browser lifecycle**
   - Close browser after each city
   - Don't use global driver

2. **Add Chrome memory limits**
   - `--max-old-space-size=512`
   - `--disable-gpu`
   - `--disable-software-rasterizer`

3. **Add memory monitoring**
   - Log memory usage after each city
   - Alert if memory exceeds threshold

4. **Clear browser cache**
   - Clear cache after each city
   - Force garbage collection

### **Short-term Improvements (Week 1-2)**

5. **Streaming CSV writes in acres99**
   - Don't accumulate all properties in memory
   - Write incrementally like proptiger

6. **Browser restart threshold**
   - Restart browser if memory > 1.5GB
   - Or after every N cities (e.g., every 10 cities)

7. **Process memory limits**
   - Set Docker container memory limits
   - Monitor and restart if exceeded

### **Long-term Improvements (Month 1)**

8. **Implement Celery task queue**
   - Better process isolation
   - Task persistence
   - Automatic retries

9. **Add memory profiling**
   - Track memory usage over time
   - Identify memory leaks
   - Optimize hot paths

10. **Container resource limits**
    ```yaml
    # docker-compose.yml
    services:
      bank-scraper-api:
        deploy:
          resources:
            limits:
              memory: 4G
            reservations:
              memory: 2G
    ```

---

## 🔧 Implementation Examples

### **Example 1: Fixed acres99 Browser Management**

```python
# acres99_property_scraper.py

driver = None  # Changed from: driver = webdriver.Chrome(...)

def create_browser():
    """Create a fresh Chrome browser instance"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--max-old-space-size=512')  # NEW: Memory limit
    chrome_options.add_argument('--disable-gpu')             # NEW: Save GPU memory
    # ... other options ...
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def scrape_city_properties(city_name):
    """Scrape all properties for a specific city"""
    global driver
    all_properties = []
    
    try:
        # Create fresh browser for this city
        driver = create_browser()
        log_memory_usage(f"After creating browser for {city_name}")
        
        # Search for the city
        if not search_city(city_name):
            return all_properties
        
        # Scrape pages
        page_num = 1
        while True:
            properties = extract_property_cards()
            all_properties.extend(properties)
            
            # Save incrementally (don't accumulate)
            if properties:
                save_to_csv_incremental(properties, city_name)
            
            # Check memory every 5 pages
            if page_num % 5 == 0:
                if check_memory_and_restart_if_needed():
                    driver = create_browser()
                    # Re-search city after restart
                    search_city(city_name)
            
            if not check_and_go_to_next_page():
                break
            page_num += 1
        
        return all_properties
        
    finally:
        # ALWAYS close browser after city
        if driver:
            try:
                clear_browser_cache(driver)
                driver.quit()
            except:
                pass
            driver = None
        
        # Force garbage collection
        import gc
        gc.collect()
        log_memory_usage(f"After closing browser for {city_name}")

def save_to_csv_incremental(properties, city_name):
    """Append properties to CSV immediately"""
    csv_path = "output/99acres_properties.csv"
    
    # Ensure file exists with header
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(properties)
        f.flush()  # Ensure written to disk
```

### **Example 2: Memory Monitoring Utility**

```python
# utils/memory_monitor.py
import psutil
import os
import gc
from typing import Dict

def get_memory_usage() -> Dict[str, float]:
    """Get current process memory usage"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return {
        "rss_mb": mem_info.rss / 1024 / 1024,
        "vms_mb": mem_info.vms / 1024 / 1024,
        "percent": process.memory_percent(),
        "available_mb": psutil.virtual_memory().available / 1024 / 1024
    }

def log_memory(context: str = ""):
    """Log memory usage"""
    mem = get_memory_usage()
    print(f"[MEMORY] {context}: RSS={mem['rss_mb']:.1f}MB, "
          f"Percent={mem['percent']:.1f}%, Available={mem['available_mb']:.1f}MB")

def should_restart_browser(threshold_mb: int = 1500) -> bool:
    """Check if browser should be restarted due to memory"""
    mem = get_memory_usage()
    if mem['rss_mb'] > threshold_mb:
        print(f"[WARN] Memory usage ({mem['rss_mb']:.1f}MB) exceeds threshold ({threshold_mb}MB)")
        return True
    return False

def force_cleanup():
    """Force garbage collection"""
    gc.collect()
    gc.collect()  # Run twice to handle circular references
```

### **Example 3: Enhanced Browser Management**

```python
# utils/browser_manager.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

class BrowserManager:
    """Manages browser lifecycle with memory optimization"""
    
    def __init__(self, max_memory_mb: int = 1500, cities_per_restart: int = 10):
        self.driver = None
        self.max_memory_mb = max_memory_mb
        self.cities_per_restart = cities_per_restart
        self.cities_scraped = 0
    
    def get_driver(self, force_new: bool = False):
        """Get browser driver, create new if needed"""
        if force_new or self.driver is None:
            self.close()
            self.driver = self._create_driver()
            self.cities_scraped = 0
        
        # Check if we should restart based on city count
        if self.cities_scraped >= self.cities_per_restart:
            print(f"[INFO] Restarting browser after {self.cities_per_restart} cities")
            self.close()
            self.driver = self._create_driver()
            self.cities_scraped = 0
        
        # Check memory threshold
        if should_restart_browser(self.max_memory_mb):
            print("[INFO] Restarting browser due to memory threshold")
            self.close()
            self.driver = self._create_driver()
            self.cities_scraped = 0
        
        return self.driver
    
    def _create_driver(self):
        """Create optimized Chrome driver"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--max-old-space-size=512')  # Memory limit
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        # ... other options ...
        
        driver = webdriver.Chrome(options=options)
        # ... anti-detection setup ...
        return driver
    
    def close(self):
        """Close browser and cleanup"""
        if self.driver:
            try:
                # Clear cache before closing
                self.driver.execute_cdp_cmd('Network.clearBrowserCache', {})
                self.driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
                self.driver.quit()
            except:
                pass
            self.driver = None
        
        # Force garbage collection
        import gc
        gc.collect()
    
    def mark_city_complete(self):
        """Mark that a city has been scraped"""
        self.cities_scraped += 1
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
```

**Usage:**
```python
browser_mgr = BrowserManager(max_memory_mb=1500, cities_per_restart=10)

for city in CITIES_TO_SEARCH:
    driver = browser_mgr.get_driver()
    # ... scrape city ...
    browser_mgr.mark_city_complete()
    # Browser auto-restarts every 10 cities or if memory > 1.5GB
```

---

## 📈 Expected Memory Improvements

### **Before Optimization:**
- **acres99**: 1.5-2 GB after 80 cities (memory leak)
- **proptiger**: 300-500 MB per city (restarts help)
- **Risk**: Out of memory errors, system crashes

### **After Optimization:**
- **acres99**: 200-400 MB (browser restarts every city)
- **proptiger**: 200-300 MB (already good, but can add limits)
- **Risk**: Minimal, controlled memory usage

### **Memory Savings:**
- **~75% reduction** in peak memory usage
- **Stable memory** over long runs
- **No memory leaks**

---

## 🎯 Action Plan

### **Phase 1: Critical Fixes (This Week)**
1. ✅ Fix acres99 browser lifecycle (close after each city)
2. ✅ Add Chrome memory limits
3. ✅ Add memory monitoring/logging
4. ✅ Clear browser cache after each city

### **Phase 2: Optimization (Next Week)**
5. ✅ Implement streaming CSV in acres99
6. ✅ Add browser restart threshold
7. ✅ Create BrowserManager utility class
8. ✅ Add Docker memory limits

### **Phase 3: Advanced (Month 1)**
9. ✅ Consider Celery task queue
10. ✅ Add memory profiling
11. ✅ Implement automatic scaling

---

## 📝 Summary

**Current State:**
- ✅ Background subprocess execution (good)
- ❌ Memory leaks in acres99 (critical)
- ⚠️ No memory limits or monitoring
- ⚠️ No proper task queue

**Recommended Approach:**
1. **Immediate**: Fix browser lifecycle in acres99
2. **Short-term**: Add memory limits and monitoring
3. **Long-term**: Consider Celery for better task management

**Expected Results:**
- 75% reduction in memory usage
- Stable long-running scrapers (days/weeks)
- No memory-related crashes
- Better resource utilization

