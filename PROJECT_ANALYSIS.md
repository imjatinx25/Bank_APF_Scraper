# Complete Project Analysis: Bank APF & Property Scrapers

## 📁 Project Structure

```
Main/
├── app.py                          # FastAPI main application
├── Dockerfile                       # Container build configuration
├── docker-compose.yml              # Local development setup
├── Jenkinsfile                     # CI/CD pipeline
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables (S3, AWS)
│
├── APF_Scripts/                    # Bank APF Scrapers
│   ├── axisbank.py
│   ├── canarabank.py
│   ├── federal_bank.py
│   ├── hsbc_bank.py
│   ├── icici_hfc.py
│   ├── pnsbank.py
│   └── ucorealty_bank.py
│
├── Property_Scripts/               # Property Listing Scrapers
│   ├── acres99_property_scraper.py
│   └── proptiger_property_scraper.py
│
└── output/                         # Generated files
    ├── *.csv                       # Scraped data
    └── run_*.log                   # Execution logs
```

---

## 🏗️ Architecture Overview

### **Core Components**

1. **FastAPI Application (`app.py`)**
   - RESTful API for triggering scrapers
   - Process management and monitoring
   - Log file handling
   - Health checks

2. **APF Bank Scrapers (`APF_Scripts/`)**
   - Scrape Approved Project Finance data from banks
   - Output: JSON files grouped by city/state
   - Upload to S3 after completion

3. **Property Scrapers (`Property_Scripts/`)**
   - Scrape property listings from real estate websites
   - Output: CSV files with property details
   - Upload to S3 incrementally (after each city)

4. **Infrastructure**
   - Docker containerization
   - Jenkins CI/CD pipeline
   - AWS S3 for data storage
   - ECR for Docker image registry

---

## 🔄 How It Works

### **API Endpoints**

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Welcome message |
| `/health` | GET | Health check |
| `/scripts` | GET | List available scrapers |
| `/scrape-apf/{bank}` | POST | Start bank APF scraper |
| `/scrape-property/{property}` | POST | Start property scraper |
| `/status` | GET | Check running processes |
| `/stop/{pid_or_run_id}` | DELETE | Stop a running scraper |

### **Execution Flow**

1. **API Request** → `POST /scrape-apf/{bank}` or `/scrape-property/{property}`
2. **Script Resolution** → Maps friendly name to script file path
3. **Process Spawning** → Launches Python script as background subprocess
4. **Logging** → Real-time logs written to `output/run_{scraper}_{timestamp}.log`
5. **Data Collection** → Scraper saves to CSV/JSON in `output/` folder
6. **S3 Upload** → Data uploaded to S3 with timestamp
7. **Process Tracking** → API tracks process status via `/status`

### **Scraper Patterns**

#### **APF Bank Scrapers:**
- Use Selenium or Playwright
- Extract project/builder data from bank websites
- Group data by city/state
- Upload as JSON to S3: `{key_prefix}/{bank}_data_{timestamp}.json`

#### **Property Scrapers:**
- Use Selenium with Chrome
- Scrape multiple cities sequentially
- Save incrementally to CSV
- Upload to S3 after each city + final upload
- Format: `{key_prefix}/{property}_properties_{timestamp}.csv`

---

## ⚠️ Issues Identified

### **1. Critical Issues**

#### **A. Dockerfile Missing Script Directories**
```dockerfile
# Current (Line 59):
COPY *.py .

# Problem: Only copies root *.py files, misses APF_Scripts/ and Property_Scripts/
```
**Impact:** Scripts won't be available in container, causing 404 errors

**Fix Required:**
```dockerfile
COPY APF_Scripts/ APF_Scripts/
COPY Property_Scripts/ Property_Scripts/
COPY app.py .
COPY .env .
```

#### **B. Inconsistent Path Handling**
- **acres99**: Uses hardcoded `"output/99acres_properties.csv"` (relative path)
- **proptiger**: Uses `OUT_DIR / "proptiger_properties.csv"` (absolute path)
- **APF scrapers**: Mix of relative and absolute paths

**Impact:** Path issues when running from different directories

#### **C. S3 Key Prefix Hardcoded**
```python
# acres99_property_scraper.py line 784:
key_prefix = "test_apf_apis/"  # Hardcoded, ignores S3_KEY env var
```
**Impact:** Can't change S3 prefix without code change

#### **D. Missing Error Recovery**
- Property scrapers don't handle browser crashes gracefully
- No retry mechanism for S3 uploads
- Process tracking can lose references if API restarts

### **2. Medium Priority Issues**

#### **E. Inconsistent S3 Upload Patterns**
- **APF scrapers**: Upload JSON after completion only
- **Property scrapers**: Upload CSV after each city + final
- **No standardization** across scrapers

#### **F. Log File Management**
- Log files accumulate indefinitely in `output/`
- No rotation or cleanup mechanism
- Can fill disk space over time

#### **G. Process Cleanup**
- `cleanup_finished_processes()` only runs on new requests
- Zombie processes possible if API crashes
- No scheduled cleanup task

#### **H. Missing Validation**
- No validation of S3 credentials before upload
- No check if bucket exists
- No validation of scraper output format

### **3. Code Quality Issues**

#### **I. Code Duplication**
- S3 upload logic duplicated across scrapers
- Similar error handling patterns repeated
- Browser initialization code duplicated

#### **J. Inconsistent Error Messages**
- Mix of `[OK]`, `[WARN]`, `[ERROR]`, `[INFO]` tags
- Some scrapers use emojis (removed in proptiger, but not standardized)
- No structured logging format

#### **K. Missing Type Hints**
- Most functions lack type annotations
- Makes code harder to maintain and debug

#### **L. Global State**
- `driver` variable in acres99 is global (not thread-safe)
- `SEEN_KEYS` in proptiger is global
- Could cause issues if scrapers run concurrently

---

## 💡 Suggestions & Improvements

### **1. Immediate Fixes (High Priority)**

#### **Fix Dockerfile**
```dockerfile
# Copy all necessary files
COPY app.py .
COPY APF_Scripts/ APF_Scripts/
COPY Property_Scripts/ Property_Scripts/
COPY .env .
COPY requirements.txt .
RUN mkdir -p output
```

#### **Standardize Path Handling**
Create a shared config module:
```python
# config.py
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUT_DIR = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

def get_csv_path(scraper_name: str) -> Path:
    return OUT_DIR / f"{scraper_name}_properties.csv"
```

#### **Fix S3 Key Prefix**
```python
# Use environment variable, not hardcoded
key_prefix = os.getenv('S3_KEY') or "test_apf_apis/"
```

### **2. Architecture Improvements**

#### **A. Shared Utilities Module**
Create `utils/` directory:
```
utils/
├── __init__.py
├── s3_upload.py      # Centralized S3 upload logic
├── logging_config.py  # Standardized logging
├── browser_setup.py   # Shared browser initialization
└── path_config.py     # Path management
```

#### **B. Standardized S3 Upload**
```python
# utils/s3_upload.py
def upload_to_s3(
    file_path: Path,
    scraper_name: str,
    file_type: str = "csv",  # or "json"
    upload_after_each: bool = False
) -> bool:
    """Standardized S3 upload with retry logic"""
    # Implementation with retries, validation, etc.
```

#### **C. Process Management Enhancement**
- Add scheduled cleanup task (every 5 minutes)
- Store process metadata in SQLite/JSON file (survives API restarts)
- Add process health monitoring

#### **D. Log Rotation**
```python
# utils/log_rotation.py
def rotate_logs(max_logs: int = 50):
    """Keep only last N log files, archive older ones"""
    # Implementation
```

### **3. Code Quality Improvements**

#### **A. Add Type Hints**
```python
def upload_csv_to_s3(csv_path: Path) -> bool:
    """Upload CSV to S3 with proper typing"""
    ...
```

#### **B. Structured Logging**
```python
import logging

logger = logging.getLogger(__name__)
logger.info("City scraping started", extra={"city": city_name})
logger.error("S3 upload failed", exc_info=True)
```

#### **C. Configuration Management**
```python
# config.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    s3_bucket: str
    s3_key_prefix: str = "test_apf_apis/"
    max_log_files: int = 50
    scraper_timeout: int = 3600
    
    class Config:
        env_file = ".env"

settings = Settings()
```

#### **D. Error Recovery**
```python
# Add retry decorator
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def upload_to_s3(...):
    ...
```

### **4. Monitoring & Observability**

#### **A. Add Metrics**
- Scraper execution time
- Success/failure rates
- Data volume scraped
- S3 upload success rate

#### **B. Health Check Enhancement**
```python
@app.get("/health")
def health():
    return {
        "status": "ok",
        "disk_space": check_disk_space(),
        "active_processes": len(_active_processes),
        "s3_connectivity": test_s3_connection()
    }
```

#### **C. Add Alerting**
- Slack notifications on failures
- Email alerts for critical errors
- Dashboard for monitoring

### **5. Testing & Reliability**

#### **A. Add Unit Tests**
```python
# tests/test_s3_upload.py
def test_upload_to_s3_success():
    ...
```

#### **B. Integration Tests**
- Test API endpoints
- Test scraper execution
- Test S3 upload flow

#### **C. Error Scenarios**
- Test with missing S3 credentials
- Test with full disk
- Test with network failures
- Test with website changes

### **6. Documentation**

#### **A. API Documentation**
- Add OpenAPI/Swagger docs
- Document all endpoints
- Add request/response examples

#### **B. Scraper Documentation**
- Document each scraper's behavior
- List required environment variables
- Document expected output format

#### **C. Deployment Guide**
- Step-by-step deployment instructions
- Troubleshooting guide
- Common issues and solutions

---

## 📊 Current State Summary

### **Strengths:**
✅ Well-structured API with FastAPI  
✅ Good process management and tracking  
✅ Real-time logging  
✅ Incremental data saving (property scrapers)  
✅ S3 integration for data persistence  
✅ Docker containerization  
✅ CI/CD pipeline  

### **Weaknesses:**
❌ Dockerfile doesn't copy script directories  
❌ Inconsistent path handling  
❌ Code duplication across scrapers  
❌ No log rotation  
❌ Hardcoded configuration values  
❌ Limited error recovery  
❌ No structured logging  
❌ Missing type hints  
❌ No automated testing  

### **Risk Assessment:**

| Risk | Severity | Likelihood | Impact |
|------|----------|------------|--------|
| Dockerfile missing scripts | **HIGH** | High | Container won't work |
| Path inconsistencies | **MEDIUM** | Medium | Scripts fail in different environments |
| Log file accumulation | **MEDIUM** | High | Disk space issues |
| S3 upload failures | **MEDIUM** | Low | Data loss |
| Process tracking loss | **LOW** | Low | Can't monitor after restart |

---

## 🎯 Recommended Action Plan

### **Phase 1: Critical Fixes (Week 1)**
1. Fix Dockerfile to copy all script directories
2. Standardize path handling
3. Fix hardcoded S3 key prefix
4. Add log rotation

### **Phase 2: Code Quality (Week 2)**
1. Create shared utilities module
2. Standardize S3 upload logic
3. Add type hints
4. Implement structured logging

### **Phase 3: Reliability (Week 3)**
1. Add retry mechanisms
2. Enhance error handling
3. Add process persistence
4. Improve health checks

### **Phase 4: Monitoring (Week 4)**
1. Add metrics collection
2. Enhance health checks
3. Add alerting
4. Create monitoring dashboard

---

## 📝 Additional Notes

- **Environment Variables Required:**
  - `S3_BUCKET_NAME`
  - `S3_KEY` (optional, defaults to "test_apf_apis/")
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION`

- **Dependencies:**
  - FastAPI for API
  - Selenium for web scraping
  - Playwright for some scrapers
  - boto3 for S3
  - pandas for data processing

- **Deployment:**
  - Jenkins pipeline builds Docker image
  - Pushes to AWS ECR
  - Deploys to production server
  - Runs on port 4000

---

**Analysis Date:** 2025-01-01  
**Analyzed By:** AI Codebase Analysis  
**Version:** 1.0

