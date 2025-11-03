# Docker Setup for Bank APF Scrapers FastAPI

This document provides instructions for running the Bank APF Scrapers FastAPI application in a Docker container.

## Prerequisites

- Docker installed on your system
- Docker Compose (optional, but recommended)
- AWS credentials configured (for S3 upload)

## Environment Variables

Make sure your `.env` file contains:

```
S3_BUCKET_NAME=your-bucket-name
S3_KEY=your/s3/key/prefix/
```

If running in Docker, you may also need to add AWS credentials:

```
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
```

## Building the Docker Image

### Option 1: Using Docker directly

```bash
docker build -t bank-scraper-api .
```

### Option 2: Using Docker Compose

```bash
docker-compose build
```

## Running the Container

### Option 1: Using Docker directly

```bash
docker run --rm \
  -p 8000:8000 \
  --env-file .env \
  -v "$(pwd)/output:/app/output" \
  bank-scraper-api
```

On Windows PowerShell:
```powershell
docker run --rm `
  -p 8000:8000 `
  --env-file .env `
  -v "${PWD}/output:/app/output" `
  bank-scraper-api
```

### Option 2: Using Docker Compose (Recommended)

```bash
docker-compose up
```

To run in detached mode:
```bash
docker-compose up -d
```

To view logs:
```bash
docker-compose logs -f
```

## Using the API

Once the container is running, the API will be available at `http://localhost:8000`

### Available Endpoints

1. **Welcome**
   ```bash
   curl http://localhost:8000/
   ```

2. **Health Check**
   ```bash
   curl http://localhost:8000/health
   ```

3. **List Available Banks**
   ```bash
   curl http://localhost:8000/scripts
   ```

4. **Start a Scraper**
   ```bash
   curl -X POST http://localhost:8000/scrape-apf/ucorealty
   ```
   
   Available banks: `axis`, `canara`, `federal`, `hsbc`, `icici_hfc`, `ucorealty`

5. **Check Status of Running Scrapers**
   ```bash
   curl http://localhost:8000/status
   ```

### Example Usage

Start UCO Realty scraper:
```bash
curl -X POST http://localhost:8000/scrape-apf/ucorealty
```

Response:
```json
{
  "message": "Started APF scraper for 'ucorealty'",
  "pid": 123,
  "log_file": "/app/output/run_ucorealty_20251103_120000.log",
  "run_id": "ucorealty_20251103_120000",
  "note": "Multiple different banks can run in parallel safely. Running the same bank concurrently may cause CSV conflicts."
}
```

Check status:
```bash
curl http://localhost:8000/status
```

## Output

The scraped data will be saved to:
- `output/<bank>_apf_data.csv` - Local CSV files
- `output/run_<bank>_<timestamp>.log` - Scraper execution logs
- S3 bucket (as configured) - JSON format with timestamp

## Troubleshooting

### Browser Issues

If you encounter browser-related errors, the image includes all necessary dependencies for Playwright Chromium. Make sure the build completed successfully.

### AWS Credentials

If S3 upload fails, verify:
1. AWS credentials are correctly set in `.env`
2. The S3 bucket exists and you have write permissions
3. The bucket region matches AWS_DEFAULT_REGION

### Permissions

On Linux/Mac, if you have permission issues with the output directory:
```bash
chmod 777 output/
```

## Image Size

The Docker image is approximately 1.5-2GB due to:
- Python runtime
- Playwright browser (Chromium)
- System dependencies

To reduce size, consider using a multi-stage build or Alpine-based images (though Playwright support for Alpine is limited).

## API Documentation

Once running, visit `http://localhost:8000/docs` for interactive API documentation (Swagger UI).

## Notes

- The API can run multiple scrapers in parallel for different banks
- Running the same bank scraper concurrently may cause CSV file conflicts
- Scrapers run in the background; use the `/status` endpoint to monitor them
- Logs are saved in real-time to `output/run_<bank>_<timestamp>.log`

## Customization

### Change Port

To run on a different port, modify `docker-compose.yml`:
```yaml
ports:
  - "3000:8000"  # Run on port 3000 instead
```

Or with Docker directly:
```bash
docker run -p 3000:8000 bank-scraper-api
```

