# Quick Start - Bank APF Scrapers API

## 🚀 Quick Start (30 seconds)

1. **Build and run the container:**
   ```bash
   docker-compose up --build
   ```

2. **Test the API:**
   ```bash
   curl http://localhost:8000/
   ```

3. **Start a scraper:**
   ```bash
   curl -X POST http://localhost:8000/scrape-apf/ucorealty
   ```

4. **Check status:**
   ```bash
   curl http://localhost:8000/status
   ```

5. **View API documentation:**
   Open in browser: http://localhost:8000/docs

## 📋 Available Banks

| Bank | Endpoint Parameter |
|------|-------------------|
| Axis Bank | `axis` |
| Canara Bank | `canara` |
| Federal Bank | `federal` |
| HSBC Bank | `hsbc` |
| ICICI HFC | `icici_hfc` |
| UCO Realty | `ucorealty` |

## 🔧 Quick Commands

**List all banks:**
```bash
curl http://localhost:8000/scripts
```

**Start multiple scrapers:**
```bash
curl -X POST http://localhost:8000/scrape-apf/axis
curl -X POST http://localhost:8000/scrape-apf/canara
curl -X POST http://localhost:8000/scrape-apf/federal
```

**View logs:**
```bash
# Container logs
docker-compose logs -f

# Scraper logs (on host machine)
tail -f output/run_ucorealty_*.log
```

**Stop the container:**
```bash
docker-compose down
```

## 📁 Output Files

All output is saved to the `output/` directory:
- `<bank>_apf_data.csv` - Scraped data
- `run_<bank>_<timestamp>.log` - Execution logs

## ⚙️ Environment Setup

Make sure `.env` file has:
```
S3_BUCKET_NAME=your-bucket-name
S3_KEY=your/s3/prefix/
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-1
```

## 🐛 Troubleshooting

**Container won't start?**
```bash
# Check logs
docker-compose logs

# Rebuild from scratch
docker-compose down
docker-compose build --no-cache
docker-compose up
```

**API not responding?**
```bash
# Check container status
docker ps

# Check health
curl http://localhost:8000/health
```

**Need to access container shell?**
```bash
docker-compose exec bank-scraper-api /bin/bash
```

## 📖 Full Documentation

See [README-DOCKER.md](README-DOCKER.md) for complete documentation.

