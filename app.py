import sys
import subprocess
import os
from pathlib import Path
from datetime import datetime
from typing import Literal
import psutil

from fastapi import FastAPI, HTTPException


BASE_DIR = Path(__file__).parent
OUT_DIR = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

# Track active scraper processes
_active_processes: dict[str, dict] = {}


# Map friendly bank names to script files in this folder
BANK_TO_SCRIPT = {
    "axis": "axisbank.py",
    "canara": "canarabank.py",
    "federal": "federal_bank.py",
    "hsbc": "hsbc_bank.py",
    "icici_hfc": "icici_hfc.py",
    "ucorealty": "ucorealty_bank.py",
}


def resolve_script(bank: str) -> Path:
    script = BANK_TO_SCRIPT.get(bank.lower())
    if not script:
        raise KeyError(bank)
    path = BASE_DIR / script
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


app = FastAPI(title="Bank APF Scrapers API")

@app.get("/")
def welcome():
    return {"status": 200, "message": "Welcome to the Bank APF Scrapers API"}


@app.get("/health")
def health():
    return {"status": "ok"}


def cleanup_finished_processes():
    """Remove finished processes from tracking and close log file handles"""
    finished = []
    for run_id, info in _active_processes.items():
        try:
            # Check if process is still running
            process = psutil.Process(info["pid"])
            if not process.is_running():
                finished.append(run_id)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            finished.append(run_id)
    
    for run_id in finished:
        info = _active_processes.pop(run_id, None)
        if info and "log_file_handle" in info:
            try:
                info["log_file_handle"].close()
            except Exception:
                pass  # File may already be closed


@app.post("/scrape-apf/{bank}")
def start_scrape(bank: Literal["axis","canara","federal","hsbc","icici_hfc","ucorealty"]):
    try:
        script_path = resolve_script(bank)
    except (KeyError, FileNotFoundError):
        raise HTTPException(status_code=404, detail=f"Unknown or missing scraper for bank '{bank}'")

    # Clean up finished processes
    cleanup_finished_processes()

    # Per-run log file with timestamp to ensure uniqueness
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = OUT_DIR / f"run_{bank}_{ts}.log"

    # Open log file in unbuffered mode for real-time logging
    log_file_handle = log_file.open("w", encoding="utf-8", newline="", buffering=1)
    
    # Launch the scraper as a background subprocess to avoid blocking the API worker
    # Use -u flag for unbuffered Python output + PYTHONUNBUFFERED for real-time logs
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    proc = subprocess.Popen(
        [sys.executable, "-u", str(script_path)],  # -u flag = unbuffered stdout/stderr
        cwd=str(BASE_DIR),
        stdout=log_file_handle,
        stderr=subprocess.STDOUT,
        bufsize=1,  # Line buffered for real-time output
        universal_newlines=True,
        env=env,
    )
    
    # Store file handle reference so it stays open during subprocess lifetime
    # The handle will be closed when the process terminates

    # Track this process (store file handle to keep it open)
    run_id = f"{bank}_{ts}"
    _active_processes[run_id] = {
        "bank": bank,
        "pid": proc.pid,
        "log_file": str(log_file),
        "log_file_handle": log_file_handle,  # Keep reference so file stays open
        "started_at": ts,
        "process": proc,  # Keep process reference
    }

    return {
        "message": f"Started APF scraper for '{bank}'",
        "pid": proc.pid,
        "log_file": str(log_file),
        "run_id": run_id,
        "note": "Multiple different banks can run in parallel safely. Running the same bank concurrently may cause CSV conflicts."
    }


@app.get("/scripts")
def list_scripts():
    return {"banks": sorted(BANK_TO_SCRIPT.keys())}


@app.get("/status")
def get_status():
    """Get status of active scraper runs"""
    cleanup_finished_processes()
    return {
        "active_runs": len(_active_processes),
        "processes": [
            {
                "run_id": run_id,
                "bank": info["bank"],
                "pid": info["pid"],
                "log_file": info["log_file"],
                "started_at": info["started_at"],
            }
            for run_id, info in _active_processes.items()
        ]
    }


