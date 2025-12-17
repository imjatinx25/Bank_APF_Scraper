import sys
import subprocess
import os
import time
from pathlib import Path
from datetime import datetime
from typing import Literal
import psutil

from fastapi import FastAPI, HTTPException

# Setup
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
        proc = info.get("process")
        pid = info.get("pid")
        
        # Method 1: Use subprocess.Popen.poll() - most reliable
        # poll() returns None if process is still running, or returncode if finished
        if proc is not None:
            returncode = proc.poll()
            if returncode is not None:
                # Process has finished (returncode is 0 for success, non-zero for error)
                finished.append(run_id)
                continue
        
        # Method 2: Fallback to psutil check (for cases where proc object might be lost)
        # Check if process exists and is actually running (not zombie)
        try:
            process = psutil.Process(pid)
            status = process.status()
            # Check if process is actually running (not zombie, dead, or zombie)
            if status in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                finished.append(run_id)
            elif not process.is_running():
                finished.append(run_id)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Process doesn't exist anymore
            finished.append(run_id)
        except Exception:
            # For any other exception, try to check via poll if we have proc
            if proc is not None:
                try:
                    if proc.poll() is not None:
                        finished.append(run_id)
                except Exception:
                    # If all checks fail, assume process is finished
                    finished.append(run_id)
    
    for run_id in finished:
        info = _active_processes.pop(run_id, None)
        if info:
            # Close log file handle
            if "log_file_handle" in info:
                try:
                    info["log_file_handle"].close()
                except Exception:
                    pass  # File may already be closed
            
            # Clean up subprocess object (just wait for it to finish if still running)
            if "process" in info:
                proc = info["process"]
                try:
                    # If process is still running (shouldn't happen, but just in case)
                    if proc.poll() is None:
                        # Give it a moment, then check again
                        try:
                            proc.wait(timeout=1)
                        except subprocess.TimeoutExpired:
                            # If it's still running after timeout, it might be stuck
                            # Log but don't kill - let the OS handle it
                            pass
                except Exception:
                    pass


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


@app.post("/scrape-99acres")
def start_99acres_scraper():
    """Start the 99acres property scraper"""
    script_name = "acres99_property_scraper.py"
    script_path = BASE_DIR / script_name
    
    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"Scraper file '{script_name}' not found")
    
    # Clean up finished processes
    cleanup_finished_processes()
    
    # Per-run log file with timestamp to ensure uniqueness
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = OUT_DIR / f"run_99acres_{ts}.log"
    
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
    
    # Track this process
    run_id = f"99acres_{ts}"
    _active_processes[run_id] = {
        "bank": "99acres",  # Using "bank" field for consistency, but it's actually a property scraper
        "pid": proc.pid,
        "log_file": str(log_file),
        "log_file_handle": log_file_handle,
        "started_at": ts,
        "process": proc,
    }
    
    return {
        "message": "Started 99acres property scraper",
        "pid": proc.pid,
        "log_file": str(log_file),
        "run_id": run_id,
        "note": "The scraper will save data to 99acres_properties.csv and upload it to S3 upon completion."
    }


@app.post("/scrape-proptiger")
def start_proptiger_scraper():
    """Start the Proptiger property scraper"""
    script_name = "proptiger_property_scraper.py"
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        raise HTTPException(status_code=404, detail=f"Scraper file '{script_name}' not found")

    # Clean up finished processes
    cleanup_finished_processes()

    # Per-run log file with timestamp to ensure uniqueness
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = OUT_DIR / f"run_proptiger_{ts}.log"

    # Open log file in unbuffered mode for real-time logging
    log_file_handle = log_file.open("w", encoding="utf-8", newline="", buffering=1)

    # Launch the scraper as a background subprocess to avoid blocking the API worker
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        [sys.executable, "-u", str(script_path)],
        cwd=str(BASE_DIR),
        stdout=log_file_handle,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        env=env,
    )

    # Track this process
    run_id = f"proptiger_{ts}"
    _active_processes[run_id] = {
        "bank": "proptiger",  # Using "bank" field for consistency, but it's a property scraper
        "pid": proc.pid,
        "log_file": str(log_file),
        "log_file_handle": log_file_handle,
        "started_at": ts,
        "process": proc,
    }

    return {
        "message": "Started Proptiger property scraper",
        "pid": proc.pid,
        "log_file": str(log_file),
        "run_id": run_id,
        "note": "The scraper will save data to output/proptiger_properties.csv."
    }


@app.delete("/stop/{pid_or_run_id}")
def stop_scraper(pid_or_run_id: str):
    """Stop a running scraper by PID or run_id"""
    cleanup_finished_processes()
    
    # Try to find by run_id first
    info = _active_processes.get(pid_or_run_id)
    
    # If not found by run_id, try to find by PID
    if not info:
        try:
            pid = int(pid_or_run_id)
            for run_id, proc_info in _active_processes.items():
                if proc_info.get("pid") == pid:
                    info = proc_info
                    pid_or_run_id = run_id  # Update to use run_id for cleanup
                    break
        except ValueError:
            pass  # Not a valid PID
    
    if not info:
        raise HTTPException(
            status_code=404, 
            detail=f"Process with PID or run_id '{pid_or_run_id}' not found or not running"
        )
    
    proc = info.get("process")
    pid = info.get("pid")
    run_id = pid_or_run_id
    bank = info.get("bank", "unknown")
    
    killed = False
    error_message = None
    
    # Method 1: Try to terminate via subprocess.Popen object
    if proc is not None:
        try:
            if proc.poll() is None:  # Process is still running
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                    killed = True
                except subprocess.TimeoutExpired:
                    # Process didn't terminate gracefully, force kill
                    proc.kill()
                    try:
                        proc.wait(timeout=2)
                        killed = True
                    except subprocess.TimeoutExpired:
                        error_message = "Process did not terminate after kill signal"
        except Exception as e:
            error_message = f"Error terminating process: {str(e)}"
    
    # Method 2: Fallback to psutil to kill process and children
    if not killed:
        try:
            process = psutil.Process(pid)
            # Kill all child processes first (like Chrome driver)
            try:
                children = process.children(recursive=True)
                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                
                # Wait a bit for children to terminate
                time.sleep(1)
                
                # Force kill any remaining children
                for child in process.children(recursive=True):
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
            except psutil.NoSuchProcess:
                pass
            
            # Now kill the main process
            try:
                process.terminate()
                try:
                    process.wait(timeout=5)
                    killed = True
                except psutil.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                    killed = True
            except psutil.NoSuchProcess:
                killed = True  # Already dead
        except psutil.NoSuchProcess:
            killed = True  # Process already finished
        except Exception as e:
            error_message = f"Error killing process: {str(e)}"
    
    # Clean up tracking
    if run_id in _active_processes:
        info = _active_processes.pop(run_id)
        # Close log file handle
        if "log_file_handle" in info:
            try:
                info["log_file_handle"].close()
            except Exception:
                pass
    
    if killed:
        return {
            "message": f"Successfully stopped scraper '{bank}' (PID: {pid}, run_id: {run_id})",
            "pid": pid,
            "run_id": run_id,
            "status": "stopped"
        }
    else:
        return {
            "message": f"Attempted to stop scraper '{bank}' (PID: {pid}, run_id: {run_id})",
            "pid": pid,
            "run_id": run_id,
            "status": "attempted",
            "warning": error_message or "Process may have already finished"
        }


@app.get("/status")
def get_status():
    """Get status of active scraper runs"""
    cleanup_finished_processes()
    
    processes_info = []
    for run_id, info in _active_processes.items():
        proc = info.get("process")
        pid = info.get("pid")
        status_detail = "unknown"
        returncode = None
        
        # Check process status
        if proc is not None:
            returncode = proc.poll()
            if returncode is None:
                status_detail = "running"
            else:
                status_detail = f"finished (exit code: {returncode})"
        else:
            # Fallback to psutil if proc object not available
            try:
                process = psutil.Process(pid)
                status = process.status()
                if status == psutil.STATUS_ZOMBIE:
                    status_detail = "zombie (finished)"
                elif status == psutil.STATUS_DEAD:
                    status_detail = "dead (finished)"
                elif process.is_running():
                    status_detail = "running"
                else:
                    status_detail = "finished"
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                status_detail = "not found (finished)"
            except Exception:
                status_detail = "unknown"
        
        processes_info.append({
            "run_id": run_id,
            "bank": info["bank"],
            "pid": pid,
            "log_file": info["log_file"],
            "started_at": info["started_at"],
            "status": status_detail,
            "returncode": returncode,
        })
    
    return {
        "active_runs": len(_active_processes),
        "processes": processes_info
    }


