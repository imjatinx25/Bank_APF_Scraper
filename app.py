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
APF_DIR = BASE_DIR / "APF_Scripts"
PROP_DIR = BASE_DIR / "Property_Scripts"

OUT_DIR = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

# Track active scraper processes
_active_processes: dict[str, dict] = {}

# Log rotation settings
MAX_LOGS = 20


# Map friendly bank names to script files in this folder
BANK_TO_SCRIPT = {
    "axis": "axisbank.py",
    "canara": "canarabank.py",
    "federal": "federal_bank.py",
    "hsbc": "hsbc_bank.py",
    "icici_hfc": "icici_hfc.py",
    "ucorealty": "ucorealty_bank.py",
    "pnsbank": "pnsbank.py",
    "yesbank": "yesbank.py"
}


PROPERTY_TO_SCRIPT = {
    "acres99": "acres99_property_scraper.py",
    "proptiger" : "proptiger_property_scraper.py"
}


def resolve_script(scraper_type: str, scraper_name: str) -> Path:
    if scraper_type == "apf":
        script = BANK_TO_SCRIPT.get(scraper_name.lower())
        if not script:
            raise KeyError(scraper_name)
        path = APF_DIR / script
    else:
        script = PROPERTY_TO_SCRIPT.get(scraper_name.lower())
        if not script:
            raise KeyError(scraper_name)
        path = PROP_DIR / script

    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


def cleanup_finished_processes():
    """Remove finished processes from tracking and close log file handles"""
    finished = []
    for run_id, info in _active_processes.items():
        proc = info.get("process")
        pid = info.get("pid")
        
        # Use subprocess.Popen.poll() - most reliable
        # poll() returns None if process is still running, or returncode if finished
        if proc is not None:
            returncode = proc.poll()
            if returncode is not None:
                # Process has finished (returncode is 0 for success, non-zero for error)
                finished.append(run_id)
                continue
        
        # Fallback to psutil check (for cases where proc object might be lost)
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
    
    # Rotate log files
    rotate_logs()


def rotate_logs():
    """Keep only the most recent N log files to save space"""
    try:
        # Get all run_*.log files sorted by modification time (newest first)
        log_files = sorted(
            OUT_DIR.glob("run_*.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        if len(log_files) > MAX_LOGS:
            # Get paths of logs currently in use by active processes
            active_log_paths = {info.get("log_file") for info in _active_processes.values()}
            
            # Delete oldest files that aren't active
            for old_log in log_files[MAX_LOGS:]:
                if str(old_log) not in active_log_paths:
                    try:
                        old_log.unlink()
                    except Exception:
                        pass
    except Exception as e:
        print(f"Error rotating logs: {e}")


app = FastAPI(title="Bank APF Scrapers API")

@app.get("/")
def welcome():
    return {"status": 200, "message": "Welcome to the Bank APF Scrapers API"}

# Need health check for this routes
@app.get("/health")
def health():
    # health checks
    return {"status": "ok"}

# check all available scripts
@app.get("/scripts")
def list_scripts():
    return {
        "apf_bank": sorted(BANK_TO_SCRIPT.keys()),
        "property": sorted(PROPERTY_TO_SCRIPT.keys())
    }


@app.post("/scrape-apf/{bank}")
def start_apf_scraper(bank):
    try:
        script_path = resolve_script("apf", bank)
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


@app.post("/scrape-property/{property}")
def start_property_scraper(property):
    """Start the property scraper"""
    try:
        script_path = resolve_script("property", property)
    except (KeyError, FileNotFoundError):
        raise HTTPException(status_code=404, detail=f"Unknown or missing scraper for property '{property}'")
    
    # Clean up finished processes
    cleanup_finished_processes()
    
    # Per-run log file with timestamp to ensure uniqueness
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = OUT_DIR / f"run_{property}_{ts}.log"
    
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
    run_id = f"{property}_{ts}"
    _active_processes[run_id] = {
        "property": property,
        "pid": proc.pid,
        "log_file": str(log_file),
        "log_file_handle": log_file_handle,
        "started_at": ts,
        "process": proc,
    }
    
    return {
        "message": f"Started {property} property scraper",
        "pid": proc.pid,
        "log_file": str(log_file),
        "run_id": run_id,
        "note": f"The scraper will save data to {property}_properties.csv and upload it to S3 upon completion."
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
    
    # Primary Method: Use psutil to kill the process and all its children (browsers, drivers, etc.)
    try:
        process = psutil.Process(pid)
        
        # Kill all child processes (recursively) first
        try:
            children = process.children(recursive=True)
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            
            # Wait for children to terminate
            psutil.wait_procs(children, timeout=3)
            
            # Force kill any remaining children
            for child in children:
                try:
                    if child.is_running():
                        child.kill()
                except psutil.NoSuchProcess:
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        # Now kill the main process
        try:
            process.terminate()
            try:
                process.wait(timeout=3)
                killed = True
            except psutil.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
                killed = True
        except psutil.NoSuchProcess:
            killed = True  # Already dead
            
    except psutil.NoSuchProcess:
        killed = True  # Main process already finished
    except Exception as e:
        error_message = f"Error killing process: {str(e)}"
    
    # Method 2 (Fallback): Try to terminate via subprocess.Popen object if not already marked as killed
    if not killed and proc is not None:
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                    killed = True
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)
                    killed = True
        except Exception as e:
            error_message = f"Error in fallback termination: {str(e)}"
    
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
        
        if info.get("bank") != None:
            scraper_name = info["bank"]
        else:
            scraper_name = info["property"]

        processes_info.append({
            "run_id": run_id,
            "scraper_name": scraper_name,
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


