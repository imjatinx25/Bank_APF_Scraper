# scrape_ucorealty_all_states_final_fixed.py
import csv
import re
from pathlib import Path
from typing import Optional, Union, List, Tuple, Set
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Frame
import os
import json
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

BASE = "https://ucorealty.uco.bank.in/Project-List.aspx"
OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
CSV_PATH = OUT_DIR / "ucorealty_apf_data.csv"

# Selectors
GRID      = "#ctl00_ContentPlaceHolder1_DgProject"
DDL       = "#ctl00_ContentPlaceHolder1_ddlStateSearch"
BTN       = "#ctl00_ContentPlaceHolder1_BtnSearch"
LBL_EMPTY = "#ctl00_ContentPlaceHolder1_lblgrid"
PANEL     = "#ctl00_ContentPlaceHolder1_UpdatePanel1"

# Timings (tune if needed)
WAIT_AFTER_SEARCH_MS      = 1400
WAIT_AFTER_PAGER_MS       = 1400
WAIT_BEFORE_CLICK_MS      = 200
WAIT_AFTER_CLICK_MS       = 900
WAIT_AFTER_MODAL_READY_MS = 250
EXPECT_POPUP_TIMEOUT_MS   = 4000
MODAL_FIELD_TIMEOUT_MS    = 9000
FINGERPRINT_TIMEOUT_MS    = 18000
MAX_CLICK_RETRIES         = 3

FIELDNAMES = [
    "state",
    "project_name",
    "builder_name",
]

PopupCtx = Union[Page, Frame]

# ---------------- utils ----------------
def tidy(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

def numtext(s: Optional[str]) -> str:
    s = (s or "")
    return "".join(ch for ch in s if ch.isdigit())

def sleep(page: Page, ms: int):
    page.wait_for_timeout(ms)

def dismiss_overlays(page: Page):
    # Sidebar/overlay sometimes appears; dismiss gently.
    try:
        page.mouse.click(5, 5)
        page.keyboard.press("Escape")
    except Exception:
        pass

# ---------------- state/options ----------------
def get_state_options(page: Page) -> List[Tuple[str, str]]:
    # Wait for dropdown to be ready before fetching options
    try:
        print("    [DEBUG] Waiting for dropdown selector...")
        page.wait_for_selector(DDL, state="visible", timeout=30000)
        print("    [DEBUG] Dropdown found, waiting for options to populate...")
        sleep(page, 1000)  # Give dropdown time to populate (may be populated via JS)
    except PWTimeout:
        print("    [!] Dropdown not found when fetching state options")
        return []
    
    # Try to check if dropdown exists and has options
    dropdown_exists = page.locator(DDL).count() > 0
    print(f"    [DEBUG] Dropdown element exists: {dropdown_exists}")
    
    if not dropdown_exists:
        print("    [!] Dropdown element not found in DOM")
        return []
    
    opts = page.locator(f"{DDL} > option")
    # Wait a bit more for options to be fully loaded
    sleep(page, 500)
    
    items = []
    option_count = opts.count()
    print(f"    [DEBUG] Found {option_count} dropdown options total")
    
    if option_count == 0:
        print("    [!] No options found in dropdown - page may not have loaded correctly")
        # Try waiting a bit more
        sleep(page, 2000)
        option_count = opts.count()
        print(f"    [DEBUG] After additional wait: {option_count} options")
    
    for i in range(option_count):
        o = opts.nth(i)
        try:
            label = tidy(o.text_content() or "")
            val = (o.get_attribute("value") or "").strip()
            print(f"    [DEBUG] Option {i}: label='{label}', value='{val}'")
            if label and val and label.upper() != "SELECT STATE":
                items.append((label, val))
        except Exception as e:
            print(f"    [!] Error reading option {i}: {e}")
            continue
    
    print(f"    [DEBUG] Returning {len(items)} valid state options")
    return items

def wait_for_results_or_empty(page: Page, timeout=25000) -> str:
    page.wait_for_function(f"""
        () => {{
          const g = document.querySelector('{GRID}');
          const m = document.querySelector('{LBL_EMPTY}');
          const mt = (m?.textContent||'').toLowerCase();
          return (g && g.offsetParent !== null) || (mt.includes('no records found'));
        }}
    """, timeout=timeout)
    if page.locator(GRID).is_visible(): return "grid"
    if "no records found" in tidy(page.locator(LBL_EMPTY).inner_text()).lower(): return "empty"
    return "unknown"

def wait_grid_visible(page: Page, timeout=10000):
    page.wait_for_selector(GRID, state="visible", timeout=timeout)

def wait_rows_present(page: Page, timeout=14000):
    r"""
    Wait until:
      - grid has >0 rows, OR
      - pager shows at least one page number, OR
      - 'No Records Found' is visible.

    (Raw docstring so '\d' in the JS regex below doesn't trigger a Python warning.)
    """
    page.wait_for_function(
        r"""(gridSel, panelSel, emptySel) => {
            const grid = document.querySelector(gridSel);
            if (!grid) return false;

            // rows present?
            const body = grid.querySelector('tbody');
            const rowsCount = (body ? body.querySelectorAll('tr').length
                                    : grid.querySelectorAll('tr').length);
            if (rowsCount > 0) return true;

            // pager numbers present? (scope to UpdatePanel)
            const scope = document.querySelector(panelSel) || document;
            const texts = Array.from(scope.querySelectorAll('a,span'))
              .map(el => (el.textContent || '').trim());
            if (texts.some(s => /^\d+$/.test(s))) return true;

            // explicit empty?
            const m = document.querySelector(emptySel);
            const mt = (m?.textContent || '').toLowerCase();
            return mt.includes('no records found');
        }""",
        arg=[GRID, PANEL, LBL_EMPTY],
        timeout=timeout
    )

# ---------------- pagination ----------------
def grid_fingerprint(page: Page) -> str:
    return page.evaluate(
        """sel => {
            const t = document.querySelector(sel);
            if (!t) return '';
            const node = t.querySelector('tbody') || t;
            const txt = (node.innerText || '').trim();
            return txt.slice(0, 6000);
        }""",
        GRID
    ) or ""

def wait_grid_changed(page: Page, prev: str, timeout=FINGERPRINT_TIMEOUT_MS):
    page.wait_for_function(
        """(args) => {
            const [sel, prev] = args;
            const t = document.querySelector(sel);
            if (!t) return false;
            const node = t.querySelector('tbody') || t;
            const now = (node.innerText || '').trim();
            return now && now !== prev;
        }""",
        arg=[GRID, prev], timeout=timeout
    )

def _pager_scopes(page: Page):
    return [page.locator(GRID), page.locator(PANEL), page.locator("body")]

def current_page_number(page: Page) -> Optional[int]:
    for scope in _pager_scopes(page):
        span_nums = scope.locator("span").filter(has_text=re.compile(r"^\s*\d+\s*$"))
        if span_nums.count():
            try: return int(span_nums.first.inner_text().strip())
            except Exception: return None
    return None

def click_next_if_any(page: Page) -> bool:
    """
    Advance strictly forward:
      - detect current and max page numbers
      - if at last page -> False
      - else click (current+1)
      - fall back to 'Next' only if numbers can't be read
      - NEVER click '2' blindly (avoids 2<->3 ping-pong)
    """
    prev = grid_fingerprint(page)

    # Find current page number
    curr = None
    for scope in _pager_scopes(page):
        span = scope.locator("span").filter(has_text=re.compile(r"^\s*\d+\s*$"))
        if span.count():
            try:
                curr = int(span.first.inner_text().strip())
                break
            except Exception:
                pass

    # Compute max page
    max_page = 0
    for scope in _pager_scopes(page):
        for loc in (scope.locator("a"), scope.locator("span")):
            c = loc.count()
            for i in range(c):
                t = (loc.nth(i).inner_text() or "").strip()
                if t.isdigit():
                    max_page = max(max_page, int(t))

    # If we can't tell where we are, try 'Next' once
    if curr is None:
        for scope in _pager_scopes(page):
            nxt = scope.locator("a").filter(has_text=re.compile(r"^\s*(Next|›|>)\s*$", re.I))
            if nxt.count():
                nxt.first.click()
                sleep(page, WAIT_AFTER_PAGER_MS)
                try:
                    wait_grid_changed(page, prev)
                    return True
                except PWTimeout:
                    return False
        return False

    # Last page? stop here
    if curr >= max_page:
        return False

    # Click (curr + 1) explicitly
    target_text = str(curr + 1)
    for scope in _pager_scopes(page):
        tgt = scope.locator("a").filter(has_text=re.compile(rf"^\s*{target_text}\s*$"))
        if tgt.count():
            tgt.first.click()
            sleep(page, WAIT_AFTER_PAGER_MS)
            try:
                wait_grid_changed(page, prev)
                return True
            except PWTimeout:
                return False

    # Last resort: 'Next'
    for scope in _pager_scopes(page):
        nxt = scope.locator("a").filter(has_text=re.compile(r"^\s*(Next|›|>)\s*$", re.I))
        if nxt.count():
            nxt.first.click()
            sleep(page, WAIT_AFTER_PAGER_MS)
            try:
                wait_grid_changed(page, prev)
                return True
            except PWTimeout:
                return False

    return False

def go_to_page_one(page: Page) -> bool:
    """Ensure pager is on page 1 after Search (GridView PageIndex sometimes sticks)."""
    prev = grid_fingerprint(page)
    for scope in _pager_scopes(page):
        # already on 1?
        span1 = scope.locator("span").filter(has_text=re.compile(r"^\s*1\s*$"))
        if span1.count():
            return True
        # otherwise click '1'
        link1 = scope.locator("a").filter(has_text=re.compile(r"^\s*1\s*$"))
        if link1.count():
            link1.first.click()
            sleep(page, 700)
            try: wait_grid_changed(page, prev, timeout=15000)
            except PWTimeout: pass
            return True
    return False  # single-page grid (no pager)

def ensure_grid_ready(page: Page) -> bool:
    """Robust guard after search/paging/closing popup. Soft-refresh if needed."""
    try:
        wait_grid_visible(page, timeout=8000)
        wait_rows_present(page, timeout=16000)
        return True
    except PWTimeout:
        # soft refresh (safe due to per-state de-dupe)
        try:
            page.click(BTN)
            sleep(page, WAIT_AFTER_SEARCH_MS)
            wait_for_results_or_empty(page, timeout=20000)
            wait_grid_visible(page, timeout=8000)
            wait_rows_present(page, timeout=16000)
            return True
        except Exception:
            return False

# ---------------- popup handling ----------------
def try_open_popup(page: Page, lnk) -> Optional[PopupCtx]:
    """Retries + escalating click strategies; returns Page/Frame that hosts #lblProjectName."""
    for attempt in range(1, MAX_CLICK_RETRIES + 1):
        try: lnk.wait_for(state="visible", timeout=2000)
        except Exception: pass

        dismiss_overlays(page)
        sleep(page, WAIT_BEFORE_CLICK_MS)

        # 1) new window
        try:
            with page.expect_popup(timeout=EXPECT_POPUP_TIMEOUT_MS) as pop_info:
                lnk.click()
            popup = pop_info.value
            try: popup.wait_for_selector("#lblProjectName", timeout=MODAL_FIELD_TIMEOUT_MS)
            except PWTimeout: pass
            sleep(page, WAIT_AFTER_CLICK_MS)
            return popup
        except PWTimeout: pass
        except Exception: pass

        # 2) normal / force / JS click on same page/frame
        try: lnk.click(timeout=1500)
        except Exception:
            try: lnk.click(timeout=1500, force=True)
            except Exception:
                try: lnk.evaluate("el => el.click()")
                except Exception:
                    sleep(page, 250 + attempt * 150); continue

        sleep(page, WAIT_AFTER_CLICK_MS)

        if page.locator("#lblProjectName").count():
            sleep(page, WAIT_AFTER_MODAL_READY_MS); return page
        for fr in page.frames:
            try:
                if fr.locator("#lblProjectName").count():
                    sleep(page, WAIT_AFTER_MODAL_READY_MS); return fr
            except Exception: pass

        sleep(page, 250 + attempt * 150)
    return None

def read_popup(ctx: PopupCtx) -> dict:
    def g(sel: str) -> str:
        loc = ctx.locator(sel)
        return tidy(loc.first.text_content()) if loc.count() else ""
    return {
        "project_name": g("#lblProjectName"),
        "builder_name": g("#lblBuilderName"),
        "city": g("#lblCity"),
        "state_in_popup": g("#lblState"),
        "price_min": numtext(g("#lblBudgetMinRange")),
        "price_max": numtext(g("#lblBudgetMaxRange")),
        "rera_id": g("#lblRERAID"),
        "uco_property_id": g("#lblPropertyID"),
        "possession_in": g("#lblPosessionIn"),
        "bhk": g("#lblTypeOfAvailableUnits"),
        "plot_size": numtext(g("#lblTotalUnits")),
        "towers": numtext(g("#lblAvailableUnits")),
        "amenities": g("#lblAmenities").replace("\n", " "),
        "bua_min_sqft": numtext(g("#lblBuildUpAreaMinRange")),
        "bua_max_sqft": numtext(g("#lblBuildUpAreaMaxRange")),
        "carpet_min_sqft": numtext(g("#lblCarpetAreaMinRange")),
        "carpet_max_sqft": numtext(g("#lblCarpetAreaMaxRange")),
        "carpet_price": numtext(g("#lblAverageRate")),
        "apartments_per_floor": numtext(g("#lblAppartmentPerFloor")),
        "branch_head": g("#lblBranchHead"),
        "branch_email": tidy(g("#lblBranchEmail").replace("[at]", "@").replace("[dot]", ".")),
        "contact_no": numtext(g("#lblContactNo")),
        "email": tidy(g("#lblEmailID").replace("[at]", "@").replace("[dot]", ".")),
        "website": g("#lblWebsite"),
    }

def try_close_popup(ctx: PopupCtx, page: Page):
    if isinstance(ctx, Page) and ctx is not page:
        try: ctx.close(); return
        except Exception: pass
    try:
        page.keyboard.press("Escape")
        sleep(page, 120)
    except Exception:
        pass
    dismiss_overlays(page)
    wait_grid_visible(page, timeout=8000)

# ---------------- per-page ----------------
def process_current_page(page: Page, state_label: str, writer, write_if_new):
    processed_keys: Set[Tuple[int, str, str]] = set()  # (row_index, text, href)
    while True:
        if not ensure_grid_ready(page):
            print("    [!] Grid not ready; skipping remainder of this page")
            return

        rows = page.locator(f"{GRID} tbody tr")
        if rows.count() == 0: rows = page.locator(f"{GRID} tr")

        anchors = []
        for i in range(rows.count()):
            row = rows.nth(i)
            tds = row.locator("td")
            if tds.count() == 0: continue
            first_cell = tidy(tds.first.inner_text())
            if first_cell.lower() in {"s.no", "s no", "serial", "sr.", "srl"}:
                continue
            a = row.locator("td:nth-child(2) a").first
            if not a.count(): continue
            txt  = tidy(a.inner_text())
            if not txt: continue
            href = a.get_attribute("href") or ""
            key  = (i, txt.lower(), href)
            anchors.append((i, a, key, txt))

        page_num = current_page_number(page)
        print(f"    [-] Page {page_num or '?'}: rows={rows.count()} clickable={len(anchors)}")

        clicked_any = False
        for i, a, key, txt in anchors:
            if key in processed_keys: continue

            ctx = try_open_popup(page, a)
            if not ctx:
                processed_keys.add(key)
                continue

            try:
                ctx.wait_for_function(
                    "() => (document.querySelector('#lblProjectName')?.textContent || '').trim().length > 0",
                    timeout=MODAL_FIELD_TIMEOUT_MS
                )
            except PWTimeout:
                pass
            sleep(page, WAIT_AFTER_MODAL_READY_MS)

            rec = read_popup(ctx)
            rec["state"] = state_label
            if write_if_new(rec):
                print(f"      [+] {rec['project_name']} - {rec['builder_name']}")
                clicked_any = True

            processed_keys.add(key)
            try_close_popup(ctx, page)
            if not ensure_grid_ready(page):
                print("    [!] Grid not ready after closing popup; stopping this page")
                return

        if not clicked_any:
            break  # page is exhausted

# ---------------- per-state ----------------
def process_state(page: Page, state_label: str, state_value: str, writer):
    print(f"[>] State: {state_label}")
    
    # Ensure dropdown is ready and visible before selecting
    try:
        page.wait_for_selector(DDL, state="visible", timeout=15000)
        # Wait a bit more for dropdown to be fully interactive
        sleep(page, 300)
    except PWTimeout:
        print(f"    [!] Dropdown not found for {state_label}, trying to reload page...")
        # Reload page if dropdown is not available
        try:
            page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector(DDL, state="visible", timeout=15000)
            sleep(page, 500)
        except PWTimeout:
            print(f"    [!] Failed to reload page or find dropdown for {state_label}, skipping")
            return
    
    try:
        page.select_option(DDL, value=state_value, timeout=20000)
    except PWTimeout:
        print(f"    [!] Timeout selecting option for {state_label}, skipping state")
        return
    except Exception as e:
        print(f"    [!] Error selecting option for {state_label}: {e}, skipping state")
        return
    
    dismiss_overlays(page)
    print("    [-] clicking Search...")
    page.click(BTN)

    print("    [-] waiting for results or empty...")
    try:
        status = wait_for_results_or_empty(page, timeout=25000)
    except PWTimeout:
        print("    [!] Timeout waiting results"); return
    if status == "empty":
        print("    [-] No Records Found"); return
    if status != "grid":
        print("    [-] Unknown result state; skipping"); return

    sleep(page, WAIT_AFTER_SEARCH_MS)

    # Always reset pager to Page 1 for each new state
    if go_to_page_one(page):
        print("    [-] Pager set to Page 1")
    else:
        print("    [-] Single-page result (no pager)")

    print("    [-] waiting for grid rows to appear...")
    if not ensure_grid_ready(page):
        print("    [!] Grid not ready after search; skipping state")
        return
    print("    [OK] grid ready")

    # per-state de-dupe
    written_keys: Set[Tuple[str, str]] = set()
    def write_if_new(rec: dict) -> bool:
        key = (
            rec.get("project_name", "").lower(),
            rec.get("builder_name", "").lower(),
        )
        if rec.get("project_name") and rec.get("builder_name") and key not in written_keys:
            row = {k: rec.get(k, "") for k in FIELDNAMES}
            writer.writerow(row)
            written_keys.add(key)
            return True
        return False

    while True:
        process_current_page(page, state_label, writer, write_if_new)
        prev = grid_fingerprint(page)
        if not click_next_if_any(page):
            break
        sleep(page, WAIT_AFTER_PAGER_MS)
        if not ensure_grid_ready(page):
            print("    [!] Grid not ready after paging; stopping this state")
            break
        try:
            wait_grid_changed(page, prev)
        except PWTimeout:
            break

# ---------------- main ----------------
def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"]) 
        context = browser.new_context(
            viewport={"width": 1366, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            device_scale_factor=1,
            has_touch=False,
            is_mobile=False,
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()
        page.set_default_timeout(30000)
        
        print("[DEBUG] Navigating to base URL...")
        page.goto(BASE, wait_until="networkidle", timeout=120_000)
        print("[DEBUG] Page loaded, dismissing overlays...")
        
        # Dismiss any overlays or consent dialogs that might block the dropdown
        dismiss_overlays(page)
        sleep(page, 1000)
        
        # Try to accept any consent dialogs
        try:
            consent_selectors = [
                "button#onetrust-accept-btn-handler",
                "button[aria-label*='Accept']",
                "button:has-text('Accept')",
                "button:has-text('I Agree')",
                ".cookie-accept",
                "#accept-cookies"
            ]
            for sel in consent_selectors:
                btn = page.locator(sel).first
                if btn.count() > 0 and btn.is_visible():
                    print(f"    [DEBUG] Found consent button: {sel}, clicking...")
                    btn.click()
                    sleep(page, 500)
                    break
        except Exception as e:
            print(f"    [DEBUG] No consent dialog found or error: {e}")
        
        print("[DEBUG] Waiting for dropdown...")
        # Wait extra time for page to fully initialize
        sleep(page, 2000)
        
        options = get_state_options(page)
        print(f"[+] States found: {len(options)}")
        print(f"[+] States: {[l for l,_ in options]}")

        # Ensure CSV header exists once
        if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
            with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()

        # Append rows state-by-state (so data is saved as each state completes)
        for state_label, state_value in options:
            with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                process_state(page, state_label, state_value, writer)

        print(f"[OK] Done. CSV appended at: {CSV_PATH}")
        browser.close()
        # After scraping, process CSV and upload to S3
        try:
            load_dotenv()
            bucket = os.getenv('S3_BUCKET_NAME')
            key_prefix = os.getenv('S3_KEY')
            if not bucket:
                raise ValueError("S3_BUCKET_NAME is not set. Please set it in environment or .env")
            if not key_prefix:
                raise ValueError("S3_KEY is not set. Please set it in environment or .env")

            import pandas as pd
            cols = ["state","project_name","builder_name"]
            df = pd.read_csv(CSV_PATH, names=cols, header=0)
            grouped = {}
            for _, r in df.iterrows():
                grouped.setdefault(r.get("state","Unknown"), []).append({
                    "builderName": r.get("builder_name","Unknown"),
                    "projectName": r.get("project_name","Unknown")
                })

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            s3_key = f"{key_prefix.rstrip('/')}/ucorealty_bank_data_{timestamp}.json"
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=json.dumps(grouped, indent=4).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"Data uploaded to S3: {s3_key}")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
