import os, re, json, pathlib, datetime, argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

import pdfplumber
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

# ---------------- CONFIG ----------------
START_URL = "https://nalcoindia.com/domestic/current-price/"

PDF_DIR  = pathlib.Path("pdfs")
DATA_DIR = pathlib.Path("data")
PDF_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

LATEST_JSON         = DATA_DIR / "latest_nalco_pdf.json"     # {last_pdf_url, download_timestamp, filename}
LAST_PROCESSED_FILE = DATA_DIR / "last_nalco_processed.txt"  # last processed filename (avoid double-parse)
PROCESSED_SET_FILE  = DATA_DIR / "processed_nalco_files.txt" # set of processed filenames (for backfill)
EXCEL_FILE          = DATA_DIR / "nalco_prices.xlsx"

DAILY_COLUMNS = ["Date", "Description", "Product Code", "Basic Price", "Circular Date", "Circular Link"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# Nalco filenames look like .../Ingot-07-08-2025.pdf
FILENAME_DATE_NUMERIC = re.compile(r"(\d{1,2})[-_/\.](\d{1,2})[-_/\.](\d{4})")

# ---------------- HTML & DOWNLOAD ----------------
def get_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text

def find_ingots_pdf_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    # Prefer anchors whose inner <p> reads "Ingots"
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "ingot" in href.lower() and href.lower().endswith(".pdf"):
            if any((t.strip().lower() == "ingots") for t in a.stripped_strings):
                return urljoin(START_URL, href)
    # Fallback: any ingot pdf
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "ingot" in href.lower() and href.lower().endswith(".pdf"):
            return urljoin(START_URL, href)
    return None

def read_latest_json() -> dict:
    if LATEST_JSON.exists():
        try:
            return json.loads(LATEST_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def write_latest_json(pdf_url: str, filename: str):
    obj = {
        "last_pdf_url": pdf_url,
        "download_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "filename": filename
    }
    LATEST_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def download_pdf(pdf_url: str) -> pathlib.Path:
    headers = {"User-Agent": UA, "Accept": "application/pdf,*/*;q=0.9", "Referer": START_URL}
    with requests.get(pdf_url, headers=headers, timeout=60, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        if "application/pdf" not in (r.headers.get("Content-Type","").lower()):
            raise RuntimeError(f"Expected PDF but got {r.headers.get('Content-Type')}")
        name = os.path.basename(urlparse(r.url).path) or "Ingots.pdf"
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{timestamp}_{name}"
        dest = PDF_DIR / fname
        with open(dest, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk: f.write(chunk)
    return dest

# ---------------- PDF PARSE (Nalco IE07) ----------------
def parse_date_from_filename(filename: str) -> str:
    """
    Extract dd.mm.yyyy from Ingot-07-08-2025.pdf (or similar)
    """
    m = FILENAME_DATE_NUMERIC.search(filename)
    if not m:
        return datetime.date.today().strftime("%d.%m.%Y")
    d, mth, y = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    try:
        dt = datetime.date(y, mth, d)
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return datetime.date.today().strftime("%d.%m.%Y")

def divide_thousands(x: str | float | int) -> float | None:
    s = str(x).replace(",", "").strip()
    if not s: return None
    try: return round(float(s)/1000.0, 3)
    except ValueError: return None

def extract_ie07_row(pdf_path: pathlib.Path) -> tuple[str, str, str]:
    """
    Return (description, product_code, raw_price) for the row containing IE07.
    Nalco table typically:
      Col2: Description (e.g., ALUMINIUM INGOT)
      Col3: Product Code (IE07)
      Col4: Basic Price (e.g., 268250)
    """
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []
            for tbl in tables or []:
                for row in tbl:
                    cells = [(c or "").strip() for c in row]
                    upper = [c.upper() for c in cells]
                    if any("IE07" in u for u in upper):
                        # Heuristic mapping
                        code_idx = next((i for i, u in enumerate(upper) if "IE07" in u), None)
                        desc, code, price = None, None, None
                        if code_idx is not None:
                            code = cells[code_idx]
                            # price: last numeric-looking to the right
                            for j in range(len(cells)-1, -1, -1):
                                if re.search(r"\d", cells[j]):
                                    price = cells[j].replace(",", "").strip()
                                    break
                            # description: nearest non-empty to the left
                            for j in range(code_idx-1, -1, -1):
                                if cells[j]:
                                    desc = cells[j]
                                    break
                        if not desc:
                            desc = max(cells, key=lambda c: len(c)) if cells else ""
                        if not price:
                            for c in reversed(cells):
                                if re.search(r"\d", c):
                                    price = c.replace(",", "").strip()
                                    break
                        return (desc or "").strip(), (code or "IE07").strip(), (price or "").strip()
    raise RuntimeError(f"Could not find IE07 row in: {pdf_path.name}")

# ---------------- EVENT/DATES HELPERS ----------------
def _to_date_any(x) -> datetime.date | None:
    """
    Accept strings like '01-11-2025' / '01.11.2025' / '2025-11-01',
    and pandas/Excel date types.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, pd.Timestamp):
        return x.date()
    if isinstance(x, (datetime.date, datetime.datetime)):
        return x.date() if isinstance(x, datetime.datetime) else x
    s = str(x).strip()
    # Try multiple common formats
    for fmt in ("%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # Last resort: pandas parser with dayfirst preference
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(dt):
            return dt.date()
    except Exception:
        pass
    return None

def _fmt_date_dash(d: datetime.date) -> str:
    return d.strftime("%d-%m-%Y")  # Date column

def _fmt_date_dot(d: datetime.date) -> str:
    return d.strftime("%d.%m.%Y")  # Circular Date column

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # Strip spaces/newlines and unify common variants
    mapping = {}
    for c in df.columns:
        k = str(c).strip().replace("\n", " ").replace("\r", " ")
        mapping[c] = k
    df = df.rename(columns=mapping)
    # Known alias normalizations (if any)
    aliases = {
        "Sl.no.": "Sl.no.",
        "Sl No": "Sl.no.",
        "Sl No.": "Sl.no.",
        "Product code": "Product Code",
        "product code": "Product Code",
        "Basic price": "Basic Price",
        "basic price": "Basic Price",
        "Circular date": "Circular Date",
        "circular date": "Circular Date",
        "Circular link": "Circular Link",
        "circular link": "Circular Link",
    }
    for k, v in list(df.columns.map(lambda x: (x, aliases.get(x, x)))):
        if k != v:
            df = df.rename(columns={k: v})
    return df

def load_events_from_excel_if_any() -> list[dict]:
    """
    Read current Excel and collapse to unique circular events:
      - If old 'Sl.no.' format -> take each row as an event.
      - If already daily -> keep last per Circular Date.
    Each event: {desc, code, price, cdate (date), clink}
    """
    if not EXCEL_FILE.exists():
        print("[INFO] Excel not found; no existing events.")
        return []
    df = pd.read_excel(EXCEL_FILE)
    df = _normalize_headers(df)
    cols = list(df.columns)
    print(f"[INFO] Loaded Excel with columns: {cols}")

    # Determine mode by presence of "Sl.no." vs "Date"
    if "Sl.no." in cols and "Date" not in cols:
        keep = ["Description","Product Code","Basic Price","Circular Date","Circular Link"]
        for k in keep:
            if k not in df.columns: df[k] = pd.NA
        ev = df[keep].copy()
        print(f"[INFO] Detected OLD format with {len(ev)} rows.")
    else:
        # Already daily
        keep = ["Description","Product Code","Basic Price","Circular Date","Circular Link"]
        for k in keep:
            if k not in df.columns: df[k] = pd.NA
        ev = df[keep].copy()
        before = len(ev)
        ev = ev.sort_values(by="Circular Date").drop_duplicates(subset=["Circular Date"], keep="last")
        print(f"[INFO] Detected DAILY format. Collapsed {before}→{len(ev)} unique Circular Dates.")

    # Coerce types
    ev["Basic Price"] = pd.to_numeric(ev["Basic Price"], errors="coerce")
    ev["Circular Date DT"] = ev["Circular Date"].apply(_to_date_any)
    ev = ev.dropna(subset=["Circular Date DT"]).sort_values("Circular Date DT")

    events = []
    for _, r in ev.iterrows():
        events.append({
            "desc": (r.get("Description", "") or "").strip(),
            "code": (r.get("Product Code", "") or "IE07").strip(),
            "price": float(r.get("Basic Price")) if pd.notna(r.get("Basic Price")) else None,
            "cdate": r["Circular Date DT"],
            "clink": (r.get("Circular Link", "") or "").strip(),
        })
    print(f"[INFO] Loaded {len(events)} event(s) from Excel.")
    return events

def add_event(events: list[dict], desc: str, code: str, price: float, circular_date_str: str, link: str):
    dt = _to_date_any(circular_date_str)
    if not dt:
        print(f"[WARN] Bad circular date: {circular_date_str!r}")
        return events
    # replace any same-date event with newer info
    events = [e for e in events if e["cdate"] != dt]
    events.append({"desc": desc, "code": code or "IE07", "price": price, "cdate": dt, "clink": link or ""})
    events.sort(key=lambda e: e["cdate"])
    return events

def build_daily_from_events(events: list[dict], end_date: datetime.date | None = None) -> pd.DataFrame:
    if not events:
        print("[INFO] No events to build daily series from.")
        return pd.DataFrame(columns=DAILY_COLUMNS)
    events = sorted(events, key=lambda e: e["cdate"])
    start = events[0]["cdate"]
    today = end_date or datetime.date.today()
    if start > today:
        start = today

    rows = []
    idx = 0
    current = events[0]
    for d in (start + datetime.timedelta(n) for n in range((today - start).days + 1)):
        while idx + 1 < len(events) and events[idx + 1]["cdate"] <= d:
            idx += 1
            current = events[idx]
        rows.append({
            "Date": _fmt_date_dash(d),
            "Description": current["desc"],
            "Product Code": current["code"] or "IE07",
            "Basic Price": round(float(current["price"]), 3) if current["price"] is not None else None,
            "Circular Date": _fmt_date_dot(current["cdate"]),
            "Circular Link": current["clink"],
        })
    df = pd.DataFrame(rows)
    df["DateDT"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
    df = df.sort_values(by="DateDT", ascending=False).drop(columns=["DateDT"])
    print(f"[INFO] Built daily series with {len(df)} rows "
          f"({start.strftime('%d.%m.%Y')} → {today.strftime('%d.%m.%Y')}).")
    return df[DAILY_COLUMNS]

# ---------------- WRITE EXCEL ----------------
def save_excel_formatted(df: pd.DataFrame, path: pathlib.Path):
    df.to_excel(path, index=False)
    wb = load_workbook(path); ws = wb.active
    center = Alignment(horizontal="center", vertical="center")
    for cidx, cname in enumerate(df.columns, start=1):
        max_len = len(str(cname))
        for v in df[cname].astype(str).values:
            max_len = max(max_len, len(v))
        ws.column_dimensions[get_column_letter(cidx)].width = max(12, min(max_len + 2, 80))
    price_col_idx = DAILY_COLUMNS.index("Basic Price") + 1
    for r in range(2, ws.max_row + 1):
        ws.cell(row=r, column=price_col_idx).number_format = "0.000"
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            ws.cell(row=r, column=c).alignment = center
    # hyperlinks
    link_col = DAILY_COLUMNS.index("Circular Link") + 1
    for r in range(2, ws.max_row + 1):
        val = ws.cell(row=r, column=link_col).value
        if isinstance(val, str) and val.startswith("http"):
            ws.cell(row=r, column=link_col).hyperlink = val
    ws.freeze_panes = "A2"
    wb.save(path)

# ---------------- STATE HELPERS ----------------
def load_processed_set() -> set[str]:
    if PROCESSED_SET_FILE.exists():
        return set(x.strip() for x in PROCESSED_SET_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    return set()

def save_processed_set(s: set[str]):
    PROCESSED_SET_FILE.write_text("\n".join(sorted(s)), encoding="utf-8")

# ---------------- MODES ----------------
def run_normal():
    events = load_events_from_excel_if_any()
    html = get_html(START_URL)
    pdf_url = find_ingots_pdf_url(html)
    latest = read_latest_json()
    last_url = latest.get("last_pdf_url")

    if pdf_url and pdf_url != last_url:
        # new circular -> download + parse + add event
        pdf_path = download_pdf(pdf_url)
        write_latest_json(pdf_url, str(pdf_path))
        print(f"[INFO] Downloaded: {pdf_path.name}")

        last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
        if pdf_path.name != last_name:
            desc, code, raw_price = extract_ie07_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")
            cdate = parse_date_from_filename(pdf_path.name)
            events = add_event(events, desc, code or "IE07", price, cdate, pdf_url)
            LAST_PROCESSED_FILE.write_text(pdf_path.name, encoding="utf-8")
            processed = load_processed_set(); processed.add(pdf_path.name); save_processed_set(processed)
            print(f"[INFO] Added event for {cdate}")
        else:
            print("[INFO] Latest PDF already processed; skipping parse.")
    else:
        print("[INFO] No new circular; will forward-fill daily series.")

    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"[INFO] Wrote daily sheet with {len(daily_df)} rows → {EXCEL_FILE}")

def run_backfill():
    events = load_events_from_excel_if_any()
    processed = load_processed_set()
    pdfs = sorted(PDF_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime)  # oldest→newest

    added = 0
    for pdf_path in pdfs:
        if pdf_path.name in processed:
            continue
        try:
            desc, code, raw_price = extract_ie07_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                print(f"[WARN] Could not parse price in {pdf_path.name}; skipping."); continue
            cdate = parse_date_from_filename(pdf_path.name)
            events = add_event(events, desc, code or "IE07", price, cdate, link="")
            processed.add(pdf_path.name); added += 1
        except Exception as e:
            print(f"[WARN] Error processing {pdf_path.name}: {e}")

    save_processed_set(processed)
    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"[INFO] Backfill complete. Added {added} event(s). Rebuilt daily with {len(daily_df)} rows.")

def run_repair():
    events = load_events_from_excel_if_any()
    if not events:
        print("[INFO] No events present to rebuild from.")
        return
    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"[INFO] Repair complete. Rebuilt daily with {len(daily_df)} rows.")

# ---------------- ENTRYPOINT ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", default="false", help="true/false (process all existing PDFs in pdfs/)")
    ap.add_argument("--repair",   default="false", help="true/false (rebuild daily sheet from existing Excel)")
    args = ap.parse_args()

    if str(args.repair).strip().lower() in ("true","1","yes","y"):
        run_repair()
    elif str(args.backfill).strip().lower() in ("true","1","yes","y"):
        run_backfill()
    else:
        run_normal()

if __name__ == "__main__":
    main()
