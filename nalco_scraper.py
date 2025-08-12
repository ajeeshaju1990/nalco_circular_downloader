import os, sys, time, pathlib, re, requests, datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

import pdfplumber
import pandas as pd

NALCO_URL = "https://nalcoindia.com/domestic/current-price/"
PDF_DIR = pathlib.Path("pdfs")
DATA_DIR = pathlib.Path("data")
LOG_FILE = DATA_DIR / "latest_nalco_pdf.txt"
EXCEL_FILE = DATA_DIR / "nalco_prices.xlsx"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

DATEY_PDF_RE = re.compile(r"Ingot-(\d{2})-(\d{2})-(\d{4})\.pdf$", re.IGNORECASE)

def ensure_dirs():
    for p in (PDF_DIR, DATA_DIR):
        if p.exists() and p.is_file():
            p.unlink()
        p.mkdir(parents=True, exist_ok=True)

def get_html(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text

def norm(s: str) -> str:
    return (s or "").strip().lower()

def find_ingots_pdf_url(html):
    soup = BeautifulSoup(html, "html.parser")
    # Strict: <a><img><p>Ingots</p></a>
    for pnode in soup.find_all("p"):
        if norm(pnode.get_text()) == "ingots":
            a = pnode.find_parent("a", href=True)
            if a:
                href = a["href"].strip()
                if href.lower().endswith(".pdf") and "spec" not in href.lower():
                    return urljoin(NALCO_URL, href)

    # Prefer filenames like Ingot-DD-MM-YYYY.pdf (exclude spec)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf") and "spec" not in href.lower():
            if DATEY_PDF_RE.search(href):
                return urljoin(NALCO_URL, href)

    # Last resort: any PDF link whose anchor text contains "ingot" (not spec)
    for a in soup.find_all("a", href=True):
        txt = norm(a.get_text())
        href = a["href"].strip()
        if href.lower().endswith(".pdf") and "ingot" in txt and "spec" not in href.lower():
            return urljoin(NALCO_URL, href)

    return None

def read_last_url():
    return LOG_FILE.read_text(encoding="utf-8").strip() if LOG_FILE.exists() else ""

def write_last_url(url):
    LOG_FILE.write_text(url or "", encoding="utf-8")

def download_pdf(url):
    headers = {
        "User-Agent": UA,
        "Referer": NALCO_URL,
        "Accept": "application/pdf,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    with requests.get(url, headers=headers, timeout=60, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        if "application/pdf" not in ctype:
            raise RuntimeError(f"Expected PDF but got Content-Type={ctype!r} from {url}")
        filename = None
        cd = r.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            filename = cd.split("filename=", 1)[1].strip('"; ')
        if not filename:
            filename = os.path.basename(urlparse(r.url).path) or f"nalco_{int(time.time())}.pdf"
        dest = PDF_DIR / filename
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        return dest

# ---------- PDF PARSE HELPERS ----------

def parse_circular_date_from_filename(pdf_path: pathlib.Path) -> str:
    """Extract DD-MM-YYYY from 'Ingot-DD-MM-YYYY.pdf' else use today."""
    m = DATEY_PDF_RE.search(pdf_path.name)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            d = datetime.date(int(yyyy), int(mm), int(dd))
            return d.strftime("%d-%m-%Y")
        except ValueError:
            pass
    # fallback: today (IST not available here; using UTC date is fine for record)
    return datetime.date.today().strftime("%d-%m-%Y")

def extract_row_ie07(pdf_path: pathlib.Path):
    """
    Find the row that contains 'IE07'.
    Returns dict: { 'Description', 'Product Code', 'Basic Price' }
    Strategy:
      - Try pdfplumber.extract_tables first.
      - If that fails/empty, use word positions to find the line with IE07 and parse neighbors.
    """
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Try structured tables
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []
            for tbl in tables or []:
                # Normalize cells
                for row in tbl:
                    cells = [(c or "").strip() for c in row]
                    if any(re.fullmatch(r"IE07", x, flags=re.IGNORECASE) for x in cells):
                        # Expect columns like: SlNo, Description, ProductCode, BasicPrice, ...
                        # We'll try to map by best guess:
                        desc = ""
                        code = ""
                        price = ""
                        # Find product code index
                        idx_code = None
                        for i, c in enumerate(cells):
                            if re.fullmatch(r"IE07", c, flags=re.IGNORECASE):
                                idx_code = i
                                code = "IE07"
                                break
                        # Description: try the previous meaningful column
                        if idx_code is not None:
                            # look left for a non-empty text that's not a number
                            for j in range(idx_code - 1, -1, -1):
                                if cells[j] and not re.fullmatch(r"\d+(\.\d+)?", cells[j]):
                                    desc = cells[j]
                                    break
                            # Price: try immediate next numeric-like field to the right
                            for j in range(idx_code + 1, len(cells)):
                                if re.search(r"\d", cells[j]):
                                    price = cells[j].replace(",", "")
                                    break
                        if code and price:
                            return {
                                "Description": desc or "ALUMINIUM INGOT",
                                "Product Code": code,
                                "Basic Price": price
                            }

            # Fallback: text line scan around 'IE07'
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
            # Group words by y (line) with small tolerance
            lines = {}
            for w in words:
                y = round(w["top"], 1)
                lines.setdefault(y, []).append(w)
            for y, wlist in lines.items():
                text_line = " ".join([w["text"] for w in sorted(wlist, key=lambda x: x["x0"])])
                if re.search(r"\bIE07\b", text_line, flags=re.IGNORECASE):
                    # Try to capture price (a large integer like 268250) and description left of code
                    # Price: last big number on the line
                    m_price = re.search(r"(\d{5,7}(?:\.\d+)?)\s*$", text_line)
                    price = (m_price.group(1) if m_price else "").replace(",", "")
                    # Description: assume contains 'INGOT' on the same line
                    m_desc = re.search(r"([A-Z ]*INGOT[A-Z ]*)\b", text_line, flags=re.IGNORECASE)
                    desc = (m_desc.group(1) if m_desc else "ALUMINIUM INGOT").strip().upper()
                    if price:
                        return {
                            "Description": desc,
                            "Product Code": "IE07",
                            "Basic Price": price
                        }

    raise RuntimeError("Could not find a row with Product Code IE07 in the PDF.")

def to_thousands(value_str: str) -> float:
    """
    Convert raw price (e.g., '268250') to thousands (e.g., 268.250).
    Returns a float with 3 decimal places preserved on write.
    """
    value_str = value_str.replace(",", "").strip()
    if not value_str:
        return None
    try:
        v = float(value_str)
        return round(v / 1000.0, 3)
    except ValueError:
        return None

def append_to_excel(excel_path: pathlib.Path, row: dict):
    """
    Append a row to Excel with headers:
    Sl.no. | Description | Product Code | Basic Price | Circular Date | Circular Link
    Sl.no. increments by existing row count (excluding header).
    """
    cols = ["Sl.no.", "Description", "Product Code", "Basic Price", "Circular Date", "Circular Link"]

    if excel_path.exists():
        df = pd.read_excel(excel_path)
        # Ensure correct columns/order if file was created manually
        df = df.reindex(columns=cols, fill_value=None)
        next_slno = int(df.shape[0] + 1)
    else:
        df = pd.DataFrame(columns=cols)
        next_slno = 1

    new_row = {
        "Sl.no.": next_slno,
        "Description": row["Description"],
        "Product Code": row["Product Code"],
        "Basic Price": row["Basic Price"],  # already divided by 1000
        "Circular Date": row["Circular Date"],
        "Circular Link": row["Circular Link"],
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    # Keep three decimals in Excel for Basic Price by writing as number; pandas keeps numeric type.
    df.to_excel(excel_path, index=False)

# ---------- MAIN FLOW ----------

def main():
    ensure_dirs()
    html = get_html(NALCO_URL)
    pdf_url = find_ingots_pdf_url(html)
    if not pdf_url:
        print("No Ingots PDF link found on the page.", file=sys.stderr)
        sys.exit(1)

    print(f"Chosen PDF URL: {pdf_url}")

    last = read_last_url()
    if pdf_url == last:
        print("No change in PDF. Skipping download & Excel update.")
        return

    pdf_path = download_pdf(pdf_url)
    write_last_url(pdf_url)
    print(f"Saved to: {pdf_path}")

    # Extract row with IE07 from the PDF
    row = extract_row_ie07(pdf_path)
    # Convert Basic Price to thousands
    thousands = to_thousands(row["Basic Price"])
    if thousands is None:
        raise RuntimeError(f"Could not parse numeric price from: {row['Basic Price']!r}")
    row["Basic Price"] = thousands

    # Circular date: from filename (fallback to today)
    row["Circular Date"] = parse_circular_date_from_filename(pdf_path)
    # Circular link: use the final chosen URL
    row["Circular Link"] = pdf_url

    append_to_excel(EXCEL_FILE, row)
    print(f"Excel updated: {EXCEL_FILE}")

if __name__ == "__main__":
    main()
