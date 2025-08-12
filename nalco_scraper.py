import os, sys, time, pathlib, requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

NALCO_URL = "https://nalcoindia.com/domestic/current-price/"
PDF_DIR = pathlib.Path("pdfs")
DATA_DIR = pathlib.Path("data")
LOG_FILE = DATA_DIR / "latest_nalco_pdf.txt"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def ensure_dirs():
    # If someone accidentally committed a *file* named 'pdfs' or 'data', remove it
    for p in (PDF_DIR, DATA_DIR):
        if p.exists() and p.is_file():
            p.unlink()
        p.mkdir(parents=True, exist_ok=True)

def get_html(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text

def find_ingots_pdf_url(html):
    soup = BeautifulSoup(html, "html.parser")
    # Most reliable: anchor whose visible text is 'Ingots'
    for a in soup.find_all("a", href=True):
        txt = (a.get_text(strip=True) or "").lower()
        if txt == "ingots":
            return urljoin(NALCO_URL, a["href"])
    # Fallbacks (rare):
    for a in soup.find_all("a", href=True):
        if a["href"].lower().endswith(".pdf") and "ingot" in a.get_text(strip=True).lower():
            return urljoin(NALCO_URL, a["href"])
    return None

def read_last_url():
    return LOG_FILE.read_text(encoding="utf-8").strip() if LOG_FILE.exists() else ""

def write_last_url(url):
    LOG_FILE.write_text(url or "", encoding="utf-8")

def download_pdf(url):
    # Add Referer to avoid being served an HTML redirect/guard
    headers = {
        "User-Agent": UA,
        "Referer": NALCO_URL,
        "Accept": "application/pdf,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    with requests.get(url, headers=headers, timeout=60, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        # sanity check: sometimes servers return text/html on failure or auth
        if "application/pdf" not in ctype:
            raise RuntimeError(f"Expected PDF but got Content-Type={ctype!r} from {url}")
        fname = os.path.basename(urlparse(r.url).path) or f"nalco_{int(time.time())}.pdf"
        dest = PDF_DIR / fname
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        return dest

def main():
    ensure_dirs()
    html = get_html(NALCO_URL)
    pdf_url = find_ingots_pdf_url(html)
    if not pdf_url:
        print("No Ingots PDF link found on the page.", file=sys.stderr)
        sys.exit(1)

    last = read_last_url()
    if pdf_url == last:
        print("No change in PDF. Skipping download.")
        return

    print(f"New PDF detected: {pdf_url}")
    path = download_pdf(pdf_url)
    write_last_url(pdf_url)
    print(f"Saved to: {path}")

if __name__ == "__main__":
    main()
