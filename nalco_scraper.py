import os
import sys
import time
import pathlib
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

NALCO_URL = "https://nalcoindia.com/domestic/current-price/"
PDF_DIR = pathlib.Path("pdfs")
DATA_DIR = pathlib.Path("data")
LOG_FILE = DATA_DIR / "latest_nalco_pdf.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                  " AppleWebKit/537.36 (KHTML, like Gecko)"
                  " Chrome/124.0.0.0 Safari/537.36"
}

def ensure_dirs():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def find_ingots_pdf_url(html, base_url):
    """
    Find a PDF link associated with 'Ingots':
      1) <a> with href *.pdf whose text contains 'Ingots'
      2) A PDF link near text mentioning 'Ingots'
      3) Fallback: first *.pdf link on the page
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) direct anchor with text mentioning Ingots
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf") and "ingot" in a.get_text(strip=True).lower():
            return urljoin(base_url, href)

    # 2) look for any anchor near nodes containing "Ingots"
    needles = ["ingot", "ingots"]
    for text_node in soup.find_all(string=True):
        t = (text_node or "").strip().lower()
        if any(n in t for n in needles):
            parent = text_node.parent
            if parent:
                for a in parent.find_all("a", href=True):
                    if a["href"].lower().endswith(".pdf"):
                        return urljoin(base_url, a["href"])
                for sib in parent.find_all_next("a", href=True, limit=4):
                    if sib["href"].lower().endswith(".pdf"):
                        return urljoin(base_url, sib["href"])

    # 3) fallback: first PDF link
    for a in soup.find_all("a", href=True):
        if a["href"].lower().endswith(".pdf"):
            return urljoin(base_url, a["href"])

    return None

def read_last_url():
    if LOG_FILE.exists():
        return LOG_FILE.read_text(encoding="utf-8").strip()
    return ""

def write_last_url(url):
    LOG_FILE.write_text(url or "", encoding="utf-8")

def download_pdf(url, dest_dir=PDF_DIR):
    filename = os.path.basename(urlparse(url).path) or f"nalco_{int(time.time())}.pdf"
    dest_path = dest_dir / filename
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)
    return dest_path

def main():
    ensure_dirs()
    resp = requests.get(NALCO_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    pdf_url = find_ingots_pdf_url(resp.text, NALCO_URL)
    if not pdf_url:
        print("No PDF link found on the page.", file=sys.stderr)
        sys.exit(1)

    last_url = read_last_url()
    if pdf_url == last_url:
        print("No change in PDF. Skipping download.")
        return

    print(f"New PDF detected:\n{pdf_url}\nDownloading...")
    path = download_pdf(pdf_url)
    write_last_url(pdf_url)
    print(f"Saved to: {path}")

if __name__ == "__main__":
    main()
