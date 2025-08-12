import os, sys, time, pathlib, re, requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

NALCO_URL = "https://nalcoindia.com/domestic/current-price/"
PDF_DIR = pathlib.Path("pdfs")
DATA_DIR = pathlib.Path("data")
LOG_FILE = DATA_DIR / "latest_nalco_pdf.txt"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# prefer "Ingot-DD-MM-YYYY.pdf" and exclude spec documents
DATEY_PDF_RE = re.compile(r"Ingot-\d{2}-\d{2}-\d{4}\.pdf$", re.IGNORECASE)

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

    # 1) STRICT: <a ...><img ...><p>Ingots</p></a>
    for pnode in soup.find_all("p"):
        if norm(pnode.get_text()) == "ingots":
            a = pnode.find_parent("a", href=True)
            if a:
                href = a["href"].strip()
                if href.lower().endswith(".pdf") and "spec" not in href.lower():
                    return urljoin(NALCO_URL, href)

    # 2) PREFER named pattern Ingot-DD-MM-YYYY.pdf (exclude spec)
    cand = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            abs_url = urljoin(NALCO_URL, href)
            # skip any spec/technical sheets
            if "spec" in href.lower():
                continue
            if DATEY_PDF_RE.search(href):
                cand.append(abs_url)
    if cand:
        # if there are multiple, return the last in the HTML (often latest)
        return cand[-1]

    # 3) LAST RESORT: any PDF link whose anchor text contains "ingot" (not spec)
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
        # honor Content-Disposition filename if present
        filename = None
        cd = r.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            # basic parse
            filename = cd.split("filename=", 1)[1].strip('"; ')
        if not filename:
            filename = os.path.basename(urlparse(r.url).path) or f"nalco_{int(time.time())}.pdf"
        dest = PDF_DIR / filename
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

    print(f"Chosen PDF URL: {pdf_url}")  # <-- visible in workflow logs

    last = read_last_url()
    if pdf_url == last:
        print("No change in PDF. Skipping download.")
        return

    path = download_pdf(pdf_url)
    write_last_url(pdf_url)
    print(f"Saved to: {path}")

if __name__ == "__main__":
    main()
