#!/usr/bin/env python3
"""
nalco_scraper.py

Replaces previous nalco scraper script.

Behavior:
- normal (default): check the Nalco page for the latest Ingots PDF, download if new,
  parse IE07 row, append events, rebuild a daily sheet up to today and write data/nalco_prices.xlsx.
- --backfill: parse all PDFs present in pdfs/ to build event list and rebuild daily sheet.
- --repair: read the existing data/nalco_prices.xlsx and convert/migrate old format into the
  new event/daily format, then rebuild daily sheet (does not fetch new PDFs).

Notes:
- The Nalco page URL can be provided with --page-url. If not provided, the script will try
  a reasonable default (please change if your target page is different).
- PDFs are stored in pdfs/ and the Excel in data/nalco_prices.xlsx by default.
- The script uses pdfplumber to extract text/tables. Parsing PDFs is inherently brittle;
  the parser tries multiple heuristics to find the IE07 row.
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import shutil
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
from dateutil import parser as dateparser

PDFS_DIR_DEFAULT = "pdfs"
DATA_DIR_DEFAULT = "data"
DATA_FILE_DEFAULT = os.path.join(DATA_DIR_DEFAULT, "nalco_prices.xlsx")
NALCO_PAGE_DEFAULT = "https://www.nalcoindia.com/Investor/Price-Circulars"  # update if wrong

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "nalco-scraper/1.0 (+https://github.com/)"})

def ensure_dirs(*dirs):
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def fetch_latest_pdf_link(page_url: str) -> Optional[str]:
    """
    Fetch the page and try to find the latest PDF link for Ingots/IE07.
    Returns an absolute URL or None.
    """
    try:
        r = SESSION.get(page_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch page {page_url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    # Find all PDF links
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            text = (a.get_text() or "").strip()
            links.append((href, text))

    if not links:
        return None

    # Prioritize links that mention IE07 or Ingots (case-insensitive)
    priority = []
    for href, text in links:
        score = 0
        txt = f"{text} {href}".lower()
        if "ie07" in txt or "ie 07" in txt:
            score += 100
        if "ingot" in txt or "ingots" in txt:
            score += 90
        # some filenames/dates might be present
        if re.search(r"\d{4}", href) or re.search(r"\d{4}", text):
            score += 1
        priority.append((score, href, text))

    priority.sort(reverse=True, key=lambda x: x[0])
    best = priority[0][1]
    # convert relative to absolute
    absolute = requests.compat.urljoin(page_url, best)
    return absolute

def download_pdf(url: str, out_dir: str) -> Optional[str]:
    fname = os.path.basename(url.split("?")[0])
    # sanitize filename
    fname = re.sub(r"[^A-Za-z0-9._-]", "_", fname)
    out_path = os.path.join(out_dir, fname)
    if os.path.exists(out_path):
        print(f"PDF already exists: {out_path}")
        return out_path
    try:
        r = SESSION.get(url, stream=True, timeout=60)
        r.raise_for_status()
        with open(out_path, "wb") as fh:
            shutil.copyfileobj(r.raw, fh)
        print(f"Downloaded PDF to {out_path}")
        return out_path
    except Exception as e:
        print(f"Failed to download {url}: {e}", file=sys.stderr)
        return None

def parse_date_from_text(text: str) -> Optional[date]:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, dayfirst=False, fuzzy=True)
        if isinstance(dt, datetime):
            return dt.date()
        return None
    except Exception:
        return None

def extract_ie07_from_pdf(pdf_path: str, circular_link: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Attempts to extract the IE07 row from PDF.
    Heuristics:
      - scan each page for 'IE07' or 'IE 07'
      - when found, try to parse a table row or nearby text into fields
    Returns dict with keys:
      Description, Product Code, Basic Price, Circular Date (date), Circular Link, SourcePDF
    """
    print(f"Parsing PDF: {pdf_path}")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # First try extracting tables and finding a row with IE07
            for page in pdf.pages:
                # try tables
                try:
                    tables = page.extract_tables()
                except Exception:
                    tables = []
                for table in tables:
                    for row in table:
                        if not row:
                            continue
                        row_text = " ".join([str(c) for c in row if c]).lower()
                        if "ie07" in row_text.replace(" ", "") or "ie 07" in row_text:
                            # Found candidate row
                            parsed = row_to_event(row, pdf_path, circular_link)
                            if parsed:
                                return parsed
                # fallback: search for IE07 in text, then attempt regex
                text = page.extract_text() or ""
                if "ie07" in text.replace(" ", "").lower() or "ie 07" in text.lower():
                    parsed = parse_line_around_text(page, pdf_path, circular_link)
                    if parsed:
                        return parsed

            # last resort: search whole document text for IE07 and extract a line
            whole = "\n".join([p.extract_text() or "" for p in pdf.pages])
            m = re.search(r".{0,200}(IE\s*0?7).{0,200}", whole, flags=re.I)
            if m:
                snippet = m.group(0)
                parsed = heuristic_from_snippet(snippet, pdf_path, circular_link)
                if parsed:
                    return parsed
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}", file=sys.stderr)

    print(f"Could not find IE07 row in {pdf_path}", file=sys.stderr)
    return None

def row_to_event(row: List[str], pdf_path: str, circular_link: Optional[str]) -> Optional[Dict[str, Any]]:
    # Normalize row to strings
    cells = [("" if c is None else str(c).strip()) for c in row]
    # try to find index of cell containing IE07
    idx = None
    for i, c in enumerate(cells):
        if re.search(r"ie\s*0?7", c, flags=re.I):
            idx = i
            break
    if idx is None:
        return None

    # Typical table layout guesses:
    # [Description, Product Code, Price, Circular Date]
    # or [SNo, Description, Code, Price, Date]
    # We'll try to map by position relative to idx.
    desc = None
    code = None
    price = None
    cdate = None

    # join full row text and try to extract numbers & date
    row_joined = " | ".join(cells)
    # find a date in the row
    for c in cells:
        d = parse_date_from_text(c)
        if d:
            cdate = d
            break
    # find price-like item (decimal or comma)
    for c in cells:
        if re.search(r"\d[\d,\.]+\d", c):
            # ignore numbers that look like product codes (all digits short)
            if len(re.sub(r"[^\d]", "", c)) >= 2:
                price = re.sub(r"[^\d\.]", "", c)
                break
    # product code pattern - often alphanumeric short
    for c in cells:
        if re.search(r"[A-Za-z]{1,3}\s?\d{1,4}", c) or re.search(r"IE\s*0?7", c, flags=re.I):
            # prefer cells with IE07 or alpha-numeric codes
            code_candidate = c
            if "ie" not in code_candidate.lower():
                code = code_candidate
                break
    # description: everything except code/price/date
    combined = [c for c in cells if c and c not in (price or "", code or "", str(cdate) if cdate else "")]
    desc = combined[0] if combined else cells[idx]

    try:
        basic_price = float(price) / 1000.0 if price else None
    except Exception:
        basic_price = None

    event = {
        "Description": desc,
        "Product Code": code or "",
        "Basic Price": basic_price,
        "Circular Date": cdate,
        "Circular Link": circular_link or "",
        "Source PDF": os.path.basename(pdf_path),
    }
    return event

def parse_line_around_text(page, pdf_path, circular_link):
    text = page.extract_text() or ""
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if re.search(r"ie\s*0?7", ln, flags=re.I):
            # examine this line and next few lines to extract tokens
            window = " ".join(lines[max(0, i-1):i+2])
            return heuristic_from_snippet(window, pdf_path, circular_link)
    return None

def heuristic_from_snippet(snippet: str, pdf_path: str, circular_link: Optional[str]):
    # Try to extract date
    cdate = None
    m = re.search(r"(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})", snippet)
    if m:
        cdate = parse_date_from_text(m.group(1))
    else:
        d = parse_date_from_text(snippet)
        cdate = d
    # price
    m2 = re.search(r"(\d[\d,\.]+\d)", snippet)
    price = None
    if m2:
        price = re.sub(r"[^\d.]", "", m2.group(1))
    # product code
    m3 = re.search(r"([A-Z]{1,3}\s?\d{1,4})", snippet)
    code = m3.group(1) if m3 else ""
    # description - take snippet up to code/price
    desc = re.sub(r"\s+", " ", snippet.strip())
    try:
        basic_price = float(price) / 1000.0 if price else None
    except Exception:
        basic_price = None

    return {
        "Description": desc,
        "Product Code": code,
        "Basic Price": basic_price,
        "Circular Date": cdate,
        "Circular Link": circular_link or "",
        "Source PDF": os.path.basename(pdf_path),
    }

def read_existing_events_from_excel(path: str) -> List[Dict[str, Any]]:
    """
    Read existing Excel and try to infer events.
    If the excel is in old format (one row per circular only) this will convert those rows
    into events. If it's already the new format with events/daily sheets, it will read the
    events sheet if present.
    """
    if not os.path.exists(path):
        return []

    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        print(f"Failed to read existing excel {path}: {e}", file=sys.stderr)
        return []

    events: List[Dict[str, Any]] = []
    # If there's an 'events' or 'circulars' sheet, prefer that
    candidate_sheets = [s for s in xls.sheet_names if s.lower() in ("events", "circulars", "circular", "events_sheet")]
    if candidate_sheets:
        df = pd.read_excel(xls, sheet_name=candidate_sheets[0])
        for _, r in df.iterrows():
            try:
                cdate = r.get("Circular Date", r.get("Date", None))
                if pd.isna(cdate):
                    continue
                if not isinstance(cdate, (datetime, date)):
                    cdate = parse_date_from_text(str(cdate))
                events.append({
                    "Description": r.get("Description", ""),
                    "Product Code": r.get("Product Code", r.get("Product_Code", "")),
                    "Basic Price": float(r.get("Basic Price", r.get("Basic_Price", 0))) if not pd.isna(r.get("Basic Price", None)) else None,
                    "Circular Date": cdate,
                    "Circular Link": r.get("Circular Link", r.get("Link", "")),
                    "Source PDF": r.get("Source PDF", ""),
                })
            except Exception:
                # skip rows we can't parse
                continue
        return events

    # Otherwise try to inspect the first sheet for rows that look like circulars (old format)
    df = pd.read_excel(xls, sheet_name=0)
    # If dataframe has a column 'Date' and only rows where date changes (old format), treat rows as events
    possible_date_cols = [c for c in df.columns if "date" in c.lower()]
    if possible_date_cols:
        date_col = possible_date_cols[0]
        for _, r in df.iterrows():
            try:
                cdate = r.get(date_col)
                if pd.isna(cdate):
                    continue
                if not isinstance(cdate, (datetime, date)):
                    cdate = parse_date_from_text(str(cdate))
                events.append({
                    "Description": r.get("Description", ""),
                    "Product Code": r.get("Product Code", r.get("Product_Code", "")),
                    "Basic Price": float(r.get("Basic Price", r.get("Basic_Price", 0))) if not pd.isna(r.get("Basic Price", None)) else None,
                    "Circular Date": cdate,
                    "Circular Link": r.get("Circular Link", ""),
                    "Source PDF": r.get("Source PDF", ""),
                })
            except Exception:
                # skip unparsable rows
                continue
        return events

    # Last fallback: no usable data
    return []

def build_daily_sheet(events: List[Dict[str, Any]], until: date) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["Date", "Description", "Product Code", "Basic Price", "Circular Date", "Circular Link"])
    # Normalize events and drop those without Circular Date
    normalized = []
    for e in events:
        cdate = e.get("Circular Date")
        if not cdate:
            continue
        if isinstance(cdate, datetime):
            cdate = cdate.date()
        normalized.append({
            "Description": e.get("Description", ""),
            "Product Code": e.get("Product Code", ""),
            "Basic Price": e.get("Basic Price"),
            "Circular Date": cdate,
            "Circular Link": e.get("Circular Link", ""),
        })
    if not normalized:
        return pd.DataFrame(columns=["Date", "Description", "Product Code", "Basic Price", "Circular Date", "Circular Link"])
    # Build events DataFrame sorted by Circular Date ascending
    evdf = pd.DataFrame(normalized)
    evdf = evdf.sort_values("Circular Date").drop_duplicates(subset=["Circular Date", "Product Code"], keep="last")
    start_date = evdf["Circular Date"].min()
    end_date = until
    all_dates = pd.date_range(start=start_date, end=end_date, freq="D").date
    rows = []
    # For faster lookup, iterate through sorted events and forward fill
    current = None
    ev_iter = evdf.to_dict("records")
    ev_idx = 0
    for d in all_dates:
        # advance events while event date <= d
        while ev_idx < len(ev_iter) and ev_iter[ev_idx]["Circular Date"] <= d:
            current = ev_iter[ev_idx]
            ev_idx += 1
        if current:
            rows.append({
                "Date": d,
                "Description": current["Description"],
                "Product Code": current["Product Code"],
                "Basic Price": current["Basic Price"],
                "Circular Date": current["Circular Date"],
                "Circular Link": current["Circular Link"],
            })
        else:
            # no event yet; skip days before first event
            pass
    df = pd.DataFrame(rows)
    return df

def save_to_excel(events: List[Dict[str, Any]], daily_df: pd.DataFrame, path: str):
    ensure_dirs(os.path.dirname(path) or ".")
    # create backup if exists
    if os.path.exists(path):
        backup = f"{path}.bak.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        shutil.copy2(path, backup)
        print(f"Backed up existing {path} to {backup}")

    events_df = pd.DataFrame([{  
        "Description": e.get("Description", ""),  
        "Product Code": e.get("Product Code", ""),  
        "Basic Price": e.get("Basic Price", ""),  
        "Circular Date": e.get("Circular Date", ""),  
        "Circular Link": e.get("Circular Link", ""),  
        "Source PDF": e.get("Source PDF", ""), 
    } for e in events])

    # Ensure Circular Date is datetime-like
    if not events_df.empty:
        events_df["Circular Date"] = pd.to_datetime(events_df["Circular Date"])

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if not events_df.empty:
            events_df.sort_values("Circular Date").to_excel(writer, sheet_name="events", index=False)
        else:
            # write empty events sheet with columns
            pd.DataFrame(columns=["Description", "Product Code", "Basic Price", "Circular Date", "Circular Link", "Source PDF"]).to_excel(writer, sheet_name="events", index=False)

        daily_df_sorted = daily_df.copy()
        if not daily_df_sorted.empty:
            daily_df_sorted["Date"] = pd.to_datetime(daily_df_sorted["Date"])
            daily_df_sorted = daily_df_sorted.sort_values("Date")
        daily_df_sorted.to_excel(writer, sheet_name="daily", index=False)
    print(f"Wrote Excel to {path}")

def load_all_pdfs(pdfs_dir: str) -> List[str]:
    if not os.path.exists(pdfs_dir):
        return []
    files = [os.path.join(pdfs_dir, f) for f in sorted(os.listdir(pdfs_dir)) if f.lower().endswith(".pdf")]
    return files

def parse_all_pdfs(pdfs: List[str], circular_link_template: Optional[str]) -> List[Dict[str, Any]]:
    events = []
    for p in pdfs:
        # For offline PDFs we can't craft a circular link reliably; use empty or a template
        circular_link = None
        if circular_link_template:
            circular_link = circular_link_template + "/" + os.path.basename(p)
        parsed = extract_ie07_from_pdf(p, circular_link)
        if parsed:
            events.append(parsed)
    # deduplicate by Circular Date + Product Code: keep latest by Source PDF name
    seen = {}
    for e in sorted(events, key=lambda x: (x.get("Circular Date") or date.min, x.get("Source PDF") or "")):
        key = (e.get("Circular Date"), e.get("Product Code"))
        seen[key] = e
    out = list(seen.values())
    return out

def main(argv=None):
    parser = argparse.ArgumentParser(description="Nalco IE07 downloader and daily builder")
    parser.add_argument("--page-url", default=os.environ.get("NALCO_PAGE_URL", NALCO_PAGE_DEFAULT),
                        help="Nalco page that lists circular PDFs")
    parser.add_argument("--pdfs-dir", default=os.environ.get("PDFS_DIR", PDFS_DIR_DEFAULT))
    parser.add_argument("--data-file", default=os.environ.get("DATA_FILE", DATA_FILE_DEFAULT))
    parser.add_argument("--backfill", action="store_true", help="Process all existing PDFs in pdfs/ and rebuild daily.")
    parser.add_argument("--repair", action="store_true", help="Rebuild daily from existing Excel (migrate old format).")
    parser.add_argument("--until", default=None, help="End date for daily build (YYYY-MM-DD). Default is today.")
    args = parser.parse_args(argv)

    ensure_dirs(args.pdfs_dir, os.path.dirname(args.data_file) or ".")

    events: List[Dict[str, Any]] = []

    if args.repair:
        print("Running in REPAIR mode: converting existing Excel into events and rebuilding daily.")
        events = read_existing_events_from_excel(args.data_file)
        if not events:
            print("No events found in existing Excel. Repair may not do anything.")
    elif args.backfill:
        print("Running in BACKFILL mode: processing all PDFs in pdfs/ to build events.")
        pdfs = load_all_pdfs(args.pdfs_dir)
        if not pdfs:
            print("No PDFs found in pdfs/ to backfill.")
        events = parse_all_pdfs(pdfs, None)
    else:
        # normal mode: fetch page, download latest PDF if new, parse it and read existing events to augment
        print("Running in NORMAL mode: check page for latest PDF and download if new.")
        latest_link = fetch_latest_pdf_link(args.page_url)
        if latest_link:
            downloaded = download_pdf(latest_link, args.pdfs_dir)
            # parse all existing PDFs to build events (we prefer parsing all to ensure we have all circulars)
            pdfs = load_all_pdfs(args.pdfs_dir)
            events = parse_all_pdfs(pdfs, args.page_url)
        else:
            print("Couldn't find any PDF link on the page. Falling back to existing PDFs in pdfs/")
            pdfs = load_all_pdfs(args.pdfs_dir)
            events = parse_all_pdfs(pdfs, None)

        # also include events from existing excel in case some were there (avoid duplicates)
        existing_from_excel = read_existing_events_from_excel(args.data_file)
        # combine and dedupe by circular date + product code
        combined = existing_from_excel + events
        # reduce duplicates
        seen = {}
        for e in combined:
            key = (e.get("Circular Date"), e.get("Product Code"))
            if key not in seen or (e.get("Source PDF") or "") > (seen[key].get("Source PDF") or ""):
                seen[key] = e
        events = list(seen.values())

    # Build daily sheet up to 'until' (default today)
    until_date = date.today() if not args.until else parse_date_from_text(args.until)
    if not isinstance(until_date, date):
        until_date = date.today()
    daily = build_daily_sheet(events, until_date)
    save_to_excel(events, daily, args.data_file)
    print("Done.")


if __name__ == "__main__":
    main()