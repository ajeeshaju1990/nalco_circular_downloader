import pathlib
import shutil
import sys

import pandas as pd

import nalco_scraper as scraper

MANUAL_PDF_DIR = scraper.BASE_DIR / "manual_pdfs"
PDF_DIR = scraper.PDF_DIR
EXCEL_FILE = scraper.EXCEL_FILE


def github_manual_link(filename: str) -> str:
    return (
        "https://github.com/ajeeshaju1990/nalco_circular_downloader/"
        f"blob/main/manual_pdfs/{filename}"
    )


def load_existing_circulars() -> pd.DataFrame:
    if not EXCEL_FILE.exists():
        return pd.DataFrame(
            columns=["Description", "Product Code", "Basic Price", "Circular Date", "Circular Link"]
        )
    existing = pd.read_excel(EXCEL_FILE)
    return scraper.derive_circulars_from_existing(existing)


def main() -> int:
    MANUAL_PDF_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    manual_pdfs = sorted(
        [p for p in MANUAL_PDF_DIR.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"],
        key=lambda p: p.name.lower(),
    )

    if not manual_pdfs:
        print("No PDFs found in manual_pdfs/. Nothing to backfill.")
        return 0

    circ_df = load_existing_circulars()
    processed = 0
    skipped = 0
    failures = []

    for manual_pdf in manual_pdfs:
        try:
            # Only accept the standard NALCO filename convention so a badly named
            # PDF can never accidentally be assigned today's date.
            date_match = scraper.DATEY_PDF_RE.search(manual_pdf.name)
            if not date_match:
                raise RuntimeError(
                    "Filename must follow Ingot-DD-MM-YYYY.pdf "
                    f"(received {manual_pdf.name!r})"
                )

            circular_date = scraper.parse_circular_date_from_filename(manual_pdf)
            parsed = scraper.extract_row_ie07(manual_pdf)
            price = scraper.to_thousands(parsed.get("Basic Price"))
            if price is None:
                raise RuntimeError(f"Invalid IE07 price: {parsed.get('Basic Price')!r}")

            # Copy into the normal PDF archive so the source PDF remains available
            # alongside automated downloads.
            archived_pdf = PDF_DIR / manual_pdf.name
            if not archived_pdf.exists() or archived_pdf.stat().st_size != manual_pdf.stat().st_size:
                shutil.copy2(manual_pdf, archived_pdf)

            row = {
                "Description": parsed.get("Description", "ALUMINIUM INGOT"),
                "Product Code": parsed.get("Product Code", "IE07"),
                "Basic Price": price,
                "Circular Date": pd.to_datetime(circular_date, dayfirst=True, errors="coerce"),
                "Circular Link": github_manual_link(manual_pdf.name),
            }

            # Upsert by Circular Date. This makes reruns idempotent and lets the
            # manually supplied PDF override an existing value for the same date.
            combined = pd.concat([circ_df, pd.DataFrame([row])], ignore_index=True)
            combined["Circular Date"] = pd.to_datetime(
                combined["Circular Date"], dayfirst=True, errors="coerce"
            )
            combined["Basic Price"] = pd.to_numeric(
                combined["Basic Price"], errors="coerce"
            ).round(3)
            combined = combined.dropna(subset=["Circular Date"])
            circ_df = (
                combined.sort_values("Circular Date")
                .drop_duplicates(subset=["Circular Date"], keep="last")
                .reset_index(drop=True)
            )

            processed += 1
            print(
                f"Backfilled {manual_pdf.name}: "
                f"circular date={circular_date}, IE07={price:.3f}"
            )

        except Exception as exc:
            skipped += 1
            failures.append(f"{manual_pdf.name}: {exc}")
            print(f"[WARN] Could not process {manual_pdf.name}: {exc}", file=sys.stderr)

    # Rebuild the full daily history using every known circular, including the
    # newly supplied manual PDFs.
    daily_df = scraper.build_daily_df_from_circulars(circ_df)
    if not daily_df.empty:
        scraper.save_excel_formatted(daily_df[scraper.DAILY_COLS], EXCEL_FILE)

    print(
        f"Manual backfill complete: processed={processed}, failed={skipped}, "
        f"circulars={len(circ_df)}, daily rows={len(daily_df)}"
    )

    if failures:
        print("Failed files:", file=sys.stderr)
        for item in failures:
            print(f" - {item}", file=sys.stderr)

    # Deliberately do not fail the workflow because one optional manual PDF was bad.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
