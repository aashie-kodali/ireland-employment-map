"""
scripts/research_company_sectors.py
=====================================
One-time utility that generates a CSV template for mapping the top 150
companies (by total permits issued 2015–2025) to their NACE sector.

This script is NOT part of the main pipeline (not in Makefile / make data).
Run it once to produce the template, then fill in the 'sector' and 'source'
columns manually or with web research before running 'make data'.

Output: data/raw/company_sector_map.csv
  Columns: company_name_clean, total_permits, sector, source

The 'sector' values must exactly match one of the 24 canonical NACE sector
names used in data/cleaned/sector_permits.csv (2020+ format), e.g.:
  Q - Health & Social Work Activities
  J - Information & Communication Activities
  K - Financial & Insurance Activities
  etc.

Safety: refuses to overwrite an existing company_sector_map.csv unless
--force is passed on the command line.

Usage:
  python scripts/research_company_sectors.py          # safe (no overwrite)
  python scripts/research_company_sectors.py --force  # overwrite existing
"""

import sys
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
COMPANY_CSV  = ROOT / "data" / "cleaned" / "company_permits.csv"
OUTPUT_CSV   = ROOT / "data" / "raw" / "company_sector_map.csv"

TOP_N = 150   # number of companies to include in the research template


def main(force: bool = False) -> None:
    if not COMPANY_CSV.exists():
        sys.exit(
            f"ERROR: {COMPANY_CSV} not found.\n"
            "Run 'python src/01_clean_data.py' and 'python src/05_clean_companies.py' first."
        )

    if OUTPUT_CSV.exists() and not force:
        sys.exit(
            f"ERROR: {OUTPUT_CSV} already exists.\n"
            "Pass --force to overwrite (WARNING: this will erase any research already done)."
        )

    df = pd.read_csv(COMPANY_CSV)

    # Aggregate total permits per company across all years
    totals = (
        df.groupby("company_name_clean", as_index=False)["issued"]
        .sum()
        .rename(columns={"issued": "total_permits"})
        .sort_values("total_permits", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )

    # Add blank columns for the researcher to fill in
    totals["sector"] = ""   # must match a canonical NACE sector name
    totals["source"] = ""   # URL, "wikipedia", "linkedin", "known", etc.

    totals.to_csv(OUTPUT_CSV, index=False)
    print(f"  ✓ Template written → {OUTPUT_CSV}  ({len(totals)} companies)")
    print()
    print("  Next steps:")
    print("  1. Fill in 'sector' and 'source' for each row in the CSV.")
    print("  2. Sector values must exactly match one of the 24 NACE names used")
    print("     in data/cleaned/sector_permits.csv (2020+ rows).")
    print("  3. Run 'make data' to rebuild the pipeline with sector tags.")


if __name__ == "__main__":
    force = "--force" in sys.argv
    main(force=force)
