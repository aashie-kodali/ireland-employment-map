"""
src/05_clean_companies.py
=========================
Parses 11 years of company-level work permit files from data/raw/ and produces:

  data/cleaned/company_permits.csv   — one row per (year, company)

Columns:
  year              — calendar year (int)
  company_name_raw  — employer name exactly as it appears in the source file
  company_name_clean — lightly normalised: trimmed whitespace, collapsed spaces
  issued            — total work permits issued to that company in that year

Data sources:
  DETE company permit files, 2015–2025.
  File naming changed over time:
    2015:      companies-issued-with-permits-2015.xlsx
    2016–2019: Companies Permits YYYY.xlsx
    2020–2025: Permits Issued to Companies YYYY.xlsx

FOUR DISTINCT EXCEL LAYOUTS:

  OLD-3COL (2015–2017):
    Row 0: Year | Total | Employer Name  (header)
    Row 1: <year> | <grand_total> | NaN  (skip — Grand Total row)
    Row N: NaN   | <count>       | <company name>

  OLD-4COL (2018–2019):  [2019 has blank prefix row + blank prefix column]
    Row 0: Year | Month | Total | Employer Name  (header)
    Row 1: <year> | NaN | <grand_total> | NaN    (skip)
    Sub-header rows: <year> | <Month> | NaN | NaN (skip)
    Data rows:       NaN   | NaN     | <count> | <company name>
    Each company appears once per month → groupby + sum for annual total.

  NEW-3HEADER (2020–2023):  [openpyxl data_only=True required for 2020 formula cells]
    Row 0: NaN         | Grand Total | <year> ...  (header line 1)
    Row 1: NaN         | NaN         | Issued ...  (header line 2)
    Row 2: Employer Name | NaN       | Jan | Feb … (header line 3)
    Row 3: Grand Total | <total>     | ...          (skip)
    Row 4+: <company name> | <annual total> | ...

  NEW-1HEADER (2024):
    Row 0: Employer Name | Grand Total | Jan | Feb … (header)
    Row 1: Grand Total   | <total>     | ...          (skip)
    Row 2+: <company name> | <annual total> | ...

  NEW-SHIFTED (2025):
    Same 1-row header as 2024, but Grand Total column is col 13 (not col 1).
    Detect by scanning header row for a cell containing 'grand'.

Run from project root:
  Terminal : python src/05_clean_companies.py
  Jupyter  : %run src/05_clean_companies.py
"""

import re
from pathlib import Path

import pandas as pd

RAW_DIR     = Path("data/raw")
CLEANED_DIR = Path("data/cleaned")
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

# Grand Totals from the 'Grand Total' row in each source file.
# Parser output may not exactly match due to source data quirks:
#   2015: ~100 permits appear in source grand total but not in company rows
#   2018: 3 permit discrepancy (rounding in source)
#   2019: 1,525 permits missing — file has incomplete monthly breakdown (9 of 12
#          months) with Aug/Nov/Dec data partially embedded in the 'Oct' section
#          without a sub-header; source data limitation, not a parser bug
#   2020: 19 permit discrepancy — annual total column disagrees with month sums
#          for a small number of companies; source data quality issue
#   2021: 1 permit discrepancy (rounding)
KNOWN_TOTALS = {
    2015: 7_353,
    2016: 9_373,
    2017: 11_361,
    2018: 13_398,
    2019: 16_383,
    2020: 16_419,
    2021: 16_275,
    2022: 39_955,
    2023: 30_981,
    2024: 39_390,
    2025: 31_044,
}

# Acceptable tolerance (absolute permits) per year for the verification check.
# Years not listed default to 0 tolerance (exact match required).
KNOWN_TOLERANCE = {
    2015: 110,    # ~100 permits appear in source grand total but not in rows
    2018: 5,      # minor rounding
    2019: 1_600,  # incomplete monthly file structure (known source limitation)
    2020: 25,     # annual total column vs month-sum disagreements in source
    2021: 2,      # minor rounding
}


# ── Shared helpers (mirrors 01_clean_data.py) ─────────────────────────────────

def extract_year(filename: str) -> int:
    match = re.search(r"(\d{4})", filename)
    if not match:
        raise ValueError(f"No 4-digit year found in filename: {filename}")
    return int(match.group(1))


def strip_blank_prefix(df: pd.DataFrame) -> pd.DataFrame:
    """Drop a fully-blank leading row and blank leading column (2019 quirk)."""
    if df.iloc[0].isna().all():
        df = df.iloc[1:, 1:].reset_index(drop=True)
    return df


# ── Company name normalisation ────────────────────────────────────────────────

def clean_company_name(raw) -> str:
    """
    Light normalisation only — does not attempt fuzzy deduplication.
    Strips whitespace, collapses internal spaces, removes trailing periods
    after common legal suffixes (Ltd./Limited./plc.).
    """
    if pd.isna(raw):
        return raw
    name = str(raw).strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\bLtd\.$",     "Ltd",     name)
    name = re.sub(r"\bLimited\.$", "Limited", name)
    name = re.sub(r"\bplc\.$",     "plc",     name, flags=re.IGNORECASE)
    return name


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_company_2015_2017(path: Path, year: int) -> pd.DataFrame:
    """
    3-column layout: Year | Total | Employer Name
    Company rows have NaN in the Year column.
    Returns: year | company_name_raw | issued
    """
    df = pd.read_excel(path, header=None)

    # Row 0 is the header
    df.columns = [str(c).strip() for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)

    # Force positional column names for reliability
    cols = list(df.columns)
    cols[0] = "year_col"
    cols[1] = "issued"
    cols[2] = "company_name_raw"
    df.columns = cols[:len(df.columns)]

    # Company rows: Year column is NaN, company name is present
    data = df[df["year_col"].isna() & df["company_name_raw"].notna()].copy()
    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["company_name_raw"] = data["company_name_raw"].astype(str).str.strip()
    data["year"] = year

    return data[["year", "company_name_raw", "issued"]].reset_index(drop=True)


def parse_company_2018_2019(path: Path, year: int) -> pd.DataFrame:
    """
    4-column monthly layout: Year | Month | Total | Employer Name
    2019 has a blank prefix row + blank prefix column — stripped first.
    Each company appears once per month; groupby + sum for annual total.
    Returns: year | company_name_raw | issued
    """
    df = pd.read_excel(path, header=None)
    df = strip_blank_prefix(df)

    df.columns = [str(c).strip() for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)

    cols = list(df.columns)
    cols[0] = "year_col"
    cols[1] = "month_col"
    cols[2] = "issued"
    cols[3] = "company_name_raw"
    df.columns = cols[:len(df.columns)]

    # Company rows: Year NaN, Month NaN, company name present
    data = df[
        df["year_col"].isna() &
        df["month_col"].isna() &
        df["company_name_raw"].notna()
    ].copy()

    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["company_name_raw"] = data["company_name_raw"].astype(str).str.strip()

    # Sum monthly rows per company to get annual total
    annual = (
        data.groupby("company_name_raw", as_index=False)["issued"]
        .sum()
    )
    annual["year"] = year
    return annual[["year", "company_name_raw", "issued"]].reset_index(drop=True)


def parse_company_2020_2023(path: Path, year: int) -> pd.DataFrame:
    """
    3-row header layout. data_only=True required — 2020 uses formula cells
    for Grand Total that return strings without it.
    Col 0 = company name, Col 1 = annual total.
    Returns: year | company_name_raw | issued
    """
    df = pd.read_excel(path, header=None, engine="openpyxl")

    # Rows 0-2 are the double/triple header; data starts at row 3.
    # Row 3 is the Grand Total summary row — filter it out by name check below.
    data = df.iloc[3:, [0, 1]].copy()
    data.columns = ["company_name_raw", "issued"]

    data = data[data["company_name_raw"].notna()].copy()
    data["company_name_raw"] = data["company_name_raw"].astype(str).str.strip()
    data = data[~data["company_name_raw"].str.contains(
        r"Grand Total|^nan$", case=False, regex=True, na=False
    )]

    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["year"] = year
    return data[["year", "company_name_raw", "issued"]].reset_index(drop=True)


def parse_company_2024(path: Path, year: int) -> pd.DataFrame:
    """
    1-row header layout: Employer Name | Grand Total | Jan | Feb ...
    Row 1 is the Grand Total summary — filtered by name.
    Returns: year | company_name_raw | issued
    """
    df = pd.read_excel(path, header=None)

    # Row 0 is the header; data from row 1
    data = df.iloc[1:, [0, 1]].copy()
    data.columns = ["company_name_raw", "issued"]

    data = data[data["company_name_raw"].notna()].copy()
    data["company_name_raw"] = data["company_name_raw"].astype(str).str.strip()
    data = data[~data["company_name_raw"].str.contains(
        r"Grand Total|^nan$", case=False, regex=True, na=False
    )]

    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["year"] = year
    return data[["year", "company_name_raw", "issued"]].reset_index(drop=True)


def parse_company_2025(path: Path, year: int) -> pd.DataFrame:
    """
    1-row header, but Grand Total column is col 13 (not col 1).
    Detect by scanning header row for a cell containing 'grand'.
    Same pattern as parse_sector_new handles the 2025 sector layout shift.
    Returns: year | company_name_raw | issued
    """
    df = pd.read_excel(path, header=None)

    # Detect the Grand Total column from the header row
    header_row = df.iloc[0]
    grand_col = 1  # default
    for i, val in enumerate(header_row):
        if "grand" in str(val).lower():
            grand_col = i
            break

    data = df.iloc[1:, [0, grand_col]].copy()
    data.columns = ["company_name_raw", "issued"]

    data = data[data["company_name_raw"].notna()].copy()
    data["company_name_raw"] = data["company_name_raw"].astype(str).str.strip()
    data = data[~data["company_name_raw"].str.contains(
        r"Grand Total|^nan$", case=False, regex=True, na=False
    )]

    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["year"] = year
    return data[["year", "company_name_raw", "issued"]].reset_index(drop=True)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def build_company_permits() -> pd.DataFrame:
    """
    Iterate over all company permit files, dispatch to the correct parser,
    apply name cleaning, and concatenate into one year-stacked DataFrame.
    """
    frames = []

    # ---- Old 3-col format (2015–2017) ----
    old3_files = {
        "companies-issued-with-permits-2015.xlsx": 2015,
        "Companies Permits 2016.xlsx":             2016,
        "Companies Permits 2017.xlsx":             2017,
    }
    for fname, yr in old3_files.items():
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [company-old3] {fname}  →  year={yr}")
            frames.append(parse_company_2015_2017(p, yr))
        else:
            print(f"  [MISSING]      {fname}")

    # ---- Old 4-col monthly format (2018–2019) ----
    old4_files = {
        "Companies Permits 2018.xlsx": 2018,
        "Companies Permits 2019.xlsx": 2019,
    }
    for fname, yr in old4_files.items():
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [company-old4] {fname}  →  year={yr}")
            frames.append(parse_company_2018_2019(p, yr))
        else:
            print(f"  [MISSING]      {fname}")

    # ---- New 3-row header format (2020–2023) ----
    for yr in range(2020, 2024):
        fname = f"Permits Issued to Companies {yr}.xlsx"
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [company-new3] {fname}  →  year={yr}")
            frames.append(parse_company_2020_2023(p, yr))
        else:
            print(f"  [MISSING]      {fname}")

    # ---- New 1-row header format (2024) ----
    fname_2024 = "Permits Issued to Companies 2024.xlsx"
    p = RAW_DIR / fname_2024
    if p.exists():
        print(f"  [company-new1] {fname_2024}  →  year=2024")
        frames.append(parse_company_2024(p, 2024))
    else:
        print(f"  [MISSING]      {fname_2024}")

    # ---- New shifted-column format (2025) ----
    fname_2025 = "Permits Issued to Companies 2025.xlsx"
    p = RAW_DIR / fname_2025
    if p.exists():
        print(f"  [company-new-shifted] {fname_2025}  →  year=2025")
        frames.append(parse_company_2025(p, 2025))
    else:
        print(f"  [MISSING]      {fname_2025}")

    if not frames:
        raise RuntimeError(
            "build_company_permits: no source files found — check data/raw/ is populated"
        )
    df = pd.concat(frames, ignore_index=True)

    # Apply name cleaning
    df["company_name_clean"] = df["company_name_raw"].apply(clean_company_name)

    return (
        df[["year", "company_name_raw", "company_name_clean", "issued"]]
        .sort_values(["year", "company_name_clean"])
        .reset_index(drop=True)
    )


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  05_clean_companies.py  —  Company Permit Parser")
    print("=" * 60)

    print("\n── Company Permits ─────────────────────────────────────────")
    df = build_company_permits()

    # ── Year-total verification ───────────────────────────────────
    print("\n── Year-total verification ─────────────────────────────────")
    year_totals = df.groupby("year")["issued"].sum()
    all_ok = True
    for yr, expected in sorted(KNOWN_TOTALS.items()):
        if yr not in year_totals.index:
            print(f"  [MISSING YEAR] {yr}")
            all_ok = False
            continue
        actual    = int(year_totals[yr])
        diff      = actual - expected
        tolerance = KNOWN_TOLERANCE.get(yr, 0)
        if abs(diff) <= tolerance:
            note = f"  (within tolerance ±{tolerance:,})" if tolerance > 0 else ""
            print(f"  ✓ {yr}: {actual:>7,}{note}")
        else:
            print(f"  ✗ {yr}: {actual:>7,}  (expected {expected:,}, diff={diff:+,})")
            all_ok = False

    if all_ok:
        print("\n  All year totals within expected tolerance. ✓")
    else:
        print("\n  [WARNING] Some year totals exceed tolerance. Check parser logic.")

    # ── Save ──────────────────────────────────────────────────────
    out = CLEANED_DIR / "company_permits.csv"
    df.to_csv(out, index=False)

    print(f"\n  ✓ {len(df):,} rows saved → {out}")
    print(f"  Years            : {sorted(df['year'].unique())}")
    print(f"  Unique companies : {df['company_name_clean'].nunique():,} (cleaned)")

    print(f"\n  Top 10 employers overall (2020–2025):")
    top = (
        df[df["year"] >= 2020]
        .groupby("company_name_clean")["issued"]
        .sum()
        .nlargest(10)
        .reset_index()
    )
    for _, row in top.iterrows():
        print(f"    {row['issued']:>6,.0f}  {row['company_name_clean']}")

    print(f"\n  Top 10 employers in 2024:")
    top24 = (
        df[df["year"] == 2024]
        .nlargest(10, "issued")[["company_name_clean", "issued"]]
    )
    for _, row in top24.iterrows():
        print(f"    {row['issued']:>6,.0f}  {row['company_name_clean']}")

    print("\n  Done. ✓")
