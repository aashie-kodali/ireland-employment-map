"""
src/01_clean_data.py
====================
Reads all raw Excel work-permit files from data/raw/ and produces four
tidy, year-stacked CSVs in data/cleaned/:

  county_permits.csv       — one row per (year, county)
  sector_permits.csv       — one row per (year, sector)
  nationality_permits.csv  — one row per (year, nationality)
  visa_decisions.csv       — one row per (year, nationality) — long-term visas only

Data sources:
  • Ireland Dept. of Enterprise, Trade and Employment (DETE) work permit
    statistics, 2015–2025.
  • Irish Immigration Service Delivery (ISD) visa decisions by year and
    nationality, 2017–2026.

KEY ASSUMPTIONS:
  • 'issued' in 2015–2019 = 'Total' column (New + Renewal permits).
    'issued' in 2020–2025 = 'Issued' column.  Both represent the same concept.
  • Year is always extracted from the filename, never from inside the sheet.
  • Visa data: only 'long term visa applications' are included (allow list).
    This category covers student, employment, and graduate visas — the three
    visa types relevant to this project. Short-term (tourist/visitor) visas
    are excluded entirely.
  • Suppressed visa counts (marked '*' in the source) are treated as NaN.
  • 2026 visa data is partial and is retained but flagged in the column name.

LIMITATIONS:
  • 2025 county file has no 'Withdrawn' column → filled with NaN.
  • Northern Ireland counties (Antrim, Armagh, Fermanagh, etc.) appear in
    some years — they are kept in the CSV but lie outside the Republic of
    Ireland, so they won't appear on ROI choropleth maps.
  • 'No County Entered' / 'Grand Total' summary rows are dropped.
  • Company-level files (Companies Permits, Permits Issued to Companies)
    are not processed here — they are not needed for the map.

Run from project root:
  Terminal : python src/01_clean_data.py
  Jupyter  : %run src/01_clean_data.py
"""

import re             # regular expressions — used to find/match text patterns
from pathlib import Path  # modern way to work with file paths in Python

import pandas as pd   # the core data manipulation library (like Excel in code)

# ── Paths ─────────────────────────────────────────────────────────────────────
# Path() is smarter than plain strings for file paths — it handles
# Mac/Windows/Linux differences automatically (forward vs. back slashes etc.)
RAW_DIR     = Path("data/raw")       # where the original Excel files live (read-only)
CLEANED_DIR = Path("data/cleaned")   # where we save our tidy output CSVs
CLEANED_DIR.mkdir(parents=True, exist_ok=True)   # create the folder if it doesn't exist yet


# ── Shared helpers ────────────────────────────────────────────────────────────

def extract_year(filename: str) -> int:
    """
    Pull the 4-digit year out of a filename like 'County Permits 2024.xlsx'.

    re.search(pattern, string) scans the string for the first match.
    r"(\\d{4})" means: find exactly 4 digits in a row.
    .group(1) returns the first captured group — the year number.
    """
    match = re.search(r"(\d{4})", filename)
    if not match:
        raise ValueError(f"No 4-digit year found in filename: {filename}")
    return int(match.group(1))


def strip_blank_prefix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some 2019 files have a fully-blank leading row AND a blank leading column.
    Detect by checking whether the entire first row is NaN, then drop both.
    """
    if df.iloc[0].isna().all():
        df = df.iloc[1:, 1:].reset_index(drop=True)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — COUNTY PERMITS
# ═══════════════════════════════════════════════════════════════════════════════
#
# Two distinct Excel layouts exist across the years:
#
#  OLD FORMAT (2015–2019)
#  ┌──────┬────────────────┬──────────────┬─────┬─────────┬───────┬─────────┬───────────┐
#  │ Year │ County/Country │ [TypePermit] │ New │ Renewal │ Total │ Refused │ Withdrawn │
#  └──────┴────────────────┴──────────────┴─────┴─────────┴───────┴─────────┴───────────┘
#  • Header is row 0; county data rows have NaN in the Year column.
#  • 2017 has an extra "Type of Permit" column (ignored in output).
#  • 2019 has a blank leading row + blank leading column (stripped first).
#  • "Total" (New + Renewal) maps to our 'issued' field.
#
#  NEW FORMAT (2020–2025)
#  ┌───────────────────┬────────┬─────────┬───────────┐
#  │ (county name)     │ Issued │ Refused │ Withdrawn │
#  └───────────────────┴────────┴─────────┴───────────┘
#  • 2020–2023: Row 0 = year indicator, Row 1 = column headers, data from Row 2.
#  • 2024:      Row 0 = column headers, data from Row 1.
#  • 2025:      Row 0 = year indicator, Row 1 = headers, no 'Withdrawn' column.
#  • We detect the header row by scanning for a cell containing 'Issued'.

def parse_county_old(path: Path, year: int) -> pd.DataFrame:
    """
    Parse county permit files for 2015–2019.
    Returns tidy DataFrame: year | county | issued | refused | withdrawn
    """
    df = pd.read_excel(path, header=None)
    df = strip_blank_prefix(df)                  # handle 2019's blank prefix

    # Promote row 0 to column headers
    df.columns = [str(c).strip() for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)

    # Positionally force column 1 (index 1) to be 'county' — more robust than
    # relying on the exact spelling "County/Country" which could vary.
    cols = list(df.columns)
    cols[1] = "county"
    df.columns = cols

    # Map remaining columns by name (case-insensitive)
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "total":
            col_map[c] = "issued"     # Total = New + Renewal = issued count
        elif lc == "refused":
            col_map[c] = "refused"
        elif lc == "withdrawn":
            col_map[c] = "withdrawn"
    df = df.rename(columns=col_map)

    # Keep only the real county data rows — drop summary/header rows.
    # Some rows have NaN in the county column (blank rows between sections) → drop those.
    df = df[df["county"].notna()].copy()
    df["county"] = df["county"].astype(str).str.strip()   # remove leading/trailing spaces

    # str.contains with regex=True uses a pattern to match any of several strings.
    # The ~ at the front means NOT — so we KEEP rows that do NOT match.
    # "Grand Total" = summary row at the bottom
    # "Jan\s*-\s*Dec" = rows labelled "Jan - Dec" (monthly header rows in old files)
    # "No County" = permits with no county recorded
    df = df[~df["county"].str.contains(
        r"Grand Total|Jan\s*-\s*Dec|No County|No county", regex=True, na=False
    )]

    df["year"] = year
    out = df[["year", "county", "issued", "refused", "withdrawn"]].copy()
    for col in ["issued", "refused", "withdrawn"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out.reset_index(drop=True)


def parse_county_new(path: Path, year: int) -> pd.DataFrame:
    """
    Parse county permit files for 2020–2025.
    Locates the header row dynamically by scanning for the word 'Issued'.
    Returns tidy DataFrame: year | county | issued | refused | withdrawn
    """
    df = pd.read_excel(path, header=None)

    # Find the row that acts as the column header (contains 'Issued')
    header_row = None
    for i, row in df.iterrows():
        if any(str(v).strip().lower() == "issued" for v in row):
            header_row = i
            break

    if header_row is None:
        raise ValueError(f"Could not locate 'Issued' header row in {path.name}")

    # Set that row as column names; unnamed cells get a placeholder
    df.columns = [
        str(c).strip() if pd.notna(c) else f"_col{i}"
        for i, c in enumerate(df.iloc[header_row])
    ]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # The first column (col 0) is always the county name — rename it
    df = df.rename(columns={df.columns[0]: "county"})

    # Normalise remaining column names
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "issued":
            col_map[c] = "issued"
        elif lc == "refused":
            col_map[c] = "refused"
        elif lc == "withdrawn":
            col_map[c] = "withdrawn"
    df = df.rename(columns=col_map)

    # Drop summary rows and blank rows
    df = df[df["county"].notna()].copy()
    df["county"] = df["county"].astype(str).str.strip()
    df = df[~df["county"].str.contains(
        r"Grand Total|No County|No county|^nan$", regex=True, case=False, na=False
    )]

    df["year"] = year

    # 2025 file has no 'Withdrawn' column — add it as NaN so schema is consistent
    if "withdrawn" not in df.columns:
        df["withdrawn"] = pd.NA

    out = df[["year", "county", "issued", "refused", "withdrawn"]].copy()
    for col in ["issued", "refused", "withdrawn"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out.reset_index(drop=True)


# The 26 counties of the Republic of Ireland.
# We use an allowlist (rather than a blocklist) so that any stray country names
# from the old "County/Country" column are automatically excluded too.
ROI_COUNTIES = {
    "Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin", "Galway", "Kerry",
    "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth",
    "Mayo", "Meath", "Monaghan", "Offaly", "Roscommon", "Sligo", "Tipperary",
    "Waterford", "Westmeath", "Wexford", "Wicklow",
}


def build_county_permits() -> pd.DataFrame:
    """
    Iterates over all county permit files, dispatches to the correct parser,
    concatenates everything into one year-stacked DataFrame, and drops any
    Northern Ireland county rows (they appear sporadically with zero/NaN values).
    """
    frames = []

    # ---- Old format (2015–2019) ----
    old_files = [
        "County Permits Issued 2015.xlsx",
        "County Permits Issued 2016.xlsx",
        "County Permits Issued 2017.xlsx",
        "County Permits 2018.xlsx",
        "County Permits 2019.xlsx",
    ]
    for fname in old_files:
        p = RAW_DIR / fname
        if p.exists():
            yr = extract_year(fname)
            print(f"  [county-old] {fname}  →  year={yr}")
            frames.append(parse_county_old(p, yr))
        else:
            print(f"  [MISSING]    {fname}")

    # ---- New format (2020–2025) ----
    for yr in range(2020, 2026):
        fname = f"County Permits {yr}.xlsx"
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [county-new] {fname}  →  year={yr}")
            frames.append(parse_county_new(p, yr))
        else:
            print(f"  [MISSING]    {fname}")

    df = pd.concat(frames, ignore_index=True)
    # Keep only the 26 ROI counties — this drops NI counties AND any stray
    # country names (e.g. "England", "India") from the old County/Country column.
    df = df[df["county"].isin(ROI_COUNTIES)]
    return df.sort_values(["year", "county"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SECTOR PERMITS
# ═══════════════════════════════════════════════════════════════════════════════
#
#  OLD FORMAT (2015–2019) — hierarchical: Year > Month > Sector rows
#  ┌──────┬───────┬─────────────────┬─────┬─────────┬───────┬─────────┬───────────┐
#  │ Year │ Month │ Sector          │ New │ Renewal │ Total │ Refused │ Withdrawn │
#  └──────┴───────┴─────────────────┴─────┴─────────┴───────┴─────────┴───────────┘
#  • Sector rows = rows where both Year and Month are NaN but Sector is filled.
#  • Each sector appears once per month; we SUM across months → annual total.
#  • The Sector column header is sometimes named "Sector", sometimes NaN in the
#    raw file, so we use its positional index (column 2) for reliability.
#
#  NEW FORMAT (2020–2025) — wide table
#  ┌─────────────────────┬─────────────┬─────┬─────┬─────┐
#  │ Economic Sector     │ Grand Total │ Jan │ Feb │ … │
#  └─────────────────────┴─────────────┴─────┴─────┴─────┘
#  • 2-row header; data starts at row 2.
#  • Col 0 = sector name, Col 1 = annual Grand Total (what we call 'issued').

def parse_sector_old(path: Path, year: int) -> pd.DataFrame:
    """
    Parse sector files for 2015–2019.
    Aggregates monthly sector rows to produce annual totals.
    Returns: year | sector | issued
    """
    df = pd.read_excel(path, header=None)
    df = strip_blank_prefix(df)

    # Row 0 → column headers
    headers = [str(c).strip() for c in df.iloc[0]]
    df.columns = headers
    df = df.iloc[1:].reset_index(drop=True)

    # Force col[2] to be named 'sector' (it can be 'Sector' or NaN depending on year)
    cols = list(df.columns)
    cols[2] = "sector"
    df.columns = cols

    # Rename year/month/total columns
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "year":
            col_map[c] = "year_col"
        elif lc == "month":
            col_map[c] = "month_col"
        elif lc == "total":
            col_map[c] = "issued"
    df = df.rename(columns=col_map)

    # Select sector rows: year_col NaN, month_col NaN, sector not NaN
    # (Each such row is one sector's count for one specific month.)
    sector_rows = df[
        df["year_col"].isna() &
        df["month_col"].isna() &
        df["sector"].notna()
    ].copy()

    sector_rows["sector"] = sector_rows["sector"].astype(str).str.strip()
    sector_rows["issued"] = pd.to_numeric(sector_rows["issued"], errors="coerce")

    # groupby("sector").sum() adds up all the monthly rows for each sector.
    # Think of it like a SUMIF in Excel: for each unique sector name,
    # add up all the "issued" values from every month of the year.
    # as_index=False keeps "sector" as a regular column rather than the index.
    annual = (
        sector_rows
        .groupby("sector", as_index=False)["issued"]
        .sum()
    )
    annual["year"] = year
    return annual[["year", "sector", "issued"]].reset_index(drop=True)


def parse_sector_new(path: Path, year: int) -> pd.DataFrame:
    """
    Parse sector files for 2020–2025.
    Uses the Grand Total column (col 1) as the annual 'issued' count.
    Returns: year | sector | issued
    """
    df = pd.read_excel(path, header=None)

    # Data begins at row 2 (rows 0 and 1 are a double header)
    # Col 0 = sector name, Col 1 = Grand Total for the year
    data = df.iloc[2:, [0, 1]].copy()
    data.columns = ["sector", "issued"]

    data = data[data["sector"].notna()].copy()
    data["sector"] = data["sector"].astype(str).str.strip()
    # Drop the Grand Total summary row
    data = data[~data["sector"].str.contains("Grand Total", case=False, na=False)]

    data["issued"] = pd.to_numeric(data["issued"], errors="coerce")
    data["year"] = year
    return data[["year", "sector", "issued"]].reset_index(drop=True)


# Canonical sector names — maps every known variant spelling/truncation to one
# consistent name so the dropdown filter works correctly across all years.
# Each entry: "raw variant" → "canonical full name"
SECTOR_NAME_NORMALISE = {
    # ── Truncated names (Excel column overflow) ───────────────────────────────
    "C - Manufacture of Computers, Electronics & Optica":
        "C - Manufacture of Computers, Electronics & Optical Equipment",
    "E - Water Supply - Sewerage Waste Management & Rem":
        "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "E - Water Supply Waste Management & Remedial Activ":
        "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "E - Water Supply - Sewerage Waste Management & Remedial Activities":
        "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "M - All other Professional, Scientific & Technical":
        "M - Professional, Scientific & Technical Activities",
    "M - All other Professional, Scientific & Technical Activities":
        "M - Professional, Scientific & Technical Activities",
    "M - Professional, Scientific&Technical Activities":
        "M - Professional, Scientific & Technical Activities",
    "M - Professional, Scientific & Technical Activities of Head Offices, Management Consultancy Services":
        "M - Professional, Scientific & Technical Activities",
    "T - Domestic- Activities of Households as Employer":
        "T - Domestic Activities of Households as Employers",
    "T - Domestic - Activities of Households as Employers":
        "T - Domestic Activities of Households as Employers",
    # ── Capitalisation variants ───────────────────────────────────────────────
    "K - Financial & insurance Activities":
        "K - Financial & Insurance Activities",
    "I - Accommodation & Food Services activities":
        "I - Accommodation & Food Services Activities",
    "S - Other Service activities":
        "S - Other Service Activities",
    # ── Punctuation variants ──────────────────────────────────────────────────
    "R - Arts , Entertainment and Recreation":
        "R - Arts, Entertainment and Recreation",
}

# Rows with these sector values are stray header/summary rows — drop them.
SECTOR_DROP_VALUES = {"Economic Sector", "Grand Total"}


def normalise_sector_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply SECTOR_NAME_NORMALISE to the 'sector' column, drop stray rows,
    then re-aggregate by (year, sector) to merge any duplicates that result
    from two variant names now mapping to the same canonical name.

    WHY is this needed?  Excel files from different years spell the same sector
    differently — truncated names, capital letters, missing commas.  If we don't
    fix this, the sector dropdown on the map will show 30+ options with near-duplicates.
    This function collapses them all to one clean name each.
    """
    # Remove rows that are stray header/summary rows, not real data
    df = df[~df["sector"].isin(SECTOR_DROP_VALUES)].copy()

    # .map(dict) replaces each value using the dictionary.
    # .fillna(df["sector"]) keeps the original value if there's no mapping for it
    # (i.e. sectors that are already correctly named stay unchanged).
    df["sector"] = df["sector"].map(SECTOR_NAME_NORMALISE).fillna(df["sector"])

    # After normalisation, two rows that were previously different spellings of the
    # same sector now have the same name.  We sum them so there's only one row per
    # (year, sector) pair.  This is like a pivot table in Excel: GROUP BY year, sector.
    df = (
        df.groupby(["year", "sector"], as_index=False)["issued"]
        .sum()
    )
    return df


def build_sector_permits() -> pd.DataFrame:
    """Collect, stack, and normalise all sector permit files."""
    frames = []

    old_files = [
        "Permits Issued by Sector 2015.xlsx",
        "Permits Issued by Sector 2016.xlsx",
        "Permits Issued by Sector 2017.xlsx",
        "Permits by Sector 2018.xlsx",
        "Permits by Sector 2019.xlsx",
    ]
    for fname in old_files:
        p = RAW_DIR / fname
        if p.exists():
            yr = extract_year(fname)
            print(f"  [sector-old] {fname}  →  year={yr}")
            frames.append(parse_sector_old(p, yr))
        else:
            print(f"  [MISSING]    {fname}")

    for yr in range(2020, 2026):
        fname = f"Permits by Sector {yr}.xlsx"
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [sector-new] {fname}  →  year={yr}")
            frames.append(parse_sector_new(p, yr))
        else:
            print(f"  [MISSING]    {fname}")

    df = pd.concat(frames, ignore_index=True)

    # Normalise sector names to fix truncations, capitalisation variants, and
    # stray header rows — this is what makes the dropdown filter reliable.
    df = normalise_sector_names(df)
    print(f"  Unique sectors after normalisation: {df['sector'].nunique()}")

    return df.sort_values(["year", "sector"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — NATIONALITY PERMITS
# ═══════════════════════════════════════════════════════════════════════════════
#
#  OLD FORMAT (2015–2019):
#  Year | Nationality | New | Renewal | Total | Refused | Withdrawn
#
#  NEW FORMAT (2020–2025):
#  Nationality | Issued | Refused | Withdrawn
#  (with optional year-indicator row above the header)

def parse_nationality_old(path: Path, year: int) -> pd.DataFrame:
    """
    Parse nationality files for 2015–2019.
    Returns: year | nationality | issued | refused | withdrawn
    """
    df = pd.read_excel(path, header=None)
    df = strip_blank_prefix(df)

    df.columns = [str(c).strip() for c in df.iloc[0]]
    df = df.iloc[1:].reset_index(drop=True)

    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if "nation" in lc:
            col_map[c] = "nationality"
        elif lc == "total":
            col_map[c] = "issued"
        elif lc == "refused":
            col_map[c] = "refused"
        elif lc == "withdrawn":
            col_map[c] = "withdrawn"
    df = df.rename(columns=col_map)

    df = df[df["nationality"].notna()].copy()
    df["nationality"] = df["nationality"].astype(str).str.strip()
    # Drop the summary rows
    df = df[~df["nationality"].str.contains(
        r"Grand Total|Jan\s*-\s*Dec", regex=True, na=False
    )]

    df["year"] = year
    for col in ["issued", "refused", "withdrawn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["year", "nationality", "issued", "refused", "withdrawn"]].reset_index(drop=True)


def parse_nationality_new(path: Path, year: int) -> pd.DataFrame:
    """
    Parse nationality files for 2020–2025.
    Locates header row dynamically (same technique as county new format).
    Returns: year | nationality | issued | refused | withdrawn
    """
    df = pd.read_excel(path, header=None)

    # Find the header row by scanning for the cell 'Issued'
    header_row = None
    for i, row in df.iterrows():
        if any(str(v).strip().lower() == "issued" for v in row):
            header_row = i
            break

    if header_row is None:
        raise ValueError(f"Could not locate 'Issued' header row in {path.name}")

    df.columns = [
        str(c).strip() if pd.notna(c) else f"_col{i}"
        for i, c in enumerate(df.iloc[header_row])
    ]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # Col 0 is the nationality column regardless of its label
    df = df.rename(columns={df.columns[0]: "nationality"})

    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "issued":
            col_map[c] = "issued"
        elif lc == "refused":
            col_map[c] = "refused"
        elif lc == "withdrawn":
            col_map[c] = "withdrawn"
    df = df.rename(columns=col_map)

    df = df[df["nationality"].notna()].copy()
    df["nationality"] = df["nationality"].astype(str).str.strip()
    df = df[~df["nationality"].str.contains(
        r"Grand Total|^nan$", regex=True, case=False, na=False
    )]

    df["year"] = year
    if "withdrawn" not in df.columns:
        df["withdrawn"] = pd.NA

    out = df[["year", "nationality", "issued", "refused", "withdrawn"]].copy()
    for col in ["issued", "refused", "withdrawn"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out.reset_index(drop=True)


def build_nationality_permits() -> pd.DataFrame:
    """Collect and stack all nationality permit files."""
    frames = []

    old_files = [
        "Permits Issued by Nationality 2015.xlsx",
        "permits-issued-by-nationality-2016.xlsx",
        "Permits Issued by Nationality 2017.xlsx",
        "Permits by Nationality 2018.xlsx",
        "permits-by-nationality-2019.xlsx",
    ]
    for fname in old_files:
        p = RAW_DIR / fname
        if p.exists():
            yr = extract_year(fname)
            print(f"  [nationality-old] {fname}  →  year={yr}")
            frames.append(parse_nationality_old(p, yr))
        else:
            print(f"  [MISSING]         {fname}")

    for yr in range(2020, 2026):
        fname = f"Permits by Nationality {yr}.xlsx"
        p = RAW_DIR / fname
        if p.exists():
            print(f"  [nationality-new] {fname}  →  year={yr}")
            frames.append(parse_nationality_new(p, yr))
        else:
            print(f"  [MISSING]         {fname}")

    df = pd.concat(frames, ignore_index=True)
    return df.sort_values(["year", "nationality"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — VISA DECISIONS
# ═══════════════════════════════════════════════════════════════════════════════
#
# Source file (wide format):
#   Type | Status | Last Updated | Nationality | 2017 | 2018 | … | 2026
#
# 'Type' contains two values:
#   'long term visa applications'   ← the only one we want (student, employment,
#   'short term visa applications'    graduate visas — our allow list)
#
# 'Status' contains three values per type per nationality:
#   'Received', 'Granted', 'Refused'
#
# Output (long format, one row per year × nationality):
#   year | nationality | received | granted | refused
#
# We reshape by:
#   1. Filter to VISA_ALLOW_LIST (long-term only)
#   2. Melt year columns → (nationality, status, year, value)
#   3. Replace '*' (suppressed counts) with NaN
#   4. Pivot status values into columns: received, granted, refused
#   5. Drop rows where all three counts are NaN or zero

# Allow list — only these Type values are kept. Using a set makes it easy to
# add further categories (e.g. 're-entry visas') without restructuring the code.
VISA_ALLOW_LIST = {"long term visa applications"}

VISA_FILE = "Visa Applications and Decisions by Year and Nationality.csv"

# Year columns present in the source file
VISA_YEAR_COLS = [str(y) for y in range(2017, 2027)]


def clean_visa_decisions() -> pd.DataFrame:
    """
    Clean and reshape the ISD visa decisions CSV.

    Steps:
      1. Read with latin-1 encoding (file contains non-UTF-8 characters).
      2. Apply allow list — keep long-term visa applications only.
      3. Melt wide year columns → long format.
      4. Replace suppressed '*' values with NaN.
      5. Pivot Status → columns (received, granted, refused).
      6. Drop rows that are entirely NaN or zero across all three status cols.

    Returns: year | nationality | received | granted | refused
    """
    path = RAW_DIR / VISA_FILE
    if not path.exists():
        raise FileNotFoundError(
            f"Expected visa file not found: {path}\n"
            "Place it in data/raw/ and re-run this script."
        )

    # latin-1 handles the occasional non-ASCII nationality name in this file
    df = pd.read_csv(path, encoding="latin-1")

    # ── Step 1: Apply allow list ───────────────────────────────────────────
    # Lowercase comparison guards against any future capitalisation changes
    df = df[df["Type"].str.lower().isin(VISA_ALLOW_LIST)].copy()
    print(f"    After allow-list filter: {len(df):,} rows "
          f"({df['Nationality'].nunique()} nationalities × "
          f"{df['Status'].nunique()} statuses)")

    # ── Step 2: Melt year columns → long format ────────────────────────────
    # The raw CSV has one column per year (wide format):
    #   Nationality | Status | 2017 | 2018 | 2019 | … | 2026
    #
    # We need it in long format — one row per (nationality, status, year):
    #   Nationality | Status | year | count
    #
    # .melt() does this transformation.  Think of it like unpivoting in Excel:
    #   id_vars   = columns that stay as-is (they identify the row)
    #   var_name  = what to call the new "year" column
    #   value_name = what to call the new "count" column
    id_cols = ["Nationality", "Status"]
    year_cols_present = [c for c in VISA_YEAR_COLS if c in df.columns]

    df_long = df[id_cols + year_cols_present].melt(
        id_vars=id_cols,
        var_name="year",
        value_name="count",
    )

    # ── Step 3: Replace suppressed values ─────────────────────────────────
    # The ISD marks small counts with '*' to protect individual privacy.
    # We replace '*' with NaN (Not a Number = missing) rather than treating
    # it as zero — because we genuinely don't know the value, not that it is zero.
    # pd.to_numeric(errors="coerce") converts everything that isn't a number to NaN.
    df_long["count"] = df_long["count"].replace("*", pd.NA)
    df_long["count"] = pd.to_numeric(df_long["count"], errors="coerce")
    df_long["year"]  = df_long["year"].astype(int)

    # ── Step 4: Pivot Status → columns ────────────────────────────────────
    # Right now each row has a "Status" value of "Received", "Granted", or "Refused".
    # We want those three statuses as separate columns — one row per (year, nationality).
    # pivot_table is like the reverse of melt — it turns row values into columns.
    #
    # Before pivot:
    #   year | Nationality | Status   | count
    #   2024 | India       | Received | 5000
    #   2024 | India       | Granted  | 4200
    #   2024 | India       | Refused  | 800
    #
    # After pivot:
    #   year | Nationality | Received | Granted | Refused
    #   2024 | India       | 5000     | 4200    | 800
    df_pivot = df_long.pivot_table(
        index=["year", "Nationality"],   # these become the row identifiers
        columns="Status",                # unique Status values become new columns
        values="count",
        aggfunc="sum",   # if there are duplicate rows (shouldn't happen), sum them
    ).reset_index()      # move year and Nationality back to regular columns

    # Normalise column names: lowercase, no spaces
    df_pivot.columns.name = None
    df_pivot = df_pivot.rename(columns={
        "Nationality": "nationality",
        "Received":    "received",
        "Granted":     "granted",
        "Refused":     "refused",
    })

    # Ensure all three status columns exist even if one was missing in the source
    for col in ["received", "granted", "refused"]:
        if col not in df_pivot.columns:
            df_pivot[col] = pd.NA

    # ── Step 5: Drop all-zero / all-NaN rows ──────────────────────────────
    # Rows where every count is NaN or 0 add no analytical value
    status_cols = ["received", "granted", "refused"]
    df_pivot = df_pivot[
        ~(df_pivot[status_cols].fillna(0) == 0).all(axis=1)
    ].copy()

    df_pivot["nationality"] = df_pivot["nationality"].astype(str).str.strip()

    return (
        df_pivot[["year", "nationality", "received", "granted", "refused"]]
        .sort_values(["year", "nationality"])
        .reset_index(drop=True)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  01_clean_data.py  —  Ireland Work Permit Data Cleaner")
    print("="*60)

    # ── County Permits ────────────────────────────────────────────
    print("\n── County Permits ──────────────────────────────────────────")
    county_df = build_county_permits()
    out = CLEANED_DIR / "county_permits.csv"
    county_df.to_csv(out, index=False)
    print(f"\n  ✓ {len(county_df):,} rows saved → {out}")
    print(f"  Years : {sorted(county_df['year'].unique())}")
    print(f"  Counties (sample): {sorted(county_df['county'].unique())[:8]}")
    print(f"  Columns: {list(county_df.columns)}")

    # ── Sector Permits ────────────────────────────────────────────
    print("\n── Sector Permits ──────────────────────────────────────────")
    sector_df = build_sector_permits()
    out = CLEANED_DIR / "sector_permits.csv"
    sector_df.to_csv(out, index=False)
    print(f"\n  ✓ {len(sector_df):,} rows saved → {out}")
    print(f"  Years   : {sorted(sector_df['year'].unique())}")
    print(f"  Sectors (sample): {sorted(sector_df['sector'].unique())[:5]}")

    # ── Nationality Permits ───────────────────────────────────────
    print("\n── Nationality Permits ─────────────────────────────────────")
    nat_df = build_nationality_permits()
    out = CLEANED_DIR / "nationality_permits.csv"
    nat_df.to_csv(out, index=False)
    print(f"\n  ✓ {len(nat_df):,} rows saved → {out}")
    print(f"  Years         : {sorted(nat_df['year'].unique())}")
    print(f"  Nationalities : {nat_df['nationality'].nunique()} unique values")

    # ── Visa Decisions ────────────────────────────────────────────
    print("\n── Visa Decisions (long-term only) ─────────────────────────")
    print(f"  Allow list    : {VISA_ALLOW_LIST}")
    print(f"  Source file   : {VISA_FILE}")
    visa_df = clean_visa_decisions()
    out = CLEANED_DIR / "visa_decisions.csv"
    visa_df.to_csv(out, index=False)
    print(f"\n  ✓ {len(visa_df):,} rows saved → {out}")
    print(f"  Years         : {sorted(visa_df['year'].unique())}")
    print(f"  Nationalities : {visa_df['nationality'].nunique()} unique values")
    print(f"  Columns       : {list(visa_df.columns)}")
    print(f"\n  Sample (top 5 by granted in 2024):")
    sample = visa_df[visa_df["year"] == 2024].nlargest(5, "granted")
    print(sample[["nationality", "received", "granted", "refused"]].to_string(index=False))

    # ── Summary ───────────────────────────────────────────────────
    print("\n── Output files in data/cleaned/ ───────────────────────────")
    for f in sorted(CLEANED_DIR.iterdir()):
        if f.suffix == ".csv":
            df_tmp = pd.read_csv(f)
            print(f"  {f.name:<35} {len(df_tmp):>6,} rows  ×  {len(df_tmp.columns)} cols")

    print("\n  Done. ✓")
