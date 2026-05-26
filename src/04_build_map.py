"""
src/04_build_map.py
===================
Generates a standalone interactive HTML choropleth map at:
  public/index.html                     ← served by AWS Amplify at the root URL

Features:
  • Leaflet.js choropleth coloured by permits issued per county
  • Year slider (2015–2025) with animated transitions
  • Hover tooltip + click popup showing county stats and % growth
  • Sidebar: year summary, top counties ranked by permits, % growth vs 2015
  • Sector breakdown bar chart in sidebar
  • Sector filter dropdown — filters the sidebar sector chart
  • Employer intelligence panel: top 20, fastest-growing, new entrants per year
  • No server required — opens directly in any browser

Requirements:
  data/geo/ireland_counties.geojson   ← YOU MUST DOWNLOAD THIS FIRST (see below)
  data/cleaned/county_permits.csv
  data/cleaned/sector_permits.csv
  data/cleaned/company_permits.csv    ← optional; employer panel hidden if absent

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 GEOJSON
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Source : simplemaps.com (via data/geo/GIS Maps of Ireland.json)
 26 features, property field: 'name'
 One spelling difference handled in COUNTY_NAME_MAP:
   'Laoighis' (GeoJSON) → 'Laois' (permit data)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Run from project root:
  Terminal : python src/04_build_map.py
  Jupyter  : %run src/04_build_map.py
"""

import json
import re
import sqlite3
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
CLEANED_DIR = Path("data/cleaned")
GEO_DIR     = Path("data/geo")

# Output goes to public/ so AWS Amplify can serve it at the root URL.
# public/index.html is tracked in git — push after rebuilding to deploy.
PUBLIC_DIR  = Path("public")
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

GEOJSON_PATH = GEO_DIR / "GIS Maps of Ireland.json"
OUTPUT_PATH  = PUBLIC_DIR / "index.html"

# ── County name normalisation ─────────────────────────────────────────────────
# Different GeoJSON sources use different spellings. This map converts whatever
# the GeoJSON uses into the canonical names in our cleaned permit data.
# The permit data uses clean single-word county names (e.g. "Dublin", "Cork").
COUNTY_NAME_MAP = {
    # GeoJSON-specific spellings → canonical permit data names
    "Laoighis": "Laois",    # this file spells it 'Laoighis'; our data uses 'Laois'

    # "County X" → "X"
    "County Carlow":    "Carlow",   "County Cavan":     "Cavan",
    "County Clare":     "Clare",    "County Cork":      "Cork",
    "County Donegal":   "Donegal",  "County Dublin":    "Dublin",
    "County Galway":    "Galway",   "County Kerry":     "Kerry",
    "County Kildare":   "Kildare",  "County Kilkenny":  "Kilkenny",
    "County Laois":     "Laois",    "County Leitrim":   "Leitrim",
    "County Limerick":  "Limerick", "County Longford":  "Longford",
    "County Louth":     "Louth",    "County Mayo":      "Mayo",
    "County Meath":     "Meath",    "County Monaghan":  "Monaghan",
    "County Offaly":    "Offaly",   "County Roscommon": "Roscommon",
    "County Sligo":     "Sligo",    "County Tipperary": "Tipperary",
    "County Waterford": "Waterford","County Westmeath": "Westmeath",
    "County Wexford":   "Wexford",  "County Wicklow":   "Wicklow",
    # "Co. X" variants
    "Co. Carlow": "Carlow", "Co. Cavan": "Cavan", "Co. Clare": "Clare",
    "Co. Cork":   "Cork",   "Co. Donegal": "Donegal", "Co. Dublin": "Dublin",
    "Co. Galway": "Galway", "Co. Kerry": "Kerry",  "Co. Kildare": "Kildare",
    "Co. Kilkenny": "Kilkenny", "Co. Laois": "Laois", "Co. Leitrim": "Leitrim",
    "Co. Limerick": "Limerick", "Co. Longford": "Longford", "Co. Louth": "Louth",
    "Co. Mayo": "Mayo", "Co. Meath": "Meath", "Co. Monaghan": "Monaghan",
    "Co. Offaly": "Offaly", "Co. Roscommon": "Roscommon", "Co. Sligo": "Sligo",
    "Co. Tipperary": "Tipperary", "Co. Waterford": "Waterford",
    "Co. Westmeath": "Westmeath", "Co. Wexford": "Wexford", "Co. Wicklow": "Wicklow",
}


def detect_county_field(geojson: dict) -> str:
    """
    Auto-detect which GeoJSON property holds the county name.
    Checks common field names used by OSi, GADM, and GitHub mirrors.
    Raises ValueError if none found.
    """
    candidate_fields = ["COUNTY", "County", "NAME_1", "Name_1", "name", "NAME",
                        "COUNTYNAME", "CountyName", "county_name"]
    if not geojson.get("features"):
        raise ValueError("GeoJSON has no 'features' array.")
    sample_props = geojson["features"][0].get("properties", {})
    for field in candidate_fields:
        if field in sample_props:
            print(f"  Detected county name field: '{field}'")
            return field
    raise ValueError(
        f"Could not auto-detect county name field.\n"
        f"Properties found: {list(sample_props.keys())}\n"
        f"Please check your GeoJSON and add the correct field name to "
        f"'candidate_fields' in detect_county_field()."
    )


def normalise_county(name: str) -> str:
    """
    Convert a raw GeoJSON county name to the canonical form used in our data.
    Falls back to stripping 'County ' prefix, then returns name as-is.
    """
    if name in COUNTY_NAME_MAP:
        return COUNTY_NAME_MAP[name]
    # Strip leading "County " as a fallback
    if name.startswith("County "):
        return name[7:]
    return name


# ── Data preparation ──────────────────────────────────────────────────────────

def load_county_data() -> tuple[dict, dict, list]:
    """
    Reads the county permits CSV and organises it into three structures
    that the JavaScript map can use directly:

      county_by_year : {year_str: {county: {issued, refused, pct_share}}}
          Nested dictionary — outer key = year (as a string), inner key = county name.
          Example: county_by_year["2024"]["Dublin"] = {issued: 12000, refused: 500, pct_share: 38.2}

      county_growth  : {county: {issued_2015, issued_latest, pct_change}}
          One entry per county comparing 2015 to 2024.  Used for the default growth column.

      years          : sorted list of year strings, e.g. ["2015", "2016", ..., "2025"]
          Powers the slider tick marks.

    All years are stored as strings because JavaScript object keys are always strings.
    """
    df = pd.read_csv(CLEANED_DIR / "county_permits.csv")

    # ── Build the per-year, per-county lookup ─────────────────────────────────
    # groupby("year") splits the DataFrame into one group per year — like
    # doing a filter for each year and processing it separately.
    county_by_year = {}
    for year, grp in df.groupby("year"):
        year_total = grp["issued"].sum()   # national total for this year
        county_by_year[str(year)] = {
            row["county"]: {
                "issued":    int(row["issued"])  if pd.notna(row["issued"])  else 0,
                "refused":   int(row["refused"]) if pd.notna(row["refused"]) else 0,
                # pct_share = this county's issued / national total × 100
                # The guard (year_total > 0 and pd.notna) prevents divide-by-zero
                "pct_share": round(100 * row["issued"] / year_total, 1)
                             if year_total > 0 and pd.notna(row["issued"]) else 0,
            }
            for _, row in grp.iterrows()
        }

    # ── Growth: compare 2015 baseline to most recent full year (2024) ─────────
    # set_index("county") turns the county column into the row label,
    # so we can look up a county's value by name: base["Dublin"] → 5432
    base   = df[df["year"] == 2015].set_index("county")["issued"]
    latest = df[df["year"] == 2024].set_index("county")["issued"]

    county_growth = {}
    for county in base.index:
        if county in latest.index and pd.notna(base[county]) and base[county] > 0:
            pct = round(100 * (latest[county] - base[county]) / base[county], 1)
            county_growth[county] = {
                "issued_2015":   int(base[county]),
                "issued_latest": int(latest[county]) if pd.notna(latest[county]) else 0,
                "pct_change":    float(pct),
            }

    # Warn if any counties appear in 2015 but are missing from 2024.
    # This means they'll show on the map but won't have a growth figure.
    # It is expected for counties with very small counts that may have been suppressed.
    missing_from_2024 = set(base.index) - set(latest.index)
    if missing_from_2024:
        print(f"  [WARNING] Counties in 2015 but missing from 2024 "
              f"(no growth calculated): {sorted(missing_from_2024)}")

    years = sorted(county_by_year.keys())
    return county_by_year, county_growth, years


def load_sector_data() -> dict:
    """
    Returns sector_by_year: {year_str: [{sector_short, sector_full, issued}]}

    Each year maps to a list of sectors, sorted by issued descending.
    Example: sector_by_year["2024"][0] = {sector_full: "J - Information & Communication",
                                          sector_short: "Information & Communication",
                                          issued: 8500}

    sector_short strips the leading letter code (e.g. "J - ") for display.
    sector_full is kept so the popup tooltip can show the complete name.

    ALL sectors are included (not just top 10) so the dropdown has the full list.
    Only 2020–2025 — pre-2020 sector names changed and can't be compared reliably.
    """
    df = pd.read_csv(CLEANED_DIR / "sector_permits.csv")
    df = df[df["year"] >= 2020]

    sector_by_year = {}
    for year, grp in df.groupby("year"):
        # Sort all sectors by issued descending — JS decides how many to display
        all_sectors = grp.sort_values("issued", ascending=False).copy()
        all_sectors["sector_short"] = all_sectors["sector"].apply(
            lambda s: re.sub(r"^[A-Z]\s*-\s*", "", str(s)).strip()
        )
        sector_by_year[str(year)] = [
            {
                "sector_full":  row["sector"],
                "sector_short": row["sector_short"],
                "issued":       int(row["issued"]) if pd.notna(row["issued"]) else 0,
            }
            for _, row in all_sectors.iterrows()
        ]

    return sector_by_year


def load_company_data() -> dict:
    """
    Reads company_permits.csv and returns three employer intelligence signals
    for injection into the map's JavaScript:

      top_by_year  : {year_str: [{company, issued}, ...]}
          Top 20 employers per year by permits issued.

      top_growers  : [{company, issued_2019, issued_2024, pct_change}, ...]
          Top 20 companies present in both 2019 and 2024, ranked by % growth.
          Uses 2019→2024 as the reference window (avoids pandemic distortion).

      new_entrants : {year_str: [company, ...]}
          Up to 20 companies first appearing in that year, ranked by permits
          issued in their debut year.

    Graceful degradation: returns {} and prints a warning if the CSV is absent.
    The employer panel in the map checks for an empty COMPANY_DATA and hides itself.
    """
    csv_path = CLEANED_DIR / "company_permits.csv"
    if not csv_path.exists():
        print("  [WARNING] company_permits.csv not found — employer panel will be hidden.")
        return {}

    df = pd.read_csv(csv_path)

    # Ensure sector column exists even if company_sector_map.csv was absent at build time
    if "sector" not in df.columns:
        df["sector"] = None

    # ── Top 50 employers per year ──────────────────────────────────────────────
    # We store 50 (not 20) so the JavaScript dynamic-growth calculation has enough
    # coverage — a company that grew from rank 40 to rank 2 would be invisible if
    # we only stored the top 20 for each year.
    #
    # sector_short strips the leading NACE letter code (e.g. "J - ") so it matches
    # the sector_short values already used in the sector-select dropdown — enabling
    # a direct string equality comparison in JavaScript.
    top_by_year = {}
    for year, grp in df.groupby("year"):
        top50 = grp.nlargest(50, "issued")[["company_name_clean", "issued", "sector"]]
        top_by_year[str(year)] = [
            {
                "company": row["company_name_clean"],
                "issued":  int(row["issued"]),
                "sector":  (
                    re.sub(r"^[A-Z]\s*-\s*", "", str(row["sector"])).strip()
                    if pd.notna(row["sector"]) else None
                ),
            }
            for _, row in top50.iterrows()
        ]

    # ── Fastest growers: 2019→2024 ────────────────────────────────────────────
    # 2019 and 2024 are both full years sitting outside the pandemic distortion
    # window, making them the most honest baseline/endpoint pair in the dataset.
    base   = df[df["year"] == 2019].set_index("company_name_clean")["issued"]
    latest = df[df["year"] == 2024].set_index("company_name_clean")["issued"]
    common = base.index.intersection(latest.index)
    growers = []
    for company in common:
        b, l = int(base[company]), int(latest[company])
        if b > 0:
            pct = round(100 * (l - b) / b, 1)
            growers.append({
                "company":     company,
                "issued_2019": b,
                "issued_2024": l,
                "pct_change":  pct,
            })
    growers = sorted(growers, key=lambda x: x["pct_change"], reverse=True)[:20]

    # ── New entrants: first year each company appears ─────────────────────────
    # first_year[company] = the earliest year that company has a row in the data.
    # Each entry is {company, sector} so the JS sector filter can apply directly.
    first_year = df.groupby("company_name_clean")["year"].min()
    new_entrants = {}
    for year, grp in df.groupby("year"):
        # Companies whose very first row in any year is this year
        debutants = first_year[first_year == year].index
        year_data = grp[grp["company_name_clean"].isin(debutants)]
        if not year_data.empty:
            top_new = year_data.nlargest(20, "issued")[["company_name_clean", "sector"]]
            new_entrants[str(year)] = [
                {
                    "company": row["company_name_clean"],
                    "sector": (
                        re.sub(r"^[A-Z]\s*-\s*", "", str(row["sector"])).strip()
                        if pd.notna(row["sector"]) else None
                    ),
                }
                for _, row in top_new.iterrows()
            ]

    return {
        "top_by_year":  top_by_year,
        "top_growers":  growers,
        "new_entrants": new_entrants,
    }


# ── GeoJSON loading + county name injection ───────────────────────────────────

def prepare_geojson(county_growth: dict) -> dict:
    """
    Load the GeoJSON, normalise county names in properties, and inject the
    'canonical_county' property so the JS can look up data by county name.
    """
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(
            f"\n  GeoJSON not found: {GEOJSON_PATH}\n"
            "  Please download it — see the docstring at the top of this file\n"
            "  for download instructions (three options provided).\n"
        )

    with open(GEOJSON_PATH, encoding="utf-8") as f:
        geo = json.load(f)

    county_field = detect_county_field(geo)
    matched, unmatched = 0, []

    for feature in geo["features"]:
        raw_name = feature["properties"].get(county_field, "")
        canonical = normalise_county(str(raw_name).strip())
        feature["properties"]["canonical_county"] = canonical

        if canonical in county_growth:
            matched += 1
        else:
            unmatched.append(f"{raw_name!r} → {canonical!r}")

    print(f"  GeoJSON: {len(geo['features'])} features, {matched} matched to permit data")
    if unmatched:
        print(f"  Unmatched counties (no permit data): {unmatched}")

    return geo


# ── HTML template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = (Path(__file__).parent / "map_template.html").read_text(encoding="utf-8")


# ── HTML generation ───────────────────────────────────────────────────────────

def build_html(county_by_year: dict, county_growth: dict,
               sector_by_year: dict, company_data: dict,
               geojson: dict, years: list) -> str:
    """Fill the HTML template with embedded JSON data blobs."""
    return HTML_TEMPLATE.replace(
        "{COUNTY_BY_YEAR_JSON}", json.dumps(county_by_year)
    ).replace(
        "{COUNTY_GROWTH_JSON}",  json.dumps(county_growth)
    ).replace(
        "{SECTOR_BY_YEAR_JSON}", json.dumps(sector_by_year)
    ).replace(
        "{COMPANY_DATA_JSON}",   json.dumps(company_data)
    ).replace(
        "{GEOJSON_JSON}",        json.dumps(geojson)
    ).replace(
        "{YEARS_JSON}",          json.dumps(years)
    )


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  04_build_map.py  —  Ireland Employment Choropleth Map")
    print("=" * 60)

    # ── Load and prepare data ──────────────────────────────────────────
    print("\n── Loading permit data")
    county_by_year, county_growth, years = load_county_data()
    print(f"  County data : {len(years)} years, "
          f"{len(next(iter(county_by_year.values())))} counties")
    print(f"  Growth data : {len(county_growth)} counties with 2015→2024 comparison")

    print("\n── Loading sector data")
    sector_by_year = load_sector_data()
    print(f"  Sector data : years {sorted(sector_by_year.keys())}")

    print("\n── Loading company data")
    company_data = load_company_data()
    if company_data:
        print(f"  Company data: {len(company_data['top_by_year'])} years, "
              f"{len(company_data['top_growers'])} fastest-growing employers")
    else:
        print("  Company data: not available — employer panel will be hidden")

    # ── Load GeoJSON ──────────────────────────────────────────────────
    print(f"\n── Loading GeoJSON from {GEOJSON_PATH}")
    geojson = prepare_geojson(county_growth)

    # ── Build and write HTML ──────────────────────────────────────────
    print(f"\n── Generating HTML")
    html = build_html(county_by_year, county_growth, sector_by_year,
                      company_data, geojson, years)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  ✓ Written → {OUTPUT_PATH}  ({size_kb:.0f} KB)")
    print(f"\n  Open in your browser:")
    print(f"    open \"{OUTPUT_PATH}\"          (macOS)")
    print(f"    start \"{OUTPUT_PATH}\"         (Windows)")
    print("\n  Done. ✓")
