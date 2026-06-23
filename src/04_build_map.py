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

# Primary output: public/index.html  — tracked in git, served by AWS Amplify.
# Secondary output: output/map/ireland_employment_map.html — local preview copy.
PUBLIC_DIR  = Path("public")
OUTPUT_DIR  = Path("output/map")

# Years where data is incomplete (partial year release) — used to label the slider.
PARTIAL_YEARS = {2026}
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GEOJSON_PATH    = GEO_DIR / "GIS Maps of Ireland.json"
OUTPUT_PATH     = PUBLIC_DIR / "index.html"
OUTPUT_PATH_ALT = OUTPUT_DIR / "ireland_employment_map.html"

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


def _short_sector(raw):
    """Strip the NACE letter prefix (e.g. 'J - ') from a sector name."""
    if pd.isna(raw) or not str(raw).strip():
        return None
    return re.sub(r"^[A-Z]\s*-\s*", "", str(raw)).strip() or None


def load_company_data() -> dict:
    """
    Reads company_permits.csv (which now has a 'county' column after the
    02_build_sqlite.py county-enrichment join) and returns three structures:

      all_companies : flat list of {c, y, i, s, co} records (issued ≥ 5)
          c  = company_name_clean
          y  = year (int)
          i  = issued (int)
          s  = sector_short (NACE letter stripped)
          co = county (or "" if unmatched)

          WHY FLAT?  Pre-computing every county × sector × tab combination
          would create a combinatorial explosion in file size.  Instead we
          embed all qualifying records and let JavaScript filter client-side
          in milliseconds.  The issued ≥ 5 threshold keeps minor companies
          while ensuring every sector has at least some entries.

      top_growers : [{company, issued_2019, issued_2024, pct_change, sector, county}]
          Top 200 fastest growers (2019→2024 window).  Expanded from 20 to 200
          so county + sector filtering always finds relevant results.

      new_entrants : {year_str: [{company, sector, county}]}
          ALL new entrants per year (not just top 20) so sector/county
          filtering never produces false-empty results.

    Graceful degradation: returns {} if company_permits.csv is absent.
    The employer panel in the map checks for empty COMPANY_DATA and hides itself.
    """
    csv_path = CLEANED_DIR / "company_permits.csv"
    if not csv_path.exists():
        print("  [WARNING] company_permits.csv not found — employer panel will be hidden.")
        return {}

    df = pd.read_csv(csv_path)

    # Ensure sector column exists
    if "sector" not in df.columns:
        df["sector"] = None

    # Join county data from company_county_map.csv (produced by 06_enrich_company_counties.py).
    # company_permits.csv never has a county column — county lives in the CRO lookup file.
    county_map_path = CLEANED_DIR / "company_county_map.csv"
    if county_map_path.exists():
        county_map = pd.read_csv(county_map_path, usecols=["company_name_clean", "county"])
        county_map = county_map.rename(columns={"county": "county"})  # no-op, keeps clarity
        df = df.merge(county_map, on="company_name_clean", how="left")
        matched = df["county"].notna().sum()
        print(f"  County join: {matched:,}/{len(df):,} rows matched ({100*matched/len(df):.0f}%)")
    else:
        print("  [WARNING] company_county_map.csv not found — county data unavailable.")
        df["county"] = None

    # Strip NACE prefixes for display; replace NaN/empty with None
    df["sector_short"] = df["sector"].apply(_short_sector)
    df["county_clean"] = df["county"].apply(
        lambda v: str(v).strip() if pd.notna(v) and str(v).strip() else ""
    )

    # ── Flat ALL_COMPANIES array (issued ≥ 5) ─────────────────────────────────
    # Single-character JSON keys minimise file size (~40% smaller than full names).
    qualifying = df[df["issued"] >= 5].copy()
    all_companies = [
        {
            "c":  row["company_name_clean"],
            "y":  int(row["year"]),
            "i":  int(row["issued"]),
            "s":  row["sector_short"],   # None if untagged
            "co": row["county_clean"],   # "" if unmatched from CRO
        }
        for _, row in qualifying.iterrows()
    ]
    print(f"  ALL_COMPANIES: {len(all_companies):,} records (issued≥5)")

    # ── Fastest growers: 2019→2024 (expanded to top 200) ─────────────────────
    # Sector and county added so JS can filter the growers list by either dimension.
    # A lookup dict (company → county/sector) picks the most common value across
    # years (mode) to handle the rare case of a company changing sector over time.
    sector_lookup = (
        df[df["sector_short"].notna()]
        .groupby("company_name_clean")["sector_short"]
        .agg(lambda x: x.mode()[0] if len(x) else None)
    )
    county_lookup = (
        df[df["county_clean"] != ""]
        .groupby("company_name_clean")["county_clean"]
        .agg(lambda x: x.mode()[0] if len(x) else "")
    )

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
                "sector":      sector_lookup.get(company),
                "county":      county_lookup.get(company, ""),
            })
    growers = sorted(growers, key=lambda x: x["pct_change"], reverse=True)[:200]
    print(f"  top_growers:   {len(growers):,} companies (expanded to 200 for county/sector filtering)")

    # ── New entrants: ALL companies first appearing that year ──────────────────
    # Storing ALL (not just top 20) ensures every sector/county combo has results.
    # Companies are sorted by issued descending; JS slices to 20 after filtering.
    #
    # Use normalised names to compute first-year so that legal-suffix aliases
    # (e.g. "Redwood UC" vs "Redwood Unlimited Company", "&" vs "and") don't
    # make a returning company look like a new entrant.
    def _norm(name):
        n = str(name).lower().strip()
        n = n.replace("&", "and")
        n = re.sub(r"unlimited company", "", n)
        n = re.sub(r"\b(limited|ltd|plc|dac|uc|ulc|clg|teoranta|co|unlimited)\b\.?", "", n)
        return re.sub(r"\s+", " ", n).strip()

    df["_norm"] = df["company_name_clean"].apply(_norm)
    first_year_by_norm = df.groupby("_norm")["year"].min()
    df["_first_year"] = df["_norm"].map(first_year_by_norm)

    new_entrants = {}
    for year, grp in df.groupby("year"):
        debutants = grp[grp["_first_year"] == year]["company_name_clean"].unique()
        year_data = grp[grp["company_name_clean"].isin(debutants)].copy()
        if not year_data.empty:
            year_data = year_data.sort_values("issued", ascending=False)
            new_entrants[str(year)] = [
                {
                    "company": row["company_name_clean"],
                    "sector":  row["sector_short"],
                    "county":  row["county_clean"],
                    "issued":  int(row["issued"]),
                }
                for _, row in year_data.iterrows()
            ]
    total_entrants = sum(len(v) for v in new_entrants.values())
    print(f"  new_entrants:  {total_entrants:,} total entries across all years")

    return {
        "all_companies": all_companies,
        "top_growers":   growers,
        "new_entrants":  new_entrants,
    }


def load_county_sector_data() -> dict:
    """
    Reads the county_sector_breakdown.csv produced by 03_analyze.py (if present)
    and returns a nested dict:

      county_sector : {county: {year_str: [{sector, issued}]}}

    This is a derived dataset — DETE never publishes county × sector breakdowns.
    It is computed by joining company county (from CRO) with company sector tags.

    Returns {} gracefully if the file is absent (i.e. 03_analyze.py hasn't run
    yet, or 06_enrich_company_counties.py hasn't enriched the county column).
    """
    csv_path = Path("output/tables") / "county_sector_breakdown.csv"
    if not csv_path.exists():
        print("  [INFO] county_sector_breakdown.csv not found — "
              "county-level sector chart will fall back to national data.")
        return {}

    df = pd.read_csv(csv_path)
    county_sector = {}
    for county, c_grp in df.groupby("county"):
        county_sector[county] = {}
        for year, y_grp in c_grp.groupby("year"):
            county_sector[county][str(year)] = [
                {
                    "sector": row["sector"],
                    "issued": int(row["issued"]) if pd.notna(row["issued"]) else 0,
                }
                for _, row in y_grp.sort_values("issued", ascending=False).iterrows()
            ]
    n_counties = len(county_sector)
    n_rows = sum(
        len(v2) for v in county_sector.values() for v2 in v.values()
    )
    print(f"  COUNTY_SECTOR: {n_counties} counties, {n_rows:,} county×year×sector rows")
    return county_sector


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

def build_year_labels(slider_years: list) -> dict:
    """
    Map each year string to its display label.
    Slider years map to themselves; partial years get a "(Partial)" label
    so the radio button JS can display the correct text.
    """
    labels = {y: y for y in slider_years}
    for py in sorted(PARTIAL_YEARS):
        labels[str(py)] = f"{py} (Partial)"
    return labels


def build_html(county_by_year: dict, county_growth: dict,
               sector_by_year: dict, company_data: dict,
               county_sector: dict, geojson: dict, years: list) -> str:
    """Fill the HTML template with embedded JSON data blobs."""
    # Slider only shows full years; partial years appear as detached radio buttons.
    slider_years  = [y for y in years if int(y) not in PARTIAL_YEARS]
    partial_years = [str(y) for y in sorted(PARTIAL_YEARS)]

    return HTML_TEMPLATE.replace(
        "{COUNTY_BY_YEAR_JSON}",  json.dumps(county_by_year)
    ).replace(
        "{COUNTY_GROWTH_JSON}",   json.dumps(county_growth)
    ).replace(
        "{SECTOR_BY_YEAR_JSON}",  json.dumps(sector_by_year)
    ).replace(
        "{COMPANY_DATA_JSON}",    json.dumps(company_data)
    ).replace(
        "{COUNTY_SECTOR_JSON}",   json.dumps(county_sector)
    ).replace(
        "{GEOJSON_JSON}",         json.dumps(geojson)
    ).replace(
        "{YEARS_JSON}",           json.dumps(slider_years)
    ).replace(
        "{YEAR_LABELS_JSON}",     json.dumps(build_year_labels(slider_years))
    ).replace(
        "{PARTIAL_YEARS_JSON}",   json.dumps(partial_years)
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
        n_all = len(company_data.get("all_companies", []))
        n_grow = len(company_data.get("top_growers", []))
        print(f"  Company data: {n_all:,} flat records, {n_grow} fastest-growers")
    else:
        print("  Company data: not available — employer panel will be hidden")

    print("\n── Loading county × sector breakdown")
    county_sector = load_county_sector_data()

    # ── Load GeoJSON ──────────────────────────────────────────────────
    print(f"\n── Loading GeoJSON from {GEOJSON_PATH}")
    geojson = prepare_geojson(county_growth)

    # ── Build and write HTML ──────────────────────────────────────────
    print(f"\n── Generating HTML")
    html = build_html(county_by_year, county_growth, sector_by_year,
                      company_data, county_sector, geojson, years)

    # Write primary output (for deployment via AWS Amplify)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  ✓ Written → {OUTPUT_PATH}  ({size_kb:.0f} KB)")

    # Write secondary output (local preview file)
    OUTPUT_PATH_ALT.write_text(html, encoding="utf-8")
    print(f"  ✓ Written → {OUTPUT_PATH_ALT}  (local preview copy)")

    print(f"\n  Open in your browser:")
    print(f"    open \"{OUTPUT_PATH_ALT}\"   (macOS)")
    print(f"    start \"{OUTPUT_PATH_ALT}\"  (Windows)")
    print("\n  Done. ✓")
