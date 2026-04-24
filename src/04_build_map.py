"""
src/04_build_map.py
===================
Generates a standalone interactive HTML choropleth map at:
  output/map/ireland_employment_map.html

Features:
  • Leaflet.js choropleth coloured by permits issued per county
  • Year slider (2015–2025) with animated transitions
  • Hover tooltip + click popup showing county stats and % growth
  • Sidebar: year summary, top counties ranked by permits, % growth vs 2015
  • Sector breakdown bar chart in sidebar
  • Sector filter dropdown — filters the sidebar sector chart
  • No server required — opens directly in any browser

Requirements:
  data/geo/ireland_counties.geojson   ← YOU MUST DOWNLOAD THIS FIRST (see below)
  data/cleaned/county_permits.csv
  data/cleaned/sector_permits.csv

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
import sqlite3
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
CLEANED_DIR = Path("data/cleaned")
GEO_DIR     = Path("data/geo")
OUTPUT_DIR  = Path("output/map")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GEOJSON_PATH = GEO_DIR / "GIS Maps of Ireland.json"
OUTPUT_PATH  = OUTPUT_DIR / "ireland_employment_map.html"

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
    import re as _re
    df = pd.read_csv(CLEANED_DIR / "sector_permits.csv")
    df = df[df["year"] >= 2020]

    sector_by_year = {}
    for year, grp in df.groupby("year"):
        # Sort all sectors by issued descending — JS decides how many to display
        all_sectors = grp.sort_values("issued", ascending=False).copy()
        all_sectors["sector_short"] = all_sectors["sector"].apply(
            lambda s: _re.sub(r"^[A-Z]\s*-\s*", "", str(s)).strip()
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

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Ireland Work Permits — Interactive Map</title>

<!-- Leaflet CSS -->
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<!-- noUiSlider CSS (two-handle range slider) -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/nouislider@15.8.1/dist/nouislider.min.css" />

<style>
  /* ── Reset & base ── */
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f8;
         color: #222; display: flex; flex-direction: column; height: 100vh; }

  /* ── Header ── */
  #header {
    background: #169B62; color: #fff; padding: 10px 20px;
    display: flex; align-items: center; justify-content: space-between;
    flex-shrink: 0;
  }
  #header h1 { font-size: 1.1rem; font-weight: 600; letter-spacing: 0.02em; }
  #header span { font-size: 0.78rem; opacity: 0.85; }

  /* ── Main layout ── */
  #main { display: flex; flex: 1; overflow: hidden; }

  /* ── Sidebar ── */
  #sidebar {
    width: 330px; min-width: 310px; background: #fff;
    border-right: 1px solid #dde2ea; overflow-y: auto;
    display: flex; flex-direction: column; padding: 14px; gap: 14px;
    flex-shrink: 0;
  }
  .sidebar-section { border: 1px solid #e8ecf0; border-radius: 8px; padding: 10px 12px; }
  .sidebar-section h3 {
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.06em; color: #169B62; margin-bottom: 8px;
  }

  /* Year display */
  #year-display {
    font-size: 2.4rem; font-weight: 700; color: #169B62;
    text-align: center; line-height: 1;
  }
  #year-subtitle { font-size: 0.72rem; color: #888; text-align: center; margin-top: 3px; }

  /* National total */
  #national-total {
    font-size: 1.05rem; font-weight: 600; text-align: center;
    color: #333; margin-top: 4px;
  }
  #national-label { font-size: 0.68rem; color: #999; text-align: center; }

  /* Top counties / sector table — 4 columns: # | Name | Permits | Growth */
  #county-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; table-layout: fixed; }
  #county-table th {
    text-align: left; color: #888; font-weight: 600; font-size: 0.68rem;
    text-transform: uppercase; padding: 3px 4px; border-bottom: 1px solid #eee;
    white-space: nowrap; overflow: hidden;
  }
  /* col 1 — rank */
  #county-table th:nth-child(1), #county-table td:nth-child(1) { width: 22px; }
  /* col 2 — name — takes remaining space */
  /* col 3 — permits */
  #county-table th:nth-child(3), #county-table td:nth-child(3) { width: 54px; text-align: right; }
  /* col 4 — growth */
  #county-table th:nth-child(4), #county-table td:nth-child(4) { width: 78px; text-align: right; }
  #county-table td {
    padding: 4px 4px; border-bottom: 1px solid #f0f0f0;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  #county-table tr:last-child td { border-bottom: none; }
  .growth-pos { color: #169B62; font-weight: 600; }
  .growth-neg { color: #e05c2a; font-weight: 600; }

  /* Sector filter */
  #sector-select {
    width: 100%; padding: 5px 7px; border: 1px solid #d0d6de;
    border-radius: 5px; font-size: 0.78rem; color: #333;
    background: #fff; cursor: pointer;
  }
  #sector-select:focus { outline: 2px solid #169B62; }

  /* Sector chart */
  #sector-chart-container { position: relative; height: 200px; }

  /* ── Map area ── */
  #map-wrap { flex: 1; display: flex; flex-direction: column; overflow: hidden;
              position: relative; }   /* needed so badge can be absolute */
  #map { flex: 1; }

  /* Sector mode badge — floats over the map */
  #sector-map-badge {
    display: none; position: absolute; top: 10px; left: 50%; z-index: 1000;
    transform: translateX(-50%);
    background: rgba(22,155,98,0.92); color: #fff;
    padding: 5px 14px; border-radius: 20px; font-size: 0.72rem; font-weight: 600;
    pointer-events: none; white-space: nowrap;
    box-shadow: 0 2px 6px rgba(0,0,0,0.2);
  }

  /* ── Year slider bar ── */
  #slider-bar {
    background: #fff; border-top: 1px solid #dde2ea;
    padding: 12px 28px 14px; flex-shrink: 0;
  }
  #slider-label {
    font-size: 0.72rem; font-weight: 600; color: #555;
    margin-bottom: 10px; text-align: center;
  }
  #year-slider { margin: 0 6px; }
  #slider-ticks {
    display: flex; justify-content: space-between;
    font-size: 0.65rem; color: #aaa; margin-top: 8px;
    padding: 0 2px;
  }

  /* noUiSlider — Ireland green theme */
  .noUi-target { background: #e8ecf0; border: none; box-shadow: none; height: 6px; }
  .noUi-connect { background: #169B62; }
  .noUi-handle {
    width: 18px !important; height: 18px !important;
    top: -7px !important; right: -9px !important;
    border-radius: 50%; background: #169B62;
    border: 2px solid #fff; box-shadow: 0 1px 4px rgba(0,0,0,0.25);
    cursor: grab;
  }
  .noUi-handle:active { cursor: grabbing; background: #0d6b44; }
  .noUi-handle::before, .noUi-handle::after { display: none; }
  /* Tooltip bubbles above each handle */
  .noUi-tooltip {
    background: #169B62; color: #fff; border: none;
    font-size: 0.7rem; font-weight: 700; padding: 2px 6px;
    border-radius: 4px; bottom: 26px;
  }

  /* ── Leaflet popup ── */
  .leaflet-popup-content { font-size: 0.82rem; line-height: 1.6; min-width: 170px; }
  .popup-county { font-size: 1rem; font-weight: 700; color: #169B62;
                  margin-bottom: 4px; border-bottom: 1px solid #eee; padding-bottom: 3px; }
  .popup-row { display: flex; justify-content: space-between; gap: 12px; }
  .popup-label { color: #888; }
  .popup-val   { font-weight: 600; }

  /* ── Legend ── */
  .info.legend {
    background: rgba(255,255,255,0.92); padding: 8px 12px;
    border-radius: 6px; font-size: 0.72rem; line-height: 1.5;
    box-shadow: 0 1px 5px rgba(0,0,0,0.15);
  }
  .legend-title { font-weight: 700; margin-bottom: 4px; color: #333; }
  .legend-row { display: flex; align-items: center; gap: 6px; }
  .legend-box { width: 14px; height: 14px; border-radius: 2px; flex-shrink: 0; }

  /* ── No data note ── */
  #no-sector-note {
    font-size: 0.7rem; color: #aaa; text-align: center;
    padding: 8px 0; display: none;
  }
</style>
</head>
<body>

<!-- Header -->
<div id="header">
  <h1>🇮🇪 Ireland Work Permits — County Explorer</h1>
  <span>Source: DETE work permit data 2015–2025</span>
</div>

<!-- Main: sidebar + map -->
<div id="main">

  <!-- Sidebar -->
  <div id="sidebar">

    <!-- Year range + national total -->
    <div class="sidebar-section">
      <h3>Selected Range</h3>
      <div id="year-display" style="font-size:1.4rem;">2015 → 2024</div>
      <div id="year-subtitle">Map shows the "to" year. Growth is from → to.</div>
      <div id="national-total">—</div>
      <div id="national-label">permits issued in selected "to" year</div>
    </div>

    <!-- Top counties -->
    <div class="sidebar-section">
      <h3>Top Counties — Permits Issued</h3>
      <table id="county-table">
        <thead>
          <tr>
            <th>#</th><th>County</th><th>Issued</th><th id="growth-col-header">Growth</th>
          </tr>
        </thead>
        <tbody id="county-table-body"></tbody>
      </table>
    </div>

    <!-- Sector filter + chart -->
    <div class="sidebar-section">
      <h3>Sector Breakdown</h3>
      <p style="font-size:0.68rem;color:#888;margin-bottom:6px;">
        💡 IT &amp; tech companies fall under <em>Information &amp; Communication</em>.
        Sector data is national-level (2020 onwards).
      </p>
      <select id="sector-select">
        <option value="all">— All sectors —</option>
      </select>
      <div id="no-sector-note" style="display:none;font-size:0.7rem;color:#aaa;text-align:center;padding:8px 0;">
        No data for this sector in the selected year
      </div>
      <div id="sector-chart-container" style="margin-top:10px;position:relative;height:200px;">
        <canvas id="sector-chart"></canvas>
      </div>
    </div>

  </div><!-- /sidebar -->

  <!-- Map -->
  <div id="map-wrap">
    <div id="map"></div>
    <div id="sector-map-badge">📊 Sector data is national — map shows total county permits</div>

    <!-- Year range slider -->
    <div id="slider-bar">
      <div id="slider-label">Drag handles to select a year range</div>
      <div id="year-slider"></div>
      <div id="slider-ticks"></div>
    </div>
  </div>

</div><!-- /main -->

<!-- Leaflet JS -->
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<!-- noUiSlider JS -->
<script src="https://cdn.jsdelivr.net/npm/nouislider@15.8.1/dist/nouislider.min.js"></script>
<!-- Chart.js for the sector bar chart -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>

<script>
// ═══════════════════════════════════════════════════════════════════════
//  EMBEDDED DATA  (injected by 04_build_map.py at build time)
//
//  All of the data from our Python pipeline is baked directly into this
//  HTML file as JavaScript constants.  This makes the map fully self-contained:
//  no server, no database connection — just open the file in a browser.
//
//  COUNTY_BY_YEAR  — {year: {county: {issued, refused, pct_share}}}
//  COUNTY_GROWTH   — {county: {issued_2015, issued_latest, pct_change}}
//  SECTOR_BY_YEAR  — {year: [{sector_short, sector_full, issued}]}
//  GEOJSON         — the county boundary shapes (GeoJSON format)
//  YEARS           — sorted list of years, e.g. ["2015","2016",...,"2025"]
// ═══════════════════════════════════════════════════════════════════════
const COUNTY_BY_YEAR  = {COUNTY_BY_YEAR_JSON};
const COUNTY_GROWTH   = {COUNTY_GROWTH_JSON};
const SECTOR_BY_YEAR  = {SECTOR_BY_YEAR_JSON};
const GEOJSON         = {GEOJSON_JSON};
const YEARS           = {YEARS_JSON};

// ═══════════════════════════════════════════════════════════════════════
//  COLOUR SCALE
// ═══════════════════════════════════════════════════════════════════════

// First, find the highest permit count across ALL years and counties.
// WHY? So that the colour scale stays consistent as you move the slider.
// If we recalculated the max for each year separately, a year with fewer
// permits overall would look just as "dark green" as a bumper year —
// which would be misleading.  One global max = honest comparison.
const GLOBAL_MAX = (function() {
  let m = 0;
  for (const yr of YEARS) {
    const yd = COUNTY_BY_YEAR[yr] || {};
    for (const c in yd) { if (yd[c].issued > m) m = yd[c].issued; }
  }
  return m;
})();

// Convert a permit count to a hex/rgb colour.
// We use a square-root scale (Math.pow(…, 0.5)) rather than a linear one.
// WHY? Dublin always dominates with ~40% of national permits.  On a linear
// scale Dublin would be very dark and everywhere else very light, making
// differences between smaller counties invisible.  The sqrt scale compresses
// the top end so all counties show meaningful colour variation.
function getColor(issued) {
  if (!issued || issued === 0) return '#f0f0f0';   // no data → light grey
  const t = Math.pow(issued / GLOBAL_MAX, 0.5);   // 0..1 on sqrt scale
  const lerp = (a, b, t) => Math.round(a + (b - a) * t);  // linear interpolation
  // Interpolate from light green (212,240,227) to dark Ireland green (13,107,68)
  const r = lerp(212, 13,  t);
  const g = lerp(240, 107, t);
  const b = lerp(227, 68,  t);
  return `rgb(${r},${g},${b})`;
}

// ═══════════════════════════════════════════════════════════════════════
//  LEAFLET MAP SETUP
// ═══════════════════════════════════════════════════════════════════════
const map = L.map('map', { zoomControl: true }).setView([53.3, -7.8], 7);

// Base tile — CartoDB Positron with no labels for a clean choropleth look.
// Labels are intentionally omitted; county names appear on hover/click instead.
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png', {
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> © <a href="https://carto.com/">CARTO</a>',
  subdomains: 'abcd', maxZoom: 19,
}).addTo(map);

// ── GeoJSON layer ──────────────────────────────────────────────────────
let geojsonLayer = null;
let activePopup  = null;

function countyStyle(feature, year) {
  const county = feature.properties.canonical_county;
  const data   = (COUNTY_BY_YEAR[year] || {})[county];
  const issued = data ? data.issued : 0;
  return {
    fillColor:   getColor(issued),
    fillOpacity: 0.8,
    color:       '#fff',
    weight:      1.2,
  };
}

function onEachFeature(feature, layer) {
  layer.on({
    mouseover(e) {
      const l = e.target;
      l.setStyle({ weight: 2.5, color: '#333', fillOpacity: 0.95 });
      l.bringToFront();
      updateTooltip(feature);
    },
    mouseout(e) {
      geojsonLayer.resetStyle(e.target);
    },
    click(e) {
      showPopup(feature, e.latlng);
    },
  });
}

function buildGeojsonLayer(year) {
  if (geojsonLayer) map.removeLayer(geojsonLayer);
  geojsonLayer = L.geoJSON(GEOJSON, {
    style:          f => countyStyle(f, year),
    onEachFeature:  onEachFeature,
  }).addTo(map);
}

function refreshGeojsonColors(year) {
  if (!geojsonLayer) return;
  geojsonLayer.eachLayer(layer => {
    layer.setStyle(countyStyle(layer.feature, year));
  });
}

// ── Tooltip (bottom-left info box) ───────────────────────────────────
const infoBox = L.control({ position: 'bottomleft' });
infoBox.onAdd = function() {
  this._div = L.DomUtil.create('div', 'info leaflet-popup-content');
  this._div.style.cssText = 'padding:8px 12px;background:rgba(255,255,255,0.92);border-radius:6px;font-size:0.78rem;min-width:160px;box-shadow:0 1px 5px rgba(0,0,0,0.15)';
  this._div.innerHTML = '<span style="color:#aaa">Hover over a county</span>';
  return this._div;
};
infoBox.addTo(map);

// ── Slider state — which years the user has selected ──────────────────
// These two variables are updated every time the slider moves.
// They are declared at the top level (outside functions) so every function
// in the page can read the current selection without being passed arguments.
let currentFromYear = YEARS[0];   // left handle — default = first year
let currentToYear   = YEARS.includes('2024') ? '2024' : YEARS[YEARS.length - 1];

// ── getGrowth: calculate % change for a county between two years ───────
// Returns an object {fromIssued, toIssued, pct} or null if no data.
// Returns null (rather than crashing) if the fromYear has zero permits,
// because you cannot calculate a percentage change from zero.
function getGrowth(county, fromYear, toYear) {
  // Safe lookup: if the year or county doesn't exist, default to 0
  const fromIssued = ((COUNTY_BY_YEAR[fromYear] || {})[county] || {}).issued || 0;
  const toIssued   = ((COUNTY_BY_YEAR[toYear]   || {})[county] || {}).issued || 0;
  if (fromIssued === 0) return null;   // can't divide by zero
  const pct = ((toIssued - fromIssued) / fromIssued * 100).toFixed(1);
  return { fromIssued, toIssued, pct: parseFloat(pct) };
}

// ── getSectorGrowth: calculate % change for a sector between two years ─
// Sector data only exists from 2020 onwards.  If the user drags the left
// slider handle to e.g. 2016, there is no sector data for 2016.
// Solution: automatically fall back to the earliest year that has sector
// data (usually 2020) so the growth column is never left blank.
// The returned 'effectiveFrom' tells the UI which year was actually used
// so it can display "2020→2024" rather than "2016→2024".
function getSectorGrowth(sectorShort, fromYear, toYear) {
  const sectorYears   = Object.keys(SECTOR_BY_YEAR).sort();
  // Use the selected fromYear if it has sector data; otherwise fall back to earliest
  const effectiveFrom = SECTOR_BY_YEAR[fromYear] ? fromYear : sectorYears[0];

  // Can't show growth if there's no valid range
  if (!effectiveFrom || effectiveFrom === toYear) return null;

  const fromList = SECTOR_BY_YEAR[effectiveFrom] || [];
  const toList   = SECTOR_BY_YEAR[toYear]        || [];

  // .find() searches the list for the sector with the matching short name
  const fromRow = fromList.find(s => s.sector_short === sectorShort);
  const toRow   = toList.find(s => s.sector_short === sectorShort);

  if (!fromRow || !toRow || fromRow.issued === 0) return null;

  const pct = ((toRow.issued - fromRow.issued) / fromRow.issued * 100).toFixed(1);
  return {
    fromIssued: fromRow.issued,
    toIssued:   toRow.issued,
    pct:        parseFloat(pct),
    effectiveFrom,   // included so the UI can label the range correctly
  };
}

// ── growthBadge: format a % as a coloured HTML span ───────────────────
// +ve numbers get green (growth-pos class), -ve get orange (growth-neg class).
// This is called wherever we display a growth figure in the sidebar or popup.
function growthBadge(pct, suffix = '') {
  const cls  = pct >= 0 ? 'growth-pos' : 'growth-neg';
  const sign = pct >= 0 ? '+' : '';
  return `<span class="${cls}">${sign}${pct}%${suffix}</span>`;
}

function updateTooltip(feature) {
  const county = feature.properties.canonical_county;
  const data   = (COUNTY_BY_YEAR[currentToYear] || {})[county] || {};
  const growth = getGrowth(county, currentFromYear, currentToYear);
  const growthStr = growth
    ? `<span class="${growth.pct >= 0 ? 'growth-pos' : 'growth-neg'}">
        ${growth.pct >= 0 ? '+' : ''}${growth.pct}%</span>
       (${currentFromYear}→${currentToYear})`
    : '—';
  infoBox._div.innerHTML = `
    <div class="popup-county">${county}</div>
    <div class="popup-row"><span class="popup-label">Permits (${currentToYear})</span>
      <span class="popup-val">${(data.issued || 0).toLocaleString()}</span></div>
    <div class="popup-row"><span class="popup-label">National share</span>
      <span class="popup-val">${data.pct_share || 0}%</span></div>
    <div class="popup-row"><span class="popup-label">Growth</span>
      <span class="popup-val">${growthStr}</span></div>
  `;
}

// ── Click popup ────────────────────────────────────────────────────────
function showPopup(feature, latlng) {
  const county   = feature.properties.canonical_county;
  const data     = (COUNTY_BY_YEAR[currentToYear] || {})[county] || {};
  const growth   = getGrowth(county, currentFromYear, currentToYear);
  const sameYear = currentFromYear === currentToYear;
  const sector   = document.getElementById('sector-select').value;

  // County growth rows
  const countyGrowthHtml = growth && !sameYear ? `
    <div class="popup-row">
      <span class="popup-label">Permits in ${currentFromYear}</span>
      <span class="popup-val">${growth.fromIssued.toLocaleString()}</span>
    </div>
    <div class="popup-row">
      <span class="popup-label">Permits in ${currentToYear}</span>
      <span class="popup-val">${growth.toIssued.toLocaleString()}</span>
    </div>
    <div class="popup-row">
      <span class="popup-label">County growth</span>
      <span class="popup-val">${growthBadge(growth.pct)}</span>
    </div>` : '';

  // National sector growth block — shown when a sector is selected
  let sectorHtml = '';
  if (sector !== 'all') {
    const sg = getSectorGrowth(sector, currentFromYear, currentToYear);
    const toRow = (SECTOR_BY_YEAR[currentToYear] || []).find(s => s.sector_short === sector);
    sectorHtml = `
      <div style="margin-top:7px;padding-top:7px;border-top:1px solid #eee;">
        <div style="font-size:0.72rem;font-weight:700;color:#169B62;margin-bottom:3px;">
          📊 ${sector} — national
        </div>
        ${toRow ? `
        <div class="popup-row">
          <span class="popup-label">National permits (${currentToYear})</span>
          <span class="popup-val">${toRow.issued.toLocaleString()}</span>
        </div>` : ''}
        ${sg ? `
        <div class="popup-row">
          <span class="popup-label">National growth</span>
          <span class="popup-val">${growthBadge(sg.pct)}</span>
        </div>
        <div class="popup-row">
          <span class="popup-label">${currentFromYear} → ${currentToYear}</span>
          <span class="popup-val">
            ${sg.fromIssued.toLocaleString()} → ${sg.toIssued.toLocaleString()}
          </span>
        </div>` : `
        <div style="font-size:0.68rem;color:#aaa;">
          Growth data available from 2020 onwards
        </div>`}
        <div style="font-size:0.65rem;color:#bbb;margin-top:3px;">
          County-level sector breakdown not available in source data
        </div>
      </div>`;
  }

  const html = `
    <div class="popup-county">${county} — ${currentToYear}</div>
    <div class="popup-row">
      <span class="popup-label">Permits issued</span>
      <span class="popup-val">${(data.issued || 0).toLocaleString()}</span>
    </div>
    <div class="popup-row">
      <span class="popup-label">Refused</span>
      <span class="popup-val">${(data.refused || 0).toLocaleString()}</span>
    </div>
    <div class="popup-row">
      <span class="popup-label">National share</span>
      <span class="popup-val">${data.pct_share || 0}%</span>
    </div>
    ${countyGrowthHtml}
    ${sectorHtml}
  `;
  L.popup().setLatLng(latlng).setContent(html).openOn(map);
}

// ── Legend ─────────────────────────────────────────────────────────────
const legend = L.control({ position: 'bottomright' });
legend.onAdd = function() {
  const div = L.DomUtil.create('div', 'info legend');
  const steps = [0, 0.1, 0.25, 0.5, 0.75, 1.0];
  div.innerHTML = '<div class="legend-title">Permits issued</div>';
  steps.forEach((t, i) => {
    const val   = Math.round(GLOBAL_MAX * t * t); // reverse sqrt
    const label = i === 0 ? '0' : val.toLocaleString() + (i === steps.length - 1 ? '+' : '');
    div.innerHTML += `
      <div class="legend-row">
        <div class="legend-box" style="background:${getColor(val + 1)}"></div>
        <span>${label}</span>
      </div>`;
  });
  return div;
};
legend.addTo(map);

// ═══════════════════════════════════════════════════════════════════════
//  SIDEBAR — COUNTY TABLE  (two modes: county ranking / sector breakdown)
// ═══════════════════════════════════════════════════════════════════════
function updateCountyTable(fromYear, toYear) {
  const sector   = document.getElementById('sector-select').value;
  const sameYear = fromYear === toYear;

  if (sector === 'all') {
    // ── County ranking mode (default) ──────────────────────────────────
    const data   = COUNTY_BY_YEAR[toYear] || {};
    const sorted = Object.entries(data)
      .map(([county, d]) => ({ county, ...d }))
      .sort((a, b) => b.issued - a.issued)
      .slice(0, 10);

    const allIssued = Object.values(data).reduce((s, d) => s + d.issued, 0);
    document.getElementById('national-total').textContent = allIssued.toLocaleString();
    document.getElementById('national-label').textContent =
      `permits issued nationally (${toYear})`;

    // Column headers for county mode — "Growth" label omitted to keep header compact
    document.querySelector('#county-table thead tr').innerHTML =
      `<th>#</th><th>County</th><th>${toYear}</th>
       <th id="growth-col-header">${sameYear ? 'Share' : `${fromYear}→${toYear}`}</th>`;

    document.getElementById('county-table-body').innerHTML = sorted.map((row, i) => {
      let lastCell;
      if (sameYear) {
        lastCell = `${row.pct_share}%`;
      } else {
        const growth = getGrowth(row.county, fromYear, toYear);
        if (growth) {
          const cls = growth.pct >= 0 ? 'growth-pos' : 'growth-neg';
          lastCell = `<span class="${cls}">${growth.pct >= 0 ? '+' : ''}${growth.pct}%</span>`;
        } else { lastCell = '—'; }
      }
      return `<tr><td>${i+1}</td><td>${row.county}</td>
              <td>${row.issued.toLocaleString()}</td><td>${lastCell}</td></tr>`;
    }).join('');

  } else {
    // ── Sector breakdown mode ───────────────────────────────────────────
    // Show all sectors ranked by issued for the "to" year, with the selected
    // one highlighted.  County-level sector data is not available in the source
    // data, so the map and county columns remain showing totals.
    const yearData = SECTOR_BY_YEAR[toYear] || [];
    const allIssued = yearData.reduce((s, r) => s + r.issued, 0);
    document.getElementById('national-total').textContent =
      allIssued > 0 ? allIssued.toLocaleString() : '—';
    document.getElementById('national-label').textContent =
      allIssued > 0
        ? `total permits across all sectors (${toYear})`
        : `sector data available from 2020 onwards`;

    // Determine the effective from-year for sector growth (falls back to 2020
    // if the slider from-handle is set before sector data begins).
    const sectorYears     = Object.keys(SECTOR_BY_YEAR).sort();
    const effectiveFrom   = SECTOR_BY_YEAR[fromYear] ? fromYear : sectorYears[0];
    const hasSectorRange  = effectiveFrom && effectiveFrom !== toYear && SECTOR_BY_YEAR[toYear];
    // Use "fromYear→toYear" as the column header — compact, no line break needed
    const growthColHeader = hasSectorRange
      ? `${effectiveFrom}→${toYear}` : 'Growth';

    // 4 columns: # | Sector | Permits | Growth  (Share removed for table width)
    document.querySelector('#county-table thead tr').innerHTML =
      `<th>#</th><th>Sector</th><th>${toYear}</th>
       <th id="growth-col-header">${growthColHeader}</th>`;

    if (yearData.length === 0) {
      document.getElementById('county-table-body').innerHTML =
        `<tr><td colspan="4" style="color:#aaa;font-size:0.72rem;padding:8px 4px;">
          Sector data available from 2020 onwards
        </td></tr>`;
      return;
    }

    document.getElementById('county-table-body').innerHTML = yearData
      .slice(0, 10)
      .map((row, i) => {
        const sg       = getSectorGrowth(row.sector_short, fromYear, toYear);
        const growthTd = sg ? growthBadge(sg.pct)
          : `<span style="color:#bbb">—</span>`;
        const isMatch  = row.sector_short === sector;
        const style    = isMatch ? 'background:#edfaf3;font-weight:700;' : '';
        return `<tr style="${style}">
          <td>${i+1}</td>
          <td title="${row.sector_full}" style="overflow:hidden;
              text-overflow:ellipsis;white-space:nowrap;">${row.sector_short}</td>
          <td>${row.issued.toLocaleString()}</td>
          <td>${growthTd}</td>
        </tr>`;
      }).join('');
  }
}

// ═══════════════════════════════════════════════════════════════════════
//  SIDEBAR — SECTOR CHART (Chart.js)
// ═══════════════════════════════════════════════════════════════════════
let sectorChart = null;
const SECTOR_COLORS = [
  '#169B62','#1db97a','#28d48a','#63e5ad','#a4f0cc',
  '#FF883E','#ff9f5e','#ffb57e','#ffd0a8','#ffe8d0',
];

// Populate sector dropdown with every unique short name across all years,
// sorted alphabetically.  Because names are normalised at source (01_clean_data.py)
// each sector appears exactly once in the list.
function populateSectorDropdown() {
  const select = document.getElementById('sector-select');
  const allSectors = new Set();
  for (const yr in SECTOR_BY_YEAR) {
    SECTOR_BY_YEAR[yr].forEach(s => allSectors.add(s.sector_short));
  }
  Array.from(allSectors).sort().forEach(name => {
    const opt = document.createElement('option');
    opt.value = name;
    opt.textContent = name;
    select.appendChild(opt);
  });
}

// Destroy and recreate the Chart.js instance whenever the chart type or
// axis orientation needs to change (all-sectors bar vs single-sector trend).
function rebuildChart(config) {
  if (sectorChart) { sectorChart.destroy(); sectorChart = null; }
  const canvas = document.getElementById('sector-chart');
  sectorChart = new Chart(canvas, config);
}

function updateSectorChart(toYear) {
  const note   = document.getElementById('no-sector-note');
  const canvas = document.getElementById('sector-chart');
  const sector = document.getElementById('sector-select').value;

  if (sector === 'all') {
    // ── All-sectors mode: horizontal bar chart of top 10 for toYear ──
    const yearData = SECTOR_BY_YEAR[toYear];
    if (!yearData || yearData.length === 0) {
      note.style.display = 'block'; canvas.style.display = 'none'; return;
    }
    note.style.display = 'none'; canvas.style.display = 'block';

    const top10  = yearData.slice(0, 10);           // already sorted desc by Python
    const labels = top10.map(r => r.sector_short);
    const values = top10.map(r => r.issued);
    const colors = top10.map((_, i) => SECTOR_COLORS[i % SECTOR_COLORS.length]);

    rebuildChart({
      type: 'bar',
      data: { labels, datasets: [{ label: 'Permits issued', data: values,
                                   backgroundColor: colors, borderRadius: 3 }] },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ` ${ctx.raw.toLocaleString()} permits` } },
          title: { display: true, text: `Top 10 sectors — ${toYear}`,
                   font: { size: 10 }, color: '#555' },
        },
        scales: {
          x: { ticks: { font: { size: 9 } }, grid: { color: '#f0f0f0' } },
          y: { ticks: { font: { size: 9 } } },
        },
      },
    });

  } else {
    // ── Single-sector mode: trend line across all available years ──
    // Build a sorted list of (year, issued) for the selected sector.
    const trendData = Object.entries(SECTOR_BY_YEAR)
      .map(([yr, sectors]) => {
        const found = sectors.find(s => s.sector_short === sector);
        return { year: yr, issued: found ? found.issued : 0 };
      })
      .sort((a, b) => a.year.localeCompare(b.year));

    const hasAnyData = trendData.some(d => d.issued > 0);
    if (!hasAnyData) {
      note.style.display = 'block'; canvas.style.display = 'none'; return;
    }
    note.style.display = 'none'; canvas.style.display = 'block';

    // Highlight the from and to year bars distinctly
    const bgColors = trendData.map(d => {
      if (d.year === toYear)   return '#0d6b44';   // darkest = "to" year
      if (d.year === currentFromYear) return '#63e5ad'; // lighter = "from" year
      return '#169B62';
    });

    // Build chart title: sector name + national growth for selected range.
    // Uses effectiveFrom from getSectorGrowth so pre-2020 slider positions
    // still show a meaningful growth figure (e.g. "2020→2024").
    const sg = getSectorGrowth(sector, currentFromYear, toYear);
    const growthLabel = sg
      ? ` · ${sg.pct >= 0 ? '+' : ''}${sg.pct}% (${sg.effectiveFrom}→${toYear})`
      : '';
    const chartTitle = `${sector}${growthLabel}`;

    rebuildChart({
      type: 'bar',
      data: {
        labels: trendData.map(d => d.year),
        datasets: [{
          label: sector, data: trendData.map(d => d.issued),
          backgroundColor: bgColors, borderRadius: 3,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ` ${ctx.raw.toLocaleString()} permits` } },
          title: {
            display: true,
            text: chartTitle,
            font: { size: 10 }, color: '#333',
            padding: { bottom: 6 },
          },
        },
        scales: {
          x: { ticks: { font: { size: 9 } } },
          y: { ticks: { font: { size: 9 } }, grid: { color: '#f0f0f0' } },
        },
      },
    });
  }
}

// ═══════════════════════════════════════════════════════════════════════
//  YEAR RANGE SLIDER (noUiSlider — two handles)
//
//  noUiSlider is a library that creates the two-handle slider at the
//  bottom of the page.  We give it numeric indexes (0, 1, 2…) as values
//  and translate those back to year strings using the YEARS array.
//  WHY indexes instead of years directly?  noUiSlider works with numbers,
//  and the years happen to be evenly spaced, so mapping index → year is simple.
// ═══════════════════════════════════════════════════════════════════════
function setupRangeSlider() {
  const el    = document.getElementById('year-slider');
  const ticks = document.getElementById('slider-ticks');

  const defaultFromIdx = 0;
  const defaultToIdx   = YEARS.includes('2024') ? YEARS.indexOf('2024') : YEARS.length - 1;

  // Create the two-handle slider
  noUiSlider.create(el, {
    start:   [defaultFromIdx, defaultToIdx],
    connect: true,                          // fills the range between handles
    step:    1,
    range:   { min: 0, max: YEARS.length - 1 },
    tooltips: [
      { to: v => YEARS[Math.round(v)] },   // left handle tooltip → year label
      { to: v => YEARS[Math.round(v)] },   // right handle tooltip → year label
    ],
  });

  // Render tick labels under the track
  ticks.innerHTML = YEARS.map(y => `<span>${y}</span>`).join('');

  // Called on every handle move — update everything in the UI
  el.noUiSlider.on('update', (values) => {
    const fromYear = YEARS[Math.round(values[0])];
    const toYear   = YEARS[Math.round(values[1])];

    // Guard: don't allow handles to cross (noUiSlider prevents it, but be safe)
    currentFromYear = fromYear;
    currentToYear   = toYear;

    // Sidebar year display
    document.getElementById('year-display').textContent =
      fromYear === toYear ? fromYear : `${fromYear} → ${toYear}`;

    // Update all dependent views
    refreshGeojsonColors(toYear);       // map colours based on the "to" year
    updateCountyTable(fromYear, toYear);
    updateSectorChart(toYear);          // sector chart shows the "to" year
  });
}

// ═══════════════════════════════════════════════════════════════════════
//  INITIALISE
// ═══════════════════════════════════════════════════════════════════════
(function init() {
  buildGeojsonLayer(currentToYear);
  setupRangeSlider();
  populateSectorDropdown();
  updateCountyTable(currentFromYear, currentToYear);
  updateSectorChart(currentToYear);

  // Sector filter — update chart, county table, and map badge together
  document.getElementById('sector-select').addEventListener('change', () => {
    const sector = document.getElementById('sector-select').value;
    // Show/hide the "sector data is national" badge on the map
    document.getElementById('sector-map-badge').style.display =
      sector === 'all' ? 'none' : 'block';
    updateSectorChart(currentToYear);
    updateCountyTable(currentFromYear, currentToYear);
  });
})();
</script>
</body>
</html>
"""


# ── HTML generation ───────────────────────────────────────────────────────────

def build_html(county_by_year: dict, county_growth: dict,
               sector_by_year: dict, geojson: dict, years: list) -> str:
    """Fill the HTML template with embedded JSON data blobs."""
    return HTML_TEMPLATE.replace(
        "{COUNTY_BY_YEAR_JSON}", json.dumps(county_by_year)
    ).replace(
        "{COUNTY_GROWTH_JSON}",  json.dumps(county_growth)
    ).replace(
        "{SECTOR_BY_YEAR_JSON}", json.dumps(sector_by_year)
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

    # ── Load GeoJSON ──────────────────────────────────────────────────
    print(f"\n── Loading GeoJSON from {GEOJSON_PATH}")
    geojson = prepare_geojson(county_growth)

    # ── Build and write HTML ──────────────────────────────────────────
    print(f"\n── Generating HTML")
    html = build_html(county_by_year, county_growth, sector_by_year, geojson, years)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  ✓ Written → {OUTPUT_PATH}  ({size_kb:.0f} KB)")
    print(f"\n  Open in your browser:")
    print(f"    open \"{OUTPUT_PATH}\"          (macOS)")
    print(f"    start \"{OUTPUT_PATH}\"         (Windows)")
    print("\n  Done. ✓")
