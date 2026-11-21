"""
src/06_enrich_company_counties.py
==================================
Enriches company_permits.csv with a county field by matching each company
name against the CRO (Companies Registration Office) public register.

CRO register: already downloaded to data/raw/cro_register.csv
  Source : https://opendata.cro.ie  (via data.gov.ie CKAN)
  815K rows; we filter to ~336K active companies.
  Column company_address_4 already isolates county:
    "MALAHIDE, DUBLIN, Ireland"  →  Dublin
    "CO. CORK, CORK, Ireland"    →  Cork

Matching strategy (four-pass):
  Pass 0  Hardcoded lookup for public-sector bodies (hospitals, HSE,
          universities) that are not registered companies and will never
          appear in the CRO.  Covers ~20–25 % of all permit volume.

  Pass 1  Exact match on normalised name  →  O(1) per lookup, covers ~50 %

  Pass 2  Blocked fuzzy — first-word inverted index (~300 candidates).
          Uses max(token_sort_ratio, token_set_ratio) so that short names
          that are subsets of longer CRO names still score well.
          e.g. "KPMG" vs "KPMG UNLIMITED COMPANY" → token_set_ratio = 100.

  Pass 3  Second-word blocking for companies where Pass 2 found zero
          candidates (e.g. "L M Ericsson" first-word "L" vs CRO "LM").

Improvements over v1:
  • Extended _LEGAL_SUFFIXES — UNLIMITED / LLP / BRANCH now stripped.
    Fixes Dawn Meats, KPMG, PwC, Kildare Chilling, Deloitte, etc.
  • preprocess_name() strips "t/a …" trading-as suffixes before normalising
    (~366 companies).  Also strips "- [City] Branch" patterns.
  • token_set_ratio in Pass 2 handles subset matches (KPMG, Wipro, etc.)
  • Pass 3 second-word blocking for spaced abbreviation mismatches.

Produces:
  data/cleaned/company_county_map.csv
    company_name_clean, county, cro_name, match_score

Run from project root:
  Terminal : python src/06_enrich_company_counties.py
  Jupyter  : %run src/06_enrich_company_counties.py
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    print("  [WARNING] rapidfuzz not installed — fuzzy passes disabled.")
    print("  Install with: pip install rapidfuzz\n")

# ── Paths ─────────────────────────────────────────────────────────────────────
RAW_DIR         = Path("data/raw")
CLEANED_DIR     = Path("data/cleaned")
CRO_RAW_PATH    = RAW_DIR     / "cro_register.csv"
COUNTY_MAP_PATH = CLEANED_DIR / "company_county_map.csv"
COMPANY_CSV     = CLEANED_DIR / "company_permits.csv"

# ── 26 ROI counties ───────────────────────────────────────────────────────────
IRISH_COUNTIES = [
    "Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin",
    "Galway", "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim",
    "Limerick", "Longford", "Louth", "Mayo", "Meath", "Monaghan",
    "Offaly", "Roscommon", "Sligo", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
]
_COUNTY_UPPER = {c.upper(): c for c in IRISH_COUNTIES}
_COUNTY_VARIANTS = {
    "CO DUBLIN":  "Dublin",  "CO. DUBLIN":  "Dublin",
    "CO CORK":    "Cork",    "CO. CORK":    "Cork",
    "CO GALWAY":  "Galway",  "CO. GALWAY":  "Galway",
    "CO KERRY":   "Kerry",   "CO. KERRY":   "Kerry",
    "CO WICKLOW": "Wicklow", "CO. WICKLOW": "Wicklow",
    "CO MEATH":   "Meath",   "CO. MEATH":   "Meath",
    "CO KILDARE": "Kildare", "CO. KILDARE": "Kildare",
    "CO LOUTH":   "Louth",   "CO. LOUTH":   "Louth",
    "CO WEXFORD": "Wexford", "CO. WEXFORD": "Wexford",
    **{f"DUBLIN {i}": "Dublin" for i in range(1, 25)},
}


def extract_county(addr4: str) -> str:
    """Parse a county from the CRO company_address_4 field."""
    if not isinstance(addr4, str) or not addr4.strip():
        return ""
    tokens = [t.strip().upper() for t in addr4.split(",")]
    for tok in reversed(tokens):
        if tok in ("IRELAND", "EIRE", ""):
            continue
        if tok in _COUNTY_UPPER:
            return _COUNTY_UPPER[tok]
        if tok in _COUNTY_VARIANTS:
            return _COUNTY_VARIANTS[tok]
        for county_upper, county_canon in _COUNTY_UPPER.items():
            if county_upper in tok:
                return county_canon
    return ""


# ── Name normalisation ────────────────────────────────────────────────────────
# v2: added UNLIMITED / UNLTD / LLP / BRANCH to fix KPMG / Deloitte / Dawn Meats etc.
_LEGAL_SUFFIXES = re.compile(
    r"\b(LIMITED|LTD|PLC|DAC|CLG|UC|UNLIMITED|UNLTD|LLP|COMPANY|CO|"
    r"TEORANTA|TEO|INCORPORATED|INC|BRANCH|"
    r"IRELAND|INTERNATIONAL|GROUP|HOLDINGS|SERVICES|SOLUTIONS|ENTERPRISES|"
    r"OPERATIONS|TECHNOLOGIES|TECHNOLOGY)\b\.?",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^A-Z0-9 ]")
_SPACES    = re.compile(r"\s+")

# Patterns stripped BEFORE normalising (not from both sides — only permit names)
_TIA_PATTERN    = re.compile(r"\s+[tT]/[aA]\s+.+$")           # "... t/a Trading Name"
_BRANCH_PATTERN = re.compile(r"\s+-\s+.{0,30}[Bb]ranch\s*$")  # "... - Dublin Branch"


def preprocess_name(name: str) -> str:
    """
    Strip trading-as and branch-indicator suffixes before normalising.
    Applied only to PERMIT company names (not CRO names, which don't use these).
      "Barberstown Holdings Ltd t/a Barberstown Castle" → "Barberstown Holdings Ltd"
      "Wipro Limited - Irish Branch"                    → "Wipro Limited"
    """
    name = _TIA_PATTERN.sub("", name.strip())
    name = _BRANCH_PATTERN.sub("", name)
    return name


def normalise(name: str) -> str:
    """
    Uppercase, strip legal suffixes and punctuation, collapse whitespace.
    Applied identically to CRO names and (preprocessed) permit names.
    """
    s = str(name).upper().strip()
    s = _LEGAL_SUFFIXES.sub(" ", s)
    s = _NON_ALNUM.sub(" ", s)
    return _SPACES.sub(" ", s).strip()


def first_word(normalised: str) -> str:
    parts = normalised.split()
    return parts[0] if parts else ""


def second_word(normalised: str) -> str:
    parts = normalised.split()
    return parts[1] if len(parts) > 1 else ""


# ── Pass 0: hardcoded public-sector lookup ────────────────────────────────────
# Hospitals, the HSE, and universities are NOT registered companies and will
# never appear in the CRO register.  We map them directly to their county.
# Names must match company_name_clean exactly (case-sensitive).
KNOWN_COUNTIES = {
    # ── Health Service Executive (national; HQ in Dublin) ──────────────────
    "Health Services Executive":                           "Dublin",
    "Health Service Executive":                            "Dublin",
    "HSE":                                                 "Dublin",
    "HSE - Health Service Executive":                      "Dublin",

    # ── Dublin hospitals ────────────────────────────────────────────────────
    "Beaumont Hospital":                                   "Dublin",
    "St. James's Hospital":                                "Dublin",
    "St James's Hospital":                                 "Dublin",
    "St. James Hospital":                                  "Dublin",
    "St Vincent's University Hospital":                    "Dublin",
    "St. Vincents University Hospital":                    "Dublin",
    "Mater Misericordiae University Hospital":             "Dublin",
    "Mater University Hospital":                           "Dublin",
    "The Rotunda Hospital":                                "Dublin",
    "Rotunda Hospital":                                    "Dublin",
    "National Maternity Hospital":                         "Dublin",
    "Tallaght University Hospital":                        "Dublin",
    "Tallaght Hospital":                                   "Dublin",
    "Children's University Hospital":                      "Dublin",
    "Children's University Hospital Limited":              "Dublin",
    "Children's Health Ireland":                           "Dublin",
    "Adelaide & Meath Hospital Incorporating The National Children's Hospital": "Dublin",
    "Connolly Hospital":                                   "Dublin",
    "Coombe Women's Hospital":                             "Dublin",
    "Coombe Women & Infants University Hospital":          "Dublin",
    "Temple Street Children's University Hospital":        "Dublin",
    "Clonskeagh, Hospital Health Authority (ERHA)":        "Dublin",
    "Our Lady's Hospital Crumlin":                         "Dublin",
    "St Michael's Hospital":                               "Dublin",
    "Blackrock Clinic":                                    "Dublin",
    "Beacon Hospital":                                     "Dublin",
    "Cappagh National Orthopaedic Hospital":               "Dublin",
    "Royal Victoria Eye and Ear Hospital":                 "Dublin",
    "National Rehabilitation Hospital":                    "Dublin",
    "St. Luke's Radiation Oncology Network":               "Dublin",

    # ── Cork hospitals ──────────────────────────────────────────────────────
    "Cork University Hospital":                            "Cork",
    "Mercy University Hospital":                           "Cork",
    "South Infirmary Victoria University Hospital":        "Cork",
    "Bon Secours Hospital, Cork":                          "Cork",
    "Bon Secours Hospital":                                "Cork",
    "Cork University Maternity Hospital":                  "Cork",
    "Marymount University Hospital and Hospice":           "Cork",
    "Cork University Hospital Group":                      "Cork",

    # ── Galway hospitals ────────────────────────────────────────────────────
    "Galway University Hospital":                          "Galway",
    "Portiuncula University Hospital":                     "Galway",
    "Bon Secours Hospital, Galway":                        "Galway",
    "Galway Clinic":                                       "Galway",
    "Galway University Hospitals":                         "Galway",

    # ── Limerick hospitals ──────────────────────────────────────────────────
    "University Hospital Limerick":                        "Limerick",
    "University Limerick Hospitals Group":                 "Limerick",
    "Mid-Western Regional Hospital":                       "Limerick",
    "Limerick University Hospital":                        "Limerick",
    "Bon Secours Hospital, Limerick":                      "Limerick",

    # ── Kerry hospitals ─────────────────────────────────────────────────────
    "University Hospital Kerry":                           "Kerry",
    "Kerry General Hospital":                              "Kerry",

    # ── Waterford hospitals ─────────────────────────────────────────────────
    "University Hospital Waterford":                       "Waterford",
    "Waterford Regional Hospital":                         "Waterford",

    # ── Tipperary hospitals ─────────────────────────────────────────────────
    "South Tipperary General Hospital":                    "Tipperary",
    "Tipperary University Hospital":                       "Tipperary",

    # ── Louth hospitals ─────────────────────────────────────────────────────
    "Our Lady of Lourdes Hospital":                        "Louth",
    "Louth County Hospital":                               "Louth",

    # ── Donegal hospitals ───────────────────────────────────────────────────
    "Letterkenny University Hospital":                     "Donegal",
    "Letterkenny General Hospital":                        "Donegal",

    # ── Mayo hospitals ──────────────────────────────────────────────────────
    "Mayo University Hospital":                            "Mayo",
    "Mayo General Hospital":                               "Mayo",

    # ── Sligo hospitals ─────────────────────────────────────────────────────
    "Sligo University Hospital":                           "Sligo",
    "Sligo General Hospital":                              "Sligo",

    # ── Wexford hospitals ───────────────────────────────────────────────────
    "Wexford General Hospital":                            "Wexford",

    # ── Kilkenny hospitals ──────────────────────────────────────────────────
    "St. Luke's General Hospital Kilkenny":                "Kilkenny",
    "St Lukes Hospital":                                   "Kilkenny",
    "St. Luke's Hospital":                                 "Kilkenny",

    # ── Cavan hospitals ─────────────────────────────────────────────────────
    "Cavan General Hospital":                              "Cavan",

    # ── Clare hospitals ─────────────────────────────────────────────────────
    "Ennis Hospital":                                      "Clare",
    "Mid-Western Regional Hospital Ennis":                 "Clare",

    # ── Monaghan hospitals ──────────────────────────────────────────────────
    "Monaghan General Hospital":                           "Monaghan",

    # ── Roscommon hospitals ─────────────────────────────────────────────────
    "Roscommon University Hospital":                       "Roscommon",
    "Roscommon County Hospital":                           "Roscommon",

    # ── Nursing home groups (largest) ───────────────────────────────────────
    "Mowlam Healthcare Services Unlimited Company":        "Dublin",
    "Mowlam Healthcare":                                   "Dublin",

    # ── Universities (Dublin) ───────────────────────────────────────────────
    "Trinity College Dublin":                              "Dublin",
    "University College Dublin":                           "Dublin",
    "Dublin City University":                              "Dublin",
    "Dublin Institute of Technology":                      "Dublin",
    "Technological University Dublin":                     "Dublin",
    "Royal College of Surgeons in Ireland":                "Dublin",
    "Institute of Technology Blanchardstown":              "Dublin",
    "Dublin Business School":                              "Dublin",
    "National College of Ireland":                         "Dublin",

    # ── Universities (other counties) ───────────────────────────────────────
    "University College Cork":                             "Cork",
    "Cork Institute of Technology":                        "Cork",
    "National University of Ireland Galway":               "Galway",
    "University of Galway":                                "Galway",
    "Galway-Mayo Institute of Technology":                 "Galway",
    "University of Limerick":                              "Limerick",
    "Limerick Institute of Technology":                    "Limerick",
    "Maynooth University":                                 "Kildare",
    "National University of Ireland Maynooth":             "Kildare",
    "Institute of Technology Tralee":                      "Kerry",
    "Munster Technological University":                    "Kerry",
    "Waterford Institute of Technology":                   "Waterford",
    "South East Technological University":                 "Waterford",
    "Dundalk Institute of Technology":                     "Louth",
    "Letterkenny Institute of Technology":                 "Donegal",
    "Atlantic Technological University":                   "Galway",
    "Sligo Institute of Technology":                       "Sligo",

    # ── Other major public / semi-state bodies ───────────────────────────────
    "An Post":                                             "Dublin",
    "Bus Éireann":                                         "Dublin",
    "Dublin Bus":                                          "Dublin",
    "Iarnród Éireann":                                     "Dublin",
    "Irish Rail":                                          "Dublin",
    "Transdev Ireland":                                    "Dublin",
    "Electricity Supply Board":                            "Dublin",
    "EirGrid PLC":                                         "Dublin",
    "EirGrid Public Limited Company":                      "Dublin",
    "Gas Networks Ireland":                                "Cork",
    "Bord na Móna":                                        "Offaly",
    "Coillte":                                             "Wicklow",
    "Teagasc":                                             "Meath",
    "Marine Institute":                                    "Galway",
    "Environmental Protection Agency":                     "Wexford",
}


def pass0_lookup(company: str):
    """Return (county, score) from the hardcoded dict, or (None, 0)."""
    county = KNOWN_COUNTIES.get(company)
    return (county, 999) if county else (None, 0)


# ── Main matching logic ───────────────────────────────────────────────────────

def build_county_map(our_companies: pd.Series,
                     cro_df: pd.DataFrame,
                     fuzzy_threshold: int = 80) -> pd.DataFrame:
    """
    Four-pass matching: hardcoded → exact → blocked-fuzzy → second-word fuzzy.
    """

    # ── Build CRO lookup structures ───────────────────────────────────────────
    print("  Building CRO lookup index...")

    exact_dict  = {}                     # normalised_name → (cro_name, county)
    block_fw    = defaultdict(list)      # first_word  → [(norm, county, cro_name)]
    block_sw    = defaultdict(list)      # second_word → [(norm, county, cro_name)]

    for _, row in cro_df.iterrows():
        cro_name = str(row["company_name"]).strip()
        county   = row["county"]
        if not county:
            continue
        norm = normalise(cro_name)
        if not norm:
            continue
        exact_dict[norm] = (cro_name, county)
        fw = first_word(norm)
        sw = second_word(norm)
        block_fw[fw].append((norm, county, cro_name))
        if sw:
            block_sw[sw].append((norm, county, cro_name))

    print(f"  CRO exact index : {len(exact_dict):,} entries")
    print(f"  FW buckets      : {len(block_fw):,}  |  SW buckets: {len(block_sw):,}")

    # ── Pass 0: hardcoded public-sector lookup ────────────────────────────────
    print("\n  Pass 0 — hardcoded public-sector lookup...")
    results  = {}
    after_p0 = []

    for company in our_companies:
        county, score = pass0_lookup(company)
        if county:
            results[company] = {
                "company_name_clean": company,
                "county":             county,
                "cro_name":           "(hardcoded)",
                "match_score":        score,
            }
        else:
            after_p0.append(company)

    print(f"  Hardcoded hits : {len(results):,}")
    print(f"  Remaining      : {len(after_p0):,}  → Pass 1")

    # ── Pass 1: exact match on normalised + preprocessed name ─────────────────
    print("\n  Pass 1 — exact match (with t/a / branch stripping)...")
    after_p1 = []

    for company in after_p0:
        norm = normalise(preprocess_name(company))
        if norm in exact_dict:
            cro_name, county = exact_dict[norm]
            results[company] = {
                "company_name_clean": company,
                "county":             county,
                "cro_name":           cro_name,
                "match_score":        100,
            }
        else:
            after_p1.append(company)

    print(f"  Exact matches  : {len(results) - (len(our_companies) - len(after_p1) - len(results) + len(after_p0) - len(after_p1)):,}",
          f"/ {len(after_p0):,}")
    # Simpler: just print how many remain
    p1_matched = len(after_p0) - len(after_p1)
    print(f"  Pass 1 matched : {p1_matched:,} / {len(after_p0):,}")
    print(f"  Remaining      : {len(after_p1):,}  → Pass 2")

    # ── Pass 2: blocked fuzzy — first-word index ──────────────────────────────
    if after_p1 and HAS_RAPIDFUZZ:
        print(f"\n  Pass 2 — first-word blocked fuzzy (threshold={fuzzy_threshold})...")
        p2_matched = 0
        after_p2   = []

        for i, company in enumerate(after_p1):
            norm = normalise(preprocess_name(company))
            fw   = first_word(norm)

            candidates = block_fw.get(fw, [])
            if not candidates:
                after_p2.append(company)
                continue

            best_score  = 0
            best_county = ""
            best_cro    = ""
            for cand_norm, cand_county, cand_cro in candidates:
                # token_set_ratio handles subset matches (KPMG vs KPMG UNLIMITED)
                score = max(
                    fuzz.token_sort_ratio(norm, cand_norm),
                    fuzz.token_set_ratio(norm, cand_norm),
                )
                if score > best_score:
                    best_score  = score
                    best_county = cand_county
                    best_cro    = cand_cro

            if best_score >= fuzzy_threshold:
                results[company] = {
                    "company_name_clean": company,
                    "county":             best_county,
                    "cro_name":           best_cro,
                    "match_score":        best_score,
                }
                p2_matched += 1
            else:
                after_p2.append(company)

            if (i + 1) % 2000 == 0:
                print(f"    … {i+1:,}/{len(after_p1):,} processed")

        print(f"  Pass 2 matched : {p2_matched:,} / {len(after_p1):,}")
        print(f"  Remaining      : {len(after_p2):,}  → Pass 3")

    else:
        after_p2 = after_p1
        print("  Pass 2 skipped (rapidfuzz unavailable)")

    # ── Pass 3: second-word blocking for still-unmatched ─────────────────────
    # Catches cases like "L M Ericsson" (first word "L") vs CRO "LM ERICSSON"
    # (first word "LM") — they share the second word "ERICSSON".
    if after_p2 and HAS_RAPIDFUZZ:
        print(f"\n  Pass 3 — second-word blocked fuzzy (threshold={fuzzy_threshold})...")
        p3_matched = 0

        for company in after_p2:
            norm = normalise(preprocess_name(company))
            sw   = second_word(norm)

            candidates = block_sw.get(sw, []) if sw else []
            best_score  = 0
            best_county = ""
            best_cro    = ""

            for cand_norm, cand_county, cand_cro in candidates:
                score = max(
                    fuzz.token_sort_ratio(norm, cand_norm),
                    fuzz.token_set_ratio(norm, cand_norm),
                )
                if score > best_score:
                    best_score  = score
                    best_county = cand_county
                    best_cro    = cand_cro

            if best_score >= fuzzy_threshold:
                results[company] = {
                    "company_name_clean": company,
                    "county":             best_county,
                    "cro_name":           best_cro,
                    "match_score":        best_score,
                }
                p3_matched += 1
            else:
                results[company] = {
                    "company_name_clean": company,
                    "county":             "",
                    "cro_name":           "",
                    "match_score":        0,
                }

        print(f"  Pass 3 matched : {p3_matched:,} / {len(after_p2):,}")

    else:
        for company in after_p2:
            results[company] = {
                "company_name_clean": company,
                "county":             "",
                "cro_name":           "",
                "match_score":        0,
            }

    return pd.DataFrame(list(results.values()))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  06_enrich_company_counties.py  —  CRO County Lookup  v2")
    print("=" * 60)

    if not COMPANY_CSV.exists():
        print(f"\n  ERROR: {COMPANY_CSV} not found.")
        sys.exit(1)

    if not CRO_RAW_PATH.exists():
        print(f"\n  ERROR: {CRO_RAW_PATH} not found.")
        sys.exit(1)

    # ── Load permit companies ─────────────────────────────────────────────────
    company_df = pd.read_csv(COMPANY_CSV)
    unique_companies = company_df["company_name_clean"].dropna().unique()
    print(f"\n  Permit companies : {len(company_df):,} rows, "
          f"{len(unique_companies):,} unique")

    # ── Load & filter CRO ─────────────────────────────────────────────────────
    print(f"\n── Loading CRO register ({CRO_RAW_PATH.stat().st_size / 1024 / 1024:.0f} MB)...")
    cro_raw = pd.read_csv(
        CRO_RAW_PATH,
        usecols=["company_name", "company_status", "company_address_4"],
        low_memory=False,
    )
    cro_active = cro_raw[
        cro_raw["company_status"].str.strip().isin({"Normal", "Liquidation"})
    ].copy()
    print(f"  Total rows      : {len(cro_raw):,}")
    print(f"  Active          : {len(cro_active):,}")

    print("  Extracting counties from company_address_4...")
    cro_active["county"] = cro_active["company_address_4"].apply(extract_county)
    has_county = cro_active["county"] != ""
    print(f"  County parsed   : {has_county.sum():,} / {len(cro_active):,} "
          f"({100*has_county.mean():.0f}%)")

    # ── Run matching ──────────────────────────────────────────────────────────
    print("\n── Matching companies to counties")
    county_map = build_county_map(pd.Series(unique_companies), cro_active)

    # ── Report ────────────────────────────────────────────────────────────────
    matched   = county_map[county_map["county"].notna() & (county_map["county"] != "")]
    unmatched = county_map[(county_map["county"].isna()) | (county_map["county"] == "")]
    match_pct = 100 * len(matched) / len(county_map) if len(county_map) else 0

    print(f"\n── Results")
    print(f"  Total unique companies : {len(county_map):,}")
    print(f"  Matched with county    : {len(matched):,}  ({match_pct:.1f}%)")
    print(f"  Unmatched              : {len(unmatched):,}")

    print(f"\n  County distribution (top 10):")
    for county, count in matched["county"].value_counts().head(10).items():
        print(f"    {county:<15} {count:>5,}")

    print(f"\n  Spot-checks:")
    for name in [
        "Google Ireland Limited",
        "Health Services Executive",
        "Beaumont Hospital",
        "Cork University Hospital",
        "Galway University Hospital",
        "Facebook Ireland Limited",
        "Dawn Meats Ireland UC",
        "Deloitte Ireland LLP",
        "KPMG",
        "PricewaterhouseCoopers",
        "Microsoft Ireland Operations Limited",
        "Pfizer Ireland Pharmaceuticals",
    ]:
        row = county_map[county_map["company_name_clean"] == name]
        if not row.empty:
            r = row.iloc[0]
            status = r["county"] if r["county"] else "(no match)"
            print(f"    {name:<52} → {status} (score={r['match_score']:.0f})")

    # ── Save ──────────────────────────────────────────────────────────────────
    county_map.to_csv(COUNTY_MAP_PATH, index=False)
    print(f"\n  ✓ Saved → {COUNTY_MAP_PATH}  ({len(county_map):,} rows)")
    print("\n  Done. ✓")
    print("\n  Next steps:")
    print("    python src/02_build_sqlite.py")
    print("    python src/03_analyze.py")
    print("    python src/04_build_map.py")
