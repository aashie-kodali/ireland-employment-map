#!/usr/bin/env python3
"""
scripts/build_sector_map_from_user_data.py
==========================================
Converts the 11 sector-matched Excel files (produced by the user's
classification work) into a cleaned, validated company_sector_map.csv
suitable for the main pipeline.

Input
-----
  data/raw/Company-Sector Data/Ireland Employment Permits Dataset/
    sector_matched/per_year/permits_<YYYY>.xlsx
  Each file has columns (header on row 1, row 0 is a title):
    Company Name | Permits Issued | NACE Code | NACE Letter | NACE Sector
    | Confidence | Match Method

  Confidence tiers:
    HIGH / EXACT  → reliable; kept as-is (after sector-name mapping)
    LOW  / MEDIUM → uncertain; kept if the sector looks plausible OR
                    if an explicit correction override exists
    UNKNOWN       → discarded unless an explicit override exists

Output
------
  data/raw/company_sector_map.csv
  Columns: company_name_clean | total_permits | sector | source
  (Existing file is backed up to company_sector_map.csv.bak before overwriting.)

Sector resolution priority (per company)
-----------------------------------------
  1. COMPANY_OVERRIDES  — hand-researched corrections for misclassified
                          companies; applied regardless of confidence tier.
  2. HIGH / EXACT rows  — user's reliable tier.
  3. LOW / MEDIUM rows  — user's uncertain tier (sector accepted as-is).
  4. UNKNOWN only       — skipped (no reliable sector, no override).

Manufacturing sub-categories
------------------------------
  The user's data uses the broad "Manufacturing" label for all five
  canonical sub-categories.  This script splits them using keyword rules
  applied to the cleaned company name (first match wins):
    C - Manufacture of Medical Devices
    C - Manufacture of Chemicals & Pharmaceuticals
    C - Manufacture of Computers, Electronics & Optical Equipment
    C - Manufacture of Food, Drink & Tobacco
    C - All Other Manufacturing
  Company-specific overrides take priority over keyword rules.

Run from project root:
  python scripts/build_sector_map_from_user_data.py
"""

import re
import sys
from pathlib import Path

import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = (
    ROOT / "data" / "raw"
    / "Company-Sector Data"
    / "Ireland Employment Permits Dataset"
    / "sector_matched" / "per_year"
)
OUT_PATH = ROOT / "data" / "raw" / "company_sector_map.csv"


# ── Company name normalisation (mirrors src/05_clean_companies.py) ─────────────

def clean_company_name(raw) -> str:
    """
    Light normalisation — trims whitespace, collapses internal spaces,
    removes trailing periods after common legal suffixes.
    Must stay byte-for-byte identical to the same function in 05_clean_companies.py
    so the join key used here matches the key in company_permits.csv exactly.
    """
    if pd.isna(raw):
        return raw
    name = str(raw).strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\bLtd\.$",     "Ltd",     name)
    name = re.sub(r"\bLimited\.$", "Limited", name)
    name = re.sub(r"\bplc\.$",     "plc",     name, flags=re.IGNORECASE)
    return name


# ── Sector name mapping (user vocabulary → canonical NACE) ────────────────────
# The user's Excel files use a shorter vocabulary (19 values).
# "Manufacturing" is handled separately by classify_manufacturing() below.
# "Extraterritorial Organisations" is excluded — not in the canonical list.

SECTOR_MAP = {
    "Accommodation & Food Services":              "I - Accommodation & Food Services Activities",
    "Administrative & Support Services":          "N - Administrative & Support Service Activities",
    "Agriculture, Forestry & Fishing":            "A - Agriculture, Forestry & Fishing",
    "Arts, Entertainment & Recreation":           "R - Arts, Entertainment and Recreation",
    "Construction":                               "F - Construction",
    "Education":                                  "P - Education",
    "Electricity, Gas, Steam & Air Conditioning": "D - Electricity & Gas & Air Conditioning Supply",
    "Extraterritorial Organisations":             None,   # excluded — not in canonical set
    "Financial & Insurance Activities":           "K - Financial & Insurance Activities",
    "Human Health & Social Work":                 "Q - Health & Social Work Activities",
    "Information & Communication":                "J - Information & Communication Activities",
    "Manufacturing":                              None,   # → classify_manufacturing()
    "Other Service Activities":                   "S - Other Service Activities",
    "Professional, Scientific & Technical":       "M - Professional, Scientific & Technical Activities",
    "Public Administration & Defence":            "O - Public Administration & Defence",
    "Real Estate Activities":                     "L - Real Estate Activities",
    "Transport & Storage":                        "H - Transport & Storage",
    "Water Supply, Sewerage & Waste Management":  "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "Wholesale & Retail Trade":                   "G - Wholesale & Retail Trade",
}

# 24 canonical NACE sector names (must match sector_permits.csv 2020+ rows exactly)
CANONICAL_SECTORS: set[str] = {
    "A - Agriculture, Forestry & Fishing",
    "B - Mining & Quarrying",
    "C - All Other Manufacturing",
    "C - Manufacture of Chemicals & Pharmaceuticals",
    "C - Manufacture of Computers, Electronics & Optical Equipment",
    "C - Manufacture of Food, Drink & Tobacco",
    "C - Manufacture of Medical Devices",
    "D - Electricity & Gas & Air Conditioning Supply",
    "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "F - Construction",
    "G - Wholesale & Retail Trade",
    "H - Transport & Storage",
    "I - Accommodation & Food Services Activities",
    "J - Information & Communication Activities",
    "K - Financial & Insurance Activities",
    "L - Real Estate Activities",
    "M - Professional, Scientific & Technical Activities",
    "N - Administrative & Support Service Activities",
    "O - Public Administration & Defence",
    "P - Education",
    "Q - Health & Social Work Activities",
    "R - Arts, Entertainment and Recreation",
    "S - Other Service Activities",
    "T - Domestic Activities of Households as Employers",
}


# ── Manufacturing sub-category keyword rules ───────────────────────────────────
# Applied to lowercased company_name_clean.  Order matters — first match wins.
# Medical Devices checked before Pharma (they share some bio/science terms).

_MFG_RULES = [
    (re.compile(
        r"medical.?device|surgical|orthop(a?ed)|vascular galway|"
        r"\bnypro\b|\bstryker\b|\bdepuy\b|\bzimmer\b|"
        r"vision care|cochlear|hearing.?aid|bd ireland"
    ), "C - Manufacture of Medical Devices"),

    (re.compile(
        r"pharma|biologic|biochem|biotech|"
        r"therapeutics|vaccine|"
        r"\blilly\b|novartis|pfizer|abbvie|allergan|"
        r"\bmerck\b|\bmsd\b|agro(?!nomy)|zoetis|grifols|takeda|"
        r"regeneron|wuxi|alexion|chanelle|"
        r"concentrate solutions"
    ), "C - Manufacture of Chemicals & Pharmaceuticals"),

    (re.compile(
        r"electronic|semiconductor|optical instruments|"
        r"\bmicrochip\b|analog.?device|xilinx|\basml\b|"
        r"cypress.?semi|\bintel\b|photon|laser.?tech|display.?tech"
    ), "C - Manufacture of Computers, Electronics & Optical Equipment"),

    (re.compile(
        r"\bmeat(s)?\b|\bbeef\b|\bpork\b|poultr|chilling|chilled|"
        r"\bseafood\b|\bfishing\b|farm.?food|"
        r"\bfoods?\b|\bdairy\b|\bdrinks?\b|beverag|"
        r"brew(?:ery|ing)|distill|whiske[y]|chocolat|"
        r"bak(?:ery|ing)|\bcasing(s)?\b|"
        r"pet.?food|dog.?food|mushroom|"
        r"milling|flour|\bsugar\b|refreshment|"
        r"meatpack|provisions|piggery|slaughter"
    ), "C - Manufacture of Food, Drink & Tobacco"),
]


def classify_manufacturing(name_clean: str) -> str:
    """
    Split the broad 'Manufacturing' label into one of five canonical sub-categories
    using keyword rules applied to the lowercased company name.
    Falls back to 'C - All Other Manufacturing' if no rule matches.
    """
    name_low = name_clean.lower()
    for pattern, sector in _MFG_RULES:
        if pattern.search(name_low):
            return sector
    return "C - All Other Manufacturing"


def map_sector(raw_sector, name_clean: str):
    """
    Map a raw NACE Sector string (user vocabulary) to a canonical NACE name.
    Returns None for sectors that should be excluded (Extraterritorial, unknown).
    """
    if raw_sector is None or pd.isna(raw_sector):
        return None
    if raw_sector == "Manufacturing":
        return classify_manufacturing(name_clean)
    return SECTOR_MAP.get(raw_sector, None)


# ── Hand-researched correction overrides ──────────────────────────────────────
# Keys are company names as they appear in the Excel files (raw).
# clean_company_name() is applied to both keys and data before matching,
# so trailing periods and extra spaces are handled automatically.
#
# Source: web research conducted during session validation (see session notes /
# conversation transcript).  Corrections target two categories:
#   (a) Companies misclassified by the fuzzy matching pipeline
#       (e.g., "Financial" assigned to Vodafone, care homes tagged as Real Estate)
#   (b) Manufacturing sub-category overrides for companies where keyword rules
#       would produce the wrong result (e.g., Boston Scientific → Medical Devices)

_OVERRIDES_RAW: dict[str, str] = {

    # ── Information & Communication ──────────────────────────────────────────
    # Groupon Dublin = Engineering & Marketing Centre of Excellence (R&D hub)
    "Groupon International Limited":                    "J - Information & Communication Activities",
    # Oracle EMEA Ltd tagged as Administrative — wrong, it's ICT
    "Oracle EMEA Limited":                              "J - Information & Communication Activities",
    # Capgemini Sogeti / Capgemini = IT services; UNKNOWN in user data
    "Capgemini Sogeti Ireland Ltd":                     "J - Information & Communication Activities",
    "Capgemini Ireland Ltd":                            "J - Information & Communication Activities",
    "Sogeti Ireland Ltd":                               "J - Information & Communication Activities",
    # Tech Mahindra = IT services; UNKNOWN in user data
    "Tech Mahindra Limited":                            "J - Information & Communication Activities",
    # SFDC = Salesforce.com; UNKNOWN in user data
    "SFDC Ireland Ltd":                                 "J - Information & Communication Activities",
    # Arista Networks = cloud networking; tagged as Administrative
    "Arista Networks Limited":                          "J - Information & Communication Activities",
    # Test Triangle = software QA/testing; tagged as Financial
    "Test Triangle Limited":                            "J - Information & Communication Activities",
    # Vodafone = telecommunications; tagged as Financial
    "Vodafone Ireland Limited":                         "J - Information & Communication Activities",
    # Indeed = job search platform/tech company; tagged as Manufacturing
    "Indeed Ireland Operations Limited":                "J - Information & Communication Activities",
    # Yahoo EMEA = tech company; tagged as Manufacturing
    "Yahoo EMEA Ltd":                                   "J - Information & Communication Activities",
    # Fleetmatics = fleet tracking software (now Verizon Connect); tagged as Professional
    "Fleetmatics Ireland Limited":                      "J - Information & Communication Activities",
    # Verizon Connect Dev = same business as above
    "Verizon Connect Development Limited":              "J - Information & Communication Activities",
    # Openet = BSS/OSS telecom software company; UNKNOWN
    "Openet Telecom Sales Ltd":                         "J - Information & Communication Activities",
    # Keywords Studios = video game localisation/services
    "Keywords International Ltd.":                      "J - Information & Communication Activities",
    # Udemy = online learning platform (tech company); UNKNOWN
    "Udemy Ireland Ltd":                                "J - Information & Communication Activities",
    # Smartbox = AAC assistive technology (primarily software/tech)
    "Smartbox Group Ltd":                               "J - Information & Communication Activities",
    "Smartbox Group Limited":                           "J - Information & Communication Activities",

    # ── Financial & Insurance ────────────────────────────────────────────────
    # Susquehanna International Group = proprietary trading; tagged as Administrative
    "Susquehanna International Group Limited":          "K - Financial & Insurance Activities",
    # Citco Fund Services = hedge fund administration; tagged as Professional
    "Citco Fund Services Ireland Limited":              "K - Financial & Insurance Activities",
    # OmniPay = payment processing (acquired by First Data/Fiserv); tagged as Professional
    "OmniPay Limited":                                  "K - Financial & Insurance Activities",
    # FISC Ireland = Fidelity Investments' Irish entity; tagged as Professional
    "FISC Ireland limited":                             "K - Financial & Insurance Activities",
    "FISC Ireland Ltd":                                 "K - Financial & Insurance Activities",
    "FISC Ireland Ltd.":                                "K - Financial & Insurance Activities",
    # Allianz = insurance; UNKNOWN
    "Allianz plc":                                      "K - Financial & Insurance Activities",
    # Aviva Group Services = insurance group services entity; tagged as Construction
    "Aviva Group Services Ireland Limited":             "K - Financial & Insurance Activities",

    # ── Health & Social Work ─────────────────────────────────────────────────
    # Belmont Care = care home; tagged as Real Estate
    "Belmont Care Limited":                             "Q - Health & Social Work Activities",
    # Little Sisters of the Poor = religious order running nursing homes; tagged as Real Estate
    "Little Sisters of the Poor":                       "Q - Health & Social Work Activities",
    # Orwell House = Orwell Healthcare nursing home (170 beds, Rathgar); tagged as Professional
    "Orwell House Limited":                             "Q - Health & Social Work Activities",
    # Dublin Simon Community = homelessness social services; tagged as Education
    "Dublin Simon Community":                           "Q - Health & Social Work Activities",
    # Centric Health = primary care / GP clinics; tagged as Financial
    "Centric Health Primary Care Limited":              "Q - Health & Social Work Activities",
    # Amber Health Care = care agency; tagged as Manufacturing
    "Amber Health Care Limited":                        "Q - Health & Social Work Activities",
    # Applewood Homecare = homecare agency; tagged as Manufacturing
    "Applewood Homecare Ltd":                           "Q - Health & Social Work Activities",
    # Lisheen Nursing Centre = nursing home; tagged as Administrative
    "Lisheen Nursing Centre Limited":                   "Q - Health & Social Work Activities",
    # Hermitage Clinic = private hospital, Lucan; tagged as Construction
    "Hermitage Clinic Limited":                         "Q - Health & Social Work Activities",
    "Hermitage Clinic":                                 "Q - Health & Social Work Activities",
    # Brehon Care = care agency; tagged as Information & Communication (wrong)
    "Brehon Care":                                      "Q - Health & Social Work Activities",
    # Privapath Diagnostics = medical diagnostic services; tagged as Professional
    "Privapath Diagnostics Limited":                    "Q - Health & Social Work Activities",
    # Caring Hands = homecare; tagged as Administrative
    "Caring Hands Ltd":                                 "Q - Health & Social Work Activities",

    # ── Professional, Scientific & Technical ─────────────────────────────────
    # RPS Group = engineering/environmental consultancy; tagged as Administrative
    "RPS Group Limited":                                "M - Professional, Scientific & Technical Activities",
    # Turner & Townsend = project management consultancy; tagged as Accommodation
    "Turner & Townsend Ltd":                            "M - Professional, Scientific & Technical Activities",
    # BDO = accounting/audit firm; tagged as Financial
    "BDO":                                              "M - Professional, Scientific & Technical Activities",
    # Greenmast = advertising agency (SIC 73110); tagged as Financial
    "Greenmast Limited":                                "M - Professional, Scientific & Technical Activities",
    # PPD Development = CRO/Thermo Fisher subsidiary (Athlone); tagged as Administrative
    "PPD Development Ireland Limited":                  "M - Professional, Scientific & Technical Activities",
    # H & MV Engineering = high-voltage engineering consultancy; tagged as Administrative
    "H & MV Engineering Ltd":                          "M - Professional, Scientific & Technical Activities",
    # Nicholas O'Dwyer = engineering consultancy; tagged as Financial
    "Nicholas O'Dwyer Ltd":                             "M - Professional, Scientific & Technical Activities",
    # Mott MacDonald = global engineering consultancy; tagged as Accommodation
    "Mott MacDonald Ireland Limited":                   "M - Professional, Scientific & Technical Activities",
    # Brian McEnery = accountant/professional services; tagged as Manufacturing
    "Brian McEnery":                                    "M - Professional, Scientific & Technical Activities",
    # Swords Laboratories = pharma testing/QC lab services
    "Swords Laboratories":                              "M - Professional, Scientific & Technical Activities",
    # Localeyes = language/translation/localisation services; tagged as ICT
    "Localeyes Ltd":                                    "M - Professional, Scientific & Technical Activities",
    # Eurofins Biopharma = contract testing services (not manufacturing); would hit pharma keyword
    "Eurofins Biopharma Product Testing Ireland Limited": "M - Professional, Scientific & Technical Activities",

    # ── Education ────────────────────────────────────────────────────────────
    # UCD = University College Dublin; UNKNOWN
    "UCD":                                              "P - Education",
    # NUI Galway (now University of Galway); tagged as Manufacturing
    "NUI Galway":                                       "P - Education",
    # Royal College of Surgeons in Ireland = medical school; UNKNOWN
    "Royal College of Surgeons in Ireland":             "P - Education",

    # ── Transport & Storage ──────────────────────────────────────────────────
    # Humar = logistics for aluminium/coal tar industries; tagged as Financial
    "Humar Ltd":                                        "H - Transport & Storage",

    # ── Construction ────────────────────────────────────────────────────────
    # H.A. O'Neil = building services contractor (Jones Engineering subsidiary); tagged as Manufacturing
    "H.A O'Neil Ltd":                                   "F - Construction",
    # Dornan Engineering = mechanical & electrical contractor; tagged as Professional
    "Dornan Engineering Limited":                       "F - Construction",

    # ── Administrative & Support Services ────────────────────────────────────
    # Tempside = temporary staffing agency; tagged as Financial
    "Tempside Ltd":                                     "N - Administrative & Support Service Activities",
    # Cpl Solutions = staffing/recruitment agency; tagged as Construction
    "Cpl Solutions Limited":                            "N - Administrative & Support Service Activities",
    # AEBE = MarketStar (sales outsourcing/business services); UNKNOWN
    "AEBE Limited":                                     "N - Administrative & Support Service Activities",

    # ── Arts, Entertainment & Recreation ────────────────────────────────────
    # Boulder Media = animation studio (Cartoon Network); tagged as Financial
    "Boulder Media Ltd":                                "R - Arts, Entertainment and Recreation",
    # Screen SPE Ireland = Sony Pictures Entertainment distribution entity
    "Screen SPE Ireland Limited":                       "R - Arts, Entertainment and Recreation",
    # Brown Bag Films = animation/TV production studio; tagged as Professional
    "Brown Bag Films Ltd":                              "R - Arts, Entertainment and Recreation",

    # ── Wholesale & Retail ───────────────────────────────────────────────────
    # Oaklands Garage = car dealership; tagged as Professional
    "Oaklands Garage Limited":                          "G - Wholesale & Retail Trade",

    # ── Electricity & Gas ────────────────────────────────────────────────────
    # Supply Board Electricity = ESB (state electricity company); UNKNOWN
    "Supply Board Electricity":                         "D - Electricity & Gas & Air Conditioning Supply",

    # ── Agriculture ──────────────────────────────────────────────────────────
    # Stablefield = Ireland's premier mushroom producer (Tipperary); tagged as Financial
    "Stablefield Ltd":                                  "A - Agriculture, Forestry & Fishing",
    "Stablefield Limited":                              "A - Agriculture, Forestry & Fishing",
    # Kearns Fruit Farm = fruit farm/horticulture; tagged as Manufacturing
    "Kearns Fruit Farm Limited":                        "A - Agriculture, Forestry & Fishing",

    # ── Manufacturing sub-category overrides ────────────────────────────────
    # (keyword rules would either miss these or pick the wrong sub-category)

    # Medical Devices
    "Abbott Ireland":                                   "C - Manufacture of Medical Devices",
    "Boston Scientific Ireland Limited":                "C - Manufacture of Medical Devices",
    "Boston Scientific Limited":                        "C - Manufacture of Medical Devices",
    "Johnson and Johnson Vision Care Ireland Unlimited Company": "C - Manufacture of Medical Devices",
    "Medtronic Vascular Galway Unlimited Company":      "C - Manufacture of Medical Devices",
    "Nypro Limited":                                    "C - Manufacture of Medical Devices",
    "Stryker Ireland Limited":                          "C - Manufacture of Medical Devices",
    "DePuy Ireland Unlimited Company":                  "C - Manufacture of Medical Devices",

    # Chemicals & Pharmaceuticals
    "Regeneron Ireland DAC":                            "C - Manufacture of Chemicals & Pharmaceuticals",
    "Janssen Sciences Ireland UC":                      "C - Manufacture of Chemicals & Pharmaceuticals",
    "Janssen Pharmaceutical Sciences Unlimited Company": "C - Manufacture of Chemicals & Pharmaceuticals",
    "Grifols Worldwide Operations Limited":             "C - Manufacture of Chemicals & Pharmaceuticals",
    "Zoetis Belgium S.A. (Irish Branch)":               "C - Manufacture of Chemicals & Pharmaceuticals",
    "Takeda Ireland Limited":                           "C - Manufacture of Chemicals & Pharmaceuticals",
    "McDermott Laboratories Limited":                   "C - Manufacture of Chemicals & Pharmaceuticals",

    # Computers, Electronics & Optical Equipment
    "Apple Operations Europe":                          "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Apple Operations Europe Limited":                  "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Apple Operations International Limited":           "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Intel Ireland Ltd":                                "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Intel Irl Ltd":                                    "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Hitachi High-Tech Ireland Ltd":                    "C - Manufacture of Computers, Electronics & Optical Equipment",
    "Honeywell Control Systems Limited":                "C - All Other Manufacturing",

    # Food, Drink & Tobacco
    "Kerry Group Services International Limited":       "C - Manufacture of Food, Drink & Tobacco",
    "Irish Dog Foods Limited":                          "C - Manufacture of Food, Drink & Tobacco",
    "Silver Hill Foods Unlimited Company":              "C - Manufacture of Food, Drink & Tobacco",
    "Dawn Farm Foods":                                  "C - Manufacture of Food, Drink & Tobacco",
    "Glanbia Management Services Ltd":                  "C - Manufacture of Food, Drink & Tobacco",
    "Connolly Meats Limited":                           "C - Manufacture of Food, Drink & Tobacco",
    "European Refreshments Unlimited Company":          "C - Manufacture of Food, Drink & Tobacco",
    "European Refreshments":                            "C - Manufacture of Food, Drink & Tobacco",
    "Carton Brothers Unlimited Company":                "C - Manufacture of Food, Drink & Tobacco",
    "Kellogg Europe Trading Limited":                   "C - Manufacture of Food, Drink & Tobacco",

    # All Other Manufacturing
    "CRH Group Services Limited":                      "C - All Other Manufacturing",
    "Lufthansa Technik Shannon Ltd":                    "C - All Other Manufacturing",
    "Abbey Machinery Limited":                          "C - All Other Manufacturing",

    # Mining & Quarrying (not in user's sector vocabulary at all)
    "Kilcarrig Quarries (Ireland) Limited":             "B - Mining & Quarrying",
}

# Build the final lookup table with clean_company_name applied to all keys
OVERRIDES = {
    clean_company_name(k): v
    for k, v in _OVERRIDES_RAW.items()
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def best_raw_sector(df: pd.DataFrame):
    """
    Return the NACE Sector string with the highest total permits in df.
    Ignores null sectors.
    """
    non_null = df.dropna(subset=["NACE Sector"])
    if non_null.empty:
        return None
    totals = non_null.groupby("NACE Sector")["Permits Issued"].sum()
    return totals.idxmax()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 60)
    print("  build_sector_map_from_user_data.py")
    print("=" * 60)

    # ── Load all 11 Excel files ───────────────────────────────────────────────
    xlsx_files = sorted(DATA_DIR.glob("permits_*.xlsx"))
    if not xlsx_files:
        print(f"[ERROR] No Excel files found in:\n  {DATA_DIR}")
        sys.exit(1)

    frames = []
    for path in xlsx_files:
        year = int(path.stem.split("_")[1])
        df = pd.read_excel(path, header=1)
        df["year"] = year
        frames.append(df)
        print(f"  Loaded {path.name}  ({len(df):,} rows)")

    all_df = pd.concat(frames, ignore_index=True)
    total_permits = all_df["Permits Issued"].sum()
    print(f"\n  Total rows: {len(all_df):,}  |  Total permits: {total_permits:,}")

    # ── Apply name cleaning ───────────────────────────────────────────────────
    all_df["company_name_clean"] = all_df["Company Name"].apply(clean_company_name)

    # Confidence tier sets
    RELIABLE  = {"HIGH", "EXACT"}
    UNCERTAIN = {"LOW", "MEDIUM"}

    # ── Resolve sector per company ────────────────────────────────────────────
    results = []
    override_count = 0
    reliable_count = 0
    uncertain_count = 0
    skipped_unknown = 0

    for name_clean, group in all_df.groupby("company_name_clean"):
        total = int(group["Permits Issued"].sum())

        # ── Priority 1: hand-researched override ──────────────────────────────
        if name_clean in OVERRIDES:
            sector = OVERRIDES[name_clean]
            source = "override"
            override_count += 1

        else:
            # ── Priority 2: HIGH / EXACT rows ─────────────────────────────────
            reliable_rows = group[group["Confidence"].isin(RELIABLE)]
            uncertain_rows = group[group["Confidence"].isin(UNCERTAIN)]

            if not reliable_rows.empty:
                raw = best_raw_sector(reliable_rows)
                sector = map_sector(raw, name_clean)
                source = "high/exact"
                reliable_count += 1

            elif not uncertain_rows.empty:
                # ── Priority 3: LOW / MEDIUM rows ─────────────────────────────
                raw = best_raw_sector(uncertain_rows)
                sector = map_sector(raw, name_clean)
                source = "low/medium"
                uncertain_count += 1

            else:
                # UNKNOWN only — discard
                skipped_unknown += 1
                continue

        # Exclude unmappable sectors (Extraterritorial, truly unknown)
        if sector is None:
            continue

        results.append({
            "company_name_clean": name_clean,
            "total_permits":      total,
            "sector":             sector,
            "source":             source,
        })

    out_df = (
        pd.DataFrame(results)
        .sort_values("total_permits", ascending=False)
        .reset_index(drop=True)
    )

    # ── Validate: all sectors must be canonical ───────────────────────────────
    unexpected = set(out_df["sector"].unique()) - CANONICAL_SECTORS
    if unexpected:
        print(f"\n[ERROR] Non-canonical sector values found:\n  {unexpected}")
        print("  Fix OVERRIDES or SECTOR_MAP and re-run.")
        sys.exit(1)

    # ── Save (with backup) ────────────────────────────────────────────────────
    if OUT_PATH.exists():
        backup = OUT_PATH.with_suffix(".csv.bak")
        OUT_PATH.rename(backup)
        print(f"\n  Backed up existing file → {backup.name}")

    out_df.to_csv(OUT_PATH, index=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    tagged_permits  = out_df["total_permits"].sum()
    unique_companies = all_df["company_name_clean"].nunique()
    tagged_companies = len(out_df)

    print(f"\n  ✓ {tagged_companies:,} / {unique_companies:,} unique companies tagged "
          f"({tagged_companies / unique_companies:.1%})")
    print(f"  ✓ {tagged_permits:,} / {total_permits:,} permits covered "
          f"({tagged_permits / total_permits:.1%})")
    print(f"\n  Source breakdown:")
    print(f"    override   : {override_count:>5,}")
    print(f"    high/exact : {reliable_count:>5,}")
    print(f"    low/medium : {uncertain_count:>5,}")
    print(f"    skipped    : {skipped_unknown:>5,}  (UNKNOWN, no override)")

    print(f"\n  Sector distribution (tagged permits):")
    sector_totals = (
        out_df.groupby("sector")["total_permits"]
        .sum()
        .sort_values(ascending=False)
    )
    for sector, t in sector_totals.items():
        pct = 100.0 * t / tagged_permits
        print(f"    {pct:5.1f}%  {sector}")

    print(f"\n  Saved → {OUT_PATH}")
    print("  Done. ✓\n")


if __name__ == "__main__":
    main()
