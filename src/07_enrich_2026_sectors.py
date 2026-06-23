"""
src/07_enrich_2026_sectors.py
==============================
Sector-enriches companies that first appeared in 2026 permit data.

Three passes, each conservative (skip if uncertain):

  A. Dedup: near-duplicate names that alias prior-year companies → inherit sector.
  B. CRO NACE: normalised join against cro_register.csv nace_v2_code column.
  C. Manual tags: hand-researched via web search for ≥5-permit new entrants.

Appends new rows to data/raw/company_sector_map.csv (backs up first).
Does NOT rebuild company_permits.csv or the map — run 05 and 04 after.

Run:
  python src/07_enrich_2026_sectors.py
"""

import re
import shutil
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
RAW_DIR     = Path("data/raw")
CLEANED_DIR = Path("data/cleaned")

SECTOR_MAP_PATH = RAW_DIR / "company_sector_map.csv"
CRO_PATH        = RAW_DIR / "cro_register.csv"
PERMITS_PATH    = CLEANED_DIR / "company_permits.csv"

# ── Canonical sector names (must match existing sector_map exactly) ────────────
CANONICAL_SECTORS = {
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

# ── NACE V2 numeric division → section letter ──────────────────────────────────
# Derived from Eurostat NACE Rev.2 structure.
def _build_nace_division_map():
    ranges = [
        (range(1,   4),  "A"), (range(5,  10),  "B"), (range(10, 34),  "C"),
        (range(35, 36),  "D"), (range(36, 40),  "E"), (range(41, 44),  "F"),
        (range(45, 48),  "G"), (range(49, 54),  "H"), (range(55, 57),  "I"),
        (range(58, 64),  "J"), (range(64, 67),  "K"), (range(68, 69),  "L"),
        (range(69, 76),  "M"), (range(77, 83),  "N"), (range(84, 85),  "O"),
        (range(85, 86),  "P"), (range(86, 89),  "Q"), (range(90, 94),  "R"),
        (range(94, 97),  "S"), (range(97, 99),  "T"),
    ]
    m = {}
    for rng, letter in ranges:
        for d in rng:
            m[d] = letter
    return m

NACE_DIVISION_MAP = _build_nace_division_map()

# Map section letter → canonical sector name (for non-C sections)
SECTION_TO_SECTOR = {
    "A": "A - Agriculture, Forestry & Fishing",
    "B": "B - Mining & Quarrying",
    "D": "D - Electricity & Gas & Air Conditioning Supply",
    "E": "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "F": "F - Construction",
    "G": "G - Wholesale & Retail Trade",
    "H": "H - Transport & Storage",
    "I": "I - Accommodation & Food Services Activities",
    "J": "J - Information & Communication Activities",
    "K": "K - Financial & Insurance Activities",
    "L": "L - Real Estate Activities",
    "M": "M - Professional, Scientific & Technical Activities",
    "N": "N - Administrative & Support Service Activities",
    "O": "O - Public Administration & Defence",
    "P": "P - Education",
    "Q": "Q - Health & Social Work Activities",
    "R": "R - Arts, Entertainment and Recreation",
    "S": "S - Other Service Activities",
    "T": "T - Domestic Activities of Households as Employers",
}

# ── Manufacturing sub-classification (reused from build_sector_map_from_user_data.py) ──
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
    name_low = name_clean.lower()
    for pattern, sector in _MFG_RULES:
        if pattern.search(name_low):
            return sector
    return "C - All Other Manufacturing"


def nace_code_to_sector(nace_raw, company_name: str):
    """
    Convert a raw CRO NACE V2 code to a canonical sector name.
    CRO stores codes as integers without leading zeros:
      4-digit: 6201  = NACE class 62.01  → division 62
      3-digit: 150   = NACE class 01.50  → division 01  (zero-padded: 0150)
      1-digit: 9     = NACE section A    → skip (too coarse to use)
    Strategy: left-pad to 4 digits, take first 2 digits as division number.
    """
    try:
        code = int(float(str(nace_raw)))
        if code < 10:
            return None                        # single-digit: too coarse
        code_str  = str(code).zfill(4)        # "150" → "0150", "6201" → "6201"
        division  = int(code_str[:2])          # "01", "62", "74", …
    except (ValueError, TypeError):
        return None
    section = NACE_DIVISION_MAP.get(division)
    if section is None:
        return None
    if section == "C":
        return classify_manufacturing(company_name)
    return SECTION_TO_SECTOR.get(section)


# ── Name normalisation for fuzzy matching ──────────────────────────────────────
def extra_normalize(name: str) -> str:
    """
    Lowercase + strip legal suffixes for dedup/CRO matching.
    More aggressive than clean_company_name() — used only for matching,
    never as an output key (we always keep the original cleaned name as the key).
    Also normalises "&" → "and" and "Unlimited Company" → "" so that
    UC ↔ Unlimited Company variants resolve to the same normalised form.
    """
    n = str(name).lower().strip()
    n = n.replace("&", "and")
    n = re.sub(r"unlimited company", "", n)
    n = re.sub(r"\b(limited|ltd|plc|dac|uc|ulc|clg|teoranta|co|unlimited)\b\.?", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# ── Manual web-search tags (companies with ≥5 permits, researched by hand) ────
# Format: cleaned company name → canonical sector
# Sources: company websites, CRO registration, LinkedIn, news.
# Left blank = not tagged (ambiguous or not findable).
MANUAL_TAGS: dict[str, str] = {
    "Redwood Extended Care Facility Unlimited Company":  "Q - Health & Social Work Activities",
    "Bus Átha Cliath - Dublin Bus":                      "H - Transport & Storage",
    "CPL Healthcare Limited":                             "Q - Health & Social Work Activities",
    "Cranesbill Productions DAC":                         "R - Arts, Entertainment and Recreation",
    "Homecare & Health Services (Ireland) Limited":       "Q - Health & Social Work Activities",
    "Kellor (Joinery) Services Limited":                  "F - Construction",
    "PricewaterhouseCoopers Services":                    "M - Professional, Scientific & Technical Activities",
    "Cowper Care Centre Designated Activity Company":     "Q - Health & Social Work Activities",
    "Dahlia Productions DAC":                             "R - Arts, Entertainment and Recreation",
    "Bank of America Europe Designated Activity Company": "K - Financial & Insurance Activities",
    "Brindley Healthcare Services Limited (Manor)":       "Q - Health & Social Work Activities",
    "MCGA Limited T/A Orwell Queen of Peace":             "Q - Health & Social Work Activities",
    "Dame Care Recruitment Agency Limited":               "N - Administrative & Support Service Activities",
    "Rototech Limited":                                   "F - Construction",          # CRO: civil engineering NEC
    "Padre Pio Windmill Churchtown Limited":              "Q - Health & Social Work Activities",
    "TerraGlen Residential Care Services Limited":        "Q - Health & Social Work Activities",
    "Balmoral Nursing Home Limited":                      "Q - Health & Social Work Activities",
    "Derry Film Initiative Limited":                      "R - Arts, Entertainment and Recreation",
    "Emerald Isle Bakery Limited":                        "C - Manufacture of Food, Drink & Tobacco",
    "Seatech Engineering Services Limited":               "F - Construction",
    "Phibblestown Community Centre Limited":              "S - Other Service Activities",
    "Errigal Construction Limited":                       "F - Construction",
    "International Seafoods Of Ireland Limited":          "C - Manufacture of Food, Drink & Tobacco",
    "Innisfree Lodge Nursing Home Limited":               "Q - Health & Social Work Activities",
    "Aspect Contracts Limited":                           "F - Construction",
    "Maplewood Nursing Home Limited":                     "Q - Health & Social Work Activities",
    "Kingswood Capital Limited":                          "K - Financial & Insurance Activities",
    "Rosewood Construction Limited":                      "F - Construction",
    "Mater Private Network Limited":                      "Q - Health & Social Work Activities",
    "Walsh Mushrooms Ireland Limited":                    "C - Manufacture of Food, Drink & Tobacco",
    "Ardan Care Limited":                                 "Q - Health & Social Work Activities",
    "Ashgrove Nursing Home Limited":                      "Q - Health & Social Work Activities",
    "Atlantic Technical University":                      "P - Education",
    "Beaumont Private Hospital Limited":                  "Q - Health & Social Work Activities",
    "Deasy's Pub":                                        "I - Accommodation & Food Services Activities",
    "EirGrid plc":                                        "D - Electricity & Gas & Air Conditioning Supply",
    "Frank Keane Holdings Limited":                       "G - Wholesale & Retail Trade",
    "Glandore Business Centres Limited":                  "L - Real Estate Activities",
    "Greenvale Care Home Limited":                        "Q - Health & Social Work Activities",
    "Hazel Hill Country House Hotel":                     "I - Accommodation & Food Services Activities",
    "Independent News & Media Limited":                   "J - Information & Communication Activities",
    "Integrated Waste Management (Ireland) Limited":      "E - Water Supply, Sewerage, Waste Management & Remedial Activities",
    "Irish Cement Limited":                               "C - All Other Manufacturing",
    "Lakeview Nursing Home Limited":                      "Q - Health & Social Work Activities",
    "Meadowbrook Nursing Home Limited":                   "Q - Health & Social Work Activities",
    "Milford Care Centre":                                "Q - Health & Social Work Activities",
    "Nua Healthcare Services Limited":                    "Q - Health & Social Work Activities",
    "Oldtown Nursing Home":                               "Q - Health & Social Work Activities",
    "Orpea Ireland":                                      "Q - Health & Social Work Activities",
    "Saint John Of God Hospital Company":                 "Q - Health & Social Work Activities",
    "Springvale Nursing Home Limited":                    "Q - Health & Social Work Activities",
    "Strategic Hospitality Limited":                      "I - Accommodation & Food Services Activities",
    "Sunbeam House Services Society":                     "Q - Health & Social Work Activities",
    "Superquinn Limited":                                 "G - Wholesale & Retail Trade",
    "Threshold Housing Association":                      "Q - Health & Social Work Activities",
    "Tivoli Community Foundation":                        "Q - Health & Social Work Activities",
    "Triton IT Limited":                                  "J - Information & Communication Activities",
    "Whitfield Clinic Limited":                           "Q - Health & Social Work Activities",
    "Windmill Healthcare Services Limited":               "Q - Health & Social Work Activities",
    # ── Verified via web search / unambiguous from name (round 2) ───────────────
    "Ard Na Ri Nursing Home":                             "Q - Health & Social Work Activities",
    "Forsythia Productions DAC":                          "R - Arts, Entertainment and Recreation",
    "Sparrow Productions DAC":                            "R - Arts, Entertainment and Recreation",
    "Padraig Brady Carpentry Limited":                    "F - Construction",
    "McMahon's Concrete Products Ltd":                    "C - All Other Manufacturing",
    "E Thomas Developments Limited":                      "F - Construction",          # civil engineering, tarmac & kerbing, Newbridge Co. Kildare
    "Season Master Double Glazing Ltd":                   "F - Construction",          # double glazing installation
    "Altrad Services (Ireland) Limited":                  "F - Construction",          # scaffolding & industrial services (Altrad Group)
    "Port Douglas Contractors Ltd":                       "F - Construction",
    "B.K.E. Care Limited":                               "Q - Health & Social Work Activities",
    "Bloomfield Hospital":                                "Q - Health & Social Work Activities",
    "C Sullivan Stone Limited":                           "C - All Other Manufacturing",       # stone/nonmetallic mineral product manufacturing, Slane
    "Errigal Contracts Ireland Limited":                  "F - Construction",
    "Ted Brennan Motors Limited":                         "G - Wholesale & Retail Trade",
    "Flynn Bros Rent A Car Ballygar Limited":             "N - Administrative & Support Service Activities",  # vehicle rental (NACE 77)
    "GoodPeople Homecare Limited":                        "Q - Health & Social Work Activities",
    "UPMC Whitfield Hospital Limited":                    "Q - Health & Social Work Activities",  # US healthcare group's Irish hospital
    "Kellor Services (IRE) Limited":                      "F - Construction",          # joinery services, same Castleisland address as Kellor (Joinery)
    "FISC-Ireland Limited":                               "M - Professional, Scientific & Technical Activities",  # IT consulting/mgmt (Fidelity shared services)
    "J.S. McCarthy Ltd":                                  "F - Construction",          # industrial painting & protective coatings contractor
    "Egis Engineering Ireland Limited":                   "M - Professional, Scientific & Technical Activities",  # civil engineering consultancy (JB Barry rebrand)
    "Kepak Ballybay Unlimited Company":                   "C - Manufacture of Food, Drink & Tobacco",  # meat processing, Ballybay Co. Monaghan
    "Beaumont Hospital Board":                            "Q - Health & Social Work Activities",  # major public teaching hospital, Dublin
    "R.K.C. Agri Limited":                               "A - Agriculture, Forestry & Fishing",  # agricultural support services, Mullingar
}


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  07_enrich_2026_sectors.py")
    print("=" * 60)

    # ── Load inputs ────────────────────────────────────────────────────────────
    df = pd.read_csv(PERMITS_PATH)
    sector_map = pd.read_csv(SECTOR_MAP_PATH)
    already_mapped = set(sector_map["company_name_clean"].str.strip())

    prior_names = set(df[df["year"] < 2026]["company_name_clean"].unique())
    df_2026     = df[df["year"] == 2026].copy()
    new_2026    = set(df_2026["company_name_clean"].unique())

    # Permits lookup for 2026 companies (for the total_permits column in output)
    issued_2026 = df_2026.groupby("company_name_clean")["issued"].sum().to_dict()

    print(f"\n  Prior companies (2015-2025): {len(prior_names):,}")
    print(f"  Companies in 2026:           {len(new_2026):,}")

    # ── Step A: dedup — identify near-duplicate names ──────────────────────────
    print("\n── Step A: Dedup (normalised name match against prior years)")
    prior_norm_map = {extra_normalize(n): n for n in prior_names}

    dedup_pairs = []
    genuine_new = []
    for name in sorted(new_2026):
        norm = extra_normalize(name)
        if norm in prior_norm_map:
            dedup_pairs.append((name, prior_norm_map[norm]))
        else:
            genuine_new.append(name)

    print(f"  Near-duplicates found:  {len(dedup_pairs)}")
    print(f"  Genuinely new:          {len(genuine_new)}")

    # ── Step B: inherit sector for near-duplicates ────────────────────────────
    print("\n── Step B: Inherit sector from prior-year canonical name")
    prior_sector = sector_map.set_index("company_name_clean")["sector"].to_dict()

    dedup_rows = []
    dedup_no_sector = 0
    for new_name, prior_name in dedup_pairs:
        if new_name in already_mapped:
            continue                         # already in sector map
        sector = prior_sector.get(prior_name)
        if sector and sector in CANONICAL_SECTORS:
            dedup_rows.append({
                "company_name_clean": new_name,
                "total_permits":      issued_2026.get(new_name, 0),
                "sector":             sector,
                "source":             "2026_dedup",
            })
        else:
            dedup_no_sector += 1

    print(f"  Inherited sector:  {len(dedup_rows)}")
    print(f"  Prior had no tag:  {dedup_no_sector}")

    # ── Step C: CRO NACE lookup for genuinely new companies ───────────────────
    print("\n── Step C: CRO NACE lookup (normalised join)")
    cro = pd.read_csv(CRO_PATH, usecols=["company_name", "nace_v2_code"],
                      low_memory=False)
    cro = cro.dropna(subset=["company_name"])
    cro["name_norm"] = cro["company_name"].apply(extra_normalize)
    # Keep one CRO row per normalised name (prefer rows that have a NACE code)
    cro_deduped = (
        cro.sort_values("nace_v2_code", na_position="last")
           .drop_duplicates(subset="name_norm", keep="first")
    )
    cro_lookup = cro_deduped.set_index("name_norm")["nace_v2_code"].to_dict()

    cro_rows = []
    cro_no_code = 0
    cro_bad_code = 0
    for name in genuine_new:
        if name in already_mapped:
            continue
        norm = extra_normalize(name)
        nace_raw = cro_lookup.get(norm)
        if nace_raw is None or pd.isna(nace_raw):
            cro_no_code += 1
            continue
        sector = nace_code_to_sector(nace_raw, name)
        if sector is None:
            cro_bad_code += 1
            continue
        cro_rows.append({
            "company_name_clean": name,
            "total_permits":      issued_2026.get(name, 0),
            "sector":             sector,
            "source":             "2026_cro",
        })

    print(f"  CRO-matched with sector: {len(cro_rows)}")
    print(f"  No CRO NACE code:        {cro_no_code}")
    print(f"  Unrecognised NACE code:  {cro_bad_code}")

    # ── Step D: Manual web-search tags ────────────────────────────────────────
    print("\n── Step D: Manual web-search tags")
    # Build set of names tagged so far (from steps B + C + existing map)
    tagged_so_far = (
        already_mapped
        | {r["company_name_clean"] for r in dedup_rows}
        | {r["company_name_clean"] for r in cro_rows}
    )

    manual_rows = []
    skipped_manual = 0
    for name, sector in MANUAL_TAGS.items():
        if name in tagged_so_far:
            skipped_manual += 1
            continue
        if sector not in CANONICAL_SECTORS:
            print(f"  [WARN] Non-canonical sector for {name!r}: {sector!r}")
            continue
        manual_rows.append({
            "company_name_clean": name,
            "total_permits":      issued_2026.get(name, 0),
            "sector":             sector,
            "source":             "2026_websearch",
        })

    print(f"  Manual tags applied: {len(manual_rows)}")
    print(f"  Already tagged (skipped): {skipped_manual}")

    # ── Step E: Append to company_sector_map.csv ──────────────────────────────
    new_rows = dedup_rows + cro_rows + manual_rows
    if not new_rows:
        print("\n  No new rows to add — exiting.")
    else:
        # Backup
        bak_path = SECTOR_MAP_PATH.with_suffix(".csv.bak4")
        shutil.copy2(SECTOR_MAP_PATH, bak_path)
        print(f"\n── Step E: Append {len(new_rows)} new rows to sector map")
        print(f"  Backup: {bak_path}")

        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([sector_map, new_df], ignore_index=True)
        combined.to_csv(SECTOR_MAP_PATH, index=False)

        print(f"  Rows before: {len(sector_map):,}")
        print(f"  Rows after:  {len(combined):,}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_tagged = len(dedup_rows) + len(cro_rows) + len(manual_rows)
    genuinely_new_untagged = sum(
        1 for n in genuine_new
        if n not in already_mapped
        and n not in {r["company_name_clean"] for r in cro_rows}
        and n not in {r["company_name_clean"] for r in manual_rows}
    )
    print(f"""
── Summary ──────────────────────────────────────────────────
  Dedup inheritances:     {len(dedup_rows):>5}
  CRO NACE lookups:       {len(cro_rows):>5}
  Web-search manual tags: {len(manual_rows):>5}
  ─────────────────────────────────
  Total new sector rows:  {total_tagged:>5}
  Genuinely new, untagged:{genuinely_new_untagged:>5}  (left blank — insufficient data)
─────────────────────────────────────────────────────────────

Next steps:
  python src/05_clean_companies.py
  python src/04_build_map.py
""")
