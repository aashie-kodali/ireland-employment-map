"""
src/08_correct_sector_tags.py
==============================
Corrects sector mis-tags identified during the 2026 new-entrant audit.
Two categories of errors:
  1. CRO NACE mis-registrations (company registered under wrong NACE code)
  2. Dedup inheritance from a prior-year entry that was itself wrong

Updates (overwrites) existing rows in data/raw/company_sector_map.csv.
Does NOT append new rows.  Backs up as .bak5 first.

Run:
  python src/08_correct_sector_tags.py
Then rebuild:
  python src/05_clean_companies.py
  python src/04_build_map.py
"""

import shutil
from pathlib import Path

import pandas as pd

SECTOR_MAP_PATH = Path("data/raw/company_sector_map.csv")

# ── Corrections ───────────────────────────────────────────────────────────────
# Format: company_name_clean → new_sector  (or None to remove the tag)
# None = set sector to "" and source to "2026_correction_removed"
CORRECTIONS = {
    # J → N  (Information & Communication → Administrative)
    "People Pro HR Limited":
        "N - Administrative & Support Service Activities",

    # J → Q  (IT → Health)
    "Kilduff Care Co. Limited":
        "Q - Health & Social Work Activities",

    # J → R  (IT → Arts: film/TV productions go under R for consistency
    #          with other production DACs already in the map)
    "Talbot Films Designated Activity Company":
        "R - Arts, Entertainment and Recreation",
    "Corked Season 2 Designated Activity Company":
        "R - Arts, Entertainment and Recreation",
    "The Cartoon Saloon Limited":
        "R - Arts, Entertainment and Recreation",

    # K → Q  (Financial → Health: holding company structure, actual biz = care)
    "Springcare Independent Living Limited":
        "Q - Health & Social Work Activities",

    # K → H  (Financial → Transport: JJ Kavanagh = major bus/coach operator)
    "JJ Kavanagh & Sons Limited":
        "H - Transport & Storage",

    # K → C  (Financial → Manufacturing: dedup inherited wrong sector)
    "Mannok Cement Limited":
        "C - All Other Manufacturing",
    "NorDan Vinduer":
        "C - All Other Manufacturing",

    # K → Q  (Financial → Health: care/residential facility)
    "Catherine Mcauley House":
        "Q - Health & Social Work Activities",

    # C-Other → C-Food  (_MFG_RULES regex missed NACE 10xx food codes)
    "Danone Infant Nutrition Macroom Ltd":
        "C - Manufacture of Food, Drink & Tobacco",
    "Oaksmoke Bakeries Limited":
        "C - Manufacture of Food, Drink & Tobacco",

    # L → Q  (Real Estate → Health: disability services org)
    "Cheeverstown House":
        "Q - Health & Social Work Activities",

    # L → F  (Real Estate → Construction: CRO NACE 6832 error)
    "Harklo Construction":
        "F - Construction",

    # L → I  (Real Estate → Accommodation: hotel group)
    "The Grafton Hospitality Unlimited Company":
        "I - Accommodation & Food Services Activities",

    # L → remove  (Real Estate is clearly wrong for a food company; can't confirm better)
    "Castleknock Foods Ltd":
        None,

    # H → remove  (Transport is wrong; NACE 4939 is a CRO error for a vending company)
    "A.I. Vending Solutions Ltd":
        None,

    # M → C  (Professional → Manufacturing: precast concrete)
    "FLI Precast Solutions Limited":
        "C - All Other Manufacturing",

    # M → K  (Professional → Financial: private equity / capital firms)
    "KKR Ireland Designated Activity Company":
        "K - Financial & Insurance Activities",
    "West 53 Capital Limited":
        "K - Financial & Insurance Activities",
    "Accel Capital Partners (Ireland) Limited":
        "K - Financial & Insurance Activities",
}

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


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  08_correct_sector_tags.py")
    print("=" * 60)

    df = pd.read_csv(SECTOR_MAP_PATH)

    # Validate all correction targets exist and have valid sectors
    not_found = []
    bad_sector = []
    for name, sector in CORRECTIONS.items():
        if name not in df["company_name_clean"].values:
            not_found.append(name)
        if sector is not None and sector not in CANONICAL_SECTORS:
            bad_sector.append((name, sector))

    if not_found:
        print(f"\n  [WARN] {len(not_found)} names not found in sector map:")
        for n in not_found:
            print(f"    {n!r}")
    if bad_sector:
        print(f"\n  [ERROR] non-canonical sectors — fix before running:")
        for n, s in bad_sector:
            print(f"    {n!r} → {s!r}")
        raise SystemExit(1)

    # Backup
    bak = SECTOR_MAP_PATH.with_suffix(".csv.bak5")
    shutil.copy2(SECTOR_MAP_PATH, bak)
    print(f"\n  Backup → {bak}")

    # Apply corrections
    applied = 0
    removed = 0
    for name, new_sector in CORRECTIONS.items():
        mask = df["company_name_clean"] == name
        if not mask.any():
            continue
        old_sector = df.loc[mask, "sector"].values[0]
        if new_sector is None:
            df.loc[mask, "sector"] = ""
            df.loc[mask, "source"] = "2026_correction_removed"
            print(f"  REMOVE  {name!r}  (was: {old_sector})")
            removed += 1
        else:
            df.loc[mask, "sector"] = new_sector
            df.loc[mask, "source"] = "2026_correction"
            arrow = f"{old_sector}  →  {new_sector}"
            print(f"  FIX     {name!r}")
            print(f"          {arrow}")
            applied += 1

    df.to_csv(SECTOR_MAP_PATH, index=False)

    print(f"""
── Summary ─────────────────────────────────────────────────
  Corrections applied: {applied}
  Tags removed:        {removed}
  Total changes:       {applied + removed}
  Not found (skipped): {len(not_found)}
─────────────────────────────────────────────────────────────

Next steps:
  python src/05_clean_companies.py
  python src/04_build_map.py
""")
