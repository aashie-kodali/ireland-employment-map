"""
tests/test_sector_cleaning.py
==============================
Integration tests for src/01_clean_data.py sector output.

Verifies that sector_permits.csv was produced correctly: correct shape,
canonical NACE sector names from 2020 onwards, and pre-2020 old-style names
confined to years before 2020.

Requires: data/cleaned/sector_permits.csv
Skip message shown if the CSV is absent — run src/01_clean_data.py first.

Run with: pytest tests/ -v
"""

from pathlib import Path

import pandas as pd
import pytest

CSV_PATH = Path("data/cleaned/sector_permits.csv")

# The 24 canonical NACE sector names used from 2020 onwards.
# These must match the normalised values produced by normalise_sector_names().
CANONICAL_NACE_SECTORS = {
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

# Pre-2020 sector names (old DETE classification — not NACE).
# These should ONLY appear in years before 2020.
OLD_SECTOR_NAMES = {
    "Agriculture & Fisheries",
    "Catering",
    "Domestic",
    "Education",
    "Entertainment",
    "Exchange Agreements",
    "Industry",
    "Medical & Nursing",
    "None specified",
    "Service Industry",
    "Sport",
}

# Known national sector totals for spot-check years (±5% tolerance)
KNOWN_SECTOR_TOTALS = {
    2016: 9_373,
    2024: 39_390,
}
TOLERANCE = 0.05


@pytest.fixture(scope="module")
def sector_df():
    if not CSV_PATH.exists():
        pytest.skip(
            f"{CSV_PATH} not found — run 'python src/01_clean_data.py' first"
        )
    return pd.read_csv(CSV_PATH)


class TestSectorParsing:
    def test_has_required_columns(self, sector_df):
        required = {"year", "sector", "issued"}
        assert required.issubset(sector_df.columns)

    def test_all_11_years_present(self, sector_df):
        years = set(sector_df["year"].unique())
        expected = set(range(2015, 2026))
        assert years == expected, f"Missing years: {expected - years}"

    def test_no_null_sectors(self, sector_df):
        nulls = sector_df["sector"].isna().sum()
        assert nulls == 0, f"{nulls} null sector values found"

    def test_no_negative_issued(self, sector_df):
        negatives = (sector_df["issued"] < 0).sum()
        assert negatives == 0, f"{negatives} rows with negative issued count"

    def test_row_count_plausible(self, sector_df):
        # 11 years × ~10–24 sectors per year ≈ 198 rows currently;
        # allow a ±5% band to accommodate minor future schema changes.
        assert 190 < len(sector_df) < 210, (
            f"Unexpected row count: {len(sector_df)} (expected ~198)"
        )

    def test_nace_sectors_present_from_2020(self, sector_df):
        """All 24 canonical NACE sectors must appear in every year 2020–2025."""
        for year in range(2020, 2026):
            year_sectors = set(sector_df[sector_df["year"] == year]["sector"].unique())
            missing = CANONICAL_NACE_SECTORS - year_sectors
            assert not missing, (
                f"Year {year}: missing NACE sectors: {missing}"
            )

    def test_no_unexpected_nace_sectors(self, sector_df):
        """For 2020+ rows, every sector value must be in the canonical set."""
        post2020 = sector_df[sector_df["year"] >= 2020]
        unexpected = set(post2020["sector"].unique()) - CANONICAL_NACE_SECTORS
        assert not unexpected, (
            f"Unexpected sector names found in 2020+ data: {unexpected}\n"
            "This may indicate a new truncation variant slipped through "
            "normalise_sector_names()."
        )

    def test_old_sectors_confined_to_pre2020(self, sector_df):
        """Old-style sector names must not appear in 2020 or later."""
        post2020 = sector_df[sector_df["year"] >= 2020]
        leaked = set(post2020["sector"].unique()) & OLD_SECTOR_NAMES
        assert not leaked, (
            f"Old pre-2020 sector names found in 2020+ data: {leaked}"
        )

    def test_year_totals_roughly_match_known(self, sector_df):
        yearly = sector_df.groupby("year")["issued"].sum()
        for year, expected in KNOWN_SECTOR_TOTALS.items():
            actual = float(yearly.get(year, 0))
            diff = abs(actual - expected) / expected
            assert diff <= TOLERANCE, (
                f"Year {year}: sector total expected ~{expected:,}, "
                f"got {actual:,.0f} ({diff:.1%} off, tolerance {TOLERANCE:.0%})"
            )
