"""
tests/test_company_cleaning.py
===============================
Integration tests for src/05_clean_companies.py output.

Verifies that company_permits.csv was produced correctly by checking
year totals against known DETE Grand Totals.

Requires: data/cleaned/company_permits.csv
Skip message shown if the CSV is absent — run src/05_clean_companies.py first.

Run with: pytest tests/ -v
"""

from pathlib import Path

import pandas as pd
import pytest

CSV_PATH        = Path("data/cleaned/company_permits.csv")
SECTOR_MAP_PATH = Path("data/raw/company_sector_map.csv")

# The 24 canonical NACE sector names used in sector_permits.csv (2020+ rows).
# sector values in company_permits.csv must be a subset of this set (or null).
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

# Known DETE Grand Totals from the source Excel files.
# Used to verify the parser produced correct output.
# 2019 is excluded: the source file only covers 9 of 12 months,
# so the total is intentionally ~9% below the official Grand Total.
KNOWN_TOTALS = {
    2015: 7_353,
    2016: 9_373,
    2024: 39_390,
}

# Tolerance: allow up to 2% variance to account for minor name-dedup effects
TOLERANCE = 0.02


@pytest.fixture(scope="module")
def company_df():
    if not CSV_PATH.exists():
        pytest.skip(
            f"{CSV_PATH} not found — run 'python src/05_clean_companies.py' first"
        )
    return pd.read_csv(CSV_PATH)


class TestCompanyParsing:
    def test_has_required_columns(self, company_df):
        required = {"year", "company_name_raw", "company_name_clean", "issued"}
        assert required.issubset(company_df.columns)

    def test_year_totals_match_grand_totals(self, company_df):
        yearly = company_df.groupby("year")["issued"].sum()
        for year, expected in KNOWN_TOTALS.items():
            actual = int(yearly.get(year, 0))
            diff = abs(actual - expected) / expected
            assert diff <= TOLERANCE, (
                f"Year {year}: expected ~{expected:,}, got {actual:,} "
                f"({diff:.1%} off, tolerance {TOLERANCE:.0%})"
            )

    def test_all_eleven_years_present(self, company_df):
        years = set(company_df["year"].unique())
        expected = set(range(2015, 2026))
        assert years == expected, f"Missing years: {expected - years}"

    def test_no_null_company_names(self, company_df):
        nulls = company_df["company_name_clean"].isna().sum()
        assert nulls == 0, f"{nulls} null company names found"

    def test_no_negative_issued(self, company_df):
        negatives = (company_df["issued"] < 0).sum()
        assert negatives == 0, f"{negatives} rows with negative issued count"

    def test_row_count_plausible(self, company_df):
        # 48k rows is expected; allow a 10% band either side
        assert 43_000 < len(company_df) < 53_000, (
            f"Unexpected row count: {len(company_df):,}"
        )


class TestSectorColumn:
    """
    Tests for the 'sector' column added by the company_sector_map.csv join.

    The sector column is always present (added as null when the map CSV is absent).
    When the map CSV exists, all non-null sector values must be valid NACE names.
    """

    def test_sector_column_always_present(self, company_df):
        assert "sector" in company_df.columns, (
            "'sector' column missing from company_permits.csv — "
            "check src/05_clean_companies.py sector enrichment logic"
        )

    def test_sector_values_valid_or_null(self, company_df):
        """Every non-null sector value must be one of the 24 canonical NACE names."""
        non_null = company_df["sector"].dropna()
        unexpected = set(non_null.unique()) - CANONICAL_NACE_SECTORS
        assert not unexpected, (
            f"Unexpected sector values found: {unexpected}\n"
            "Check data/raw/company_sector_map.csv for typos."
        )

    def test_no_unexpected_sector_strings(self, company_df):
        """Alias for test_sector_values_valid_or_null — explicit guard for typos."""
        non_null = company_df["sector"].dropna()
        for val in non_null.unique():
            assert val in CANONICAL_NACE_SECTORS, (
                f"'{val}' is not a canonical NACE sector name. "
                "Values must match sector_permits.csv (2020+ rows) exactly."
            )

    def test_sector_coverage_nonzero_when_map_exists(self, company_df):
        """If company_sector_map.csv is present, at least one company must be tagged."""
        if not SECTOR_MAP_PATH.exists():
            pytest.skip("company_sector_map.csv absent — sector tagging not active")
        tagged = company_df["sector"].notna().sum()
        assert tagged > 0, (
            "company_sector_map.csv exists but no sector values were joined — "
            "check that company_name_clean join keys match."
        )
