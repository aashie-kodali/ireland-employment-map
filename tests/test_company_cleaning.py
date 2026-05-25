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

CSV_PATH = Path("data/cleaned/company_permits.csv")

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
