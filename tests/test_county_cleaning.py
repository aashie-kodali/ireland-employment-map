"""
tests/test_county_cleaning.py
==============================
Integration tests for src/01_clean_data.py county output.

Verifies that county_permits.csv was produced correctly by checking
shape, coverage, and year totals against known DETE Grand Totals.

Requires: data/cleaned/county_permits.csv
Skip message shown if the CSV is absent — run src/01_clean_data.py first.

Run with: pytest tests/ -v
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

CSV_PATH = Path("data/cleaned/county_permits.csv")

# Known county-level year totals (sum of 'issued' across all 26 ROI counties).
# These differ slightly from the company-level totals in test_company_cleaning.py
# because the county data uses suppression ('*') for small counts whereas the
# company data does not — expect a ~1–3% gap.
KNOWN_COUNTY_TOTALS = {
    2015: 7_134,
    2016: 9_267,
    2024: 39_320,
}

TOLERANCE = 0.03   # allow up to 3% variance

# Northern Ireland counties — these must never appear in the ROI dataset
NI_COUNTIES = {"Antrim", "Armagh", "Down", "Fermanagh", "Londonderry", "Tyrone"}

# ── Load ROI_COUNTIES from 01_clean_data.py ───────────────────────────────────
# The filename starts with a digit so we use importlib instead of a normal import.
_src = Path(__file__).parent.parent / "src" / "01_clean_data.py"
_spec = importlib.util.spec_from_file_location("clean_data", _src)
_mod  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
ROI_COUNTIES = _mod.ROI_COUNTIES


@pytest.fixture(scope="module")
def county_df():
    if not CSV_PATH.exists():
        pytest.skip(
            f"{CSV_PATH} not found — run 'python src/01_clean_data.py' first"
        )
    return pd.read_csv(CSV_PATH)


class TestCountyParsing:
    def test_has_required_columns(self, county_df):
        required = {"year", "county", "issued", "refused", "withdrawn"}
        assert required.issubset(county_df.columns)

    def test_exactly_286_rows(self, county_df):
        # 26 ROI counties × 11 years (2015–2025) = 286 rows, no more, no less
        assert len(county_df) == 286, (
            f"Expected 286 rows (26 counties × 11 years), got {len(county_df)}"
        )

    def test_all_11_years_present(self, county_df):
        years = set(county_df["year"].unique())
        expected = set(range(2015, 2026))
        assert years == expected, f"Missing years: {expected - years}"

    def test_exactly_26_counties(self, county_df):
        n = county_df["county"].nunique()
        assert n == 26, f"Expected 26 unique counties, got {n}"

    def test_only_roi_counties(self, county_df):
        present = set(county_df["county"].unique())
        unexpected = present - ROI_COUNTIES
        assert not unexpected, f"Non-ROI counties found: {unexpected}"

    def test_no_northern_ireland_counties(self, county_df):
        present = set(county_df["county"].unique())
        overlap = present & NI_COUNTIES
        assert not overlap, f"Northern Ireland counties found: {overlap}"

    def test_no_negative_issued(self, county_df):
        negatives = (county_df["issued"] < 0).sum()
        assert negatives == 0, f"{negatives} rows with negative issued count"

    def test_year_totals_match_known(self, county_df):
        yearly = county_df.groupby("year")["issued"].sum()
        for year, expected in KNOWN_COUNTY_TOTALS.items():
            actual = float(yearly.get(year, 0))
            diff = abs(actual - expected) / expected
            assert diff <= TOLERANCE, (
                f"Year {year}: expected ~{expected:,}, got {actual:,.0f} "
                f"({diff:.1%} off, tolerance {TOLERANCE:.0%})"
            )

    def test_each_county_present_every_year(self, county_df):
        # Every (county, year) combination must appear — no gaps allowed
        counts = county_df.groupby("county")["year"].count()
        missing = counts[counts != 11]
        assert missing.empty, (
            f"Counties with missing years:\n{missing.to_string()}"
        )

    def test_withdrawn_null_in_2025(self, county_df):
        # The 2025 source file does not include a 'withdrawn' column
        rows_2025 = county_df[county_df["year"] == 2025]
        assert rows_2025["withdrawn"].isna().all(), (
            "Expected 'withdrawn' to be null for all 2025 rows "
            "(source file does not include this column yet)"
        )
