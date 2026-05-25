"""
tests/test_cleaning.py
======================
Unit tests for the pure utility functions in src/01_clean_data.py.

These tests use constructed DataFrames — no raw Excel files required.
Run with: pytest tests/ -v
"""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ── Import 01_clean_data.py ────────────────────────────────────────────────────
# The filename starts with a digit, so we can't use a normal import statement.
# importlib.util.spec_from_file_location loads it by path instead.
_src = Path(__file__).parent.parent / "src" / "01_clean_data.py"
spec = importlib.util.spec_from_file_location("clean_data", _src)
clean_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clean_data)

extract_year         = clean_data.extract_year
strip_blank_prefix   = clean_data.strip_blank_prefix
normalise_sector_names = clean_data.normalise_sector_names
ROI_COUNTIES         = clean_data.ROI_COUNTIES


# ══════════════════════════════════════════════════════════════════════════════
# extract_year
# ══════════════════════════════════════════════════════════════════════════════

class TestExtractYear:
    def test_standard_name(self):
        assert extract_year("County Permits Issued 2024.xlsx") == 2024

    def test_hyphenated_name(self):
        assert extract_year("Permits-2019.xlsx") == 2019

    def test_year_at_start(self):
        assert extract_year("2015_data.xlsx") == 2015

    def test_no_year_raises(self):
        with pytest.raises(ValueError, match="No 4-digit year"):
            extract_year("Permits.xlsx")


# ══════════════════════════════════════════════════════════════════════════════
# strip_blank_prefix
# ══════════════════════════════════════════════════════════════════════════════

class TestStripBlankPrefix:
    def test_strips_all_nan_first_row_and_column(self):
        # Simulates a 2019-style file: row 0 is all NaN, col 0 is blank prefix
        df = pd.DataFrame([
            [np.nan, np.nan, np.nan],   # blank prefix row
            [np.nan, "County", "Total"],
            [np.nan, "Dublin", 100],
        ])
        result = strip_blank_prefix(df)
        # Row 0 and col 0 should both be dropped
        assert result.shape == (2, 2)

    def test_leaves_intact_when_no_blank_prefix(self):
        df = pd.DataFrame([
            ["Year", "County", "Total"],
            [2020, "Dublin", 500],
        ])
        result = strip_blank_prefix(df)
        assert result.shape == (2, 3)

    def test_leaves_intact_when_first_row_has_partial_data(self):
        # Only fully-NaN first rows should be stripped
        df = pd.DataFrame([
            [np.nan, "County", np.nan],  # not all NaN
            [2020, "Dublin", 500],
        ])
        result = strip_blank_prefix(df)
        assert result.shape == (2, 3)


# ══════════════════════════════════════════════════════════════════════════════
# normalise_sector_names
# ══════════════════════════════════════════════════════════════════════════════

class TestNormaliseSectorNames:
    def test_truncated_sector_name_fixed(self):
        df = pd.DataFrame({
            "year":   [2020],
            "sector": ["C - Manufacture of Computers, Electronics & Optica"],
            "issued": [100],
        })
        result = normalise_sector_names(df)
        assert result["sector"].iloc[0] == (
            "C - Manufacture of Computers, Electronics & Optical Equipment"
        )

    def test_grand_total_row_dropped(self):
        df = pd.DataFrame({
            "year":   [2020, 2020],
            "sector": ["Grand Total", "J - Information & Communication"],
            "issued": [800, 300],
        })
        result = normalise_sector_names(df)
        assert "Grand Total" not in result["sector"].values
        assert len(result) == 1

    def test_economic_sector_header_dropped(self):
        df = pd.DataFrame({
            "year":   [2020, 2020],
            "sector": ["Economic Sector", "Q - Human Health & Social Work Activities"],
            "issued": [0, 500],
        })
        result = normalise_sector_names(df)
        assert "Economic Sector" not in result["sector"].values

    def test_duplicate_variant_names_summed(self):
        # Two different spellings → same canonical name → should be merged
        df = pd.DataFrame({
            "year":   [2020, 2020],
            "sector": [
                "M - All other Professional, Scientific & Technical",
                "M - Professional, Scientific & Technical Activities",
            ],
            "issued": [100, 200],
        })
        result = normalise_sector_names(df)
        assert len(result) == 1
        assert result["issued"].iloc[0] == 300

    def test_already_correct_sector_preserved(self):
        df = pd.DataFrame({
            "year":   [2020],
            "sector": ["J - Information & Communication"],
            "issued": [500],
        })
        result = normalise_sector_names(df)
        assert result["sector"].iloc[0] == "J - Information & Communication"
        assert result["issued"].iloc[0] == 500


# ══════════════════════════════════════════════════════════════════════════════
# ROI_COUNTIES allowlist
# ══════════════════════════════════════════════════════════════════════════════

class TestROICounties:
    def test_exactly_26_counties(self):
        assert len(ROI_COUNTIES) == 26

    def test_northern_ireland_counties_absent(self):
        ni_counties = {"Antrim", "Armagh", "Down", "Fermanagh", "Londonderry", "Tyrone"}
        assert ni_counties.isdisjoint(ROI_COUNTIES)

    def test_known_roi_counties_present(self):
        for county in ("Dublin", "Cork", "Galway", "Mayo", "Leitrim"):
            assert county in ROI_COUNTIES, f"{county} missing from ROI_COUNTIES"


# ══════════════════════════════════════════════════════════════════════════════
# Suppressed value handling  ('*' → NaN)
# ══════════════════════════════════════════════════════════════════════════════

class TestSuppressedValues:
    """
    The ISD marks small counts with '*' to protect privacy.
    clean_visa_decisions() replaces '*' with pd.NA.
    We test the underlying pandas behaviour directly since the
    replacement is a one-liner (not a separately importable function).
    """

    def _apply_suppression(self, series: pd.Series) -> pd.Series:
        return series.replace("*", pd.NA)

    def test_star_becomes_na(self):
        s = pd.Series(["*", "100", "*"])
        result = self._apply_suppression(s)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[2])

    def test_numeric_strings_preserved(self):
        s = pd.Series(["*", "100", "200"])
        result = self._apply_suppression(s)
        assert result.iloc[1] == "100"
        assert result.iloc[2] == "200"

    def test_empty_series_unchanged(self):
        s = pd.Series([], dtype=object)
        result = self._apply_suppression(s)
        assert len(result) == 0
