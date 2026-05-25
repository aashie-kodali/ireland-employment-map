"""
tests/test_pipeline.py
=======================
End-to-end pipeline output tests.

These tests verify that the full pipeline (make data → make analyze → make map)
has run successfully by inspecting the output files. They do NOT re-trigger the
pipeline — run 'make data' and 'make map' first.

All tests are marked @pytest.mark.slow and are excluded from the default
'make test' target. Run them with:

    make test-slow           # or
    pytest tests/ -v -m slow

Run from the project root (the path resolution uses Path(__file__).parent.parent).
"""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

# ── Project root — works regardless of which directory pytest is invoked from ──
ROOT = Path(__file__).parent.parent

CLEANED_DIR = ROOT / "data" / "cleaned"
DB_PATH     = ROOT / "data" / "employment.db"
MAP_PATH    = ROOT / "output" / "map" / "ireland_employment_map.html"

EXPECTED_CSVS = [
    "county_permits.csv",
    "sector_permits.csv",
    "nationality_permits.csv",
    "visa_decisions.csv",
    "company_permits.csv",
]

EXPECTED_TABLES = {
    "county_permits",
    "sector_permits",
    "nationality_permits",
    "visa_decisions",
    "company_permits",
}

# Strings that must be present in the generated HTML (injected JS constants)
REQUIRED_MARKERS = ["COUNTY_BY_YEAR", "COMPANY_DATA", "GEOJSON"]


@pytest.mark.slow
class TestPipelineOutputs:
    def test_cleaned_csvs_exist(self):
        """All five cleaned CSVs must be present in data/cleaned/."""
        for fname in EXPECTED_CSVS:
            path = CLEANED_DIR / fname
            assert path.exists(), (
                f"{path} not found — run 'make data' first"
            )

    def test_cleaned_csvs_non_empty(self):
        """Each cleaned CSV must have at least one data row."""
        for fname in EXPECTED_CSVS:
            path = CLEANED_DIR / fname
            if not path.exists():
                pytest.skip(f"{path} absent — run 'make data' first")
            df = pd.read_csv(path)
            assert len(df) > 0, f"{fname} is empty"

    def test_database_exists(self):
        """The SQLite database must exist at data/employment.db."""
        assert DB_PATH.exists(), (
            f"{DB_PATH} not found — run 'make data' first"
        )

    def test_database_has_five_tables(self):
        """The database must contain exactly the five expected tables."""
        if not DB_PATH.exists():
            pytest.skip(f"{DB_PATH} absent — run 'make data' first")
        with sqlite3.connect(DB_PATH) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert tables == EXPECTED_TABLES, (
            f"Table mismatch.\n  Expected: {EXPECTED_TABLES}\n  Got: {tables}"
        )

    def test_database_tables_non_empty(self):
        """Every table in the database must have at least one row."""
        if not DB_PATH.exists():
            pytest.skip(f"{DB_PATH} absent — run 'make data' first")
        with sqlite3.connect(DB_PATH) as conn:
            for table in EXPECTED_TABLES:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                assert count > 0, f"Table '{table}' is empty"

    def test_map_html_exists(self):
        """The interactive map HTML file must be present."""
        assert MAP_PATH.exists(), (
            f"{MAP_PATH} not found — run 'make map' first"
        )

    def test_map_html_over_100kb(self):
        """The map file must exceed 100 KB — catches accidental empty builds."""
        if not MAP_PATH.exists():
            pytest.skip(f"{MAP_PATH} absent — run 'make map' first")
        size = MAP_PATH.stat().st_size
        assert size > 100 * 1024, (
            f"Map HTML is only {size:,} bytes — expected > 102,400 bytes. "
            "The build may have failed or produced empty output."
        )

    def test_map_html_contains_data_markers(self):
        """The map HTML must contain the injected JS data constants."""
        if not MAP_PATH.exists():
            pytest.skip(f"{MAP_PATH} absent — run 'make map' first")
        text = MAP_PATH.read_text(encoding="utf-8")
        for marker in REQUIRED_MARKERS:
            assert marker in text, (
                f"Marker '{marker}' not found in map HTML — "
                "the template substitution in build_html() may have failed."
            )
