"""
src/02_build_sqlite.py
======================
Loads the four cleaned CSVs from data/cleaned/ into a SQLite database at
data/employment.db.  Creates one table per dataset, adds indexes for the
columns we query most often, then runs row-count and spot-check queries to
confirm everything loaded correctly.

Think of SQLite as a mini spreadsheet program that lives inside a single file
and understands SQL.  We use it so that 03_analyze.py can run proper SQL
queries instead of complicated pandas code.

Tables created:
  county_permits      — year, county, issued, refused, withdrawn
  sector_permits      — year, sector, issued
  nationality_permits — year, nationality, issued, refused, withdrawn
  visa_decisions      — year, nationality, received, granted, refused
                        (long-term visa applications only — student, employment,
                        graduate visas; short-term/tourist excluded by allow list)

Run from project root:
  Terminal : python src/02_build_sqlite.py
  Jupyter  : %run src/02_build_sqlite.py
"""

import shutil          # used to copy the finished DB file into the workspace
import sqlite3         # Python's built-in SQLite library — no install needed
import tempfile        # gives us the right temp folder for any OS (Mac/Windows/Linux)
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
CLEANED_DIR = Path("data/cleaned")   # where 01_clean_data.py wrote our CSVs
DB_PATH     = Path("data/employment.db")  # final destination in the workspace

# WHY a scratch location?
# SQLite needs to create and delete a temporary "journal" file while it writes.
# Our workspace folder restricts file deletion, which breaks SQLite's write process.
# Solution: build the database in the OS temp folder (which has no restrictions),
# then copy the finished file into the workspace at the end.
# tempfile.gettempdir() returns the right temp folder on any OS:
#   Mac/Linux → /tmp        Windows → C:\Users\<you>\AppData\Local\Temp
SCRATCH_DB = Path(tempfile.gettempdir()) / "employment.db"


def load_table(conn: sqlite3.Connection, csv_path: Path, table_name: str) -> int:
    """
    Read one CSV file into a pandas DataFrame, then write it to a SQLite table.

    Think of this like importing a spreadsheet sheet into a database table.

    'if_exists=replace' means: if the table already exists from a previous run,
    wipe it and start fresh.  This way re-running the script always gives clean data.

    Returns the number of rows written, so we can print a confirmation.
    """
    df = pd.read_csv(csv_path)

    # to_sql writes the whole DataFrame to SQLite in one shot.
    # index=False means we don't want pandas' row numbers (0, 1, 2…) as a column.
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    return len(df)


def add_indexes(conn: sqlite3.Connection) -> None:
    """
    Create indexes on the columns we filter and group by most often.

    What is an index?  Think of it like the index at the back of a book.
    Without it, a query like WHERE year = 2024 has to read every single row.
    With an index on 'year', SQLite jumps straight to the right rows — much faster.

    'CREATE INDEX IF NOT EXISTS' is safe to re-run: SQLite ignores the command
    if the index already exists, so no duplicates are created.
    """
    statements = [
        # county_permits — we filter by year and group/sort by county name
        "CREATE INDEX IF NOT EXISTS idx_county_year ON county_permits(year)",
        "CREATE INDEX IF NOT EXISTS idx_county_name ON county_permits(county)",

        # sector_permits — filtered by year; sector name used for lookups
        "CREATE INDEX IF NOT EXISTS idx_sector_year ON sector_permits(year)",
        "CREATE INDEX IF NOT EXISTS idx_sector_name ON sector_permits(sector)",

        # nationality_permits — same pattern
        "CREATE INDEX IF NOT EXISTS idx_nat_year    ON nationality_permits(year)",
        "CREATE INDEX IF NOT EXISTS idx_nat_name    ON nationality_permits(nationality)",

        # visa_decisions — filtered by year; nationality used for lookups
        "CREATE INDEX IF NOT EXISTS idx_visa_year   ON visa_decisions(year)",
        "CREATE INDEX IF NOT EXISTS idx_visa_nat    ON visa_decisions(nationality)",
    ]
    for sql in statements:
        conn.execute(sql)
    conn.commit()


def run_checks(conn: sqlite3.Connection) -> None:
    """
    Run a set of simple SQL queries and print the results so we can eyeball
    the data immediately after loading — no need to open a separate database tool.

    This is like a quick sanity check: if the numbers look wildly wrong,
    something went wrong upstream in 01_clean_data.py.
    """
    checks = [

        # ── How many rows did each table get? ─────────────────────────────────
        # UNION ALL stacks multiple SELECT results into one table.
        # This is just a quick count to confirm all four tables loaded.
        ("Row counts — all four tables",
         """
         SELECT 'county_permits'      AS tbl, COUNT(*) AS rows FROM county_permits
         UNION ALL
         SELECT 'sector_permits',              COUNT(*)         FROM sector_permits
         UNION ALL
         SELECT 'nationality_permits',         COUNT(*)         FROM nationality_permits
         UNION ALL
         SELECT 'visa_decisions',              COUNT(*)         FROM visa_decisions
         """),

        # ── What years are in the county table? ───────────────────────────────
        # MIN and MAX give the first and last year; COUNT(DISTINCT year) counts
        # how many unique years we have (should be 11 for 2015–2025).
        ("Year range (county_permits)",
         "SELECT MIN(year), MAX(year), COUNT(DISTINCT year) AS num_years FROM county_permits"),

        # ── Top 5 counties in the most recent full year ───────────────────────
        ("Top 5 counties by permits issued (2024)",
         """
         SELECT county, issued
         FROM   county_permits
         WHERE  year = 2024
         ORDER  BY issued DESC
         LIMIT  5
         """),

        # ── National total per year ────────────────────────────────────────────
        # SUM(issued) adds up all county values for each year.
        # This tells us how many permits were issued across all of Ireland each year.
        ("Annual total permits issued (all ROI counties)",
         """
         SELECT year, SUM(issued) AS total_issued
         FROM   county_permits
         GROUP  BY year
         ORDER  BY year
         """),

        # ── Top 5 sectors in 2024 ─────────────────────────────────────────────
        ("Top 5 sectors in 2024",
         """
         SELECT sector, issued
         FROM   sector_permits
         WHERE  year = 2024
         ORDER  BY issued DESC
         LIMIT  5
         """),

        # ── Top 5 nationalities by long-term visas granted (2024) ─────────────
        # NULLIF(received, 0) prevents a divide-by-zero error.
        # NULLIF returns NULL when received = 0, and NULL / anything = NULL in SQL
        # (rather than crashing with an error).
        ("Top 5 nationalities by long-term visas granted (2024)",
         """
         SELECT nationality, received, granted, refused,
                ROUND(100.0 * granted / NULLIF(received, 0), 1) AS grant_rate_pct
         FROM   visa_decisions
         WHERE  year = 2024
         ORDER  BY granted DESC
         LIMIT  5
         """),
    ]

    for title, sql in checks:
        print(f"\n  ┌─ {title}")
        df = pd.read_sql_query(sql, conn)
        for line in df.to_string(index=False).splitlines():
            print(f"  │  {line}")
        print(f"  └{'─' * (len(title) + 4)}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  02_build_sqlite.py  —  Load cleaned CSVs into SQLite")
    print("=" * 60)

    # Remove any leftover scratch DB from a previous run so we always start clean
    if SCRATCH_DB.exists():
        SCRATCH_DB.unlink()

    # Open (create) a new SQLite database at the scratch location
    conn = sqlite3.connect(SCRATCH_DB)
    print(f"\n  Building database in scratch: {SCRATCH_DB}\n")

    # ── Load all four tables ──────────────────────────────────────────────────
    # Each tuple is (csv filename, target table name in SQLite).
    # We loop so that adding a new dataset only requires one new line here.
    tables = [
        ("county_permits.csv",      "county_permits"),
        ("sector_permits.csv",      "sector_permits"),
        ("nationality_permits.csv", "nationality_permits"),
        ("visa_decisions.csv",      "visa_decisions"),
    ]

    for fname, tname in tables:
        path = CLEANED_DIR / fname
        n = load_table(conn, path, tname)
        print(f"  ✓ Loaded {n:>6,} rows  →  table '{tname}'")

    conn.commit()   # commit = permanently save the writes to the file

    # ── Add indexes for faster queries ────────────────────────────────────────
    print("\n  Adding indexes...")
    add_indexes(conn)
    print("  ✓ Indexes created on year, county, sector, nationality")

    # ── Run validation queries ────────────────────────────────────────────────
    print("\n── Validation queries ──────────────────────────────────────")
    run_checks(conn)

    conn.close()   # always close the connection when done

    # Copy the finished database from scratch into the workspace folder.
    # shutil.copy2 preserves file metadata (timestamps etc.) — like cp -p on Linux.
    shutil.copy2(SCRATCH_DB, DB_PATH)
    print(f"\n  Copied to workspace: {DB_PATH}  ({DB_PATH.stat().st_size / 1024:.0f} KB)")
    print("  Done. ✓")
