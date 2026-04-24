"""
src/03_analyze.py
=================
Queries the cleaned CSVs (loaded into an in-memory SQLite database) and produces:
  output/tables/  — CSV summary tables for use in the map and further analysis
  output/charts/  — Interactive Plotly HTML charts (open in any browser)

Think of this script as the "number crunching" step.  It takes the clean data
from step 1, asks SQL questions about it, and saves the answers as tables and charts.

Analyses performed:
  1. National trend        — total permits issued per year across all ROI counties
  2. County rankings       — top counties by year; each county's % share of national total
  3. County growth         — absolute and % change from 2015 to most recent year (2024)
  4. Sector breakdown      — top sectors by year (2020–2025 only, where names are consistent)
  5. Nationality breakdown — top source nationalities by year (work permits)
  6. Visa decisions        — long-term visa applications: trends, approval rates, top
                             nationalities (student / employment / graduate visas only;
                             short-term / tourist visas excluded by allow list in 01_clean_data.py)

LIMITATION: DETE renamed several sectors around 2020 (e.g. "Agriculture & Fisheries"
became "A - Agriculture, Forestry & Fishing"), so sector trend lines that cross
the 2019/2020 boundary are not directly comparable and are excluded from charts.

Run from project root:
  Terminal : python src/03_analyze.py
  Jupyter  : %run src/03_analyze.py
"""

import sqlite3
from pathlib import Path

import pandas as pd

# Plotly draws interactive HTML charts.  If it isn't installed the script still
# runs and saves all CSV tables; chart files are simply skipped with a warning.
# Install with:  pip install plotly
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("  [WARNING] plotly not found — tables will be saved but charts skipped.")
    print("  Install with: pip install plotly\n")

# ── Paths ─────────────────────────────────────────────────────────────────────
CLEANED_DIR = Path("data/cleaned")   # tidy CSVs produced by 01_clean_data.py
DB_PATH     = Path("data/employment.db")
TABLES_DIR  = Path("output/tables")  # where we save CSV summary tables
CHARTS_DIR  = Path("output/charts")  # where we save interactive Plotly charts

# Create the output folders if they don't exist yet
TABLES_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Colour palette ────────────────────────────────────────────────────────────
# Using Ireland's flag colours throughout keeps the charts visually consistent.
IRELAND_GREEN  = "#169B62"
IRELAND_ORANGE = "#FF883E"
PLOTLY_TEMPLATE = "plotly_white"   # clean white background for all charts


# ── Database helper ───────────────────────────────────────────────────────────

def query(sql: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Run a SQL query and return the results as a pandas DataFrame.

    This is just a thin wrapper so we can write:
        df = query("SELECT ...", conn)
    instead of:
        df = pd.read_sql_query("SELECT ...", conn)
    — a bit less to type each time.
    """
    return pd.read_sql_query(sql, conn)


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 1 — NATIONAL TREND
# ═══════════════════════════════════════════════════════════════════════════════

def analysis_national_trend(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Total permits issued across all 26 ROI counties, by year.
    Also calculates year-on-year (YoY) change and % change.

    Year-on-year means: how did this year compare to the year before?
    The first year (2015) will always show NaN for YoY — that is correct,
    because there is no prior year to compare it against.
    """
    df = query("""
        SELECT
            year,
            SUM(issued)    AS total_issued,
            SUM(refused)   AS total_refused,
            SUM(withdrawn) AS total_withdrawn
        FROM   county_permits
        GROUP  BY year
        ORDER  BY year
    """, conn)

    # .diff() calculates the difference between each row and the one above it.
    # So for 2016 it gives (2016 total − 2015 total).  The 2015 row gets NaN.
    df["yoy_change"] = df["total_issued"].diff()

    # .pct_change() does the same but as a fraction — multiply by 100 for %.
    df["yoy_pct"] = (df["total_issued"].pct_change() * 100).round(1)

    return df


def chart_national_trend(df: pd.DataFrame):
    """
    Dual-axis chart: total permits issued (filled area) on the left axis,
    year-on-year % change (bar) on the right axis.
    This makes it easy to see both the volume and the rate of change together.
    """
    # Guard: if Plotly isn't installed, exit early and return nothing.
    # The caller checks for None and skips saving.
    if not PLOTLY_AVAILABLE:
        return None

    # make_subplots with secondary_y=True creates a chart with two y-axes:
    # left axis for the main values, right axis for the % change overlay.
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # ── Left axis: total permits issued (filled area line) ────────────────────
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["total_issued"],
        name="Permits issued",
        mode="lines+markers",
        line=dict(color=IRELAND_GREEN, width=3),
        marker=dict(size=8),
        fill="tozeroy",                          # fills area down to zero
        fillcolor="rgba(22,155,98,0.15)",        # light green, semi-transparent
    ), secondary_y=False)

    # ── Right axis: YoY % change (bar chart) ─────────────────────────────────
    # Positive years get green bars, negative years (e.g. COVID dip) get orange.
    fig.add_trace(go.Bar(
        x=df["year"], y=df["yoy_pct"],
        name="YoY % change",
        marker_color=[IRELAND_GREEN if v >= 0 else IRELAND_ORANGE
                      for v in df["yoy_pct"].fillna(0)],
        opacity=0.6,
    ), secondary_y=True)

    fig.update_layout(
        title="Ireland Work Permits Issued — National Total (2015–2025)",
        xaxis=dict(title="Year", tickmode="linear", dtick=1),
        yaxis=dict(title="Total permits issued"),
        yaxis2=dict(title="Year-on-year % change", ticksuffix="%"),
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",   # shows all values for a year when you hover
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 2 — COUNTY RANKINGS & SHARE
# ═══════════════════════════════════════════════════════════════════════════════

def analysis_county_share(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    For every (year, county) pair: permits issued and that county's % share
    of the national total for that year.

    The SQL JOIN here works like a VLOOKUP in Excel:
      - The inner query calculates the national total for each year.
      - We join that back onto every county row so each row knows the national total.
      - Then we can divide county issued / national total to get the share %.
    """
    df = query("""
        SELECT
            c.year,
            c.county,
            c.issued,
            c.refused,
            c.withdrawn,
            ROUND(100.0 * c.issued / t.total, 2) AS pct_share
        FROM county_permits c
        JOIN (
            -- Sub-query: one row per year, showing the national total
            SELECT year, SUM(issued) AS total
            FROM   county_permits
            GROUP  BY year
        ) t ON c.year = t.year    -- match each county row to its year's total
        ORDER BY c.year, c.issued DESC
    """, conn)
    return df


def chart_county_top10(df: pd.DataFrame, year: int = 2024):
    """
    Horizontal bar chart: top 10 counties by permits issued for a given year.
    The text on each bar shows the county's % share of the national total.
    """
    if not PLOTLY_AVAILABLE:
        return None

    top10 = (
        df[df["year"] == year]
        .nlargest(10, "issued")
        .sort_values("issued")     # ascending so the largest bar appears at the top
    )

    fig = px.bar(
        top10, x="issued", y="county",
        orientation="h",
        text=top10["pct_share"].apply(lambda v: f"{v:.1f}%"),  # label = share %
        color="issued",
        color_continuous_scale=[[0, "#d4f0e3"], [1, IRELAND_GREEN]],
        labels={"issued": "Permits issued", "county": "County"},
        title=f"Top 10 Counties by Work Permits Issued ({year})",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Permits issued",
        yaxis_title="",
        uniformtext_minsize=10,
    )
    return fig


def chart_county_trends(df: pd.DataFrame, top_n: int = 6):
    """
    Line chart: how the top N counties changed over time (2015–2025).
    Counties are ranked by their 2024 totals so we always see the most
    relevant players.
    """
    if not PLOTLY_AVAILABLE:
        return None

    # Find which counties had the highest totals in 2024
    top_counties = (
        df[df["year"] == 2024]
        .nlargest(top_n, "issued")["county"]
        .tolist()
    )

    # Filter down to only those counties across all years
    filtered = df[df["county"].isin(top_counties)]

    fig = px.line(
        filtered, x="year", y="issued", color="county",
        markers=True,
        labels={"issued": "Permits issued", "year": "Year", "county": "County"},
        title=f"Work Permit Trends — Top {top_n} Counties (2015–2025)",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        xaxis=dict(tickmode="linear", dtick=1),
        hovermode="x unified",
        legend_title="County",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 3 — COUNTY GROWTH
# ═══════════════════════════════════════════════════════════════════════════════

def analysis_county_growth(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    For each county: permits in 2015, permits in 2024, and the % change.
    We use 2024 as the endpoint because 2025 is a partial year (not complete yet).
    Counties that don't appear in both 2015 AND 2024 are excluded from this table
    — they simply have no valid comparison to make.

    The SQL self-join works like this:
      - Query 'a' gets every county's 2015 number.
      - Query 'b' gets every county's 2024 number.
      - JOIN ON a.county = b.county matches them up by county name.
      - Only counties that appear in BOTH queries survive the JOIN.
    """
    df = query("""
        SELECT
            a.county,
            a.issued  AS issued_2015,
            b.issued  AS issued_2024,
            (b.issued - a.issued)                              AS abs_change,
            ROUND(100.0 * (b.issued - a.issued) / a.issued, 1) AS pct_change
        FROM
            (SELECT county, issued FROM county_permits WHERE year = 2015) a
        JOIN
            (SELECT county, issued FROM county_permits WHERE year = 2024) b
            ON a.county = b.county    -- only keeps counties present in both years
        ORDER BY pct_change DESC      -- highest growers at the top
    """, conn)
    return df


def chart_county_growth(df: pd.DataFrame):
    """
    Horizontal bar chart: % growth per county from 2015 to 2024.
    Green bars = growth, orange bars = decline.
    A vertical dashed line at 0 makes it easy to see the dividing line.
    """
    if not PLOTLY_AVAILABLE:
        return None

    df_sorted = df.sort_values("pct_change")   # smallest at bottom, largest at top

    fig = px.bar(
        df_sorted, x="pct_change", y="county",
        orientation="h",
        color="pct_change",
        color_continuous_scale=[
            [0.0, IRELAND_ORANGE],    # negative end → orange
            [0.5, "#ffffcc"],         # midpoint (0%) → light yellow
            [1.0, IRELAND_GREEN],     # positive end → green
        ],
        labels={"pct_change": "% change", "county": "County"},
        title="Work Permit Growth by County (2015 → 2024)",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title="% change in permits issued",
        yaxis_title="",
    )
    # Vertical line at zero so it's immediately obvious which counties grew vs. shrank
    fig.add_vline(x=0, line_dash="dash", line_color="grey")
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 4 — SECTOR BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════════
# We use 2020–2025 only.  DETE renamed sectors around 2020, so older sector
# names (e.g. "Agriculture & Fisheries") can't be reliably matched to the new
# NACE-style names (e.g. "A - Agriculture, Forestry & Fishing").
# Mixing the two would give wrong trend lines, so we exclude pre-2020 sector data.

def analysis_sector(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Permits issued per sector per year, 2020–2025 only.
    Sorted by issued descending so the top sectors come first in the output.
    """
    df = query("""
        SELECT year, sector, issued
        FROM   sector_permits
        WHERE  year >= 2020
        ORDER  BY year, issued DESC
    """, conn)
    return df


def chart_sector_top10(df: pd.DataFrame, year: int = 2024):
    """
    Horizontal bar chart: top 10 sectors for a given year.
    Long sector names (which include a letter code like "A - ") are shortened
    for readability on the axis.
    """
    if not PLOTLY_AVAILABLE:
        return None

    top10 = (
        df[df["year"] == year]
        .nlargest(10, "issued")
        .sort_values("issued")
    )

    # Strip the NACE letter prefix (e.g. "J - " from "J - Information & Communication")
    # so axis labels are shorter and easier to read.
    top10 = top10.copy()
    top10["sector_short"] = top10["sector"].str.replace(
        r"^[A-Z]\s*-\s*", "", regex=True
    ).str.strip()

    fig = px.bar(
        top10, x="issued", y="sector_short",
        orientation="h",
        color="issued",
        color_continuous_scale=[[0, "#d4f0e3"], [1, IRELAND_GREEN]],
        labels={"issued": "Permits issued", "sector_short": "Sector"},
        title=f"Top 10 Sectors by Work Permits Issued ({year})",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Permits issued",
        yaxis_title="",
    )
    return fig


def chart_sector_trends(df: pd.DataFrame, top_n: int = 6):
    """
    Line chart: how the top N sectors changed over time (2020–2025).
    Ranked by 2024 totals — same approach as the county trends chart.
    """
    if not PLOTLY_AVAILABLE:
        return None

    # Identify the top N sectors by 2024 totals
    top_sectors = (
        df[df["year"] == 2024]
        .nlargest(top_n, "issued")["sector"]
        .tolist()
    )

    filtered = df[df["sector"].isin(top_sectors)].copy()

    # Shorten names for the legend (same strip as above)
    filtered["sector_short"] = filtered["sector"].str.replace(
        r"^[A-Z]\s*-\s*", "", regex=True
    ).str.strip()

    fig = px.line(
        filtered, x="year", y="issued", color="sector_short",
        markers=True,
        labels={"issued": "Permits issued", "year": "Year", "sector_short": "Sector"},
        title=f"Work Permit Trends — Top {top_n} Sectors (2020–2025)",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        xaxis=dict(tickmode="linear", dtick=1),
        hovermode="x unified",
        legend_title="Sector",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 5 — NATIONALITY BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════════

def analysis_nationality(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Top 15 source nationalities by permits issued in the most recent full year (2024).
    We limit to 15 because beyond that the numbers get small and the chart gets cluttered.
    """
    df = query("""
        SELECT nationality, issued, refused, withdrawn
        FROM   nationality_permits
        WHERE  year = 2024
        ORDER  BY issued DESC
        LIMIT  15
    """, conn)
    return df


def chart_nationality_top15(df: pd.DataFrame):
    """
    Horizontal bar chart: top 15 source nationalities in 2024.
    The colour gradient makes it easy to compare volumes at a glance.
    """
    if not PLOTLY_AVAILABLE:
        return None

    df_sorted = df.sort_values("issued")    # ascending so largest is at the top

    fig = px.bar(
        df_sorted, x="issued", y="nationality",
        orientation="h",
        color="issued",
        color_continuous_scale=[[0, "#d4f0e3"], [1, IRELAND_GREEN]],
        labels={"issued": "Permits issued", "nationality": "Nationality"},
        title="Top 15 Source Nationalities — Work Permits Issued (2024)",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Permits issued",
        yaxis_title="",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS 6 — VISA DECISIONS (LONG-TERM ONLY)
# ═══════════════════════════════════════════════════════════════════════════════
# Source: ISD visa decisions CSV, filtered to 'long term visa applications'.
# This covers student, employment, and graduate visas.  Short-term / tourist
# visas were excluded at the cleaning step (01_clean_data.py) via an allow list.
#
# Three sub-analyses:
#   a) National trend  — total applications received, granted, refused per year
#   b) Approval rates  — top nationalities by applications in 2024, coloured by
#                        grant rate % (shows which groups face higher refusal rates)
#   c) Top nationalities — biggest receivers of long-term visas in 2024

def analysis_visa_trend(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Annual totals for long-term visa applications: received, granted, refused.
    Also calculates grant rate (%) and refusal rate (%) for each year.
    Excludes 2026 as it is a partial year (data collection still ongoing).
    """
    df = query("""
        SELECT
            year,
            SUM(received) AS total_received,
            SUM(granted)  AS total_granted,
            SUM(refused)  AS total_refused
        FROM   visa_decisions
        WHERE  year < 2026          -- exclude 2026 partial year
        GROUP  BY year
        ORDER  BY year
    """, conn)

    # Calculate grant and refusal rates safely.
    # We use .where(condition, other=None) to avoid dividing by zero:
    # if total_received is 0 or NaN for a year, the rate is set to NaN instead
    # of crashing or showing infinity.
    total_received = df["total_received"]
    has_data = total_received.notna() & (total_received > 0)

    df["grant_rate_pct"] = (
        (df["total_granted"] / total_received * 100)
        .where(has_data)     # sets rows where has_data=False to NaN
        .round(1)
    )
    df["refusal_rate_pct"] = (
        (df["total_refused"] / total_received * 100)
        .where(has_data)
        .round(1)
    )
    return df


def analysis_visa_approval_rates(conn: sqlite3.Connection, year: int = 2024) -> pd.DataFrame:
    """
    For the top 20 nationalities by applications received in a given year:
    show received, granted, refused, and the grant rate %.
    NULLIF(received, 0) in SQL prevents a divide-by-zero error — it turns
    any zero into NULL, and NULL divided by anything is NULL (not a crash).
    """
    df = query(f"""
        SELECT
            nationality,
            received,
            granted,
            refused,
            ROUND(100.0 * granted / NULLIF(received, 0), 1) AS grant_rate_pct
        FROM   visa_decisions
        WHERE  year = {year}
          AND  received IS NOT NULL
        ORDER  BY received DESC
        LIMIT  20
    """, conn)
    return df


def analysis_visa_top_nationalities(conn: sqlite3.Connection, year: int = 2024) -> pd.DataFrame:
    """
    Top 15 nationalities by long-term visas granted in a given year.
    """
    df = query(f"""
        SELECT nationality, received, granted, refused,
               ROUND(100.0 * granted / NULLIF(received, 0), 1) AS grant_rate_pct
        FROM   visa_decisions
        WHERE  year = {year}
          AND  granted IS NOT NULL
        ORDER  BY granted DESC
        LIMIT  15
    """, conn)
    return df


def chart_visa_trend(df: pd.DataFrame):
    """
    Dual-axis chart: granted and refused volumes (stacked bars) on the left axis,
    grant rate % (dotted line) on the right axis.
    The stacked bars show volume; the line shows whether the approval rate is rising or falling.
    """
    if not PLOTLY_AVAILABLE:
        return None

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Stacked bars: granted (green) + refused (orange) stacked on top of each other
    fig.add_trace(go.Bar(
        x=df["year"], y=df["total_granted"],
        name="Granted",
        marker_color=IRELAND_GREEN,
        opacity=0.85,
    ), secondary_y=False)

    fig.add_trace(go.Bar(
        x=df["year"], y=df["total_refused"],
        name="Refused",
        marker_color=IRELAND_ORANGE,
        opacity=0.85,
    ), secondary_y=False)

    # Dotted line: grant rate % on the right axis
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["grant_rate_pct"],
        name="Grant rate %",
        mode="lines+markers",
        line=dict(color="#333333", width=2, dash="dot"),
        marker=dict(size=7),
    ), secondary_y=True)

    fig.update_layout(
        barmode="stack",
        title="Ireland Long-Term Visa Decisions — Annual Trend (2017–2025)",
        xaxis=dict(title="Year", tickmode="linear", dtick=1),
        yaxis=dict(title="Visa applications"),
        yaxis2=dict(title="Grant rate %", ticksuffix="%", range=[0, 100]),
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def chart_visa_approval_rates(df: pd.DataFrame, year: int = 2024):
    """
    Horizontal bar chart: top 20 nationalities by applications, coloured by
    grant rate %.  Green = high approval, orange = high refusal.
    This immediately reveals which nationalities face higher refusal rates
    despite high application volumes — a useful pattern for analysis.
    """
    if not PLOTLY_AVAILABLE:
        return None

    df_sorted = df.sort_values("received")    # ascending so largest is at top

    fig = px.bar(
        df_sorted, x="received", y="nationality",
        orientation="h",
        color="grant_rate_pct",
        color_continuous_scale=[
            [0.0, IRELAND_ORANGE],   # 0% grant rate → orange
            [0.5, "#ffffcc"],        # 50% → neutral yellow
            [1.0, IRELAND_GREEN],    # 100% → green
        ],
        range_color=[0, 100],
        labels={"received": "Applications received", "nationality": "Nationality",
                "grant_rate_pct": "Grant rate %"},
        title=f"Long-Term Visa Applications & Approval Rates by Nationality ({year})",
        template=PLOTLY_TEMPLATE,
        hover_data={"granted": True, "refused": True, "grant_rate_pct": True},
    )
    fig.update_layout(
        xaxis_title="Applications received",
        yaxis_title="",
        coloraxis_colorbar=dict(title="Grant rate %", ticksuffix="%"),
    )
    return fig


def chart_visa_top15_granted(df: pd.DataFrame, year: int = 2024):
    """
    Horizontal bar chart: top 15 nationalities by long-term visas granted.
    The text label on each bar shows the grant rate % for extra context.
    """
    if not PLOTLY_AVAILABLE:
        return None

    df_sorted = df.sort_values("granted")

    fig = px.bar(
        df_sorted, x="granted", y="nationality",
        orientation="h",
        color="granted",
        color_continuous_scale=[[0, "#d4f0e3"], [1, IRELAND_GREEN]],
        text=df_sorted["grant_rate_pct"].apply(lambda v: f"{v}%" if pd.notna(v) else ""),
        labels={"granted": "Visas granted", "nationality": "Nationality"},
        title=f"Top 15 Nationalities — Long-Term Visas Granted ({year})",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Visas granted",
        yaxis_title="",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  03_analyze.py  —  Ireland Work Permit Analysis")
    print("=" * 60)

    # ── Load data into an in-memory SQLite database ───────────────────────────
    # WHY in-memory? Our workspace folder restricts file operations that SQLite needs.
    # An in-memory database (":memory:") lives entirely in RAM — no files created.
    # It is fast, disposable, and lets us write all the same SQL queries we would
    # use against a real database file.
    # Think of it as a temporary spreadsheet that disappears when the script ends.
    conn = sqlite3.connect(":memory:")
    for csv_file, table_name in [
        (CLEANED_DIR / "county_permits.csv",      "county_permits"),
        (CLEANED_DIR / "sector_permits.csv",      "sector_permits"),
        (CLEANED_DIR / "nationality_permits.csv", "nationality_permits"),
        (CLEANED_DIR / "visa_decisions.csv",      "visa_decisions"),
    ]:
        # read_csv → to_sql: reads the CSV into a DataFrame, then writes it into
        # the in-memory database as a table.  if_exists="replace" clears any
        # previous version of the table first.
        pd.read_csv(csv_file).to_sql(table_name, conn, if_exists="replace", index=False)
    print("  Loaded 4 tables into in-memory SQLite database.")

    def save_chart(fig, filename: str) -> None:
        """Write a Plotly figure to output/charts/.  Skip gracefully if Plotly is missing."""
        if fig is not None:
            # write_html saves the chart as a self-contained HTML file.
            # include_plotlyjs="cdn" means the chart loads Plotly from the internet
            # instead of bundling it — keeps the file size small.
            fig.write_html(CHARTS_DIR / filename, include_plotlyjs="cdn")
            print(f"    → chart saved: {filename}")
        else:
            print(f"    → chart skipped (plotly not installed): {filename}")

    # ── 1. National trend ─────────────────────────────────────────────────────
    print("\n── 1. National trend")
    national_df = analysis_national_trend(conn)
    national_df.to_csv(TABLES_DIR / "national_trend.csv", index=False)
    print(national_df[["year", "total_issued", "yoy_pct"]].to_string(index=False))
    save_chart(chart_national_trend(national_df), "01_national_trend.html")

    # ── 2. County rankings & share ────────────────────────────────────────────
    print("\n── 2. County share")
    county_df = analysis_county_share(conn)
    county_df.to_csv(TABLES_DIR / "county_share.csv", index=False)
    top5 = county_df[county_df["year"] == 2024].head(5)
    print(top5[["county", "issued", "pct_share"]].to_string(index=False))
    save_chart(chart_county_top10(county_df, year=2024), "02_county_top10_2024.html")
    save_chart(chart_county_trends(county_df, top_n=6),  "03_county_trends.html")

    # ── 3. County growth ──────────────────────────────────────────────────────
    print("\n── 3. County growth (2015 → 2024)")
    growth_df = analysis_county_growth(conn)
    growth_df.to_csv(TABLES_DIR / "county_growth.csv", index=False)
    print(growth_df.head(5)[["county", "issued_2015", "issued_2024",
                              "pct_change"]].to_string(index=False))
    save_chart(chart_county_growth(growth_df), "04_county_growth.html")

    # ── 4. Sector breakdown ───────────────────────────────────────────────────
    print("\n── 4. Sectors (2020–2025)")
    sector_df = analysis_sector(conn)
    sector_df.to_csv(TABLES_DIR / "sector_permits.csv", index=False)
    top5_sectors = sector_df[sector_df["year"] == 2024].head(5)
    print(top5_sectors[["sector", "issued"]].to_string(index=False))
    save_chart(chart_sector_top10(sector_df, year=2024), "05_sector_top10_2024.html")
    save_chart(chart_sector_trends(sector_df, top_n=6),  "06_sector_trends.html")

    # ── 5. Nationality breakdown ──────────────────────────────────────────────
    print("\n── 5. Nationalities (2024)")
    nat_df = analysis_nationality(conn)
    nat_df.to_csv(TABLES_DIR / "nationality_top15_2024.csv", index=False)
    print(nat_df[["nationality", "issued"]].head(5).to_string(index=False))
    save_chart(chart_nationality_top15(nat_df), "07_nationality_top15.html")

    # ── 6. Visa decisions (long-term: student / employment / graduate) ────────
    print("\n── 6. Visa decisions — long-term only (2017–2025)")
    print("   (short-term/tourist visas excluded via allow list)")

    visa_trend_df = analysis_visa_trend(conn)
    visa_trend_df.to_csv(TABLES_DIR / "visa_trend.csv", index=False)
    print(visa_trend_df[["year", "total_received", "total_granted",
                          "grant_rate_pct"]].to_string(index=False))
    save_chart(chart_visa_trend(visa_trend_df), "08_visa_trend.html")

    visa_rates_df = analysis_visa_approval_rates(conn, year=2024)
    visa_rates_df.to_csv(TABLES_DIR / "visa_approval_rates_2024.csv", index=False)
    print(f"\n  Top 5 by applications (2024):")
    print(visa_rates_df[["nationality", "received", "granted",
                          "grant_rate_pct"]].head(5).to_string(index=False))
    save_chart(chart_visa_approval_rates(visa_rates_df, year=2024),
               "09_visa_approval_rates_2024.html")

    visa_top_df = analysis_visa_top_nationalities(conn, year=2024)
    visa_top_df.to_csv(TABLES_DIR / "visa_top15_granted_2024.csv", index=False)
    save_chart(chart_visa_top15_granted(visa_top_df, year=2024),
               "10_visa_top15_granted_2024.html")

    conn.close()   # release the in-memory database

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n── Output files")
    print("\n  Tables (output/tables/):")
    for f in sorted(TABLES_DIR.iterdir()):
        print(f"    {f.name}")
    print("\n  Charts (output/charts/):")
    for f in sorted(CHARTS_DIR.iterdir()):
        print(f"    {f.name}")

    print("\n  Done. ✓")
