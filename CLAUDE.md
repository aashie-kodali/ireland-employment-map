# CLAUDE.md — Ireland Employment Map

Guidance for Claude when working in this repository. Keep responses concise, step-by-step, and practical.

## Project goal
Build a portfolio-quality data project that visualises employment statistics across Ireland (by county / region) using open CSO (Central Statistics Office) data. Produce clean CSVs, a SQLite database, summary tables, charts, and an interactive choropleth map.

## Stack
- **Python** (pandas, SQLite via `sqlite3`, Plotly, GeoPandas where needed)
- **SQL** (SQLite — queries live in `src/` as `.sql` or embedded in Python)
- **GitHub** for version control and portfolio presentation
- **Jupyter** for exploration; **Claude Code / CLI** for running scripts

## Folder structure
```
Ireland Employment Map/
├── CLAUDE.md           # This file
├── README.md           # Portfolio overview
├── .gitignore
├── src/                # Python scripts (numbered in run order)
│   ├── 01_clean_data.py
│   ├── 02_build_sqlite.py
│   ├── 03_analyze.py
│   └── 04_build_map.py
├── data/
│   ├── raw/            # Original source files (never edited)
│   ├── cleaned/        # Converted/cleaned CSVs
│   └── geo/            # County/region boundary files (GeoJSON, shapefiles)
└── output/
    ├── charts/         # PNG / HTML charts
    ├── tables/         # Summary CSVs
    └── map/            # Final interactive HTML map
```

## Working rules for Claude
1. **Always name the file** being created or edited (e.g. "edit `src/01_clean_data.py`").
2. **Copy-paste-ready code** — include imports, paths, and a `if __name__ == "__main__":` block where relevant.
3. **Comment descriptively** — explain *why*, not just *what*.
4. **Never overwrite `data/raw/`** — treat it as read-only.
5. **Prefer pandas + SQLite + Plotly** over heavier alternatives.
6. **Flag assumptions and limitations** in plain English after any analysis.
7. **Summarise findings** at the end of any analytical step in 2–4 sentences.
8. **Show how to run** each script both in the terminal (`python src/01_clean_data.py`) and in a Jupyter cell (`%run src/01_clean_data.py`).
9. **Small, reviewable steps** — do not skip intermediate checks.
10. **Tone**: supportive and educational; explain the reasoning behind choices.

## Typical workflow
1. Drop source files into `data/raw/`.
2. Run `src/01_clean_data.py` → produces tidy CSVs in `data/cleaned/`.
3. Run `src/02_build_sqlite.py` → loads cleaned CSVs into `data/employment.db`.
4. Run `src/03_analyze.py` → writes summary tables to `output/tables/` and charts to `output/charts/`.
5. Run `src/04_build_map.py` → writes interactive choropleth to `output/map/ireland_employment_map.html`.

## Running commands
- Terminal: `python src/<script>.py`
- Jupyter: `%run src/<script>.py` (from the project root)
- Install deps: `pip install pandas plotly geopandas jupyter`
