# Ireland Work Permits — Interactive County Map

An interactive, data-driven look at work permits across Ireland — built as a portfolio project using **Python**, **SQL**, and **Leaflet.js**.

## Overview
This project ingests official work permit and visa statistics from Ireland's Department of Enterprise, Trade and Employment (DETE) and the Irish Immigration Service Delivery (ISD), cleans and stores them in a SQLite database, and produces summary tables, charts, and an interactive **choropleth map** of permits issued by county.

## Key questions answered
- Which counties attract the most work permits — and how has that changed since 2015?
- Which sectors are driving Ireland's international hiring (IT, healthcare, engineering…)?
- Which nationalities receive the most work permits and long-term visas?
- Where are the biggest regional growth stories?

## Tech stack
| Layer | Tool |
|-------|------|
| Data wrangling | Python + pandas |
| Storage | SQLite (`sqlite3`) |
| Analysis | SQL + pandas |
| Charts | Plotly (interactive HTML) |
| Map | Leaflet.js choropleth (self-contained HTML) |
| Slider | noUiSlider (two-handle year range) |
| Geography | GeoJSON county boundaries (simplemaps.com) |
| Workflow | Python scripts + Jupyter |
| Version control | Git + GitHub |

## Folder structure
```
Ireland Employment Map/
├── src/                 # Python scripts — run in order: 01 → 04
│   ├── 01_clean_data.py      # Read raw Excel/CSV → tidy CSVs
│   ├── 02_build_sqlite.py    # Load CSVs → SQLite database
│   ├── 03_analyze.py         # SQL analysis → tables + Plotly charts
│   └── 04_build_map.py       # Generate interactive choropleth map
├── data/
│   ├── raw/             # Original source files (never edited)
│   ├── cleaned/         # Tidy CSVs produced by 01_clean_data.py
│   └── geo/             # Ireland county GeoJSON boundary file
└── output/
    ├── charts/          # Interactive Plotly HTML charts
    ├── tables/          # Summary CSV tables
    └── map/             # Final interactive map (ireland_employment_map.html)
```

## How to run
```bash
# 1. Install dependencies
pip install pandas plotly openpyxl jupyter

# 2. Run the pipeline in order from the project root
python src/01_clean_data.py      # cleans raw data → data/cleaned/
python src/02_build_sqlite.py    # loads into SQLite → data/employment.db
python src/03_analyze.py         # analysis → output/tables/ + output/charts/
python src/04_build_map.py       # builds map → output/map/ireland_employment_map.html

# 3. Open the interactive map in your browser
open output/map/ireland_employment_map.html        # macOS
start output/map/ireland_employment_map.html       # Windows
```

Or run any script inside Jupyter:
```python
%run src/01_clean_data.py
```

## Data sources
- **DETE (Dept. of Enterprise, Trade and Employment)** — work permit statistics by county, sector, and nationality, 2015–2025
- **ISD (Irish Immigration Service Delivery)** — visa decisions by nationality and year, 2017–2026 (long-term visas only: student, employment, graduate)
- **simplemaps.com** — Ireland county boundary GeoJSON

## Key findings
- **Dublin dominates** with ~38–40% of all work permits issued nationally, every year.
- **Information & Communication** is consistently the top sector (IT, tech companies).
- **India and Brazil** are the two largest source nationalities for work permits.
- **COVID-19** caused a sharp dip in 2020, but permits recovered strongly by 2022.
- **Kildare and Cork** show the strongest growth since 2015, reflecting tech sector expansion outside Dublin.

## Assumptions & limitations
- `issued` in 2015–2019 files = "Total" (New + Renewal); in 2020–2025 = "Issued" column. Both represent the same concept.
- Sector data is only available in a consistent format from 2020 onwards — DETE renamed sectors around that year.
- Northern Ireland counties appear in some older files but are excluded from the map (ROI only).
- 2025 data is a partial year and may undercount compared to prior full years.
- Visa data uses `*` to suppress small counts (confidentiality) — these are treated as missing, not zero.

## Author
**Aashie Kodali** — portfolio project, 2026.

## License
MIT
