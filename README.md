# 🇮🇪 Ireland Work Permits — Interactive County Map

I built this project to explore a question I kept wondering about: *where exactly in Ireland are international workers going, and has that changed over time?*

Using ten years of official government data (2015–2025), I built a full data pipeline — from messy Excel files to a self-contained interactive map you can open in any browser. No server required, no login, just open the HTML file and explore.

---

## What it shows

The map lets you pick any year range with a two-handle slider and see:
- Which counties issued the most work permits (and how much they've grown)
- Which sectors are driving international hiring nationally
- How the picture changed before, during, and after COVID

A few things that surprised me in the data:
- **Dublin takes ~40% of all permits every year** — but Kildare and Cork have grown the fastest since 2015, suggesting the tech sector is spreading out
- **IT and Communication** is by far the largest sector, followed by Healthcare
- **India and Brazil** are consistently the top two source nationalities
- **2020 saw a sharp drop** (COVID), but by 2022 permits had not just recovered — they surpassed pre-pandemic levels

---

## How it was built

The pipeline runs in four steps:

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `01_clean_data.py` | Reads raw Excel/CSV files, handles inconsistent layouts across years, outputs tidy CSVs |
| 2 | `02_build_sqlite.py` | Loads the CSVs into a SQLite database for proper SQL querying |
| 3 | `03_analyze.py` | Runs the analysis — summary tables and interactive Plotly charts |
| 4 | `04_build_map.py` | Generates the self-contained interactive choropleth map |

**Tools used:** Python, pandas, SQLite, Plotly, Leaflet.js, noUiSlider, Chart.js

---

## Running it yourself

```bash
# Install dependencies
pip install pandas plotly openpyxl jupyter

# Run the pipeline (from the project root)
python src/01_clean_data.py
python src/02_build_sqlite.py
python src/03_analyze.py
python src/04_build_map.py

# Open the map
open output/map/ireland_employment_map.html
```

---

## Data sources

- **DETE** (Dept. of Enterprise, Trade and Employment) — work permit statistics by county, sector, and nationality, 2015–2025
- **ISD** (Irish Immigration Service Delivery) — long-term visa decisions by nationality, 2017–2026
- **simplemaps.com** — Ireland county boundary GeoJSON

A few things worth knowing about the data: sector names changed around 2020 so pre-2020 sector trends aren't directly comparable; 2025 is a partial year; and small visa counts are suppressed with `*` in the source data (treated as missing, not zero).

---

## Author

**Aashie Kodali** — built in 2026 as a portfolio project while learning Python and data analysis.

---

*MIT License*
