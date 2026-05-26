# Interactive County Map for Work Permits in Ireland

**[Live demo → hosted on AWS Amplify]()**

[![Ireland Employment Map](docs/Map%20Screenshot.png)]()

I built this project to answer the question of where international workers are being hired over time in Ireland and which sectors are hiring, to effectively target my job search efforts. This map helps you navigate hiring trends by county and sector over time.

Using eleven years of official government data (2015–2025), I built a full data pipeline from messy Excel files to a self-contained interactive map you can open in any browser. No server required, no login, just open the HTML file and explore.

---

## What it shows

The map lets you pick any year range with a two-handle slider and see which counties issued the most work permits, which sectors are driving international hiring nationally, and how the picture shifted over time.

An **employer intelligence panel** sits alongside the map. It shows the top 20 employers by permits issued, the fastest-growing employers, and companies that appeared as new entrants, all filterable by NACE sector. Sector tags cover 94.9% of all permits issued across the eleven-year period.

A few things that surprised me in the data:

**Healthcare has consistently led, with one exception where IT overtook the board.** Health & Social Work has been the top sector for work permits every year since 2020 except for 2022, when a surge in tech hiring briefly pushed Information & Communication to the top (10,800 vs 9,800). By 2023, the pattern had reasserted itself, and by 2024 the gap had widened again. Healthcare issued nearly twice as many permits as IT (~12,500 vs ~6,800).

**COVID barely registered.** Permits in 2019, 2020, and 2021 were virtually identical — within 1% of each other. The real shock came in 2022, when national totals more than doubled in a single year, going from around 16,000 to nearly 40,000. Whatever the pandemic froze, it unfroze all at once.

**Agriculture more than doubled from 2023 to 2024**, rebounding strongly after an unusual dip the year before. The sector had been growing steadily since 2015, collapsed in 2023 for reasons that are not obvious from the data alone, then came back sharply.

**Meath and Monaghan stand out** as counties with meaningful absolute growth that rarely make the headlines. Meath went from 72 permits in 2015 to over 1,500 in 2024. Monaghan went from 20 to over 500 in the same period. Neither is a tech hub, which makes the growth more interesting.

**2025 shows a broad cooldown across almost every county and sector.** National permits fell from around 39,000 in 2024 to 31,000 in 2025, almost exactly back to 2023 levels, suggesting 2024 was the outlier peak rather than the new normal. Only five counties grew: Kilkenny (+34%), Laois (+19%), Donegal (+14%), Wicklow (+5%), and Leitrim (+48% but on a very small base).

---

## How it was built

The pipeline runs in five steps:

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `src/01_clean_data.py` | Reads raw Excel/CSV files, handles inconsistent layouts across years, outputs tidy CSVs for county, sector, nationality, and visa data |
| 2 | `src/05_clean_companies.py` | Parses eleven years of company-level permit files, normalises employer names, joins sector tags |
| 3 | `src/02_build_sqlite.py` | Loads all five CSVs into a SQLite database with indexes for fast querying |
| 4 | `src/03_analyze.py` | Runs summary analysis — tables and interactive Plotly charts |
| 5 | `src/04_build_map.py` | Generates the self-contained interactive choropleth map at `public/index.html` |

The HTML template lives separately in `src/map_template.html` and is injected with data at build time.

**Tools used:** Python, pandas, SQLite, Plotly, Leaflet.js, noUiSlider, Chart.js

---

## Running it yourself

```bash
# Install dependencies
pip install pandas plotly openpyxl geopandas jupyter pytest

# Run the full pipeline (from the project root)
make all

# Or run steps individually
make data    # steps 1–3: clean → company permits → SQLite
make analyze # step 4: summary tables and charts
make map     # step 5: build public/index.html

# Open the map locally
open public/index.html
```

**Tests**

```bash
make test       # fast unit + integration tests (recommended)
make test-slow  # end-to-end pipeline output checks (requires make all first)
make test-all   # everything
```

**Sector tagging**

The employer panel's sector filter is powered by `data/raw/company_sector_map.csv`, built from eleven years of NACE-classified company data. To rebuild it after adding new source files:

```bash
python scripts/build_sector_map_from_user_data.py
make data
make map
```

---

## Deploying

The map is hosted on **AWS Amplify** via GitHub. `amplify.yml` at the repo root tells Amplify to serve `public/` as a static site. No build step runs in the cloud.

To update the live site after rebuilding the map locally:

```bash
make map
git add public/index.html
git commit -m "rebuild map"
git push   # Amplify deploys automatically on push
```

---

## Data sources

- **DETE** (Dept. of Enterprise, Trade and Employment) — Work permit statistics by county, sector, nationality, and employer, 2015–2025
- **ISD** (Irish Immigration Service Delivery) — Long-term visa decisions by nationality, 2017–2026
- **NACE sector classification** — Hand-validated employer → sector mapping across 20,662 unique companies (94.9% of all permits), built from eleven years of company-level data
- **simplemaps.com** — Ireland county boundary GeoJSON

A few things worth knowing about the data: sector names changed around 2020 so pre-2020 sector trends are not directly comparable to later years. The sector breakdown shown in the map is national — DETE does not publish a county-level sector breakdown. Small visa counts are suppressed with `*` in the source data (treated as missing, not zero).

---

## Project structure

```
├── src/
│   ├── 01_clean_data.py          # county, sector, nationality, visa cleaning
│   ├── 02_build_sqlite.py        # CSV → SQLite
│   ├── 03_analyze.py             # summary tables + charts
│   ├── 04_build_map.py           # choropleth map builder
│   ├── 05_clean_companies.py     # employer permit parser + sector join
│   └── map_template.html         # HTML/JS template injected at build time
├── scripts/
│   ├── build_sector_map_from_user_data.py  # builds company_sector_map.csv
│   └── research_company_sectors.py         # generates CSV template for tagging
├── tests/
│   ├── test_cleaning.py          # unit tests for cleaning helpers
│   ├── test_company_cleaning.py  # integration tests for company_permits.csv
│   ├── test_county_cleaning.py   # integration tests for county_permits.csv
│   ├── test_sector_cleaning.py   # integration tests for sector_permits.csv
│   └── test_pipeline.py          # end-to-end output checks (slow)
├── data/
│   ├── raw/                      # original source files (read-only)
│   ├── cleaned/                  # pipeline-generated CSVs
│   └── geo/                      # county boundary GeoJSON
├── public/
│   └── index.html                # built map — committed and served by Amplify
├── output/
│   ├── charts/                   # Plotly charts
│   └── tables/                   # summary CSVs
├── amplify.yml                   # AWS Amplify hosting config
└── Makefile                      # pipeline + test shortcuts
```

---

## Author

**Aashie Kodali** — Built as a portfolio project while learning Python and data analysis.

---

*MIT License*
