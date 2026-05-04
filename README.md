# Interactive County Map for Work Permits in Ireland

**[Live demo → ireland-employment-map-adarsh-kodali.s3-website-eu-west-1.amazonaws.com](http://ireland-employment-map-adarsh-kodali.s3-website-eu-west-1.amazonaws.com)**

I built this project to answer the question of where international workers are being hired over time in Ireland and which sectors are hiring, to effectively target my job search efforts. This map helps you navigate hiring trends by county and sector over time.

Using ten years of official government data (2015–2025), I built a full data pipeline from messy Excel files to a simple self-contained interactive map you can open in any browser. No server required, no login, just open the HTML file and explore.

---

## What it shows

The map lets you pick any year range with a two-handle slider and see which counties issued the most work permits, which sectors are driving international hiring nationally, and how the picture shifted over time.

A few things that surprised me in the data:

**Healthcare has overtaken tech.** By 2024, Health and Social Work issued nearly twice as many work permits (~12,500) as IT (~6,800). The leading sectors driving employment permits have quietly shifted.

**COVID barely registered.** Permits in 2019, 2020, and 2021 were virtually identical, they were within 1% of each other. The real shock came in 2022, when national totals more than doubled in a single year, going from around 16,000 to nearly 40,000. Whatever the pandemic froze, it unfroze all at once.

**Agriculture more than doubled from 2023 to 2024**, rebounding strongly after an unusual dip the year before. The sector had been growing steadily since 2015, collapsed in 2023 for reasons that are not obvious from the data alone, then came back sharply. Points to growing demand for rural and agricultural hires.

**Meath and Monaghan stand out** as counties with meaningful absolute growth that rarely make the headlines. Meath went from 72 permits in 2015 to over 1,500 in 2024. Monaghan went from 20 to over 500 in the same period. Neither is a tech hub, which makes the growth more interesting.

**2025 shows a broad cooldown across almost every county and sector.** National permits fell from around 39,000 in 2024 to 31,000 in 2025, which is almost exactly back to 2023 levels, suggesting 2024 was the outlier peak rather than the new normal. Only five counties grew: Kilkenny (+34%), Laois (+19%), Donegal (+14%), Wicklow (+5%), and Leitrim (+48% but on a very small base). The sharpest falls were in Kildare (-49%) and Waterford (-46%), both heavily weighted towards sectors that have pulled back on international hiring.

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

- **DETE** (Dept. of Enterprise, Trade and Employment) — Work permit statistics by county, sector, and nationality, 2015–2025
- **ISD** (Irish Immigration Service Delivery) — Long-term visa decisions by nationality, 2017–2026
- **simplemaps.com** — Ireland county boundary GeoJSON

A few things worth knowing about the data: sector names changed around 2020 so pre-2020 sector trends aren't directly comparable. The sector breakdown shown in the map is national. The DETE does not publish a county-level sector breakdown, and small visa counts are suppressed with `*` in the source data (treated as missing, not zero).

---

## Author

**Aashie Kodali** — Built as a portfolio project while learning Python and data analysis.

---

*MIT License*
