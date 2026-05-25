.PHONY: all data analyze map test test-slow test-all clean

# Run the full pipeline end-to-end
all: data analyze map

# Step 1 + 2: clean raw Excel files → CSVs → SQLite DB
data:
	python src/01_clean_data.py
	python src/05_clean_companies.py
	python src/02_build_sqlite.py

# Step 3: query the DB, write summary tables + charts
analyze:
	python src/03_analyze.py

# Step 4: build the interactive choropleth map
map:
	python src/04_build_map.py

# Run fast unit + integration tests (skips end-to-end pipeline tests)
test:
	pytest tests/ -v -m "not slow"

# Run only the slow end-to-end pipeline tests (requires 'make data' + 'make map' first)
test-slow:
	pytest tests/ -v -m slow

# Run the complete test suite including slow tests
test-all:
	pytest tests/ -v

# Remove generated files (keeps raw data and geo files intact)
clean:
	rm -f data/cleaned/*.csv data/employment.db
