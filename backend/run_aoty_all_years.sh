#!/bin/bash
set -e

cd "$(dirname "$0")"

# Get all years present in data.json
years=$(python3 -c "
import json
with open('../data.json') as f:
    data = json.load(f)
years = sorted({a['year'] for a in data if a.get('year')}, reverse=True)
print(' '.join(map(str, years)))
")

echo "Years to scrape: $years"
echo ""

for year in $years; do
    echo "=== Scraping $year ==="
    python3 enrich_aoty_year.py "$year"
    sleep 2
done

echo ""
echo "All years scraped. Now running match & enrich..."
python3 enrich_aoty.py
