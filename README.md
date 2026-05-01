# Consensus

Aggregating album reviews from multiple sources into a single consensus score.

## How It Works

1. **Collect** — Scrape album lists from AOTY year-end charts
2. **Enrich** — Query Wikipedia for Metacritic scores, Discogs for community ratings
3. **Aggregate** — Normalize all scores to 0-100, average with equal weights
4. **Present** — Browse, filter, and search across decades

## Running the Pipeline

```bash
cd backend
pip install playwright
python3 -m playwright install chromium

# Build a single decade
python3 pipeline.py --decade 2020 --limit 100

# Build all decades
python3 pipeline.py --all --limit 100
```

## Deployment

GitHub Pages serves the static site. Push to deploy:

```bash
git add -A && git commit -m "Update" && git push origin main
```

## Data Sources

| Source | What We Get | Status |
|--------|-------------|--------|
| AOTY | Critic & user scores | ✅ Scrapable via Playwright |
| Wikipedia | Metacritic scores | ✅ Open API |
| Discogs | Community ratings | ✅ Open API (60/min) |
| iTunes | Cover art | ✅ Open API |
| Deezer | Cover art fallback | ✅ Open API |
| RateYourMusic | Community scores | ❌ Bot-protected |
| Metacritic | Direct scores | ❌ Bot-protected |

## Consensus Formula

For each album, we collect all available scores (each normalized to 0-100):
- AOTY Critic score × 10 (if on 0-10 scale)
- Metacritic score (already 0-100)
- Discogs rating × 20 (converts 1-5 to 0-100)

**Consensus = average of all available sources**

Missing sources are simply excluded — no penalty.
