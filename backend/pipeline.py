#!/usr/bin/env python3
"""
Music Consensus Pipeline
Scrapes album data from multiple sources and builds data.json
Usage: python3 pipeline.py --decade 2020 --limit 100
"""
import argparse
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

# Paths
ROOT = Path(__file__).parent.parent
DATA_FILE = ROOT / "data.json"

# Rate limiting helpers
_last_call = {}
def rate_limit(domain, min_gap=1.0):
    now = time.time()
    if domain in _last_call:
        elapsed = now - _last_call[domain]
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
    _last_call[domain] = time.time()

def fetch_json(url, headers=None, timeout=30):
    """Fetch JSON from URL with error handling."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"  ❌ Fetch error: {e}")
        return None

def itunes_search(artist, album):
    """Search iTunes for album cover art."""
    rate_limit('itunes', 0.3)
    query = urllib.parse.quote(f"{artist} {album}")
    url = f"https://itunes.apple.com/search?term={query}&entity=album&limit=5"
    data = fetch_json(url)
    if not data or not data.get('results'):
        return None
    for result in data['results']:
        # Match artist and album loosely
        it_artist = result.get('artistName', '').lower()
        it_album = result.get('collectionName', '').lower()
        if artist.lower() in it_artist or it_artist in artist.lower():
            art = result.get('artworkUrl100', '')
            if art:
                return art.replace('100x100bb', '600x600bb')
    # Fallback: return first result
    art = data['results'][0].get('artworkUrl100', '')
    return art.replace('100x100bb', '600x600bb') if art else None

def deezer_search(artist, album):
    """Fallback cover art from Deezer."""
    rate_limit('deezer', 0.5)
    query = urllib.parse.quote(f"{artist} {album}")
    url = f"https://api.deezer.com/search/album?q={query}&limit=5"
    data = fetch_json(url)
    if not data or not data.get('data'):
        return None
    for result in data['data']:
        dz_artist = result.get('artist', {}).get('name', '').lower()
        dz_album = result.get('title', '').lower()
        if artist.lower() in dz_artist or dz_artist in artist.lower():
            cover = result.get('cover_xl') or result.get('cover_big') or result.get('cover')
            if cover:
                return cover
    return data['data'][0].get('cover_xl') or data['data'][0].get('cover_big')

def get_cover(artist, album):
    """Try iTunes first, then Deezer."""
    cover = itunes_search(artist, album)
    if cover:
        return cover
    return deezer_search(artist, album)

def wikipedia_search(artist, album):
    """Search Wikipedia for album page and extract Metacritic score."""
    rate_limit('wikipedia', 0.5)
    # Try album-specific page first
    titles = [
        f"{album} ({artist} album)",
        f"{album} (album)",
        album,
    ]
    for title in titles:
        url = f"https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&titles={urllib.parse.quote(title)}&rvslots=main"
        data = fetch_json(url)
        if not data:
            continue
        pages = data.get('query', {}).get('pages', {})
        for page_id, page in pages.items():
            if page_id == '-1':
                continue
            content = page.get('revisions', [{}])[0].get('slots', {}).get('main', {}).get('*', '')
            mc = extract_metacritic(content)
            if mc:
                return {'metacritic': mc}
    return {}

def extract_metacritic(wikitext):
    """Extract Metacritic score from Wikipedia wikitext."""
    import re
    # Look for | MC = X/100 or | MC = X
    match = re.search(r'\|\s*MC\s*=\s*(\d+)(?:/100)?', wikitext)
    if match:
        return int(match.group(1))
    # Look for Metacritic in prose
    match = re.search(r'Metacritic[^\d]*(\d{2,3})', wikitext)
    if match:
        val = int(match.group(1))
        if val <= 10:
            return val * 10
        return val
    return None

def discogs_search(artist, album):
    """Search Discogs for community rating."""
    rate_limit('discogs', 1.0)
    query = urllib.parse.quote(f"{artist} {album}")
    url = f"https://api.discogs.com/database/search?q={query}&type=release&per_page=5"
    headers = {'User-Agent': 'MusicConsensus/1.0'}
    data = fetch_json(url, headers=headers)
    if not data or not data.get('results'):
        return None
    for result in data['results']:
        # Verify artist match
        artists = [a.get('name', '').lower() for a in result.get('artist', [])] if isinstance(result.get('artist'), list) else [result.get('artist', [{}])[0].get('name', '').lower()] if result.get('artist') else []
        title = result.get('title', '').lower()
        if any(artist.lower() in a for a in artists) or artist.lower() in title:
            # Get release details for rating
            release_url = f"https://api.discogs.com{result.get('uri', '').replace('/release/', '/releases/')}"
            if '/releases/' in release_url:
                rate_limit('discogs', 1.0)
                release = fetch_json(release_url, headers=headers)
                if release:
                    rating = release.get('community', {}).get('rating', {})
                    score = rating.get('average')
                    count = rating.get('count', 0)
                    if score and count >= 3:
                        return round(score * 20)  # Convert 1-5 to 0-100
    return None

def scrape_aoty_year(year, limit=25):
    """
    Scrape AOTY's year-end chart for a given year.
    Returns list of {artist, album, aoty_critic, aoty_user, year}
    """
    print(f"  Scraping AOTY {year}...")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ⚠️  Playwright not installed. Install with: python3 -m playwright install chromium")
        return []

    albums = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        try:
            url = f"https://www.albumoftheyear.org/list/summary/{year}/"
            page.goto(url, wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(3000)

            # Try to extract album data from the page
            items = page.query_selector_all('.albumListRow')
            if not items:
                items = page.query_selector_all('.listRow')
            if not items:
                items = page.query_selector_all('.albumBlock')

            for item in items[:limit]:
                try:
                    artist_el = item.query_selector('.artist') or item.query_selector('.albumListArtist') or item.query_selector('a[href*="artist"]')
                    album_el = item.query_selector('.album') or item.query_selector('.albumListTitle') or item.query_selector('a[href*="album"]')
                    score_el = item.query_selector('.score') or item.query_selector('.rating')

                    artist = artist_el.inner_text().strip() if artist_el else ''
                    album = album_el.inner_text().strip() if album_el else ''
                    score_text = score_el.inner_text().strip() if score_el else ''

                    if not artist or not album:
                        continue

                    aoty_critic = None
                    if score_text:
                        try:
                            aoty_critic = round(float(score_text) * 10)
                        except:
                            pass

                    albums.append({
                        'artist': artist,
                        'album': album,
                        'year': year,
                        'aoty_critic': aoty_critic,
                    })
                except Exception as e:
                    continue
        except Exception as e:
            print(f"  ❌ Error scraping AOTY {year}: {e}")
        finally:
            browser.close()

    print(f"  ✅ Found {len(albums)} albums from AOTY {year}")
    return albums

def enrich_album(album):
    """Enrich a single album with scores from all sources."""
    artist = album['artist']
    album_name = album['album']
    print(f"  Enriching: {artist} - {album_name}")

    # Wikipedia / Metacritic
    wp = wikipedia_search(artist, album_name)
    if wp.get('metacritic'):
        album['metacritic'] = wp['metacritic']
        print(f"    ✅ Metacritic: {wp['metacritic']}")

    # Discogs
    dg = discogs_search(artist, album_name)
    if dg:
        album['discogs'] = dg
        print(f"    ✅ Discogs: {dg}")

    # Cover art
    cover = get_cover(artist, album_name)
    if cover:
        album['cover'] = cover
        print(f"    ✅ Cover found")

    time.sleep(0.5)
    return album

def build_decade_data(decade, limit=100):
    """Build data for a decade by scraping year charts."""
    start_year = int(decade)
    end_year = start_year + 9
    if end_year > 2025:
        end_year = 2025

    print(f"\n🎵 Building {decade}s data ({start_year}-{end_year}), target: top {limit} albums")

    all_albums = []
    # Scrape each year
    for year in range(start_year, end_year + 1):
        year_albums = scrape_aoty_year(year, limit=30)
        all_albums.extend(year_albums)
        time.sleep(2)

    print(f"\n  Total raw albums: {len(all_albums)}")

    # Deduplicate by artist|||album
    seen = set()
    unique = []
    for a in all_albums:
        key = f"{a['artist'].lower()}|||{a['album'].lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(a)

    print(f"  Unique albums: {len(unique)}")

    # Sort by AOTY score, take top limit
    unique.sort(key=lambda x: x.get('aoty_critic') or 0, reverse=True)
    top_albums = unique[:limit]

    # Enrich each album
    print(f"\n🔍 Enriching {len(top_albums)} albums with external data...")
    for i, album in enumerate(top_albums):
        print(f"\n  [{i+1}/{len(top_albums)}]")
        enrich_album(album)

    return top_albums

def load_existing():
    """Load existing data.json if present."""
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return []

def save_data(albums):
    """Save albums to data.json."""
    with open(DATA_FILE, 'w') as f:
        json.dump(albums, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Saved {len(albums)} albums to {DATA_FILE}")

def main():
    parser = argparse.ArgumentParser(description='Music Consensus Pipeline')
    parser.add_argument('--decade', type=str, default='2020', help='Decade to build (e.g., 2020, 2010, 1990)')
    parser.add_argument('--limit', type=int, default=100, help='Number of albums per decade')
    parser.add_argument('--all', action='store_true', help='Build all decades')
    args = parser.parse_args()

    decades = ['2020', '2010', '1990', '1980', '1970', '1960'] if args.all else [args.decade]

    # Load existing data
    all_data = load_existing()
    existing_keys = {f"{a['artist'].lower()}|||{a['album'].lower()}" for a in all_data}

    for decade in decades:
        new_albums = build_decade_data(decade, args.limit)

        # Merge, avoiding duplicates
        for album in new_albums:
            key = f"{album['artist'].lower()}|||{album['album'].lower()}"
            if key not in existing_keys:
                existing_keys.add(key)
                all_data.append(album)

        save_data(all_data)
        print(f"\n⏸️  Waiting 5s before next decade...")
        time.sleep(5)

    print(f"\n✅ Done! Total albums in dataset: {len(all_data)}")

if __name__ == '__main__':
    main()
