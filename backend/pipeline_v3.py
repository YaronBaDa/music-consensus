#!/usr/bin/env python3
"""
Consensus Pipeline v3
Maximizes album coverage by scraping from every available source.
Outputs scraped_raw.json for enrichment.
"""
import json
import time
import re
import urllib.request
import urllib.parse
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import requests

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"
HEADERS = {"User-Agent": USER_AGENT}

# =============================================================================
# WIKIPEDIA API HELPERS
# =============================================================================

def wiki_api(action, **params):
    url = "https://en.wikipedia.org/w/api.php"
    params["action"] = action
    params["format"] = "json"
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params))
    req.add_header("User-Agent", USER_AGENT)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

def parse_wiki_page(title):
    data = wiki_api("parse", page=title, prop="text")
    return data["parse"]["text"]["*"]

# =============================================================================
# METACRITIC SCRAPER (existing, expanded)
# =============================================================================

def scrape_metacritic_year(year, limit=100):
    url = f"https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected={year}"
    print(f"  Metacritic {year}...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        albums = []
        for elem in soup.find_all(class_="clamp-summary-wrap"):
            parent = elem.find_parent("tr")
            if not parent:
                continue
            title_el = elem.find("h3")
            artist_el = elem.find(class_="artist")
            score_el = elem.find(class_="metascore_anchor")
            title = title_el.get_text(strip=True) if title_el else ""
            artist = artist_el.get_text(strip=True) if artist_el else ""
            score = score_el.get_text(strip=True) if score_el else ""
            artist = artist.replace("by ", "") if artist.startswith("by ") else artist
            date = ""
            spans = parent.find_all("span")
            for span in spans:
                text = span.get_text(strip=True)
                if re.match(r"[A-Za-z]+ \d{1,2}, \d{4}", text):
                    date = text
                    break
            year_match = re.search(r"(\d{4})", date) if date else None
            album_year = int(year_match.group(1)) if year_match else year
            if title and artist and score:
                albums.append({
                    "artist": artist, "album": title, "year": album_year,
                    "metacritic": int(score), "source": f"Metacritic {year}"
                })
        print(f"    {len(albums)} albums")
        return albums[:limit]
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_metacritic_alltime(limit=100):
    url = "https://www.metacritic.com/browse/albums/score/metascore/all/filtered"
    print("  Metacritic all-time...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        albums = []
        for elem in soup.find_all(class_="clamp-summary-wrap"):
            parent = elem.find_parent("tr")
            if not parent:
                continue
            title_el = elem.find("h3")
            artist_el = elem.find(class_="artist")
            score_el = elem.find(class_="metascore_anchor")
            title = title_el.get_text(strip=True) if title_el else ""
            artist = artist_el.get_text(strip=True) if artist_el else ""
            score = score_el.get_text(strip=True) if score_el else ""
            artist = artist.replace("by ", "") if artist.startswith("by ") else artist
            date = ""
            spans = parent.find_all("span")
            for span in spans:
                text = span.get_text(strip=True)
                if re.match(r"[A-Za-z]+ \d{1,2}, \d{4}", text):
                    date = text
                    break
            year_match = re.search(r"(\d{4})", date) if date else None
            album_year = int(year_match.group(1)) if year_match else None
            if title and artist and score and album_year:
                albums.append({
                    "artist": artist, "album": title, "year": album_year,
                    "metacritic": int(score), "source": "Metacritic All-Time"
                })
        print(f"    {len(albums)} albums")
        return albums[:limit]
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

# =============================================================================
# WIKIPEDIA LIST SCRAPERS
# =============================================================================

def extract_wikitable_albums(html, source_name, year_col=0, album_col=1, artist_col=2):
    """Generic wikitable album extractor."""
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    tables = soup.find_all("table", {"class": "wikitable"})
    for table in tables:
        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all(["td", "th"])
            if len(cols) > max(year_col, album_col, artist_col):
                try:
                    year_text = cols[year_col].get_text(strip=True)
                    album = cols[album_col].get_text(strip=True)
                    artist = cols[artist_col].get_text(strip=True)
                    # Clean up citations like [1][2]
                    album = re.sub(r'\[\d+\]', '', album).strip()
                    artist = re.sub(r'\[\d+\]', '', artist).strip()
                    year_match = re.search(r"(\d{4})", year_text)
                    year = int(year_match.group(1)) if year_match else None
                    if album and artist and year:
                        albums.append({
                            "artist": artist, "album": album, "year": year,
                            "source": source_name
                        })
                except Exception:
                    continue
    return albums

def scrape_wikipedia_decade(decade):
    page_title = f"List of {decade}s albums considered the best"
    print(f"  Wikipedia {page_title}...")
    try:
        html = parse_wiki_page(page_title)
        albums = extract_wikitable_albums(html, f"Wikipedia {decade}s")
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_apple_music_100():
    print("  Apple Music 100 Best Albums...")
    try:
        html = parse_wiki_page("Apple Music 100 Best Albums")
        albums = extract_wikitable_albums(html, "Apple Music 100", year_col=3, album_col=1, artist_col=2)
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_rolling_stone_500():
    """Scrape Rolling Stone 500 Greatest Albums of All Time."""
    print("  Rolling Stone 500...")
    try:
        html = parse_wiki_page("Rolling Stone's 500 Greatest Albums of All Time")
        albums = extract_wikitable_albums(html, "Rolling Stone 500")
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_rolling_stone_2023_500():
    """Scrape Rolling Stone 2023 updated 500 list."""
    print("  Rolling Stone 2023 500...")
    try:
        html = parse_wiki_page("Rolling Stone's 500 Greatest Albums of All Time (2023)")
        albums = extract_wikitable_albums(html, "Rolling Stone 2023")
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_nme_500():
    """Scrape NME 500 Greatest Albums."""
    print("  NME 500...")
    try:
        html = parse_wiki_page("NME's The 500 Greatest Albums of All Time")
        albums = extract_wikitable_albums(html, "NME 500")
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_time_100():
    """Scrape Time All-Time 100 Albums."""
    print("  Time 100...")
    try:
        html = parse_wiki_page("Time All-Time 100 Albums")
        albums = extract_wikitable_albums(html, "Time 100")
        print(f"    {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

def scrape_pitchfork_best_albums():
    """Scrape Pitchfork's best albums lists (various)."""
    print("  Pitchfork Best Albums...")
    albums = []
    # Try a few Pitchfork list pages
    pages = [
        "Pitchfork's best albums of the 2010s",
        "Pitchfork's best albums of the 2000s",
        "Pitchfork's best albums of the 1990s",
    ]
    for page in pages:
        try:
            html = parse_wiki_page(page)
            found = extract_wikitable_albums(html, f"Pitchfork {page.split(' ')[-1]}")
            print(f"    {page}: {len(found)} albums")
            albums.extend(found)
        except Exception as e:
            print(f"    {page}: ERROR {e}")
    return albums

# =============================================================================
# ACCLAIMED MUSIC SCRAPER
# =============================================================================

def scrape_acclaimed_music_year(year):
    """Scrape Acclaimed Music year chart."""
    url = f"https://www.acclaimedmusic.net/{year}.htm"
    print(f"  Acclaimed Music {year}...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        albums = []
        # Acclaimed Music uses a specific table structure
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 4:
                try:
                    rank = tds[0].get_text(strip=True)
                    artist = tds[1].get_text(strip=True)
                    album = tds[2].get_text(strip=True)
                    year_text = tds[3].get_text(strip=True)
                    # Validate it's a number rank
                    if rank.isdigit() and artist and album:
                        albums.append({
                            "artist": artist, "album": album, "year": year,
                            "source": f"Acclaimed Music {year}"
                        })
                except Exception:
                    continue
        print(f"    {len(albums)} albums")
        return albums[:100]
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

# =============================================================================
# BESTEVERALBUMS SCRAPER
# =============================================================================

def scrape_besteveralbums_decade(decade):
    """Scrape BestEverAlbums decade chart."""
    url = f"https://www.besteveralbums.com/overall.php?o=&f=&k=&fv=&y={decade}&page=1"
    print(f"  BestEverAlbums {decade}s...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        albums = []
        for row in soup.find_all("tr", class_="chartrow"):
            tds = row.find_all("td")
            if len(tds) >= 3:
                try:
                    artist_el = tds[1].find("a", class_="artist")
                    album_el = tds[1].find("a", class_="album")
                    year_el = tds[2]
                    artist = artist_el.get_text(strip=True) if artist_el else ""
                    album = album_el.get_text(strip=True) if album_el else ""
                    year_text = year_el.get_text(strip=True) if year_el else ""
                    year_match = re.search(r"(\d{4})", year_text)
                    year_val = int(year_match.group(1)) if year_match else None
                    if artist and album and year_val:
                        albums.append({
                            "artist": artist, "album": album, "year": year_val,
                            "source": f"BestEverAlbums {decade}s"
                        })
                except Exception:
                    continue
        print(f"    {len(albums)} albums")
        return albums[:100]
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

# =============================================================================
# BUILD DATASET
# =============================================================================

def deduplicate(albums):
    seen = set()
    unique = []
    for album in albums:
        key = f"{album['artist'].lower()}|||{album['album'].lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(album)
    return unique

def build_dataset():
    print("=" * 60)
    print("Consensus Pipeline v3 — Maximum Coverage")
    print("=" * 60)
    
    all_albums = []
    
    # 1. Metacritic year charts (2000-2026)
    print("\n[1/6] Metacritic Year Charts...")
    for year in range(2026, 1999, -1):
        albums = scrape_metacritic_year(year, limit=100)
        all_albums.extend(albums)
    
    # 2. Metacritic all-time
    print("\n[2/6] Metacritic All-Time...")
    alltime = scrape_metacritic_alltime(limit=100)
    all_albums.extend(alltime)
    
    # 3. Wikipedia lists
    print("\n[3/6] Wikipedia Lists...")
    for decade in ["1990", "1980", "1970"]:
        albums = scrape_wikipedia_decade(decade)
        all_albums.extend(albums)
    all_albums.extend(scrape_apple_music_100())
    all_albums.extend(scrape_rolling_stone_500())
    all_albums.extend(scrape_rolling_stone_2023_500())
    all_albums.extend(scrape_nme_500())
    all_albums.extend(scrape_time_100())
    all_albums.extend(scrape_pitchfork_best_albums())
    
    # 4. Acclaimed Music
    print("\n[4/6] Acclaimed Music...")
    for year in range(2024, 1999, -1):
        albums = scrape_acclaimed_music_year(year)
        all_albums.extend(albums)
    
    # 5. BestEverAlbums
    print("\n[5/6] BestEverAlbums...")
    for decade in ["2020", "2010", "2000", "1990", "1980", "1970", "1960"]:
        albums = scrape_besteveralbums_decade(decade)
        all_albums.extend(albums)
    
    # 6. Deduplicate
    print("\n[6/6] Deduplicating...")
    all_albums = deduplicate(all_albums)
    print(f"Total unique albums: {len(all_albums)}")
    
    # Save raw
    with open("scraped_raw.json", "w") as f:
        json.dump(all_albums, f, indent=2)
    print("Saved to scraped_raw.json")
    
    # Decade breakdown
    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990,
               "1980s": 1980, "1970s": 1970, "1960s": 1960, "Pre-1960": 1900}
    print("\nDecade coverage:")
    for name, start in decades.items():
        if name == "Pre-1960":
            count = len([a for a in all_albums if a["year"] < 1960])
        else:
            count = len([a for a in all_albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")
    
    return all_albums

if __name__ == "__main__":
    build_dataset()
