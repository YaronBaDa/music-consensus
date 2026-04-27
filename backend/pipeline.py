#!/usr/bin/env python3
"""
Music Consensus Pipeline v2
Scrapes Metacritic year charts + Wikipedia decade lists
Enriches with Discogs ratings and iTunes cover art
Outputs data.json for the MusicConsensus frontend
"""
import json
import time
import re
import urllib.request
import urllib.parse
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import requests

USER_AGENT = "MusicConsensusBot/1.0 (contact@example.com)"
HEADERS = {"User-Agent": USER_AGENT}

# =============================================================================
# METACRITIC SCRAPER
# =============================================================================

def scrape_metacritic_year(year):
    """Scrape top albums for a given year from Metacritic."""
    url = f"https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected={year}"
    print(f"Scraping Metacritic {year}...")
    
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
                    "artist": artist,
                    "album": title,
                    "year": album_year,
                    "metacritic": int(score),
                    "source": f"Metacritic {year}",
                })
        
        print(f"  Extracted {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


def scrape_metacritic_alltime():
    """Scrape Metacritic all-time best albums list."""
    url = "https://www.metacritic.com/browse/albums/score/metascore/all/filtered"
    print("Scraping Metacritic all-time...")
    
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
                    "artist": artist,
                    "album": title,
                    "year": album_year,
                    "metacritic": int(score),
                    "source": "Metacritic All-Time",
                })
        
        print(f"  Extracted {len(albums)} albums")
        return albums
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


# =============================================================================
# WIKIPEDIA SCRAPER
# =============================================================================

def wiki_api(action, **params):
    """Call Wikipedia API."""
    url = "https://en.wikipedia.org/w/api.php"
    params["action"] = action
    params["format"] = "json"
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params))
    req.add_header("User-Agent", USER_AGENT)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def parse_wiki_page(title):
    """Get parsed HTML of a Wikipedia page."""
    data = wiki_api("parse", page=title, prop="text")
    return data["parse"]["text"]["*"]


def extract_wikipedia_decade(decade):
    """Extract albums from 'List of X albums considered the best'."""
    page_title = f"List of {decade}s albums considered the best"
    print(f"Fetching Wikipedia {page_title}...")
    
    try:
        html = parse_wiki_page(page_title)
    except Exception as e:
        print(f"  ERROR: {e}")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    tables = soup.find_all("table", {"class": "wikitable"})
    print(f"  Found {len(tables)} tables")
    
    for table in tables:
        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all(["td", "th"])
            if len(cols) >= 3:
                try:
                    date_text = cols[0].get_text(strip=True)
                    album = cols[1].get_text(strip=True)
                    artist = cols[2].get_text(strip=True)
                    
                    year_match = re.search(r"(\d{4})", date_text)
                    year = int(year_match.group(1)) if year_match else None
                    
                    if album and artist and year:
                        albums.append({
                            "artist": artist,
                            "album": album,
                            "year": year,
                            "source": f"Wikipedia {decade}s",
                        })
                except Exception:
                    continue
    
    print(f"  Extracted {len(albums)} albums")
    return albums


def extract_apple_music_100():
    """Extract albums from Apple Music 100 Best Albums."""
    print("Fetching Apple Music 100 Best Albums...")
    try:
        html = parse_wiki_page("Apple Music 100 Best Albums")
    except Exception as e:
        print(f"  ERROR: {e}")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    albums = []
    tables = soup.find_all("table", {"class": "wikitable"})
    if not tables:
        return albums
    
    table = tables[0]
    rows = table.find_all("tr")[1:]
    
    for row in rows:
        cols = row.find_all(["td", "th"])
        if len(cols) >= 4:
            try:
                rank = cols[0].get_text(strip=True)
                album = cols[1].get_text(strip=True)
                artist = cols[2].get_text(strip=True)
                year_text = cols[3].get_text(strip=True)
                year = int(re.search(r"\d{4}", year_text).group()) if re.search(r"\d{4}", year_text) else None
                
                if album and artist and year:
                    albums.append({
                        "artist": artist,
                        "album": album,
                        "year": year,
                        "source": "Apple Music 100",
                    })
            except Exception:
                continue
    
    print(f"  Extracted {len(albums)} albums")
    return albums


# =============================================================================
# ENRICHMENT
# =============================================================================

def search_discogs(artist, album):
    """Search Discogs for album and return rating."""
    try:
        query = f"{artist} {album}"
        url = f"https://api.discogs.com/database/search?q={urllib.parse.quote(query)}&type=release&per_page=5"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        
        if not data.get("results"):
            return None
        
        for result in data["results"][:3]:
            title = result.get("title", "").lower()
            if album.lower() in title or artist.lower() in title:
                release_url = result.get("resource_url")
                if release_url:
                    req2 = urllib.request.Request(release_url)
                    req2.add_header("User-Agent", USER_AGENT)
                    with urllib.request.urlopen(req2, timeout=15) as resp2:
                        release_data = json.loads(resp2.read())
                    
                    rating = release_data.get("community", {}).get("rating", {})
                    average = rating.get("average")
                    count = rating.get("count", 0)
                    
                    if average and count >= 3:
                        return round((average / 5) * 100)
        
        return None
    except Exception:
        return None


def search_itunes(artist, album):
    """Search iTunes for album cover art."""
    try:
        query = f"{artist} {album}"
        url = f"https://itunes.apple.com/search?term={urllib.parse.quote(query)}&entity=album&limit=5"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        
        if data.get("resultCount", 0) > 0:
            for result in data["results"]:
                collection = result.get("collectionName", "").lower()
                if album.lower() in collection or collection in album.lower():
                    artwork = result.get("artworkUrl100", "")
                    if artwork:
                        return artwork.replace("100x100bb", "600x600bb")
        
        return None
    except Exception:
        return None


# =============================================================================
# BUILD DATASET
# =============================================================================

def build_dataset():
    print("=" * 60)
    print("Music Consensus Pipeline v2")
    print("=" * 60)
    
    all_albums = []
    
    # 1. Metacritic year charts (2000-2025)
    for year in range(2025, 1999, -1):
        albums = scrape_metacritic_year(year)
        all_albums.extend(albums)
    
    # 2. Metacritic all-time list
    alltime = scrape_metacritic_alltime()
    all_albums.extend(alltime)
    
    # 3. Wikipedia decade lists
    for decade in ["1990", "1980", "1970"]:
        albums = extract_wikipedia_decade(decade)
        all_albums.extend(albums)
    
    # 4. Apple Music 100
    apple = extract_apple_music_100()
    all_albums.extend(apple)
    
    # Deduplicate by artist + album
    seen = set()
    unique_albums = []
    for album in all_albums:
        key = f"{album['artist'].lower()}|||{album['album'].lower()}"
        if key not in seen:
            seen.add(key)
            unique_albums.append(album)
    
    print(f"\nTotal unique albums before enrichment: {len(unique_albums)}")
    
    # Enrich with Discogs and iTunes
    print("\nEnriching albums (this may take a few minutes)...")
    for i, album in enumerate(unique_albums):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(unique_albums)}] {album['artist']} - {album['album']}")
        
        discogs = search_discogs(album["artist"], album["album"])
        if discogs:
            album["discogs"] = discogs
        
        time.sleep(0.1)
        
        cover = search_itunes(album["artist"], album["album"])
        if cover:
            album["cover"] = cover
        
        time.sleep(0.1)
    
    # Calculate consensus scores
    for album in unique_albums:
        scores = []
        if "metacritic" in album:
            scores.append(album["metacritic"])
        if "discogs" in album:
            scores.append(album["discogs"])
        
        if scores:
            album["consensus"] = round(sum(scores) / len(scores))
        else:
            album["consensus"] = None
        
        album["reviews"] = len(scores)
        album["genre"] = album.get("genre", "Various")
    
    return unique_albums


def filter_decade(albums, start_year):
    """Filter and sort albums for a specific decade."""
    decade = [a for a in albums if start_year <= a["year"] < start_year + 10]
    decade.sort(key=lambda x: (x.get("consensus") or 0, x.get("metacritic") or 0), reverse=True)
    return decade[:100]


def main():
    albums = build_dataset()
    
    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)
    print(f"\nSaved {len(albums)} albums to data.json")
    
    decades = {
        "2020s": 2020,
        "2010s": 2010,
        "2000s": 2000,
        "1990s": 1990,
        "1980s": 1980,
        "1970s": 1970,
        "1960s": 1960,
    }
    
    print("\nDecade coverage:")
    for name, start in decades.items():
        count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
