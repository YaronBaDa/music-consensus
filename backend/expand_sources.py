#!/usr/bin/env python3
"""
Expand album sources: add Metacritic genre charts, retry Wikipedia with delays,
add curated 1960s. Merges into scraped_raw.json.
"""
import json
import time
import re
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import requests

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"
HEADERS = {"User-Agent": USER_AGENT}

def load_existing():
    with open("scraped_raw.json") as f:
        return json.load(f)

def save_all(albums):
    with open("scraped_raw.json", "w") as f:
        json.dump(albums, f, indent=2)
    print(f"Saved {len(albums)} albums to scraped_raw.json")

def deduplicate(albums):
    seen = set()
    unique = []
    for album in albums:
        key = f"{album['artist'].lower()}|||{album['album'].lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(album)
    return unique

# =============================================================================
# METACRITIC GENRE CHARTS
# =============================================================================

GENRES = [
    "rock", "alternative", "pop", "electronic", "hip-hop", "rnb",
    "jazz", "country", "folk", "metal", "punk", "soul", "blues",
    "world", "classical", "latin", "reggae", "ambient", "experimental"
]

def scrape_metacritic_genre(genre, limit=100):
    url = f"https://www.metacritic.com/browse/albums/genre/{genre}/metascore"
    print(f"  Metacritic genre: {genre}...")
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
                    "metacritic": int(score), "source": f"Metacritic {genre.title()}"
                })
        print(f"    {len(albums)} albums")
        return albums[:limit]
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

# =============================================================================
# WIKIPEDIA WITH DELAYS
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

def extract_wikitable_albums(html, source_name, year_col=0, album_col=1, artist_col=2):
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

def scrape_wikipedia_list(page_title, source_name, year_col=0, album_col=1, artist_col=2):
    print(f"  {page_title}...")
    try:
        html = parse_wiki_page(page_title)
        albums = extract_wikitable_albums(html, source_name, year_col, album_col, artist_col)
        print(f"    {len(albums)} albums")
        time.sleep(1.5)  # Rate limit
        return albums
    except Exception as e:
        print(f"    ERROR: {e}")
        time.sleep(2)
        return []

# =============================================================================
# CURATED 1960s
# =============================================================================

ALBUMS_1960S = [
    ("The Beatles", "Sgt. Pepper's Lonely Hearts Club Band", 1967),
    ("The Beach Boys", "Pet Sounds", 1966),
    ("The Beatles", "Revolver", 1966),
    ("Bob Dylan", "Highway 61 Revisited", 1965),
    ("The Beatles", "Rubber Soul", 1965),
    ("The Beatles", "The White Album", 1968),
    ("Jimi Hendrix", "Are You Experienced", 1967),
    ("The Velvet Underground", "The Velvet Underground & Nico", 1967),
    ("The Beatles", "Abbey Road", 1969),
    ("The Rolling Stones", "Beggars Banquet", 1968),
    ("Bob Dylan", "Blonde on Blonde", 1966),
    ("The Who", "Tommy", 1969),
    ("The Rolling Stones", "Let It Bleed", 1969),
    ("The Doors", "The Doors", 1967),
    ("The Beatles", "A Hard Day's Night", 1964),
    ("James Brown", "Live at the Apollo", 1963),
    ("Aretha Franklin", "I Never Loved a Man the Way I Love You", 1967),
    ("Led Zeppelin", "Led Zeppelin II", 1969),
    ("The Rolling Stones", "Aftermath", 1966),
    ("John Coltrane", "A Love Supreme", 1965),
    ("The Kinks", "The Kinks Are the Village Green Preservation Society", 1968),
    ("Simon & Garfunkel", "Bookends", 1968),
    ("The Band", "Music from Big Pink", 1968),
    ("Van Morrison", "Astral Weeks", 1968),
    ("Cream", "Disraeli Gears", 1967),
    ("The Doors", "Strange Days", 1967),
    ("The Byrds", "Sweetheart of the Rodeo", 1968),
    ("The Rolling Stones", "Between the Buttons", 1967),
    ("Aretha Franklin", "Lady Soul", 1968),
    ("The Jimi Hendrix Experience", "Electric Ladyland", 1968),
    ("The Beatles", "Help!", 1965),
    ("Bob Dylan", "Bringing It All Back Home", 1965),
    ("The Rolling Stones", "The Rolling Stones, Now!", 1965),
    ("Dusty Springfield", "Dusty in Memphis", 1969),
    ("The Zombies", "Odessey and Oracle", 1968),
    ("The Supremes", "Where Did Our Love Go", 1964),
    ("Love", "Forever Changes", 1967),
    ("The Beatles", "Please Please Me", 1963),
    ("Otis Redding", "Otis Blue", 1965),
    ("The Stooges", "The Stooges", 1969),
    ("Creedence Clearwater Revival", "Green River", 1969),
    ("The Band", "The Band", 1969),
    ("Johnny Cash", "At Folsom Prison", 1968),
    ("The Rolling Stones", "Out of Our Heads", 1965),
    ("The Beatles", "With the Beatles", 1963),
    ("The Kinks", "Something Else by The Kinks", 1967),
    ("The Mothers of Invention", "Freak Out!", 1966),
    ("The Beach Boys", "Today!", 1965),
    ("Buffalo Springfield", "Buffalo Springfield Again", 1967),
    ("The Rolling Stones", "December's Children", 1965),
    ("The Supremes", "I Hear a Symphony", 1966),
    ("Ray Charles", "Modern Sounds in Country and Western Music", 1962),
    ("Sam Cooke", "Night Beat", 1963),
    ("Marvin Gaye", "In the Groove", 1968),
    ("The Temptations", "Cloud Nine", 1969),
    ("The Doors", "Waiting for the Sun", 1968),
    ("The Rolling Stones", "Their Satanic Majesties Request", 1967),
    ("The Kinks", "Face to Face", 1966),
    ("Jefferson Airplane", "Surrealistic Pillow", 1967),
    ("Bob Dylan", "The Times They Are a-Changin'", 1964),
    ("The Beatles", "Beatles for Sale", 1964),
    ("The Byrds", "Mr. Tambourine Man", 1965),
    ("The Beach Boys", "Summer Days (And Summer Nights!!)", 1965),
    ("Aretha Franklin", "Aretha Now", 1968),
    ("Cream", "Wheels of Fire", 1968),
    ("Simon & Garfunkel", "Parsley, Sage, Rosemary and Thyme", 1966),
    ("The Mamas & the Papas", "If You Can Believe Your Eyes and Ears", 1966),
    ("The Rolling Stones", "Flowers", 1967),
    ("The Kinks", "The Kink Kontroversy", 1965),
    ("The Supremes", "The Supremes A' Go-Go", 1966),
    ("Marvin Gaye", "M.P.G.", 1969),
    ("James Brown", "I Got You (I Feel Good)", 1965),
    ("The Temptations", "The Temptations Sing Smokey", 1965),
    ("The Four Tops", "Reach Out", 1967),
    ("The Beach Boys", "Surfer Girl", 1963),
    ("The Dave Clark Five", "Glad All Over", 1964),
    ("The Byrds", "Turn! Turn! Turn!", 1965),
    ("The Hollies", "For Certain Because", 1966),
    ("The Spencer Davis Group", "Gimme Some Lovin'", 1967),
    ("The Rascals", "Groovin'", 1967),
    ("The Association", "Insight Out", 1967),
    ("The Monkees", "Headquarters", 1967),
    ("The Bee Gees", "Bee Gees' 1st", 1967),
    ("The Turtles", "Happy Together", 1967),
    ("Scott Walker", "Scott", 1967),
    ("The Beach Boys", "All Summer Long", 1964),
    ("Bob Dylan", "Another Side of Bob Dylan", 1964),
    ("The Beatles", "Yellow Submarine", 1969),
    ("Cream", "Fresh Cream", 1966),
    ("The Who", "The Who Sell Out", 1967),
    ("Small Faces", "Ogdens' Nut Gone Flake", 1968),
    ("The Grateful Dead", "Anthem of the Sun", 1968),
    ("Iron Butterfly", "In-A-Gadda-Da-Vida", 1968),
    ("Big Brother and the Holding Company", "Cheap Thrills", 1968),
    ("The Rolling Stones", "12 X 5", 1964),
    ("The Yardbirds", "Having a Rave Up", 1965),
    ("The Moody Blues", "Days of Future Passed", 1967),
    ("Procol Harum", "Procol Harum", 1967),
    ("The Doors", "The Soft Parade", 1969),
    ("Crosby, Stills & Nash", "Crosby, Stills & Nash", 1969),
    ("Blind Faith", "Blind Faith", 1969),
    ("The Beatles", "Magical Mystery Tour", 1967),
    ("The Velvet Underground", "White Light/White Heat", 1968),
    ("Captain Beefheart", "Trout Mask Replica", 1969),
    ("Sly and the Family Stone", "Stand!", 1969),
    ("Bob Dylan", "John Wesley Harding", 1967),
    ("The Kinks", "Kinda Kinks", 1965),
    ("The Troggs", "From Nowhere", 1966),
    ("The Animals", "Animal Tracks", 1965),
    ("The Zombies", "Begin Here", 1965),
    ("The Left Banke", "Walk Away Renée/Pretty Ballerina", 1967),
    ("The Flying Burrito Brothers", "The Gilded Palace of Sin", 1969),
    ("The Byrds", "Younger Than Yesterday", 1967),
    ("Dusty Springfield", "A Girl Called Dusty", 1964),
    ("Nina Simone", "I Put a Spell on You", 1965),
    ("The Rolling Stones", "The Rolling Stones No. 2", 1965),
]

def get_curated_60s():
    albums = []
    for artist, album, year in ALBUMS_1960S:
        albums.append({
            "artist": artist, "album": album, "year": year,
            "source": "Curated 1960s"
        })
    return albums

# =============================================================================
# MAIN
# =============================================================================

def main():
    albums = load_existing()
    print(f"Starting with {len(albums)} albums")

    # 1. Metacritic genre charts
    print("\n[1/3] Metacritic Genre Charts...")
    for genre in GENRES:
        found = scrape_metacritic_genre(genre, limit=100)
        albums.extend(found)
        time.sleep(0.5)

    # 2. Wikipedia lists with delays
    print("\n[2/3] Wikipedia Lists (with rate limiting)...")
    wiki_lists = [
        ("Rolling Stone's 500 Greatest Albums of All Time", "Rolling Stone 500"),
        ("Rolling Stone's 500 Greatest Albums of All Time (2023)", "Rolling Stone 2023"),
        ("NME's The 500 Greatest Albums of All Time", "NME 500"),
        ("Time All-Time 100 Albums", "Time 100"),
        ("Apple Music 100 Best Albums", "Apple Music 100"),
    ]
    for page, source in wiki_lists:
        found = scrape_wikipedia_list(page, source)
        albums.extend(found)

    # Also try Pitchfork lists
    pitchfork_pages = [
        "Pitchfork's best albums of the 2010s",
        "Pitchfork's best albums of the 2000s",
        "Pitchfork's best albums of the 1990s",
    ]
    for page in pitchfork_pages:
        found = scrape_wikipedia_list(page, f"Pitchfork {page.split(' ')[-1]}")
        albums.extend(found)

    # 3. Curated 1960s
    print("\n[3/3] Curated 1960s...")
    found = get_curated_60s()
    albums.extend(found)
    print(f"    {len(found)} albums")

    # Deduplicate
    print("\nDeduplicating...")
    albums = deduplicate(albums)
    print(f"Total unique albums: {len(albums)}")

    save_all(albums)

    # Stats
    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990,
               "1980s": 1980, "1970s": 1970, "1960s": 1960, "Pre-1960": 1900}
    print("\nDecade coverage:")
    for name, start in decades.items():
        if name == "Pre-1960":
            count = len([a for a in albums if a["year"] < 1960])
        else:
            count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")

if __name__ == "__main__":
    main()
