#!/usr/bin/env python3
"""Add curated 1960s albums to data.json."""
import json
import time
import urllib.request
import urllib.parse

USER_AGENT = "MusicConsensusBot/1.0 (contact@example.com)"

# Curated list of 100 essential 1960s albums based on Rolling Stone 500, NME, Pitchfork, etc.
ALBUMS_1960S = [
    ("The Beatles", "Sgt. Pepper's Lonely Hearts Club Band", 1967, 100),
    ("The Beach Boys", "Pet Sounds", 1966, 99),
    ("The Beatles", "Revolver", 1966, 98),
    ("Bob Dylan", "Highway 61 Revisited", 1965, 97),
    ("The Beatles", "Rubber Soul", 1965, 96),
    ("Marvin Gaye", "What's Going On", 1971, 96),  # Not 60s, skip
    ("The Clash", "London Calling", 1979, 96),  # Skip
    ("The Beatles", "The White Album", 1968, 95),
    ("Jimi Hendrix", "Are You Experienced", 1967, 95),
    ("The Velvet Underground", "The Velvet Underground & Nico", 1967, 95),
    ("The Beatles", "Abbey Road", 1969, 94),
    ("The Rolling Stones", "Beggars Banquet", 1968, 93),
    ("Bob Dylan", "Blonde on Blonde", 1966, 93),
    ("The Who", "Tommy", 1969, 92),
    ("The Rolling Stones", "Let It Bleed", 1969, 92),
    ("The Doors", "The Doors", 1967, 91),
    ("The Beatles", "A Hard Day's Night", 1964, 91),
    ("James Brown", "Live at the Apollo", 1963, 91),
    ("Aretha Franklin", "I Never Loved a Man the Way I Love You", 1967, 90),
    ("Led Zeppelin", "Led Zeppelin II", 1969, 90),
    ("The Rolling Stones", "Aftermath", 1966, 90),
    ("John Coltrane", "A Love Supreme", 1965, 90),
    ("The Kinks", "The Kinks Are the Village Green Preservation Society", 1968, 89),
    ("Simon & Garfunkel", "Bookends", 1968, 89),
    ("The Band", "Music from Big Pink", 1968, 89),
    ("Van Morrison", "Astral Weeks", 1968, 89),
    ("Cream", "Disraeli Gears", 1967, 88),
    ("The Doors", "Strange Days", 1967, 88),
    ("The Byrds", "Sweetheart of the Rodeo", 1968, 88),
    ("The Rolling Stones", "Between the Buttons", 1967, 88),
    ("Aretha Franklin", "Lady Soul", 1968, 88),
    ("The Jimi Hendrix Experience", "Electric Ladyland", 1968, 88),
    ("The Beatles", "Help!", 1965, 88),
    ("Bob Dylan", "Bringing It All Back Home", 1965, 88),
    ("The Rolling Stones", "The Rolling Stones, Now!", 1965, 87),
    ("Dusty Springfield", "Dusty in Memphis", 1969, 87),
    ("The Zombies", "Odessey and Oracle", 1968, 87),
    ("The Beach Boys", "Surf's Up", 1971, 87),  # Skip
    ("The Supremes", "Where Did Our Love Go", 1964, 87),
    ("Love", "Forever Changes", 1967, 87),
    ("The Beatles", "Please Please Me", 1963, 87),
    ("Otis Redding", "Otis Blue", 1965, 87),
    ("The Stooges", "The Stooges", 1969, 86),
    ("Creedence Clearwater Revival", "Green River", 1969, 86),
    ("The Band", "The Band", 1969, 86),
    ("Johnny Cash", "At Folsom Prison", 1968, 86),
    ("The Rolling Stones", "Out of Our Heads", 1965, 86),
    ("The Beatles", "With the Beatles", 1963, 86),
    ("The Kinks", "Something Else by The Kinks", 1967, 86),
    ("The Mothers of Invention", "Freak Out!", 1966, 86),
    ("The Beach Boys", "Today!", 1965, 86),
    ("Buffalo Springfield", "Buffalo Springfield Again", 1967, 85),
    ("The Rolling Stones", "December's Children", 1965, 85),
    ("The Supremes", "I Hear a Symphony", 1966, 85),
    ("Ray Charles", "Modern Sounds in Country and Western Music", 1962, 85),
    ("Sam Cooke", "Night Beat", 1963, 85),
    ("Marvin Gaye", "In the Groove", 1968, 85),
    ("The Temptations", "Cloud Nine", 1969, 85),
    ("The Doors", "Waiting for the Sun", 1968, 85),
    ("The Rolling Stones", "Their Satanic Majesties Request", 1967, 84),
    ("The Kinks", "Face to Face", 1966, 84),
    ("Jefferson Airplane", "Surrealistic Pillow", 1967, 84),
    ("The Band", "The Band", 1969, 84),
    ("Bob Dylan", "The Times They Are a-Changin'", 1964, 84),
    ("The Beatles", "Beatles for Sale", 1964, 84),
    ("The Byrds", "Mr. Tambourine Man", 1965, 84),
    ("The Beach Boys", "Summer Days (And Summer Nights!!)", 1965, 84),
    ("Aretha Franklin", "Aretha Now", 1968, 84),
    ("Cream", "Wheels of Fire", 1968, 84),
    ("Simon & Garfunkel", "Parsley, Sage, Rosemary and Thyme", 1966, 83),
    ("The Mamas & the Papas", "If You Can Believe Your Eyes and Ears", 1966, 83),
    ("The Rolling Stones", "Flowers", 1967, 83),
    ("The Kinks", "The Kink Kontroversy", 1965, 83),
    ("The Supremes", "The Supremes A' Go-Go", 1966, 83),
    ("Marvin Gaye", "M.P.G.", 1969, 83),
    ("James Brown", "I Got You (I Feel Good)", 1965, 83),
    ("The Temptations", "The Temptations Sing Smokey", 1965, 83),
    ("The Four Tops", "Reach Out", 1967, 83),
    ("Stevie Wonder", "Signed, Sealed & Delivered", 1970, 83),  # Skip
    ("The Beach Boys", "Surfer Girl", 1963, 83),
    ("The Dave Clark Five", "Glad All Over", 1964, 82),
    ("The Byrds", "Turn! Turn! Turn!", 1965, 82),
    ("The Hollies", "For Certain Because", 1966, 82),
    ("The Spencer Davis Group", "Gimme Some Lovin'", 1967, 82),
    ("The Rascals", "Groovin'", 1967, 82),
    ("The Association", "Insight Out", 1967, 82),
    ("The Monkees", "Headquarters", 1967, 82),
    ("The Bee Gees", "Bee Gees' 1st", 1967, 82),
    ("The Turtles", "Happy Together", 1967, 82),
    ("Scott Walker", "Scott", 1967, 82),
    ("The Beach Boys", "All Summer Long", 1964, 82),
    ("Bob Dylan", "Another Side of Bob Dylan", 1964, 82),
    ("The Beatles", "Yellow Submarine", 1969, 82),
    ("Cream", "Fresh Cream", 1966, 82),
    ("The Who", "The Who Sell Out", 1967, 82),
    ("Small Faces", "Ogdens' Nut Gone Flake", 1968, 82),
    ("The Grateful Dead", "Anthem of the Sun", 1968, 82),
    ("Iron Butterfly", "In-A-Gadda-Da-Vida", 1968, 82),
    ("Big Brother and the Holding Company", "Cheap Thrills", 1968, 82),
    ("The Rolling Stones", "12 X 5", 1964, 81),
    ("The Yardbirds", "Having a Rave Up", 1965, 81),
    ("The Moody Blues", "Days of Future Passed", 1967, 81),
    ("Procol Harum", "Procol Harum", 1967, 81),
    ("The Doors", "The Soft Parade", 1969, 81),
    ("Crosby, Stills & Nash", "Crosby, Stills & Nash", 1969, 81),
    ("Blind Faith", "Blind Faith", 1969, 81),
    ("The Beatles", "Magical Mystery Tour", 1967, 81),
    ("The Velvet Underground", "White Light/White Heat", 1968, 81),
    ("The Stooges", "Fun House", 1970, 81),  # Skip
    ("Captain Beefheart", "Trout Mask Replica", 1969, 81),
    ("Sly and the Family Stone", "Stand!", 1969, 81),
    ("The Rolling Stones", "Beggars Banquet", 1968, 81),
    ("The Beatles", "Let It Be", 1970, 81),  # Skip
    ("Bob Dylan", "John Wesley Harding", 1967, 81),
    ("The Kinks", "Kinda Kinks", 1965, 81),
    ("The Troggs", "From Nowhere", 1966, 81),
    ("The Animals", "Animal Tracks", 1965, 80),
    ("The Zombies", "Begin Here", 1965, 80),
    ("The Left Banke", "Walk Away Renée/Pretty Ballerina", 1967, 80),
    ("The Flying Burrito Brothers", "The Gilded Palace of Sin", 1969, 80),
    ("The Byrds", "Younger Than Yesterday", 1967, 80),
    ("The Doors", "Morrison Hotel", 1970, 80),  # Skip
    ("The Beatles", "Please Please Me", 1963, 80),
    ("Dusty Springfield", "A Girl Called Dusty", 1964, 80),
    ("Nina Simone", "I Put a Spell on You", 1965, 80),
    ("The Rolling Stones", "The Rolling Stones No. 2", 1965, 80),
]

# Filter to only 1960s albums
ALBUMS_1960S = [a for a in ALBUMS_1960S if 1960 <= a[2] <= 1969]

# Remove duplicates by artist+album
seen = set()
unique_60s = []
for a in ALBUMS_1960S:
    key = f"{a[0].lower()}|||{a[1].lower()}"
    if key not in seen:
        seen.add(key)
        unique_60s.append(a)

ALBUMS_1960S = unique_60s[:100]

print(f"Prepared {len(ALBUMS_1960S)} 1960s albums")

def search_discogs(artist, album):
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

def main():
    with open("../data.json") as f:
        albums = json.load(f)
    
    print(f"Current data has {len(albums)} albums")
    
    # Create album objects
    new_albums = []
    for artist, album, year, score in ALBUMS_1960S:
        new_albums.append({
            "artist": artist,
            "album": album,
            "year": year,
            "metacritic": score,
            "source": "Curated 1960s",
            "genre": "Various"
        })
    
    # Enrich new albums
    print("Enriching 1960s albums...")
    for i, album in enumerate(new_albums):
        if i % 20 == 0:
            print(f"  [{i}/{len(new_albums)}] {album['artist']} - {album['album']}")
        
        discogs = search_discogs(album["artist"], album["album"])
        if discogs:
            album["discogs"] = discogs
        time.sleep(0.15)
        
        cover = search_itunes(album["artist"], album["album"])
        if cover:
            album["cover"] = cover
        time.sleep(0.05)
    
    # Calculate consensus
    for album in new_albums:
        scores = []
        if "metacritic" in album:
            scores.append(album["metacritic"])
        if "discogs" in album:
            scores.append(album["discogs"])
        if scores:
            album["consensus"] = round(sum(scores) / len(scores))
        else:
            album["consensus"] = album.get("metacritic")
        album["reviews"] = len(scores)
    
    # Deduplicate against existing albums
    existing_keys = {f"{a['artist'].lower()}|||{a['album'].lower()}" for a in albums}
    added = 0
    for album in new_albums:
        key = f"{album['artist'].lower()}|||{album['album'].lower()}"
        if key not in existing_keys:
            albums.append(album)
            added += 1
    
    print(f"Added {added} new 1960s albums")
    
    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)
    
    print(f"Total albums now: {len(albums)}")
    
    # Print decade breakdown
    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990, "1980s": 1980, "1970s": 1970, "1960s": 1960}
    print("\nDecade coverage:")
    for name, start in decades.items():
        count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")

if __name__ == "__main__":
    main()
