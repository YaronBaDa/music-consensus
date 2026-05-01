#!/usr/bin/env python3
"""
Consensus Enrichment v3
Maximizes per-album data with improved matching, genres, and more sources.
Checkpoint/resume mandatory for large datasets.
"""
import json
import time
import os
import re
import urllib.request
import urllib.parse
from difflib import SequenceMatcher

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def search_discogs(artist, album):
    """Search Discogs with fuzzy matching and artist-only fallback."""
    def _search(query):
        try:
            url = f"https://api.discogs.com/database/search?q={urllib.parse.quote(query)}&type=release&per_page=10"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", USER_AGENT)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            return data.get("results", [])
        except Exception:
            return []

    # Strategy 1: artist + album
    results = _search(f"{artist} {album}")
    best = None
    best_score = 0
    for result in results[:5]:
        title = result.get("title", "").lower()
        ratio = max(fuzzy_ratio(album, title), fuzzy_ratio(artist, title))
        if ratio > best_score:
            best_score = ratio
            best = result

    # Strategy 2: album only
    if not best or best_score < 0.4:
        results = _search(album)
        for result in results[:3]:
            title = result.get("title", "").lower()
            ratio = fuzzy_ratio(album, title)
            if ratio > best_score:
                best_score = ratio
                best = result

    if not best or best_score < 0.3:
        return None, None

    # Fetch release details for rating
    try:
        release_url = best.get("resource_url")
        if not release_url:
            return None, None
        req2 = urllib.request.Request(release_url)
        req2.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req2, timeout=15) as resp2:
            release_data = json.loads(resp2.read())

        rating = release_data.get("community", {}).get("rating", {})
        average = rating.get("average")
        count = rating.get("count", 0)

        discogs_score = None
        if average and count >= 3:
            discogs_score = round((average / 5) * 100)

        # Try to get genres from Discogs
        genres = release_data.get("genres", [])
        styles = release_data.get("styles", [])
        discogs_genre = None
        if genres:
            discogs_genre = genres[0]
        elif styles:
            discogs_genre = styles[0]

        return discogs_score, discogs_genre
    except Exception:
        return None, None


def search_itunes(artist, album):
    """Search iTunes for cover art."""
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
                if fuzzy_ratio(album, collection) > 0.5 or album.lower() in collection or collection in album.lower():
                    artwork = result.get("artworkUrl100", "")
                    if artwork:
                        return artwork.replace("100x100bb", "600x600bb")
        return None
    except Exception:
        return None


def search_musicbrainz(artist, album):
    """Search MusicBrainz for genre and metadata."""
    try:
        query = f'artist:"{artist}" AND release:"{album}"'
        url = f"https://musicbrainz.org/ws/2/release/?query={urllib.parse.quote(query)}&fmt=json&limit=3"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        releases = data.get("releases", [])
        if not releases:
            return None, None

        # Get first release's tags/genres
        release = releases[0]
        release_id = release.get("id")
        if not release_id:
            return None, None

        # Fetch release group for genres
        rg_id = release.get("release-group", {}).get("id")
        if not rg_id:
            return None, None

        time.sleep(0.3)  # Rate limit
        url2 = f"https://musicbrainz.org/ws/2/release-group/{rg_id}?fmt=json&inc=tags"
        req2 = urllib.request.Request(url2)
        req2.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req2, timeout=15) as resp2:
            rg_data = json.loads(resp2.read())

        tags = rg_data.get("tags", [])
        genres = [t["name"] for t in tags if t.get("count", 0) > 0]
        primary_genre = genres[0].title() if genres else None

        # Also try to get rating
        time.sleep(0.3)
        url3 = f"https://musicbrainz.org/ws/2/release-group/{rg_id}?fmt=json&inc=ratings"
        req3 = urllib.request.Request(url3)
        req3.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req3, timeout=15) as resp3:
            rating_data = json.loads(resp3.read())

        rating = rating_data.get("rating", {})
        vote_count = rating.get("votes-count", 0)
        rating_value = rating.get("value")
        mb_score = None
        if rating_value and vote_count >= 3:
            mb_score = round(rating_value * 20)  # 0-5 to 0-100

        return mb_score, primary_genre
    except Exception:
        return None, None


def search_allmusic(artist, album):
    """Try to get AllMusic rating."""
    try:
        # AllMusic search page
        query = f"{artist} {album}"
        url = f"https://www.allmusic.com/search/albums/{urllib.parse.quote(query)}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")

        import re
        # Look for rating in the HTML
        # AllMusic uses star ratings, e.g. 4/5 or 4.5/5
        match = re.search(r'class="allmusic-rating[^"]*"[^>]*>([\d.]+)</span>', html)
        if match:
            rating = float(match.group(1))
            return round((rating / 5) * 100)

        # Alternative pattern
        match = re.search(r'rating[^>]*>([\d.]+)\s*/\s*5', html)
        if match:
            rating = float(match.group(1))
            return round((rating / 5) * 100)

        return None
    except Exception:
        return None


def normalize_genre(genre):
    """Normalize Discogs/MusicBrainz genres to consistent categories."""
    if not genre:
        return "Various"
    genre = genre.lower()
    mapping = {
        "rock": "Rock",
        "pop": "Pop",
        "hip hop": "Hip Hop",
        "hip-hop": "Hip Hop",
        "rap": "Hip Hop",
        "electronic": "Electronic",
        "electronica": "Electronic",
        "house": "Electronic",
        "techno": "Electronic",
        "edm": "Electronic",
        "synth-pop": "Electronic",
        "synthpop": "Electronic",
        "indie": "Indie",
        "indie rock": "Indie",
        "indie pop": "Indie",
        "alternative": "Alternative",
        "alternative rock": "Alternative",
        "metal": "Metal",
        "heavy metal": "Metal",
        "jazz": "Jazz",
        "soul": "Soul",
        "r&b": "R&B",
        "rhythm and blues": "R&B",
        "funk": "Funk",
        "blues": "Blues",
        "country": "Country",
        "folk": "Folk",
        "punk": "Punk",
        "punk rock": "Punk",
        "reggae": "Reggae",
        "classical": "Classical",
        "ambient": "Ambient",
        "experimental": "Experimental",
        "psychedelic": "Psychedelic",
        "psychedelic rock": "Psychedelic",
    }
    for key, value in mapping.items():
        if key in genre:
            return value
    return genre.title()


def enrich_album(album):
    """Enrich a single album with all available sources."""
    artist = album["artist"]
    title = album["album"]

    # Discogs (rating + genre)
    discogs_score, discogs_genre = search_discogs(artist, title)
    if discogs_score:
        album["discogs"] = discogs_score
    time.sleep(0.15)

    # iTunes cover
    cover = search_itunes(artist, title)
    if cover:
        album["cover"] = cover
    time.sleep(0.05)

    # MusicBrainz (rating + genre)
    mb_score, mb_genre = search_musicbrainz(artist, title)
    if mb_score:
        album["musicbrainz"] = mb_score
    time.sleep(0.15)

    # AllMusic
    am_score = search_allmusic(artist, title)
    if am_score:
        album["allmusic"] = am_score
    time.sleep(0.5)

    # Determine best genre
    best_genre = discogs_genre or mb_genre or album.get("genre")
    album["genre"] = normalize_genre(best_genre)

    return album


def enrich_chunk(albums, chunk_start):
    for i, album in enumerate(albums):
        idx = chunk_start + i
        if idx % 50 == 0:
            print(f"  [{idx}] {album['artist']} - {album['album']}")
        enrich_album(album)


def calculate_consensus(albums):
    """Calculate consensus from all available sources."""
    for album in albums:
        scores = []
        if "metacritic" in album:
            scores.append(album["metacritic"])
        if "discogs" in album:
            scores.append(album["discogs"])
        if "musicbrainz" in album:
            scores.append(album["musicbrainz"])
        if "allmusic" in album:
            scores.append(album["allmusic"])

        if scores:
            album["consensus"] = round(sum(scores) / len(scores))
        else:
            album["consensus"] = album.get("metacritic", 0)

        album["reviews"] = len(scores)
        album["genre"] = album.get("genre", "Various")


def main():
    with open("scraped_raw.json") as f:
        albums = json.load(f)

    checkpoint_file = "enrich_checkpoint_v3.json"

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        start_idx = checkpoint["next_idx"]
        albums = checkpoint["albums"]
        print(f"Resuming from album {start_idx} of {len(albums)}...")
    else:
        start_idx = 0
        print(f"Starting enrichment of {len(albums)} albums...")

    chunk_size = 200
    total = len(albums)

    while start_idx < total:
        end_idx = min(start_idx + chunk_size, total)
        chunk = albums[start_idx:end_idx]

        print(f"\nProcessing chunk {start_idx}-{end_idx} of {total}...")
        enrich_chunk(chunk, start_idx)

        with open(checkpoint_file, "w") as f:
            json.dump({"next_idx": end_idx, "albums": albums}, f)

        print(f"  Chunk done. Saved checkpoint.")
        start_idx = end_idx

    # Calculate consensus
    print("\nCalculating consensus scores...")
    calculate_consensus(albums)

    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)

    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    print(f"\nSaved {len(albums)} albums to data.json")

    # Stats
    source_counts = {"metacritic": 0, "discogs": 0, "musicbrainz": 0, "allmusic": 0, "cover": 0}
    genre_counts = {}
    for a in albums:
        for src in source_counts:
            if src in a:
                source_counts[src] += 1
        g = a.get("genre", "Various")
        genre_counts[g] = genre_counts.get(g, 0) + 1

    print("\nSource coverage:")
    for src, count in source_counts.items():
        pct = round(count / len(albums) * 100, 1)
        print(f"  {src}: {count} ({pct}%)")

    print("\nTop genres:")
    for g, count in sorted(genre_counts.items(), key=lambda x: -x[1])[:15]:
        pct = round(count / len(albums) * 100, 1)
        print(f"  {g}: {count} ({pct}%)")

    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990,
               "1980s": 1980, "1970s": 1970, "1960s": 1960}
    print("\nDecade coverage:")
    for name, start in decades.items():
        count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")


if __name__ == "__main__":
    main()
