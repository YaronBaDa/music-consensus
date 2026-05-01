#!/usr/bin/env python3
"""
Consensus Lightweight Enrichment
Skips Discogs (rate limited) and AllMusic.
Does iTunes covers + MusicBrainz genres only.
Fast enough to complete in ~15 minutes.
"""
import json
import time
import os
import re
import urllib.request
import urllib.parse

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"

def load_old_data():
    cache = {}
    try:
        with open("../data.json") as f:
            old = json.load(f)
        for a in old:
            key = f"{a['artist'].lower()}|||{a['album'].lower()}"
            cache[key] = a
        print(f"Loaded {len(cache)} albums from old data.json as cache")
    except Exception:
        pass
    return cache

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

def search_musicbrainz_genre(artist, album):
    try:
        query = f'artist:"{artist}" AND release:"{album}"'
        url = f"https://musicbrainz.org/ws/2/release/?query={urllib.parse.quote(query)}&fmt=json&limit=3"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        releases = data.get("releases", [])
        if not releases:
            return None
        rg_id = releases[0].get("release-group", {}).get("id")
        if not rg_id:
            return None
        time.sleep(0.25)
        url2 = f"https://musicbrainz.org/ws/2/release-group/{rg_id}?fmt=json&inc=tags"
        req2 = urllib.request.Request(url2)
        req2.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req2, timeout=15) as resp2:
            rg_data = json.loads(resp2.read())
        tags = rg_data.get("tags", [])
        genres = [t["name"] for t in tags if t.get("count", 0) > 0]
        return genres[0].title() if genres else None
    except Exception:
        return None

def normalize_genre(genre):
    if not genre:
        return "Various"
    genre = genre.lower()
    mapping = {
        "rock": "Rock", "pop": "Pop", "hip hop": "Hip Hop", "hip-hop": "Hip Hop",
        "rap": "Hip Hop", "electronic": "Electronic", "electronica": "Electronic",
        "house": "Electronic", "techno": "Electronic", "edm": "Electronic",
        "synth-pop": "Electronic", "synthpop": "Electronic", "indie": "Indie",
        "indie rock": "Indie", "indie pop": "Indie", "alternative": "Alternative",
        "alternative rock": "Alternative", "metal": "Metal", "heavy metal": "Metal",
        "jazz": "Jazz", "soul": "Soul", "r&b": "R&B", "rhythm and blues": "R&B",
        "funk": "Funk", "blues": "Blues", "country": "Country", "folk": "Folk",
        "punk": "Punk", "punk rock": "Punk", "reggae": "Reggae", "classical": "Classical",
        "ambient": "Ambient", "experimental": "Experimental", "psychedelic": "Psychedelic",
        "psychedelic rock": "Psychedelic", "progressive rock": "Progressive",
        "post-punk": "Post-Punk", "new wave": "New Wave", "grunge": "Grunge",
        "shoegaze": "Shoegaze", "dream pop": "Dream Pop", "noise": "Noise",
        "industrial": "Industrial", "disco": "Disco", "gospel": "Gospel",
        "latin": "Latin", "world": "World", "afrobeat": "Afrobeat",
    }
    for key, value in mapping.items():
        if key in genre:
            return value
    return genre.title()

def enrich_album(album, cache):
    key = f"{album['artist'].lower()}|||{album['album'].lower()}"
    cached = cache.get(key)

    # Try cache for cover
    if cached and "cover" in cached and "cover" not in album:
        album["cover"] = cached["cover"]

    # iTunes cover
    if "cover" not in album:
        cover = search_itunes(album["artist"], album["album"])
        if cover:
            album["cover"] = cover

    # MusicBrainz genre
    mb_genre = search_musicbrainz_genre(album["artist"], album["album"])

    # Determine best genre
    best_genre = mb_genre or album.get("genre")
    if best_genre and best_genre != "Various":
        album["genre"] = normalize_genre(best_genre)

    return album

def enrich_chunk(albums, chunk_start, cache):
    for i, album in enumerate(albums):
        idx = chunk_start + i
        if idx % 100 == 0:
            print(f"  [{idx}] {album['artist']} - {album['album']}")
        enrich_album(album, cache)

def calculate_consensus(albums):
    for album in albums:
        scores = []
        if "metacritic" in album:
            scores.append(album["metacritic"])
        if "discogs" in album:
            scores.append(album["discogs"])
        if scores:
            album["consensus"] = round(sum(scores) / len(scores))
        else:
            album["consensus"] = album.get("metacritic", 0)
        album["reviews"] = len(scores)
        album["genre"] = album.get("genre", "Various")

def main():
    with open("scraped_raw.json") as f:
        albums = json.load(f)

    cache = load_old_data()
    checkpoint_file = "enrich_checkpoint_light.json"

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        start_idx = checkpoint["next_idx"]
        albums = checkpoint["albums"]
        print(f"Resuming from album {start_idx} of {len(albums)}...")
    else:
        start_idx = 0
        print(f"Starting lightweight enrichment of {len(albums)} albums...")

    chunk_size = 500
    total = len(albums)

    while start_idx < total:
        end_idx = min(start_idx + chunk_size, total)
        chunk = albums[start_idx:end_idx]

        print(f"\nProcessing chunk {start_idx}-{end_idx} of {total}...")
        enrich_chunk(chunk, start_idx, cache)

        with open(checkpoint_file, "w") as f:
            json.dump({"next_idx": end_idx, "albums": albums}, f)

        print(f"  Chunk done. Saved checkpoint.")
        start_idx = end_idx

    print("\nCalculating consensus scores...")
    calculate_consensus(albums)

    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)

    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    print(f"\nSaved {len(albums)} albums to data.json")

    has_cover = sum(1 for a in albums if "cover" in a)
    genre_counts = {}
    for a in albums:
        g = a.get("genre", "Various")
        genre_counts[g] = genre_counts.get(g, 0) + 1

    print(f"\nCoverage:")
    print(f"  Cover art: {has_cover} ({round(has_cover/len(albums)*100,1)}%)")
    print(f"\nTop genres:")
    for g, count in sorted(genre_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {g}: {count} ({round(count/len(albums)*100,1)}%)")

    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990,
               "1980s": 1980, "1970s": 1970, "1960s": 1960}
    print("\nDecade coverage:")
    for name, start in decades.items():
        count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")

if __name__ == "__main__":
    main()
