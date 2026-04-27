#!/usr/bin/env python3
"""Enrich scraped data in chunks."""
import json
import time
import os
import urllib.request
import urllib.parse

USER_AGENT = "MusicConsensusBot/1.0 (contact@example.com)"

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

def enrich_chunk(albums, chunk_start):
    for i, album in enumerate(albums):
        idx = chunk_start + i
        if idx % 50 == 0:
            print(f"  [{idx}] {album['artist']} - {album['album']}")
        
        discogs = search_discogs(album["artist"], album["album"])
        if discogs:
            album["discogs"] = discogs
        time.sleep(0.15)
        
        cover = search_itunes(album["artist"], album["album"])
        if cover:
            album["cover"] = cover
        time.sleep(0.05)

def main():
    with open("scraped_raw.json") as f:
        albums = json.load(f)
    
    checkpoint_file = "enrich_checkpoint.json"
    
    # Load checkpoint if exists
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        start_idx = checkpoint["next_idx"]
        albums = checkpoint["albums"]
        print(f"Resuming from album {start_idx}...")
    else:
        start_idx = 0
    
    chunk_size = 500
    total = len(albums)
    
    while start_idx < total:
        end_idx = min(start_idx + chunk_size, total)
        chunk = albums[start_idx:end_idx]
        
        print(f"Processing chunk {start_idx}-{end_idx} of {total}...")
        enrich_chunk(chunk, start_idx)
        
        # Save checkpoint
        with open(checkpoint_file, "w") as f:
            json.dump({"next_idx": end_idx, "albums": albums}, f)
        
        print(f"  Chunk done. Saved checkpoint.")
        start_idx = end_idx
    
    # Calculate consensus
    print("Calculating consensus scores...")
    for album in albums:
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
    
    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)
    
    # Clean up checkpoint
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
    
    print(f"Saved {len(albums)} albums to data.json")
    
    # Print decade breakdown
    decades = {"2020s": 2020, "2010s": 2010, "2000s": 2000, "1990s": 1990, "1980s": 1980, "1970s": 1970, "1960s": 1960}
    print("\nDecade coverage:")
    for name, start in decades.items():
        count = len([a for a in albums if start <= a["year"] < start + 10])
        print(f"  {name}: {count} albums")

if __name__ == "__main__":
    main()
