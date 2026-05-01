#!/usr/bin/env python3
"""
Slow Discogs enrichment for top albums.
Rate limited to 1 request every 2s to avoid 429s.
Processes top albums by Metacritic score first.
"""
import json
import time
import os
import urllib.request
import urllib.parse
from difflib import SequenceMatcher

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def search_discogs(artist, album):
    def _search(query):
        try:
            url = f"https://api.discogs.com/database/search?q={urllib.parse.quote(query)}&type=release&per_page=5"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", USER_AGENT)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            return data.get("results", [])
        except Exception:
            return []

    results = _search(f"{artist} {album}")
    best = None
    best_score = 0
    for result in results[:5]:
        title = result.get("title", "").lower()
        ratio = max(fuzzy_ratio(album, title), fuzzy_ratio(artist, title))
        if ratio > best_score:
            best_score = ratio
            best = result

    if not best or best_score < 0.4:
        results = _search(album)
        for result in results[:3]:
            title = result.get("title", "").lower()
            ratio = fuzzy_ratio(album, title)
            if ratio > best_score:
                best_score = ratio
                best = result

    if not best or best_score < 0.3:
        return None

    try:
        release_url = best.get("resource_url")
        if not release_url:
            return None
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

def main():
    with open("../data.json") as f:
        albums = json.load(f)

    # Sort by metacritic descending (process best albums first)
    albums_sorted = sorted(albums, key=lambda x: x.get("metacritic", 0), reverse=True)
    
    checkpoint_file = "enrich_discogs_checkpoint.json"
    processed = set()
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        processed = set(checkpoint["processed_keys"])
        print(f"Resuming with {len(processed)} albums already processed")
    
    print(f"Processing {len(albums_sorted)} albums with 2s Discogs delay...")
    
    count = 0
    for a in albums_sorted:
        key = f"{a['artist'].lower()}|||{a['album'].lower()}"
        if key in processed:
            continue
        if "discogs" in a:
            processed.add(key)
            continue
        
        if count % 50 == 0:
            print(f"  [{count}] {a['artist']} - {a['album']} (metacritic: {a.get('metacritic', 0)})")
        
        score = search_discogs(a["artist"], a["album"])
        if score:
            # Update both the sorted copy and find original in albums
            a["discogs"] = score
            # Also update the original album object
            for orig in albums:
                if orig["artist"] == a["artist"] and orig["album"] == a["album"]:
                    orig["discogs"] = score
                    # Recalculate consensus
                    scores = [orig["metacritic"]] if "metacritic" in orig else []
                    scores.append(score)
                    orig["consensus"] = round(sum(scores) / len(scores))
                    orig["reviews"] = len(scores)
                    break
        
        processed.add(key)
        count += 1
        time.sleep(2.0)
        
        # Save checkpoint every 50 albums
        if count % 50 == 0:
            with open(checkpoint_file, "w") as f:
                json.dump({"processed_keys": list(processed)}, f)
            with open("../data.json", "w") as f:
                json.dump(albums, f, indent=2)
            print(f"  Checkpoint saved. {len(processed)} total processed.")
    
    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)
    
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
    
    has_discogs = sum(1 for a in albums if "discogs" in a)
    print(f"\nDone! {has_discogs}/{len(albums)} albums have Discogs scores ({round(has_discogs/len(albums)*100,1)}%)")

if __name__ == "__main__":
    main()
