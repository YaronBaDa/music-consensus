#!/usr/bin/env python3
"""
Discogs enrichment with API token support.
Fast mode: 60 req/min with token. Slow mode: ~25 req/hour without.

Usage:
    DISCOGS_TOKEN=your_token python3 enrich_discogs_fast.py
    # or without token (very slow):
    python3 enrich_discogs_fast.py
"""
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from difflib import SequenceMatcher

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"
API_TOKEN = os.environ.get("DISCOGS_TOKEN", "")

# Rate limits
if API_TOKEN:
    DELAY = 1.1  # 60/min with token
    print(f"Using Discogs API token. Processing at ~{int(60/DELAY)}/min")
else:
    DELAY = 150  # ~25/hour without token
    print("No Discogs token. Processing at ~25/hour (very slow).")
    print("Get a free token at: https://www.discogs.com/settings/developers")

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def discogs_request(url):
    """Make a Discogs API request with token if available."""
    if API_TOKEN:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}token={API_TOKEN}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

def search_discogs(artist, album):
    def _search(query):
        try:
            url = f"https://api.discogs.com/database/search?q={urllib.parse.quote(query)}&type=release&per_page=5"
            data = discogs_request(url)
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
        release_data = discogs_request(release_url)

        rating = release_data.get("community", {}).get("rating", {})
        average = rating.get("average")
        count = rating.get("count", 0)
        if average and count >= 3:
            return round((average / 5) * 100)
        return None
    except Exception:
        return None

def recalculate_consensus(album):
    """Recalculate consensus from all available sources."""
    scores = []
    for key in ["aoty_critic", "aoty_user", "metacritic", "rym", "discogs", "musicbrainz"]:
        if album.get(key) is not None:
            scores.append(album[key])
    if scores:
        album["consensus"] = round(sum(scores) / len(scores))
        album["reviews"] = len(scores)
    else:
        album["consensus"] = 0
        album["reviews"] = 0

def main():
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(repo_dir, "data.json")
    checkpoint_path = os.path.join(repo_dir, "backend", "discogs_checkpoint.json")

    with open(data_path) as f:
        albums = json.load(f)

    # Load checkpoint
    processed = set()
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            checkpoint = json.load(f)
        processed = set(checkpoint.get("processed_keys", []))
        print(f"Resuming: {len(processed)} already processed")

    # Find albums needing Discogs enrichment
    need_enrich = [a for a in albums if not a.get("discogs")]
    need_enrich.sort(key=lambda x: x.get("metacritic", 0), reverse=True)

    print(f"Total albums: {len(albums)}")
    print(f"Already have Discogs: {len(albums) - len(need_enrich)}")
    print(f"Need Discogs: {len(need_enrich)}")
    print(f"Processing with {DELAY}s delay...\n")

    added = 0
    checked = 0
    for a in need_enrich:
        key = f"{a['artist'].lower()}|||{a['album'].lower()}"
        if key in processed:
            continue

        score = search_discogs(a["artist"], a["album"])
        if score:
            a["discogs"] = score
            recalculate_consensus(a)
            added += 1
            print(f"  ✓ {a['artist']} - {a['album']}: {score}")
        else:
            if checked % 50 == 0:
                print(f"  ... checked {checked}, found {added} (last: {a['artist']} - {a['album']})")

        processed.add(key)
        checked += 1
        time.sleep(DELAY)

        # Save checkpoint every 20 albums
        if checked % 20 == 0:
            with open(checkpoint_path, "w") as f:
                json.dump({"processed_keys": list(processed)}, f)
            with open(data_path, "w") as f:
                json.dump(albums, f, indent=2)

    # Final save
    with open(data_path, "w") as f:
        json.dump(albums, f, indent=2)

    if os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)

    has_discogs = sum(1 for a in albums if a.get("discogs"))
    print(f"\nDone! {has_discogs}/{len(albums)} albums have Discogs scores ({round(has_discogs/len(albums)*100,1)}%)")
    print(f"New scores added this run: {added}")

if __name__ == "__main__":
    main()
