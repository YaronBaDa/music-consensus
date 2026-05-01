#!/usr/bin/env python3
"""
Discogs enrichment — batch mode.
Processes up to BATCH_SIZE albums per run, saves after every album.
Designed for cron: run every 2 hours to finish overnight.
"""
import json
import time
import os
import urllib.request
import urllib.parse
from difflib import SequenceMatcher

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"
BATCH_SIZE = 100  # ~3.5 min per run at 2s delay

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

    checkpoint_file = "enrich_discogs_batch_checkpoint.json"
    processed = set()

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        processed = set(checkpoint["processed_keys"])
        print(f"Resuming. Already processed: {len(processed)}")

    # Sort by metacritic desc (best albums first)
    albums_sorted = sorted(albums, key=lambda x: x.get("metacritic", 0), reverse=True)

    count = 0
    found = 0
    save_interval = 10  # Write data.json every N albums to reduce I/O
    pending_scores = []  # Buffer scores to apply in batches

    for a in albums_sorted:
        key = f"{a['artist'].lower()}|||{a['album'].lower()}"
        if key in processed:
            continue
        if "discogs" in a:
            processed.add(key)
            continue

        score = search_discogs(a["artist"], a["album"])
        if score:
            pending_scores.append((a["artist"], a["album"], score))
            found += 1

        processed.add(key)
        count += 1

        # Save checkpoint after every album (small, fast)
        with open(checkpoint_file, "w") as f:
            json.dump({"processed_keys": list(processed)}, f)

        # Apply buffered scores and save data.json periodically
        if count % save_interval == 0 or count >= BATCH_SIZE:
            for artist, album, score in pending_scores:
                for orig in albums:
                    if orig["artist"] == artist and orig["album"] == album:
                        orig["discogs"] = score
                        scores = []
                        if "metacritic" in orig:
                            scores.append(orig["metacritic"])
                        scores.append(score)
                        if "musicbrainz" in orig:
                            scores.append(orig["musicbrainz"])
                        orig["consensus"] = round(sum(scores) / len(scores))
                        orig["reviews"] = len(scores)
                        break
            pending_scores = []
            with open("../data.json", "w") as f:
                json.dump(albums, f, indent=2)

        time.sleep(2.0)

        if count >= BATCH_SIZE:
            print(f"Batch limit reached. Processed {count} this run, found {found} scores.")
            print(f"Total processed: {len(processed)}/{len(albums_sorted)}")
            return

    # Flush any remaining buffered scores
    for artist, album, score in pending_scores:
        for orig in albums:
            if orig["artist"] == artist and orig["album"] == album:
                orig["discogs"] = score
                scores = []
                if "metacritic" in orig:
                    scores.append(orig["metacritic"])
                scores.append(score)
                if "musicbrainz" in orig:
                    scores.append(orig["musicbrainz"])
                orig["consensus"] = round(sum(scores) / len(scores))
                orig["reviews"] = len(scores)
                break
    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)

    # All done
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
    has_discogs = sum(1 for a in albums if "discogs" in a)
    print(f"COMPLETE! {has_discogs}/{len(albums)} albums have Discogs scores.")

if __name__ == "__main__":
    main()
