#!/usr/bin/env python3
"""
Safe Discogs enrichment — ultra-conservative rate limiting.
Processes ~30 albums per run with 2.5s delays (24/min).
Designed for cron: run every 4 hours to finish ~3500 albums in ~2 weeks.
Never hits rate limits. Always resumes from checkpoint.
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
BATCH_SIZE = 30
DELAY = 2.5  # 24 requests/min — well under 60/min limit

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def discogs_request(url):
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
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"    RATE LIMITED — waiting 60s...")
                time.sleep(60)
                return _search(query)
            return []
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
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"    RATE LIMITED on release — waiting 60s...")
            time.sleep(60)
        return None
    except Exception:
        return None

def recalculate_consensus(album):
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
    checkpoint_path = os.path.join(repo_dir, "backend", "discogs_safe_checkpoint.json")

    with open(data_path) as f:
        albums = json.load(f)

    processed = set()
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            checkpoint = json.load(f)
        processed = set(checkpoint.get("processed_keys", []))

    need_enrich = [a for a in albums if not a.get("discogs")]
    need_enrich.sort(key=lambda x: x.get("metacritic", 0), reverse=True)

    to_process = [a for a in need_enrich
                  if f"{a['artist'].lower()}|||{a['album'].lower()}" not in processed][:BATCH_SIZE]

    print(f"Safe Discogs enrichment — {len(to_process)} albums this run")
    print(f"Total processed so far: {len(processed)}")
    print(f"Remaining: {len(need_enrich) - len(processed)}")
    print()

    added = 0
    for a in to_process:
        key = f"{a['artist'].lower()}|||{a['album'].lower()}"
        score = search_discogs(a["artist"], a["album"])
        if score:
            a["discogs"] = score
            recalculate_consensus(a)
            added += 1
            print(f"  ✓ {a['artist']} - {a['album']}: {score}")

        processed.add(key)
        time.sleep(DELAY)

    with open(checkpoint_path, "w") as f:
        json.dump({"processed_keys": list(processed)}, f)
    with open(data_path, "w") as f:
        json.dump(albums, f, indent=2)

    has_discogs = sum(1 for a in albums if a.get("discogs"))
    print(f"\nDone this run: {added} new scores")
    print(f"Total with Discogs: {has_discogs}/{len(albums)} ({round(has_discogs/len(albums)*100,1)}%)")

if __name__ == "__main__":
    main()
