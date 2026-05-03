#!/usr/bin/env python3
"""
Enrich data.json with AOTY (albumoftheyear.org) critic and user scores.
Uses curl_cffi to bypass Cloudflare TLS fingerprinting.

Phases:
  1. Scrape AOTY highest-rated list pages for all years in the dataset.
  2. Match scraped albums against data.json (normalized artist|||album keys).
  3. Optionally fetch user scores from AOTY album detail pages.
  4. Recalculate consensus scores and save data.json.

Checkpoint file: backend/aoty_checkpoint.json
Cache file:      backend/aoty_cache.json
"""

import json
import os
import re
import sys
import time
from curl_cffi import requests
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────
DATA_PATH       = os.path.join(os.path.dirname(__file__), '..', 'data.json')
CHECKPOINT_PATH = os.path.join(os.path.dirname(__file__), 'aoty_checkpoint.json')
CACHE_PATH      = os.path.join(os.path.dirname(__file__), 'aoty_cache.json')

LIST_DELAY      = 1.2
DETAIL_DELAY    = 2.0
MAX_PAGES_YEAR  = 40
FETCH_USER      = True

# ── Helpers ────────────────────────────────────────────────────────

def normalize(text):
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def make_key(artist, album):
    return f"{normalize(artist)}|||{normalize(album)}"

def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default if default is not None else {}

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def recalculate_consensus(album):
    sources = ['metacritic', 'discogs', 'musicbrainz', 'aoty_critic', 'aoty_user']
    scores = [album[s] for s in sources if album.get(s) is not None]
    if scores:
        album['consensus'] = round(sum(scores) / len(scores))
        album['reviews'] = len(scores)
    else:
        album['consensus'] = 75
        album['reviews'] = 0

# ── Phase 1: Scrape AOTY list pages ───────────────────────────────

def scrape_year_list(year, checkpoint, dataset_keys_for_year=None):
    cache = load_json(CACHE_PATH, {})
    year_cache = cache.get(str(year), {})
    if year_cache.get('complete'):
        print(f"  [{year}] cached ({len(year_cache['albums'])} albums)")
        return year_cache['albums']

    albums = {}
    page = 1
    empty_pages = 0
    no_match_streak = 0
    no_new_streak = 0
    backoff = False
    prev_total = 0

    while page <= MAX_PAGES_YEAR and empty_pages < 2 and no_match_streak < 3 and no_new_streak < 3:
        url = f"https://www.albumoftheyear.org/ratings/6-highest-rated/{year}/{page}"
        try:
            r = requests.get(url, impersonate="chrome124", timeout=20)
            r.raise_for_status()
        except Exception as e:
            if "403" in str(e) and not backoff:
                print(f"  [{year}] 403 hit, backing off 30s and retrying once...")
                time.sleep(30)
                backoff = True
                continue
            print(f"  [{year}] page {page} error: {e}")
            break
        else:
            backoff = False

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.find_all("div", class_="albumListRow")

        if not rows:
            empty_pages += 1
            page += 1
            time.sleep(LIST_DELAY)
            continue
        else:
            empty_pages = 0

        page_matches = 0
        for row in rows:
            title_link = row.find("a", href=lambda h: h and "/album/" in h)
            score_container = row.find("div", class_="albumListScoreContainer")
            if not title_link or not score_container:
                continue
            full_text = title_link.get_text(strip=True)
            parts = full_text.split(" - ", 1)
            if len(parts) < 2:
                continue
            artist = parts[0].strip()
            album = parts[1].strip()
            score_val = score_container.find("div", class_="scoreValue")
            critic_score = int(score_val.get_text(strip=True)) if score_val else None
            score_text = score_container.find("div", class_="scoreText")
            review_count = None
            if score_text:
                strong = score_text.find("strong")
                review_count = int(strong.get_text(strip=True)) if strong else None
            key = make_key(artist, album)
            albums[key] = {
                "artist": artist,
                "album": album,
                "aoty_critic": critic_score,
                "aoty_reviews": review_count,
                "aoty_url": "https://www.albumoftheyear.org" + title_link["href"],
            }
            if dataset_keys_for_year and key in dataset_keys_for_year:
                page_matches += 1

        new_total = len(albums)
        print(f"  [{year}] page {page}: {len(rows)} rows, total {new_total}, matches {page_matches}")

        if new_total == prev_total:
            no_new_streak += 1
        else:
            no_new_streak = 0
        prev_total = new_total

        if dataset_keys_for_year:
            if page_matches == 0:
                no_match_streak += 1
            else:
                no_match_streak = 0

        page += 1
        time.sleep(LIST_DELAY)

    cache[str(year)] = {"albums": albums, "complete": True}
    save_json(CACHE_PATH, cache)
    checkpoint["completed_years"].append(year)
    save_json(CHECKPOINT_PATH, checkpoint)
    return albums

# ── Phase 2: Match & enrich ───────────────────────────────────────

def match_and_enrich(data, aoty_by_year):
    matched = 0
    for album in data:
        year = album.get("year")
        if year is None:
            continue
        year_aoty = aoty_by_year.get(str(year), {})
        if not year_aoty:
            continue

        key = make_key(album.get("artist", ""), album.get("album", ""))
        match = year_aoty.get(key)

        if not match:
            for k, v in year_aoty.items():
                if normalize(v["album"]) == normalize(album.get("album", "")):
                    match = v
                    break

        if match:
            album["aoty_critic"] = match["aoty_critic"]
            album["aoty_reviews"] = match.get("aoty_reviews")
            album["aoty_url"] = match.get("aoty_url")
            matched += 1

    return matched

# ── Phase 3: Fetch user scores ────────────────────────────────────

def fetch_user_scores(data, checkpoint):
    to_fetch = [
        (i, album) for i, album in enumerate(data)
        if album.get("aoty_critic") and "aoty_user" not in album
        and i not in checkpoint.get("user_fetched_indices", [])
    ]

    print(f"\nFetching user scores for {len(to_fetch)} albums...")
    fetched = 0

    for idx, album in to_fetch:
        url = album.get("aoty_url")
        if not url:
            checkpoint["user_fetched_indices"].append(idx)
            continue

        try:
            r = requests.get(url, impersonate="chrome124", timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            user_score_elem = soup.find("div", class_="albumUserScore")
            if user_score_elem:
                txt = user_score_elem.get_text(strip=True)
                if txt.isdigit():
                    album["aoty_user"] = int(txt)
                    fetched += 1
        except Exception as e:
            print(f"  user score error for {album.get('artist')} - {album.get('album')}: {e}")

        checkpoint["user_fetched_indices"].append(idx)

        if len(checkpoint["user_fetched_indices"]) % 50 == 0:
            save_json(CHECKPOINT_PATH, checkpoint)
            save_json(DATA_PATH, data)
            print(f"  ...saved checkpoint after {len(checkpoint['user_fetched_indices'])} user fetches")

        time.sleep(DETAIL_DELAY)

    print(f"  Fetched {fetched} new user scores")
    return fetched

# ── Main ──────────────────────────────────────────────────────────

def main():
    data = load_json(DATA_PATH, [])
    if not data:
        print("data.json not found or empty")
        sys.exit(1)

    checkpoint = load_json(CHECKPOINT_PATH, {"completed_years": [], "user_fetched_indices": []})
    years = sorted({a["year"] for a in data if a.get("year")}, reverse=True)

    print(f"Dataset: {len(data)} albums across {len(years)} years")
    print(f"Already completed years: {checkpoint['completed_years']}")

    print("\n=== Phase 1: Scrape AOTY list pages ===")
    aoty_by_year = {}

    dataset_keys_by_year = {}
    for album in data:
        y = album.get("year")
        if y is None:
            continue
        dataset_keys_by_year.setdefault(y, set()).add(make_key(album.get("artist", ""), album.get("album", "")))

    for year in years:
        if year in checkpoint["completed_years"]:
            cache = load_json(CACHE_PATH, {})
            aoty_by_year[str(year)] = cache.get(str(year), {}).get("albums", {})
            continue

        print(f"\nScraping {year}...")
        year_albums = scrape_year_list(year, checkpoint, dataset_keys_by_year.get(year))
        aoty_by_year[str(year)] = year_albums
        print(f"  Total AOTY albums for {year}: {len(year_albums)}")

    print("\n=== Phase 2: Match against data.json ===")
    matched = match_and_enrich(data, aoty_by_year)
    print(f"Matched {matched} albums with AOTY critic scores")

    for album in data:
        recalculate_consensus(album)
    save_json(DATA_PATH, data)
    print("Saved data.json with critic scores")

    if FETCH_USER:
        fetched = fetch_user_scores(data, checkpoint)
        for album in data:
            recalculate_consensus(album)
        save_json(DATA_PATH, data)
        save_json(CHECKPOINT_PATH, checkpoint)
        print(f"\nSaved data.json with user scores (+{fetched})")

    with_aoty_critic = sum(1 for a in data if a.get("aoty_critic"))
    with_aoty_user   = sum(1 for a in data if a.get("aoty_user"))
    print(f"\n=== Summary ===")
    print(f"Albums with AOTY critic score: {with_aoty_critic}/{len(data)} ({100*with_aoty_critic//len(data)}%)")
    print(f"Albums with AOTY user score:   {with_aoty_user}/{len(data)} ({100*with_aoty_user//len(data)}%)")

if __name__ == "__main__":
    main()
