#!/usr/bin/env python3
"""Scrape AOTY list pages for a single year and save to cache.
Stops early if no dataset matches found for 3 consecutive pages."""
import json
import os
import re
import sys
import time
from curl_cffi import requests
from bs4 import BeautifulSoup

CACHE_PATH = os.path.join(os.path.dirname(__file__), 'aoty_cache.json')
LIST_DELAY = 1.2
MAX_PAGES = 40

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

def main():
    year = int(sys.argv[1])
    cache = load_json(CACHE_PATH, {})

    if str(year) in cache and cache[str(year)].get('complete'):
        print(f"[{year}] already cached ({len(cache[str(year)]['albums'])} albums)")
        return

    data = load_json(os.path.join(os.path.dirname(__file__), '..', 'data.json'), [])
    dataset_keys = set()
    for album in data:
        if album.get('year') == year:
            dataset_keys.add(make_key(album.get('artist', ''), album.get('album', '')))

    albums = {}
    page = 1
    empty_pages = 0
    no_match_streak = 0
    no_new_streak = 0
    backoff = False
    prev_total = 0

    while page <= MAX_PAGES and empty_pages < 2 and no_match_streak < 3 and no_new_streak < 3:
        url = f"https://www.albumoftheyear.org/ratings/6-highest-rated/{year}/{page}"
        try:
            r = requests.get(url, impersonate="chrome124", timeout=20)
            r.raise_for_status()
        except Exception as e:
            if "403" in str(e) and not backoff:
                time.sleep(30)
                backoff = True
                continue
            print(f"[{year}] page {page} error: {e}")
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
            if key in dataset_keys:
                page_matches += 1

        new_total = len(albums)
        print(f"[{year}] page {page}: {len(rows)} rows, total {new_total}, matches {page_matches}")

        if new_total == prev_total:
            no_new_streak += 1
        else:
            no_new_streak = 0
        prev_total = new_total

        if page_matches == 0:
            no_match_streak += 1
        else:
            no_match_streak = 0

        page += 1
        time.sleep(LIST_DELAY)

    cache[str(year)] = {"albums": albums, "complete": True}
    save_json(CACHE_PATH, cache)
    print(f"[{year}] done: {len(albums)} albums cached")

if __name__ == "__main__":
    main()
