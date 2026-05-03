#!/usr/bin/env python3
"""
Fetch AOTY user scores from album detail pages in small batches.
Designed for cron: processes BATCH_SIZE albums per run, resumes from checkpoint.
"""

import json
import os
import sys
import time

# Ensure curl_cffi is available
try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("curl_cffi not available, installing...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'curl_cffi', 'beautifulsoup4', '-q'])
    from curl_cffi import requests
    from bs4 import BeautifulSoup

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data.json')
CHECKPOINT_PATH = os.path.join(os.path.dirname(__file__), 'aoty_user_checkpoint.json')
BATCH_SIZE = 50
DETAIL_DELAY = 2.0

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
        album['consensus'] = album.get('metacritic', 75)
        album['reviews'] = 1 if album.get('metacritic') else 0

def main():
    data = load_json(DATA_PATH, [])
    checkpoint = load_json(CHECKPOINT_PATH, {'fetched_indices': []})
    fetched_set = set(checkpoint.get('fetched_indices', []))

    # Find albums needing user scores
    need_fetch = [
        (i, album) for i, album in enumerate(data)
        if album.get('aoty_critic') and 'aoty_user' not in album
        and i not in fetched_set
    ]

    if not need_fetch:
        print("No albums need user score fetching.")
        # Still recalculate consensus for any that might have been missed
        for album in data:
            recalculate_consensus(album)
        save_json(DATA_PATH, data)
        print("Consensus recalculated.")
        return

    batch = need_fetch[:BATCH_SIZE]
    print(f"Fetching user scores for {len(batch)} albums ({len(need_fetch)} remaining)")

    new_scores = 0
    for idx, album in batch:
        url = album.get('aoty_url')
        if not url:
            fetched_set.add(idx)
            continue

        try:
            r = requests.get(url, impersonate='chrome124', timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            user_score_elem = soup.find('div', class_='albumUserScore')
            if user_score_elem:
                txt = user_score_elem.get_text(strip=True)
                if txt.isdigit():
                    album['aoty_user'] = int(txt)
                    new_scores += 1
        except Exception as e:
            print(f"  Error for {album.get('artist', '?')} - {album.get('album', '?')}: {e}")

        fetched_set.add(idx)
        time.sleep(DETAIL_DELAY)

    # Recalculate consensus for all albums
    for album in data:
        recalculate_consensus(album)

    checkpoint['fetched_indices'] = sorted(fetched_set)
    save_json(CHECKPOINT_PATH, checkpoint)
    save_json(DATA_PATH, data)

    total_with_user = sum(1 for a in data if a.get('aoty_user'))
    print(f"Batch done. New user scores: {new_scores}. Total with user scores: {total_with_user}/{len(data)}")
    print(f"Remaining to fetch: {len(need_fetch) - len(batch)}")

if __name__ == '__main__':
    main()
