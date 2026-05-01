#!/usr/bin/env python3
"""
Weekly update script for Consensus 2026 albums.
Fetches latest Metacritic 2026 chart, deduplicates, enriches, commits & pushes.
"""
import json
import re
import time
import urllib.request
import urllib.parse
import subprocess
from bs4 import BeautifulSoup

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"
HEADERS = {"User-Agent": USER_AGENT}


def run(cmd, cwd=None):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        print(f"CMD FAILED: {cmd}")
        print(f"STDERR: {result.stderr}")
    return result


def fetch_metacritic_2026():
    url = "https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected=2026"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode()

    soup = BeautifulSoup(html, "html.parser")
    albums = []
    for elem in soup.find_all(class_="clamp-summary-wrap"):
        parent = elem.find_parent("tr")
        if not parent:
            continue
        title_el = elem.find("h3")
        artist_el = elem.find(class_="artist")
        score_el = elem.find(class_="metascore_anchor")
        title = title_el.get_text(strip=True) if title_el else ""
        artist = artist_el.get_text(strip=True) if artist_el else ""
        score = score_el.get_text(strip=True) if score_el else ""
        artist = artist.replace("by ", "") if artist.startswith("by ") else artist
        date = ""
        for span in parent.find_all("span"):
            text = span.get_text(strip=True)
            if re.match(r"[A-Za-z]+ \d{1,2}, \d{4}", text):
                date = text
                break
        year_match = re.search(r"(\d{4})", date) if date else None
        album_year = int(year_match.group(1)) if year_match else 2026
        if title and artist and score:
            albums.append({
                "artist": artist, "album": title, "year": album_year,
                "metacritic": int(score), "source": "Metacritic 2026"
            })
    return albums


def enrich_album(album):
    artist = album["artist"]
    title = album["album"]

    # Deezer cover
    if not album.get("cover"):
        try:
            query = f"{artist} {title}"
            url = f"https://api.deezer.com/search/album?q={urllib.parse.quote(query)}"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
            if result.get("data"):
                cover = result["data"][0].get("cover_xl") or result["data"][0].get("cover_big") or result["data"][0].get("cover")
                if cover:
                    album["cover"] = cover
        except Exception as e:
            print(f"    Cover fail: {artist} - {title}: {e}")

    # AudioDB genre
    if not album.get("genre"):
        try:
            url = f"https://www.theaudiodb.com/api/v1/json/2/search.php?s={urllib.parse.quote(artist)}"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
            if result.get("artists"):
                genre = result["artists"][0].get("strGenre", "Various")
                if genre:
                    album["genre"] = genre
        except Exception as e:
            print(f"    Genre fail: {artist}: {e}")

    # Consensus
    scores = []
    for key in ["aoty_critic", "aoty_user", "metacritic", "rym", "discogs", "musicbrainz"]:
        if album.get(key) is not None:
            scores.append(album[key])
    if scores:
        album["consensus"] = round(sum(scores) / len(scores))
        album["reviews"] = len(scores)
    else:
        album["consensus"] = album.get("metacritic", 75)
        album["reviews"] = 1


def main():
    repo_dir = "/root/music-consensus"

    # Ensure repo exists
    run("git config --global user.email 'ludwig@consensus.dev' && git config --global user.name 'Ludwig'")
    result = run(f"test -d {repo_dir}/.git")
    if result.returncode != 0:
        print("Cloning repo...")
        run("git clone https://github.com/YaronBaDa/music-consensus.git /root/music-consensus")
    else:
        print("Pulling latest...")
        run("git pull", cwd=repo_dir)

    # Load existing
    with open(f"{repo_dir}/data.json") as f:
        existing = json.load(f)

    seen = {f"{a['artist'].lower()}|||{a['album'].lower()}" for a in existing}
    print(f"Existing albums: {len(existing)}")

    # Fetch 2026
    print("Fetching Metacritic 2026...")
    new_albums = fetch_metacritic_2026()
    print(f"Fetched {len(new_albums)} albums from Metacritic 2026")

    added = 0
    for album in new_albums:
        key = f"{album['artist'].lower()}|||{album['album'].lower()}"
        if key not in seen:
            print(f"  New: {album['artist']} - {album['album']} ({album['metacritic']})")
            enrich_album(album)
            existing.append(album)
            seen.add(key)
            added += 1
            time.sleep(0.3)
        else:
            # Update metacritic score if changed
            for existing_album in existing:
                if f"{existing_album['artist'].lower()}|||{existing_album['album'].lower()}" == key:
                    if existing_album.get("metacritic") != album["metacritic"]:
                        print(f"  Updated score: {album['artist']} - {album['album']}: {existing_album.get('metacritic', '?')} -> {album['metacritic']}")
                        existing_album["metacritic"] = album["metacritic"]
                        # Recalculate consensus
                        scores = []
                        for k in ["aoty_critic", "aoty_user", "metacritic", "rym", "discogs", "musicbrainz"]:
                            if existing_album.get(k) is not None:
                                scores.append(existing_album[k])
                        if scores:
                            existing_album["consensus"] = round(sum(scores) / len(scores))
                            existing_album["reviews"] = len(scores)
                    break

    print(f"Added {added} new albums")

    if added == 0:
        print("No new albums. Checking for any pending changes...")
        # Still push if there were score updates

    # Save
    with open(f"{repo_dir}/data.json", "w") as f:
        json.dump(existing, f, indent=2)

    # Git commit & push
    run("git add -A", cwd=repo_dir)
    status = run("git diff --cached --stat", cwd=repo_dir)
    if status.stdout.strip():
        run(f'git commit -m "Weekly update: {added} new 2026 albums"', cwd=repo_dir)
        push_result = run("git push origin main", cwd=repo_dir)
        if push_result.returncode == 0:
            print("Pushed successfully!")
        else:
            print("Push failed!")
    else:
        print("No changes to commit.")


if __name__ == "__main__":
    main()
