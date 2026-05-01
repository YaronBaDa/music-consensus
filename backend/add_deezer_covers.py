#!/usr/bin/env python3
"""Add Deezer cover art for albums missing covers."""
import json
import time
import urllib.request
import urllib.parse

USER_AGENT = "ConsensusBot/1.0 (contact@example.com)"

def search_deezer(artist, album):
    try:
        query = f"{artist} {album}"
        url = f"https://api.deezer.com/search/album?q={urllib.parse.quote(query)}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        results = data.get("data", [])
        for result in results[:3]:
            title = result.get("title", "").lower()
            result_artist = result.get("artist", {}).get("name", "").lower()
            if album.lower() in title or title in album.lower():
                cover = result.get("cover_xl") or result.get("cover_big") or result.get("cover")
                if cover:
                    return cover
        # Fallback: just take first result
        if results:
            cover = results[0].get("cover_xl") or results[0].get("cover_big") or results[0].get("cover")
            if cover:
                return cover
        return None
    except Exception:
        return None

def main():
    with open("../data.json") as f:
        albums = json.load(f)

    missing = [i for i, a in enumerate(albums) if "cover" not in a]
    print(f"Adding Deezer covers for {len(missing)} of {len(albums)} albums...")

    added = 0
    for idx in missing:
        a = albums[idx]
        if idx % 100 == 0:
            print(f"  [{idx}] {a['artist']} - {a['album']}")
        cover = search_deezer(a["artist"], a["album"])
        if cover:
            albums[idx]["cover"] = cover
            added += 1
        time.sleep(0.05)  # Small delay to be polite

    with open("../data.json", "w") as f:
        json.dump(albums, f, indent=2)

    total_covers = sum(1 for a in albums if "cover" in a)
    print(f"\nAdded {added} covers. Total coverage: {total_covers}/{len(albums)} ({round(total_covers/len(albums)*100,1)}%)")

if __name__ == "__main__":
    main()
