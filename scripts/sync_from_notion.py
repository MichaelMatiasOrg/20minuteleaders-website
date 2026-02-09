#!/usr/bin/env python3
"""
Sync episodes from Notion to website.
Pulls from Notion database, transforms to website format, updates JSON files.

Requires environment variable: NOTION_API_KEY
"""
import json
import urllib.request
import re
import os
import sys
from datetime import datetime

# Config from environment
NOTION_KEY = os.environ.get("NOTION_API_KEY")
if not NOTION_KEY:
    # Try reading from file
    key_file = os.path.expanduser("~/.config/notion/api_key_michael")
    if os.path.exists(key_file):
        with open(key_file) as f:
            NOTION_KEY = f.read().strip()

if not NOTION_KEY:
    print("Error: NOTION_API_KEY not set and ~/.config/notion/api_key_michael not found")
    sys.exit(1)

DB_ID = "13fb1a3e-b70a-4c63-afd6-08bba2e05a3e"
REPO_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def notion_request(url, method='GET', data=None):
    headers = {"Authorization": f"Bearer {NOTION_KEY}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}
    req = urllib.request.Request(url, headers=headers, method=method)
    if data:
        req.data = json.dumps(data).encode()
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

def extract_youtube_id(url):
    if not url: return None
    for p in [r'youtu\.be/([a-zA-Z0-9_-]{11})', r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', r'youtube\.com/embed/([a-zA-Z0-9_-]{11})']:
        m = re.search(p, url)
        if m: return m.group(1)
    return None

def get_text(prop, prop_type='rich_text'):
    arr = prop.get('title' if prop_type == 'title' else 'rich_text', [])
    return ''.join([x.get('plain_text', '') for x in arr]).strip()

def main():
    print(f"🔄 Syncing from Notion at {datetime.now().isoformat()}", flush=True)

    # Fetch all
    all_eps = []
    cursor = None
    page = 0
    while True:
        page += 1
        body = {"page_size": 100, "sorts": [{"property": "Episode No.", "direction": "descending"}]}
        if cursor: body["start_cursor"] = cursor
        data = notion_request(f"https://api.notion.com/v1/databases/{DB_ID}/query", method='POST', data=body)
        all_eps.extend(data.get("results", []))
        print(f"  Page {page}: {len(data.get('results', []))} episodes", flush=True)
        if not data.get("has_more"): break
        cursor = data.get("next_cursor")

    print(f"  Total: {len(all_eps)} episodes", flush=True)

    # Transform
    website_eps = []
    for nep in all_eps:
        props = nep.get("properties", {})
        ep_no = props.get("Episode No.", {}).get("number")
        guest = get_text(props.get("Episode Name", {}), 'title')
        topic = get_text(props.get("Podcast Episode Title", {}))
        desc = get_text(props.get("Episode Summary", {})) or get_text(props.get("Podcast Episode Description", {}))
        spotify = props.get("Spotify Link", {}).get("url", "")
        yt_url = props.get("YouTube Link", {}).get("url", "")
        yt_id = extract_youtube_id(yt_url)
        
        pub_date = props.get("Publication Date", {}).get("date", {})
        date_str = pub_date.get("start", "") if pub_date else ""
        
        series_prop = props.get("Series", {}).get("select", {})
        series = series_prop.get("name", "") if series_prop else ""
        
        # Image
        image = ""
        for img_field in ["Key Graphic", "AI Image"]:
            files = props.get(img_field, {}).get("files", [])
            if files:
                f = files[0]
                if f.get("type") == "external": image = f.get("external", {}).get("url", "")
                elif f.get("type") == "file": image = f.get("file", {}).get("url", "")
                if image: break
        
        if ep_no and guest:
            title = f"Ep{ep_no}: {guest}: {topic}" if topic else f"Ep{ep_no}: {guest}"
        elif guest:
            title = f"{guest}: {topic}" if topic else guest
        else:
            title = topic or "Untitled"
        
        if not (guest or ep_no): continue
        
        ep_data = {"episode": str(int(ep_no)) if ep_no else "", "title": title, "guest": guest, 
                   "topic": topic or title, "description": f"<p>{desc}</p>" if desc else "", 
                   "link": spotify, "date": date_str, "series": series}
        if yt_id: ep_data["youtubeId"] = yt_id
        if image: ep_data["image"] = image
        website_eps.append(ep_data)

    print(f"  Transformed: {len(website_eps)} valid episodes", flush=True)
    print(f"  With YouTube: {sum(1 for e in website_eps if e.get('youtubeId'))}", flush=True)

    # Write files
    with open(os.path.join(REPO_PATH, "episodes.json"), 'w') as f:
        json.dump(website_eps, f, indent=2, ensure_ascii=False)

    with open(os.path.join(REPO_PATH, "js/episodes.js"), 'w') as f:
        f.write("// Episode data - auto-synced from Notion\n")
        f.write(f"// Last sync: {datetime.now().isoformat()}\n")
        f.write("const EPISODES = \n")
        json.dump(website_eps, f, indent=2, ensure_ascii=False)
        f.write(";\n")

    print("✅ Sync complete!", flush=True)

if __name__ == "__main__":
    main()
