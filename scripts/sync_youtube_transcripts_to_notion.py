#!/usr/bin/env python3
"""
Sync YouTube transcripts to Notion episode pages.
Runs alongside the download script - syncs completed transcripts as they appear.
"""
import json
import os
import urllib.request
import time
from pathlib import Path

# Config
NOTION_KEY = open(os.path.expanduser("~/.config/notion/api_key_michael")).read().strip()
DB_ID = "13fb1a3e-b70a-4c63-afd6-08bba2e05a3e"
TRANSCRIPTS_DIR = Path(__file__).parent.parent / "transcripts" / "youtube"
SYNCED_FILE = Path(__file__).parent / "notion_sync_progress.json"

def notion_request(url, method='GET', data=None):
    headers = {
        "Authorization": f"Bearer {NOTION_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    req = urllib.request.Request(url, headers=headers, method=method)
    if data:
        req.data = json.dumps(data).encode()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  Notion error: {e.code} - {e.read().decode()[:200]}")
        return None
    except Exception as e:
        print(f"  Request error: {e}")
        return None

def find_episode_page(episode_num):
    """Find Notion page for an episode number."""
    data = notion_request(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        method='POST',
        data={
            "filter": {
                "property": "Episode No.",
                "number": {"equals": int(episode_num)}
            }
        }
    )
    if data and data.get('results'):
        return data['results'][0]['id']
    return None

def check_page_has_transcript(page_id):
    """Check if page already has a transcript section."""
    data = notion_request(f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=20")
    if data and data.get('results'):
        for block in data['results']:
            if block.get('type') == 'heading_2':
                text = block.get('heading_2', {}).get('rich_text', [])
                if text and 'Transcript' in text[0].get('text', {}).get('content', ''):
                    return True
    return False

def add_transcript_to_page(page_id, transcript_text):
    """Add transcript text as blocks to a Notion page."""
    # Split into chunks (Notion has 2000 char limit per block)
    chunks = []
    words = transcript_text.split()
    current_chunk = []
    current_len = 0
    
    for word in words:
        if current_len + len(word) + 1 > 1900:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_len = len(word)
        else:
            current_chunk.append(word)
            current_len += len(word) + 1
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    # Create blocks - header + paragraphs
    blocks = [
        {
            "object": "block",
            "type": "divider",
            "divider": {}
        },
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "📝 Transcript"}}]
            }
        }
    ]
    
    # Add paragraphs (Notion limit: 100 blocks per request)
    for chunk in chunks[:97]:
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}]
            }
        })
    
    if len(chunks) > 97:
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": f"[... transcript truncated, {len(chunks) - 97} more paragraphs ...]"}}]
            }
        })
    
    # Append blocks to page
    result = notion_request(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        method='PATCH',
        data={"children": blocks}
    )
    return result is not None

def load_synced():
    if SYNCED_FILE.exists():
        return json.load(open(SYNCED_FILE))
    return {"synced": [], "failed": [], "skipped": []}

def save_synced(data):
    with open(SYNCED_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def parse_vtt_to_text(vtt_content):
    """Convert VTT content to plain text."""
    import re
    lines = vtt_content.split('\n')
    text_lines = []
    
    for line in lines:
        line = line.strip()
        # Skip empty, timestamps, WEBVTT header
        if not line or '-->' in line or line.startswith('WEBVTT') or line.startswith('Kind:') or line.startswith('Language:'):
            continue
        if line.isdigit():
            continue
        # Remove HTML-like tags
        line = re.sub(r'<[^>]+>', '', line)
        if line:
            text_lines.append(line)
    
    # Remove duplicates (common in auto-captions)
    deduped = []
    prev = None
    for line in text_lines:
        if line != prev:
            deduped.append(line)
            prev = line
    
    return ' '.join(deduped)

def main():
    print("YouTube → Notion Transcript Sync")
    print("=" * 50)
    
    synced_data = load_synced()
    synced_set = set(synced_data['synced'] + synced_data['failed'] + synced_data['skipped'])
    
    # Find all .vtt files
    vtt_files = list(TRANSCRIPTS_DIR.glob("*.vtt"))
    print(f"Found {len(vtt_files)} VTT files")
    print(f"Already processed: {len(synced_set)}")
    
    to_process = []
    for vtt in vtt_files:
        # Parse episode number from filename (ep1171_xxxxx.en.vtt)
        name = vtt.stem  # ep1171_8AkPm4Zy3MU.en
        if name.startswith('ep'):
            ep_num = name.split('_')[0][2:]  # "1171"
            if ep_num not in synced_set:
                to_process.append((ep_num, vtt))
    
    print(f"To sync: {len(to_process)}")
    print("-" * 50)
    
    for i, (ep_num, vtt_path) in enumerate(to_process):
        if not ep_num or not ep_num.isdigit():
            print(f"[{i+1}/{len(to_process)}] Skipping invalid ep_num: '{ep_num}'")
            continue
        print(f"[{i+1}/{len(to_process)}] Episode {ep_num}...", end=" ", flush=True)
        
        # Load and parse transcript
        try:
            with open(vtt_path, 'r', encoding='utf-8', errors='ignore') as f:
                vtt_content = f.read()
            transcript = parse_vtt_to_text(vtt_content)
        except Exception as e:
            print(f"read error: {e}")
            synced_data['failed'].append(ep_num)
            continue
        
        if len(transcript) < 100:
            print(f"too short ({len(transcript)} chars)")
            synced_data['skipped'].append(ep_num)
            continue
        
        # Find Notion page
        page_id = find_episode_page(ep_num)
        if not page_id:
            print("page not found")
            synced_data['failed'].append(ep_num)
            continue
        
        # Check if already has transcript
        if check_page_has_transcript(page_id):
            print("already has transcript")
            synced_data['skipped'].append(ep_num)
            continue
        
        # Add transcript
        if add_transcript_to_page(page_id, transcript):
            print(f"✓ synced ({len(transcript)} chars)")
            synced_data['synced'].append(ep_num)
        else:
            print("✗ sync failed")
            synced_data['failed'].append(ep_num)
        
        # Save progress and rate limit
        if (i + 1) % 5 == 0:
            save_synced(synced_data)
        time.sleep(0.4)  # Notion rate limit
    
    save_synced(synced_data)
    
    print("\n" + "=" * 50)
    print(f"Done!")
    print(f"  Synced: {len(synced_data['synced'])}")
    print(f"  Skipped: {len(synced_data['skipped'])}")
    print(f"  Failed: {len(synced_data['failed'])}")

if __name__ == '__main__':
    main()
